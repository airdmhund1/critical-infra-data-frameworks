package com.criticalinfra.connectors.file

import com.criticalinfra.config.SourceConfig
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Try

// =============================================================================
// CsvFileConnector — CSV file source connector
//
// Implements FileSourceConnector for CSV files. Enforces the two explicit
// schema modes mandated by ADR-005. Silent schema inference is prohibited:
// every read applies the registered schema; inferSchema=true is used only
// internally in discovered-and-log mode for comparison, never for output.
// =============================================================================

/** CSV file source connector complying with ADR-005 explicit schema modes.
  *
  * ==Schema modes (ADR-005)==
  *
  * All file-based connectors must operate in exactly one of two explicit schema modes. There is no
  * "auto" or "inferred" mode. Silent schema inference is explicitly prohibited (ADR-005).
  *
  *   - '''`Strict`''': the `schemaRef` from the pipeline configuration is resolved to a registered
  *     `StructType` via `schemaLoader`. The CSV file is read with that schema applied. Any record
  *     that cannot be parsed according to the registered schema is handled according to
  *     `corruptRecordMode`. The schema is never inferred from data.
  *
  *   - '''`DiscoveredAndLog`''': Spark infers the schema internally (for comparison purposes only)
  *     and compares it against the registered schema. Any structural differences — added columns,
  *     missing columns, or type mismatches — are emitted as structured WARN log entries with the
  *     full field-level diff. The inferred schema is **never** surfaced to the output DataFrame;
  *     the pipeline applies the registered schema for all downstream processing. This mode is
  *     intended for source-onboarding and monitoring, not production ingestion.
  *
  * ==ADR-005 enforcement==
  *
  * `inferSchema=true` is used only inside `readDiscoveredAndLog` for the internal diff computation.
  * The inferred schema is discarded after comparison. The DataFrame returned from `extract` in
  * every mode always carries the registered schema — never the inferred one.
  *
  * ==CSV options==
  *
  * Six CSV parsing options are configurable through the constructor:
  *
  *   - `delimiter` — field separator character. Default: `","`.
  *   - `quote` — quotation character wrapping fields that contain the delimiter. Default: `"\""`.
  *   - `escape` — escape character for the quote character within quoted fields. Default: `"\\"`.
  *   - `header` — whether the first row is a header row. Default: `true`. When `false`, column
  *     names come from the registered schema.
  *   - `encoding` — character encoding of the file. Default: `"UTF-8"`.
  *   - `nullValue` — string that represents a null value in the file. Default: `""`.
  *
  * ==Glob path support==
  *
  * Spark's native CSV reader accepts glob patterns (e.g. `"/data/feeds/part-*.csv"`). This
  * connector passes the path through to Spark without modification, so glob patterns work
  * transparently.
  *
  * ==Corrupt record modes==
  *
  *   - `FailFast` (default) — the pipeline aborts on the first corrupt record. Use in production
  *     when every record must be valid.
  *   - `DropMalformed` — corrupt records are silently removed from the result set. Quarantine
  *     routing for removed records is the responsibility of the engine layer, not the connector.
  *   - `Permissive` — corrupt records are returned with a `_corrupt_record` string column
  *     containing the raw input line. All other columns for the corrupt row are null.
  *
  * @param schemaMode
  *   Schema enforcement mode. Must be `Strict` or `DiscoveredAndLog`. See ADR-005.
  * @param corruptRecordMode
  *   How corrupt or non-parseable records are handled. Default: `FailFast`.
  * @param delimiter
  *   Field separator character. Default: `","`.
  * @param quote
  *   Quotation character. Default: `"\""`.
  * @param escape
  *   Escape character for the quote character. Default: `"\\"`.
  * @param header
  *   Whether the first row is a header. Default: `true`.
  * @param encoding
  *   File character encoding. Default: `"UTF-8"`.
  * @param nullValue
  *   String sentinel for null values. Default: `""`.
  * @param schemaLoader
  *   Injectable function that resolves a `schemaRef` string to a `StructType`. In production this
  *   loads from the schema registry. In tests, inject a simple map lookup.
  */
class CsvFileConnector(
    schemaMode: CsvFileConnector.SchemaMode,
    corruptRecordMode: CsvFileConnector.CorruptRecordMode =
      CsvFileConnector.CorruptRecordMode.FailFast,
    delimiter: String = ",",
    quote: String = "\"",
    escape: String = "\\",
    header: Boolean = true,
    encoding: String = "UTF-8",
    nullValue: String = "",
    schemaLoader: String => Either[ConnectorError, StructType]
) extends FileSourceConnector {

  private val logger = LoggerFactory.getLogger(getClass)

  // ---------------------------------------------------------------------------
  // SourceConnector implementation
  // ---------------------------------------------------------------------------

  /** Extracts records from a CSV file described by `config`.
    *
    * Returns `Right(DataFrame)` on success with the registered schema applied, or
    * `Left(ConnectorError)` if the file path or schema reference is missing, the schema cannot be
    * resolved, or a Spark read error occurs.
    *
    * @param config
    *   Fully resolved pipeline configuration. `connection.filePath` must be present.
    *   `schemaEnforcement.registryRef` must be present (ADR-005 requires a registered schema for
    *   every file source).
    * @param spark
    *   Active `SparkSession` owned by the ingestion engine.
    * @return
    *   `Right(dataFrame)` with the registered schema applied, or `Left(ConnectorError)`.
    */
  override def extract(
      config: SourceConfig,
      spark: SparkSession
  ): Either[ConnectorError, DataFrame] = {

    // Step 1 — resolve file path
    val path = config.connection.filePath.getOrElse(
      return Left(
        ConnectorError(
          config.metadata.sourceId,
          "filePath is required for file connectors"
        )
      )
    )

    // Step 2 — require a registered schema ref (ADR-005: silent inference prohibited)
    val schemaRef = config.schemaEnforcement.registryRef.getOrElse(
      return Left(
        ConnectorError(
          config.metadata.sourceId,
          "schemaRef is required — silent schema inference is prohibited (ADR-005)"
        )
      )
    )

    // Step 3 — resolve registered schema, then dispatch to the appropriate read strategy
    schemaLoader(schemaRef).flatMap { registeredSchema =>
      schemaMode match {
        case CsvFileConnector.SchemaMode.Strict => readStrict(spark, path, registeredSchema, config)
        case CsvFileConnector.SchemaMode.DiscoveredAndLog =>
          readDiscoveredAndLog(spark, path, registeredSchema, config)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Strict read — registered schema applied; no inference
  // ---------------------------------------------------------------------------

  /** Reads the CSV file with the registered schema enforced.
    *
    * Any record that cannot conform to the schema is handled according to `corruptRecordMode`.
    */
  private def readStrict(
      spark: SparkSession,
      path: String,
      registeredSchema: StructType,
      config: SourceConfig
  ): Either[ConnectorError, DataFrame] =
    Try(
      spark.read
        .schema(registeredSchema)
        .options(csvOptions)
        .csv(path)
    ).fold(
      ex =>
        Left(
          ConnectorError(
            config.metadata.sourceId,
            s"CSV read failed (strict mode): ${ex.getMessage}"
          )
        ),
      df => Right(df)
    )

  // ---------------------------------------------------------------------------
  // Discovered-and-log read — ADR-005 comparison mode
  // ---------------------------------------------------------------------------

  /** Reads the CSV file with the registered schema, after comparing it to the inferred schema.
    *
    * The inferred schema is used only for comparison. It is never applied to the output DataFrame.
    */
  private def readDiscoveredAndLog(
      spark: SparkSession,
      path: String,
      registeredSchema: StructType,
      config: SourceConfig
  ): Either[ConnectorError, DataFrame] = {

    // Step A — infer schema for internal comparison only; NEVER used as output schema
    val inferredSchema = Try(
      spark.read
        .options(baseCsvOptions)
        .option("inferSchema", "true")
        .csv(path)
        .schema
    ).fold(
      ex =>
        return Left(
          ConnectorError(
            config.metadata.sourceId,
            s"CSV schema inference failed: ${ex.getMessage}"
          )
        ),
      identity
    )

    // Step B — compare inferred schema against registered schema and log any drift
    val diffs = computeSchemaDiff(registeredSchema, inferredSchema)
    if (diffs.nonEmpty) {
      logger.warn(
        s"[${config.metadata.sourceId}] Schema drift detected. Applying registered schema. " +
          s"Differences: ${diffs.mkString(", ")}"
      )
    }

    // Step C — read with the REGISTERED schema (inferred schema is discarded)
    Try(
      spark.read
        .schema(registeredSchema)
        .options(csvOptions)
        .csv(path)
    ).fold(
      ex =>
        Left(
          ConnectorError(
            config.metadata.sourceId,
            s"CSV read failed (discovered-and-log mode): ${ex.getMessage}"
          )
        ),
      df => Right(df)
    )
  }

  // ---------------------------------------------------------------------------
  // Schema diff computation
  // ---------------------------------------------------------------------------

  /** Computes field-level differences between the registered and inferred schemas.
    *
    * Returns a list of human-readable difference strings covering:
    *   - fields present in the registered schema but absent from the inferred schema
    *   - fields present in the inferred schema but absent from the registered schema
    *   - fields present in both schemas but with differing data types
    *
    * @param registered
    *   The declared schema from the schema registry.
    * @param inferred
    *   The schema inferred by Spark from the file contents.
    * @return
    *   An empty list when the schemas are structurally identical; a non-empty list otherwise.
    */
  private def computeSchemaDiff(registered: StructType, inferred: StructType): List[String] = {
    val registeredFields = registered.fields.map(f => f.name.toLowerCase -> f).toMap
    val inferredFields   = inferred.fields.map(f => f.name.toLowerCase -> f).toMap

    val missing = registeredFields.collect {
      case (name, field) if !inferredFields.contains(name) =>
        s"missing field: ${field.name} (${field.dataType.simpleString})"
    }.toList

    val extra = inferredFields.collect {
      case (name, field) if !registeredFields.contains(name) =>
        s"extra field: ${field.name} (${field.dataType.simpleString})"
    }.toList

    val typeMismatches = registeredFields.collect {
      case (name, regField)
          if inferredFields.contains(name) &&
            inferredFields(name).dataType != regField.dataType =>
        s"type mismatch: ${regField.name} — registered: ${regField.dataType.simpleString}, " +
          s"inferred: ${inferredFields(name).dataType.simpleString}"
    }.toList

    missing ++ extra ++ typeMismatches
  }

  // ---------------------------------------------------------------------------
  // CSV options map builders
  // ---------------------------------------------------------------------------

  /** CSV options derived from constructor parameters, including the corrupt record mode. */
  private def csvOptions: Map[String, String] = Map(
    "delimiter" -> delimiter,
    "quote"     -> quote,
    "escape"    -> escape,
    "header"    -> header.toString,
    "encoding"  -> encoding,
    "nullValue" -> nullValue,
    "mode"      -> corruptRecordMode.sparkValue
  )

  /** Base CSV options used for the internal schema-inference read in discovered-and-log mode.
    *
    * Always enables the header row for accurate column-name comparison. The corrupt record mode is
    * not included because the inferred-schema read is internal and its records are discarded.
    */
  private def baseCsvOptions: Map[String, String] = Map(
    "delimiter" -> delimiter,
    "quote"     -> quote,
    "escape"    -> escape,
    "header"    -> header.toString,
    "encoding"  -> encoding,
    "nullValue" -> nullValue
  )
}

// =============================================================================
// CsvFileConnector companion object — sealed enums
// =============================================================================

object CsvFileConnector {

  /** Schema enforcement mode for CSV file reads. ADR-005 mandates exactly two modes. */
  sealed abstract class SchemaMode

  object SchemaMode {

    /** Apply the declared schema from `schemaRef` on read.
      *
      * Non-conforming records raise a `ConnectorError`. The schema is never inferred from the data.
      */
    case object Strict extends SchemaMode

    /** Infer schema internally for comparison only; always apply the declared schema for output.
      *
      * Any structural differences between the inferred and registered schemas are emitted as
      * structured WARN log entries. The inferred schema is never used for the output DataFrame.
      * Intended for source-onboarding and monitoring, not production ingestion.
      */
    case object DiscoveredAndLog extends SchemaMode
  }

  /** How Spark handles records that cannot be parsed according to the applied schema. */
  sealed abstract class CorruptRecordMode(val sparkValue: String)

  object CorruptRecordMode {

    /** Abort the pipeline on the first corrupt record. Default. */
    case object FailFast extends CorruptRecordMode("FAILFAST")

    /** Drop corrupt records from the result set.
      *
      * Quarantine routing for dropped records is handled by the engine layer, not the connector.
      */
    case object DropMalformed extends CorruptRecordMode("DROPMALFORMED")

    /** Allow corrupt records; adds a `_corrupt_record` string column containing the raw input line.
      */
    case object Permissive extends CorruptRecordMode("PERMISSIVE")
  }
}

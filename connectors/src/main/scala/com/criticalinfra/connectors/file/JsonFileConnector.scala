package com.criticalinfra.connectors.file

import com.criticalinfra.config.SourceConfig
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Try

// =============================================================================
// JsonFileConnector — JSON file source connector
//
// Implements FileSourceConnector for JSON files. Supports two JSON formats:
// JSON Lines (one object per line, default) and multiline array (a single JSON
// array spanning multiple lines). Enforces the two explicit schema modes
// mandated by ADR-005. Silent schema inference is prohibited: every read applies
// the registered schema; schema inference is used only internally in
// discovered-and-log mode for comparison, never for output.
//
// Optional nested-struct flattening is available via `flattenDepth`: depth 0
// preserves the schema as-is; depth 1 (and values > 1, treated as 1) expands
// each top-level StructType column into `parent_child` top-level columns.
// =============================================================================

/** JSON file source connector complying with ADR-005 explicit schema modes.
  *
  * ==JSON format modes==
  *
  * Two JSON wire formats are supported, controlled by the `jsonFormat` constructor parameter:
  *
  *   - '''`JsonLines`''' (default): each line of the file is a self-contained JSON object (JSON
  *     Lines / NDJSON format). Spark reads these with `multiLine = false`.
  *   - '''`MultilineArray`''': the file contains a single JSON array whose opening `[` and closing
  *     `]` may span multiple lines, with individual objects inside the array separated by commas.
  *     Spark reads these with `multiLine = true`. This mode requires a real file path; passing a
  *     `Dataset[String]` source is not supported by Spark when `multiLine = true`.
  *
  * ==Schema modes (ADR-005)==
  *
  * All file-based connectors must operate in exactly one of two explicit schema modes. There is no
  * "auto" or "inferred" mode. Silent schema inference is explicitly prohibited (ADR-005).
  *
  *   - '''`Strict`''': the `schemaRef` from the pipeline configuration is resolved to a registered
  *     `StructType` via `schemaLoader`. The JSON file is read with that schema applied. Any record
  *     that cannot be parsed according to the registered schema is handled according to
  *     `corruptRecordMode`. The schema is never inferred from data.
  *
  *   - '''`DiscoveredAndLog`''': Spark infers the schema internally (for comparison purposes only)
  *     and compares it against the registered schema. Any structural differences — added fields,
  *     missing fields, or type mismatches — are emitted as structured WARN log entries with the
  *     full field-level diff. The inferred schema is '''never''' surfaced to the output DataFrame;
  *     the pipeline applies the registered schema for all downstream processing. This mode is
  *     intended for source-onboarding and monitoring, not production ingestion.
  *
  * ==ADR-005 enforcement==
  *
  * `schemaRef` is required for JSON sources — there is no zero-configuration onboarding path. This
  * is the same requirement as CSV sources and is stricter than Parquet, which has an embedded
  * schema. Spark schema inference (`multiLine` read without an explicit schema) is used only inside
  * `readDiscoveredAndLog` for the internal diff computation. The inferred schema is discarded after
  * comparison. The DataFrame returned from `extract` in every mode always carries the registered
  * schema — never the inferred one.
  *
  * ==Nested structure flattening==
  *
  * The `flattenDepth` parameter controls how nested `StructType` columns are handled:
  *
  *   - `0` (default): nested struct columns are preserved as-is in the output DataFrame. A column
  *     `event` of type `StructType(type: String, ts: Long)` remains as a single `event` column.
  *   - `1`: each top-level column whose data type is `StructType` is expanded into individual
  *     top-level columns named `parent_child` (e.g. `event.type` becomes `event_type`). Non-struct
  *     top-level columns pass through unchanged.
  *   - Values greater than `1` are treated as `1` for this implementation — only the first level of
  *     nesting is flattened; inner structs nested two or more levels deep remain as `StructType`
  *     columns in the output.
  *
  * ==Corrupt record modes==
  *
  *   - `FailFast` (default) — the pipeline aborts on the first corrupt record. Use in production
  *     when every record must be valid.
  *   - `DropMalformed` — corrupt records are removed from the result set. Quarantine routing for
  *     removed records is the responsibility of the engine layer, not the connector.
  *   - `Permissive` — corrupt records are returned with a `_corrupt_record` string column
  *     containing the raw input line. All other columns for the corrupt row are null.
  *
  * ==Glob path support==
  *
  * Spark's native JSON reader accepts glob patterns (e.g. `"/data/feeds/part-*.json"`). This
  * connector passes the path through to Spark without modification, so glob patterns work
  * transparently for `JsonLines` format. Note that glob patterns with `MultilineArray` format are
  * not recommended: Spark reads each file independently as a self-contained JSON document.
  *
  * @param schemaMode
  *   Schema enforcement mode. Must be `Strict` or `DiscoveredAndLog`. See ADR-005.
  * @param jsonFormat
  *   JSON wire format. `JsonLines` (default) for one-object-per-line NDJSON files; `MultilineArray`
  *   for a single JSON array spanning multiple lines. Controls the Spark `multiLine` read option.
  * @param corruptRecordMode
  *   How corrupt or non-parseable records are handled. Default: `FailFast`.
  * @param flattenDepth
  *   Nested struct flattening depth. `0` preserves nesting; `1` (and values > 1, treated as 1)
  *   expands top-level `StructType` columns to `parent_child` top-level columns. Default: `0`.
  * @param schemaLoader
  *   Injectable function that resolves a `schemaRef` string to a `StructType`. In production this
  *   loads from the schema registry. In tests, inject a simple map lookup.
  */
class JsonFileConnector(
    schemaMode: JsonFileConnector.SchemaMode,
    jsonFormat: JsonFileConnector.JsonFormat = JsonFileConnector.JsonFormat.JsonLines,
    corruptRecordMode: JsonFileConnector.CorruptRecordMode =
      JsonFileConnector.CorruptRecordMode.FailFast,
    flattenDepth: Int = 0,
    schemaLoader: String => Either[ConnectorError, StructType]
) extends FileSourceConnector {

  private val logger = LoggerFactory.getLogger(getClass)

  // ---------------------------------------------------------------------------
  // SourceConnector implementation
  // ---------------------------------------------------------------------------

  /** Extracts records from a JSON file described by `config`.
    *
    * Returns `Right(DataFrame)` on success with the registered schema applied (and optional
    * flattening), or `Left(ConnectorError)` if the file path or schema reference is missing, the
    * schema cannot be resolved, or a Spark read error occurs.
    *
    * @param config
    *   Fully resolved pipeline configuration. `connection.filePath` must be present.
    *   `schemaEnforcement.registryRef` must be present (ADR-005 requires a registered schema for
    *   every JSON file source).
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
        case JsonFileConnector.SchemaMode.Strict =>
          readStrict(spark, path, registeredSchema, config)
        case JsonFileConnector.SchemaMode.DiscoveredAndLog =>
          readDiscoveredAndLog(spark, path, registeredSchema, config)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Strict read — registered schema applied; no inference
  // ---------------------------------------------------------------------------

  /** Reads the JSON file with the registered schema enforced.
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
        .options(jsonOptions)
        .json(path)
    ).fold(
      ex =>
        Left(
          ConnectorError(
            config.metadata.sourceId,
            s"JSON read failed (strict mode): ${ex.getMessage}"
          )
        ),
      df => Right(applyFlatten(df))
    )

  // ---------------------------------------------------------------------------
  // Discovered-and-log read — ADR-005 comparison mode
  // ---------------------------------------------------------------------------

  /** Reads the JSON file with the registered schema, after comparing it to the inferred schema.
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
        .options(jsonOptions)
        .json(path)
        .schema
    ).fold(
      ex =>
        return Left(
          ConnectorError(
            config.metadata.sourceId,
            s"JSON schema inference failed: ${ex.getMessage}"
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
        .options(jsonOptions)
        .json(path)
    ).fold(
      ex =>
        Left(
          ConnectorError(
            config.metadata.sourceId,
            s"JSON read failed (discovered-and-log mode): ${ex.getMessage}"
          )
        ),
      df => Right(applyFlatten(df))
    )
  }

  // ---------------------------------------------------------------------------
  // Flatten helpers
  // ---------------------------------------------------------------------------

  /** Applies nested struct flattening according to `flattenDepth`.
    *
    * If `flattenDepth == 0` the DataFrame is returned unchanged. For any depth ≥ 1, one level of
    * flattening is applied via [[flattenOnce]].
    */
  private def applyFlatten(df: DataFrame): DataFrame =
    if (flattenDepth == 0) df
    else flattenOnce(df)

  /** Flattens one level of top-level `StructType` columns.
    *
    * For each top-level field of type `StructType`, the sub-fields are expanded to top-level
    * columns named `parent_child`. Non-struct top-level fields pass through unchanged. Inner
    * structs nested two or more levels deep are not recursed into and remain as `StructType`
    * columns.
    */
  private def flattenOnce(df: DataFrame): DataFrame = {
    val flatCols = df.schema.fields.flatMap {
      case StructField(name, st: StructType, _, _) =>
        st.fields.map(sub => col(s"`$name`.`${sub.name}`").alias(s"${name}_${sub.name}")).toSeq
      case StructField(name, _, _, _) =>
        Seq(col(s"`$name`"))
    }
    df.select(flatCols: _*)
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
  // JSON options map builder
  // ---------------------------------------------------------------------------

  /** JSON read options derived from constructor parameters, including corrupt record mode and
    * multiLine setting.
    *
    * `multiLine = false` for `JsonLines` (one object per line); `multiLine = true` for
    * `MultilineArray` (a JSON array spanning multiple lines).
    */
  private def jsonOptions: Map[String, String] =
    Map(
      "mode"      -> corruptRecordMode.sparkValue,
      "multiLine" -> (jsonFormat == JsonFileConnector.JsonFormat.MultilineArray).toString
    )
}

// =============================================================================
// JsonFileConnector companion object — sealed enums
// =============================================================================

object JsonFileConnector {

  /** JSON wire format for the source file. Controls the Spark `multiLine` read option. */
  sealed abstract class JsonFormat

  object JsonFormat {

    /** One JSON object per line (JSON Lines / NDJSON). Default.
      *
      * Spark reads these with `multiLine = false`. Each line must be a self-contained, valid JSON
      * object. This is the most common format for streaming and batch JSON feeds.
      */
    case object JsonLines extends JsonFormat

    /** A single JSON array containing multiple objects, possibly spanning multiple lines.
      *
      * Spark reads these with `multiLine = true`. The entire file is parsed as a single JSON
      * document. Use this format when the upstream producer writes a JSON array with
      * pretty-printing or when individual objects span more than one line.
      */
    case object MultilineArray extends JsonFormat
  }

  /** Schema enforcement mode for JSON file reads. ADR-005 mandates exactly two modes. */
  sealed abstract class SchemaMode

  object SchemaMode {

    /** Apply the declared schema from `schemaRef` on read.
      *
      * Non-conforming records are handled according to `corruptRecordMode`. The schema is never
      * inferred from the data.
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

    /** Allow corrupt records; adds a `_corrupt_record` string column containing the raw input.
      */
    case object Permissive extends CorruptRecordMode("PERMISSIVE")
  }
}

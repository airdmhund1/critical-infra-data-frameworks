package com.criticalinfra.connectors.file

import com.criticalinfra.config.SourceConfig
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Try

// =============================================================================
// ParquetFileConnector — Parquet file source connector
//
// Implements FileSourceConnector for Apache Parquet files. Unlike CSV
// connectors, Parquet carries its own embedded schema in every file footer,
// so schema inference is deterministic and non-silent. Schema drift detection
// is therefore optional (activated only when schemaRef is configured) rather
// than mandatory as it is for CSV sources.
//
// Three complementary capabilities are layered on top of Spark's native
// Parquet reader:
//
//   1. Hive partition discovery — Spark auto-discovers date=YYYY-MM-DD
//      directories when reading a root directory path.
//   2. Partition filter pushdown — startDate/endDate parameters restrict
//      which partitions are loaded, avoiding full dataset scans.
//   3. Schema drift detection + column pruning — when schemaRef is configured,
//      the connector compares the registered schema against the embedded
//      Parquet schema and logs any structural differences; it then selects
//      only the columns declared in the registered schema (ADR-005).
// =============================================================================

/** Parquet file source connector with Hive partition support and optional schema drift detection.
  *
  * ==Parquet embedded schema and why drift detection still matters==
  *
  * Every Parquet file stores its schema in the file footer, so Spark can read Parquet without
  * schema inference sampling. However, upstream producers can silently change their Parquet writer
  * schema between pipeline runs — adding columns, removing columns, or changing field types. When
  * this happens, the embedded schema the connector reads from the file on the next run will differ
  * from the registered schema in the schema registry. Without drift detection this divergence is
  * invisible until a downstream consumer surfaces incorrect results, which in regulated
  * environments may be after a reporting deadline. Configuring `schemaRef` enables structural
  * comparison on every read and emits a WARN log entry whenever a difference is found, allowing
  * operators to catch and respond to upstream producer changes promptly.
  *
  * ==Hive partition discovery==
  *
  * When the `filePath` in the pipeline configuration points to a directory that contains Hive-style
  * `key=value` sub-directories (e.g. `date=2024-01-01/`, `date=2024-01-02/`), Spark's Parquet
  * reader discovers those partitions automatically. The partition column (e.g. `date`) is added to
  * the DataFrame schema as a string column derived from the directory name, not from the Parquet
  * footer. No special connector option is required — `spark.read.parquet(path)` handles partition
  * discovery natively for directory paths.
  *
  * ==Partition filter pushdown==
  *
  * The `startDate` and `endDate` constructor parameters bound which partitions are loaded. When
  * both are `None`, all partitions are read. When either is set, a DataFrame filter on the
  * `partitionDateColumn` column is applied after the initial read. Spark evaluates this filter
  * against partition directory names, which avoids reading Parquet data blocks for out-of-range
  * partitions — this is the standard Spark partition pruning mechanism. Date values must be
  * `"YYYY-MM-DD"` strings so that lexicographic comparison matches chronological order.
  *
  * ==Column pruning==
  *
  * When `schemaRef` is configured, the connector selects only the columns whose names appear in the
  * registered `StructType`. Partition columns that are not declared in the registered schema are
  * dropped from the output DataFrame. This ensures the DataFrame returned from `extract` has
  * exactly the shape the pipeline configuration describes, regardless of how many extra columns the
  * Parquet files carry or how many partition columns Hive discovery adds.
  *
  * ==ADR-005 context==
  *
  * ADR-005 mandates explicit schema modes for all file-based connectors. For Parquet, `schemaRef`
  * is optional rather than required because Parquet's embedded schema already prevents the silent
  * inference risk that ADR-005 was written to eliminate. Providing `schemaRef` activates the
  * additional drift-detection and column-pruning layers described above.
  *
  * @param schemaLoader
  *   Injectable function that resolves a `schemaRef` string to a `StructType`. In production this
  *   calls the schema registry service. In tests, inject a simple map-based lookup. The default
  *   implementation returns a `Left(ConnectorError)` so that tests that omit a loader fail
  *   explicitly rather than silently loading an unexpected schema.
  * @param partitionDateColumn
  *   Name of the Hive partition column used for date-range filter pushdown. Default: `"date"`. Must
  *   match the directory key in `date=YYYY-MM-DD` paths.
  * @param startDate
  *   Inclusive lower bound for partition filter pushdown in `"YYYY-MM-DD"` format. When `None`, no
  *   lower bound is applied.
  * @param endDate
  *   Inclusive upper bound for partition filter pushdown in `"YYYY-MM-DD"` format. When `None`, no
  *   upper bound is applied.
  */
class ParquetFileConnector(
    schemaLoader: String => Either[ConnectorError, StructType] = _ =>
      Left(ConnectorError("ParquetFileConnector", "No schemaLoader configured")),
    partitionDateColumn: String = "date",
    startDate: Option[String] = None,
    endDate: Option[String] = None
) extends FileSourceConnector {

  private val logger = LoggerFactory.getLogger(getClass)

  // ---------------------------------------------------------------------------
  // SourceConnector implementation
  // ---------------------------------------------------------------------------

  /** Extracts records from a Parquet file or partitioned Parquet directory described by `config`.
    *
    * Returns `Right(DataFrame)` on success, or `Left(ConnectorError)` if the file path is missing,
    * the schema loader fails, or a Spark read error occurs.
    *
    * When `schemaRef` is absent, the full Parquet-embedded schema is returned after optional date
    * filtering. When `schemaRef` is present, drift is detected and logged, and only the columns
    * declared in the registered schema are selected in the output.
    *
    * @param config
    *   Fully resolved pipeline configuration. `connection.filePath` must be present.
    *   `schemaEnforcement.registryRef` is optional; providing it activates drift detection and
    *   column pruning.
    * @param spark
    *   Active `SparkSession` owned by the ingestion engine.
    * @return
    *   `Right(dataFrame)` on success, or `Left(ConnectorError)`.
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

    // Step 2 — read Parquet with Hive partition discovery
    // mergeSchema=false is explicit: we do not want Spark to silently widen
    // schemas across part files — that would mask drift between files.
    val rawDf = Try(
      spark.read
        .option("mergeSchema", "false")
        .parquet(path)
    ).fold(
      ex => return Left(ConnectorError(config.metadata.sourceId, ex.getMessage)),
      identity
    )

    // Step 3 — apply date range filter (partition pushdown)
    val filtered = (startDate, endDate) match {
      case (None, None)       => rawDf
      case (Some(s), None)    => rawDf.filter(col(partitionDateColumn) >= s)
      case (None, Some(e))    => rawDf.filter(col(partitionDateColumn) <= e)
      case (Some(s), Some(e)) => rawDf.filter(col(partitionDateColumn).between(s, e))
    }

    // Step 4 — schema comparison and column pruning (only when schemaRef is configured)
    val schemaRefOpt = config.schemaEnforcement.registryRef
    schemaRefOpt match {
      case None =>
        // No schemaRef — return all columns with no drift check (Parquet embedded schema suffices)
        Right(filtered)

      case Some(ref) =>
        schemaLoader(ref).map { registeredSchema =>
          // Drift detection: compare registered schema against the Parquet embedded schema
          val embeddedSchema = filtered.schema
          val diffs          = computeSchemaDiff(registeredSchema, embeddedSchema)
          if (diffs.nonEmpty) {
            logger.warn(
              s"[${config.metadata.sourceId}] Parquet schema drift detected. " +
                s"Differences: ${diffs.mkString(", ")}"
            )
          }

          // Column pruning — select only columns declared in registeredSchema that exist
          // in the DataFrame (partition columns not in registeredSchema are dropped)
          val registeredCols = registeredSchema.fieldNames.filter(filtered.columns.contains)
          filtered.select(registeredCols.toIndexedSeq.map(col): _*)
        }
    }
  }

  // ---------------------------------------------------------------------------
  // Schema diff computation
  // ---------------------------------------------------------------------------

  /** Computes field-level differences between the registered and embedded Parquet schemas.
    *
    * Returns a list of human-readable difference strings covering:
    *   - fields present in the registered schema but absent from the embedded schema
    *   - fields present in the embedded schema but absent from the registered schema
    *   - fields present in both schemas but with differing data types
    *
    * @param registered
    *   The declared schema from the schema registry.
    * @param embedded
    *   The schema embedded in the Parquet file footer as read by Spark.
    * @return
    *   An empty list when the schemas are structurally identical; a non-empty list otherwise.
    */
  private def computeSchemaDiff(registered: StructType, embedded: StructType): List[String] = {
    val registeredFields = registered.fields.map(f => f.name.toLowerCase -> f).toMap
    val embeddedFields   = embedded.fields.map(f => f.name.toLowerCase -> f).toMap

    val missing = registeredFields.collect {
      case (name, field) if !embeddedFields.contains(name) =>
        s"missing field: ${field.name} (${field.dataType.simpleString})"
    }.toList

    val extra = embeddedFields.collect {
      case (name, field) if !registeredFields.contains(name) =>
        s"extra field: ${field.name} (${field.dataType.simpleString})"
    }.toList

    val typeMismatches = registeredFields.collect {
      case (name, regField)
          if embeddedFields.contains(name) &&
            embeddedFields(name).dataType != regField.dataType =>
        s"type mismatch: ${regField.name} — registered: ${regField.dataType.simpleString}, " +
          s"embedded: ${embeddedFields(name).dataType.simpleString}"
    }.toList

    missing ++ extra ++ typeMismatches
  }
}

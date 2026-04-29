package com.criticalinfra.engine

import com.criticalinfra.config.SourceConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit}

import scala.util.Try

// =============================================================================
// DeltaBronzeLayerWriter — production Delta Lake implementation of BronzeLayerWriter
//
// This is the Branch 2 production implementation. It injects four CIDF metadata
// columns and two partition columns into the DataFrame before writing, enforces
// the delta.appendOnly table property after every write, and returns a fully
// populated WriteResult on success.
//
// Checksum computation (SHA-256) is deferred to Branch 3; the `_cidf_checksum`
// column and `WriteResult.checksum` field carry an empty string placeholder here.
// =============================================================================

/** Production Delta Lake implementation of [[BronzeLayerWriter]].
  *
  * Enriches each incoming DataFrame with four CIDF audit metadata columns and two partition columns
  * before writing to the Bronze lakehouse layer in Delta Lake append mode. After every successful
  * write, the `delta.appendOnly` table property is set on the target path — making the Bronze table
  * immutable by design and satisfying the compliance-by-design architecture principle.
  *
  * ==Metadata columns injected==
  *   - `_cidf_ingestion_ts` — wall-clock timestamp of the pipeline run (`current_timestamp()`).
  *   - `_cidf_source_name` — human-readable source name from `config.metadata.sourceName`.
  *   - `_cidf_run_id` — pipeline run correlation key (`runId.toString`).
  *   - `_cidf_checksum` — integrity checksum placeholder; empty string until Branch 3.
  *
  * ==Partition columns added==
  *   - `ingestion_date` — calendar date of the pipeline run (`LocalDate.now()`, format
  *     `YYYY-MM-DD`).
  *   - `source_name` — same value as `_cidf_source_name`; used for partition pruning.
  *
  * ==Error handling==
  * The entire write operation is wrapped in `scala.util.Try` and converted to `Either`. Any Spark
  * or Delta Lake exception is captured and returned as `Left(StorageWriteError)`. Exceptions are
  * never propagated to the caller.
  *
  * ==Thread safety==
  * Instances are stateless. A single instance may be shared across pipeline runs.
  */
final class DeltaBronzeLayerWriter extends BronzeLayerWriter {

  /** Enriches `data` with CIDF metadata and partition columns and writes it to the Delta Bronze
    * path.
    *
    * The record count is computed on the enriched DataFrame (after metadata columns are added) so
    * that `WriteResult.recordsWritten` reflects the exact number of rows committed to storage. The
    * `delta.appendOnly` table property is set after every write; this call is idempotent.
    *
    * @param data
    *   The validated DataFrame to persist. Must not be null.
    * @param config
    *   Fully resolved pipeline configuration supplying the Bronze storage path and source metadata.
    * @param runId
    *   Unique identifier for this pipeline run; embedded in the `_cidf_run_id` metadata column.
    * @return
    *   `Right(WriteResult)` on success, or `Left(StorageWriteError(path, cause))` on any failure.
    */
  def write(
      data: DataFrame,
      config: SourceConfig,
      runId: java.util.UUID
  ): Either[StorageWriteError, WriteResult] = {
    val path       = config.storage.path
    val sourceName = config.metadata.sourceName
    val today      = java.time.LocalDate.now().toString

    Try {
      // -----------------------------------------------------------------------
      // Step 1 — Inject CIDF audit metadata columns
      // -----------------------------------------------------------------------
      val enriched = data
        .withColumn("_cidf_ingestion_ts", current_timestamp())
        .withColumn("_cidf_source_name", lit(sourceName))
        .withColumn("_cidf_run_id", lit(runId.toString))
        .withColumn("_cidf_checksum", lit("")) // placeholder; SHA-256 computed in Branch 3

      // -----------------------------------------------------------------------
      // Step 2 — Add partition columns
      // -----------------------------------------------------------------------
      val partitioned = enriched
        .withColumn("ingestion_date", lit(today))
        .withColumn("source_name", lit(sourceName))

      // -----------------------------------------------------------------------
      // Step 3 — Count before write so the result is always precise
      // -----------------------------------------------------------------------
      val count = partitioned.count()

      // -----------------------------------------------------------------------
      // Step 4 — Write to Delta in append mode, partitioned by date and source
      // -----------------------------------------------------------------------
      partitioned.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date", "source_name")
        .save(path)

      // -----------------------------------------------------------------------
      // Step 5 — Enforce appendOnly after write (idempotent)
      // -----------------------------------------------------------------------
      val spark = data.sparkSession
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.appendOnly' = 'true')"
      )

      WriteResult(
        recordsWritten = count,
        path = path,
        partitionDate = java.time.LocalDate.now(),
        checksum = "" // placeholder; SHA-256 computed in Branch 3
      )
    }.toEither.left.map(t => StorageWriteError(path, t.getMessage))
  }
}

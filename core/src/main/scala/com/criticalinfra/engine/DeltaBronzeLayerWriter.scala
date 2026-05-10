package com.criticalinfra.engine

import com.criticalinfra.config.SourceConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit}

import scala.util.Try

// =============================================================================
// DeltaBronzeLayerWriter — production Delta Lake implementation of BronzeLayerWriter
//
// Branch 3 additions:
//   - SHA-256 batch checksum computed from raw input rows before enrichment
//   - Record count validation: source count vs written count must not exceed
//     mismatchThreshold; any excess throws IllegalStateException caught by Try
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
  *   - `_cidf_checksum` — SHA-256 hex digest of all raw input rows (sorted, deterministic).
  *
  * ==Partition columns added==
  *   - `ingestion_date` — calendar date of the pipeline run (`LocalDate.now()`, format
  *     `YYYY-MM-DD`).
  *   - `source_name` — same value as `_cidf_source_name`; used for partition pruning.
  *
  * ==Record count validation==
  * The source record count (before enrichment) is compared to the written record count (after
  * enrichment). Since enrichment only adds columns and never filters rows, these must always be
  * equal. If the absolute difference exceeds `mismatchThreshold`, an [[IllegalStateException]] is
  * thrown before the Delta write; it is caught by the outer `Try` and returned as
  * `Left(StorageWriteError)`.
  *
  * ==Error handling==
  * The entire write operation is wrapped in `scala.util.Try` and converted to `Either`. Any Spark
  * or Delta Lake exception is captured and returned as `Left(StorageWriteError)`. Exceptions are
  * never propagated to the caller.
  *
  * ==Thread safety==
  * Instances are stateless. A single instance may be shared across pipeline runs.
  *
  * @param mismatchThreshold
  *   Maximum tolerated absolute difference between source record count and written record count.
  *   Default `0L` means any discrepancy fails immediately. A negative value forces failure on every
  *   write (useful in testing).
  */
final class DeltaBronzeLayerWriter(mismatchThreshold: Long = 0L) extends BronzeLayerWriter {

  /** Computes a deterministic SHA-256 hex digest of all rows in `data`.
    *
    * Rows are collected to the driver, converted to strings, sorted (to eliminate partition-order
    * non-determinism), concatenated, and hashed. The result is a 64-character lowercase hex string.
    *
    * @param data
    *   The raw input DataFrame before any enrichment columns are added.
    * @return
    *   64-character lowercase hexadecimal SHA-256 digest.
    */
  private def computeChecksum(data: DataFrame): String = {
    val rows      = data.collect().map(_.toString).sorted.mkString
    val digest    = java.security.MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(rows.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }

  /** Enriches `data` with CIDF metadata and partition columns and writes it to the Delta Bronze
    * path.
    *
    * ==Order of operations==
    *   1. `computeChecksum(data)` — SHA-256 of raw rows, before any enrichment. 2. `data.count()` —
    *      source record count, before enrichment. 3. Inject four `_cidf_*` metadata columns. 4. Add
    *      two partition columns (`ingestion_date`, `source_name`). 5. `partitioned.count()` —
    *      written record count, after enrichment. 6. Count mismatch check — throws if `|sourceCount
    *      \- writtenCount| > mismatchThreshold`. 7. Delta append write, partitioned by
    *      `ingestion_date` and `source_name`. 8. `ALTER TABLE … SET TBLPROPERTIES (delta.appendOnly
    *      \= true)` — idempotent. 9. Return `Right(WriteResult)`.
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
      // Step 1 — Compute SHA-256 checksum of raw input rows (before enrichment)
      // -----------------------------------------------------------------------
      val checksum = computeChecksum(data)

      // -----------------------------------------------------------------------
      // Step 2 — Capture source record count (before enrichment)
      // -----------------------------------------------------------------------
      val sourceCount = data.count()

      // -----------------------------------------------------------------------
      // Step 3 — Inject CIDF audit metadata columns
      // -----------------------------------------------------------------------
      val enriched = data
        .withColumn("_cidf_ingestion_ts", current_timestamp())
        .withColumn("_cidf_source_name", lit(sourceName))
        .withColumn("_cidf_run_id", lit(runId.toString))
        .withColumn("_cidf_checksum", lit(checksum))

      // -----------------------------------------------------------------------
      // Step 4 — Add partition columns
      // -----------------------------------------------------------------------
      val partitioned = enriched
        .withColumn("ingestion_date", lit(today))
        .withColumn("source_name", lit(sourceName))

      // -----------------------------------------------------------------------
      // Step 5 — Count after enrichment (must equal sourceCount)
      // -----------------------------------------------------------------------
      val writtenCount = partitioned.count()

      // -----------------------------------------------------------------------
      // Step 6 — Record count mismatch validation
      // -----------------------------------------------------------------------
      if (math.abs(sourceCount - writtenCount) > mismatchThreshold)
        throw new IllegalStateException(
          s"Record count mismatch: source=$sourceCount written=$writtenCount " +
            s"threshold=$mismatchThreshold path=$path"
        )

      // -----------------------------------------------------------------------
      // Step 7 — Write to Delta in append mode, partitioned by date and source
      // -----------------------------------------------------------------------
      partitioned.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date", "source_name")
        .save(path)

      // -----------------------------------------------------------------------
      // Step 8 — Enforce appendOnly after write (idempotent)
      // -----------------------------------------------------------------------
      val spark = data.sparkSession
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.appendOnly' = 'true')"
      )

      // -----------------------------------------------------------------------
      // Step 9 — Return populated WriteResult
      // -----------------------------------------------------------------------
      WriteResult(
        recordsWritten = writtenCount,
        path = path,
        partitionDate = java.time.LocalDate.now(),
        checksum = checksum
      )
    }.toEither.left.map(t => StorageWriteError(path, t.getMessage))
  }
}

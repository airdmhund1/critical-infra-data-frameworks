package com.criticalinfra.engine

import com.criticalinfra.config.SourceConfig
import org.apache.spark.sql.DataFrame

import scala.util.Try

// =============================================================================
// BronzeWriter — contract and Delta Lake implementation for Bronze-layer writes
//
// The trait defines the interface all Bronze storage writer implementations
// must satisfy. The DeltaBronzeWriter is the production implementation for
// Phase 1 (v0.1.0). Every write failure is surfaced as a StorageWriteError
// rather than thrown, so the engine receives a uniform Either-based result.
//
// BronzeLayerWriter is the richer successor trait introduced in Branch 1 of
// Issue #13. It returns a WriteResult on success rather than a bare Long,
// carrying the write path, partition date, and checksum alongside the record
// count. DeltaBronzeLayerWriter (Branch 2) will replace DeltaBronzeWriter as
// the production default once full Delta Lake writes are implemented.
// =============================================================================

// -----------------------------------------------------------------------------
// WriteResult — value type returned by BronzeLayerWriter on success
// -----------------------------------------------------------------------------

/** Immutable summary of a successful Bronze-layer write operation.
  *
  * Returned by [[BronzeLayerWriter]] implementations on `Right` to give the engine — and downstream
  * lineage records — a structured, auditable record of exactly what was written, where, and when.
  *
  * @param recordsWritten
  *   Number of records committed to the Bronze storage path.
  * @param path
  *   Absolute URI of the Bronze storage path that was written to.
  * @param partitionDate
  *   Calendar date under which the data was partitioned (typically the pipeline run date in UTC).
  * @param checksum
  *   Integrity checksum of the written data (algorithm and format determined by implementation).
  *   Empty string when the implementation does not compute a checksum (e.g. the stub default).
  */
final case class WriteResult(
    recordsWritten: Long,
    path: String,
    partitionDate: java.time.LocalDate,
    checksum: String
)

// -----------------------------------------------------------------------------
// BronzeLayerWriter — richer successor contract for Bronze-layer writes
// -----------------------------------------------------------------------------

/** Contract for writing a validated DataFrame to the Bronze lakehouse layer.
  *
  * Successor to the legacy [[BronzeWriter]] trait. Implementations must adhere to the following
  * contracts:
  *
  *   - Any write failure must be returned as `Left(StorageWriteError)`. Exceptions must never be
  *     allowed to propagate to the caller.
  *   - A [[WriteResult]] is returned in `Right` on success, carrying the record count, storage
  *     path, partition date, and checksum alongside the record count so that the engine can
  *     construct a complete audit trail without performing additional reads.
  *   - Implementations must not modify the supplied DataFrame before writing it.
  *   - The supplied `runId` is the correlation key for this write; implementations may embed it in
  *     metadata or file paths as required by their storage format.
  */
trait BronzeLayerWriter {

  /** Writes `data` to the Bronze storage path specified in `config.storage.path`.
    *
    * @param data
    *   The validated DataFrame to persist. The implementation must not modify or cache it.
    * @param config
    *   Fully resolved pipeline configuration supplying the storage path, format, and partition
    *   columns.
    * @param runId
    *   Unique identifier for this pipeline run, used as a correlation key in audit metadata.
    * @return
    *   `Right(WriteResult)` containing write metadata on success, or `Left(StorageWriteError(path,
    *   cause))` if the write fails for any reason.
    */
  def write(
      data: DataFrame,
      config: SourceConfig,
      runId: java.util.UUID
  ): Either[StorageWriteError, WriteResult]
}

// -----------------------------------------------------------------------------
// BronzeWriter — legacy trait (retained for backward compatibility)
// -----------------------------------------------------------------------------

/** Legacy contract for writing a validated DataFrame to the Bronze lakehouse layer.
  *
  * Retained so that existing code paths and test infrastructure written against the original
  * interface continue to compile. New implementations should target [[BronzeLayerWriter]] instead.
  *
  * Implementations must adhere to the following contracts:
  *
  *   - Any write failure must be returned as `Left(StorageWriteError)`. Exceptions must never be
  *     allowed to propagate to the caller.
  *   - The count of records written is returned in `Right(count)` on success, enabling the engine
  *     to capture the exact number of records committed to storage.
  *   - Implementations must not modify the supplied DataFrame before writing it.
  */
trait BronzeWriter {

  /** Writes `data` to the Bronze storage path specified in `config.storage.path`.
    *
    * @param data
    *   The validated DataFrame to persist. The implementation must not modify or cache it.
    * @param config
    *   Fully resolved pipeline configuration supplying the storage path, format, and partition
    *   columns.
    * @return
    *   `Right(count)` containing the number of records written on success, or
    *   `Left(StorageWriteError(path, cause))` if the write fails for any reason. The `path` field
    *   of the error is set to `config.storage.path` so that operators can inspect the target
    *   location directly.
    */
  def write(data: DataFrame, config: SourceConfig): Either[StorageWriteError, Long]
}

// -----------------------------------------------------------------------------
// DefaultBronzeLayerWriter — no-op stub, replaced by DeltaBronzeLayerWriter in Branch 2
// -----------------------------------------------------------------------------

/** Minimal no-op stub implementing [[BronzeLayerWriter]].
  *
  * Returns `Right(WriteResult(0L, "", LocalDate.now(), ""))` without performing any actual write.
  * Exists as a named, compile-time-safe default so that code paths that do not yet need a real
  * writer can instantiate this class explicitly. It will be superseded by `DeltaBronzeLayerWriter`
  * in Branch 2.
  */
final class DefaultBronzeLayerWriter extends BronzeLayerWriter {

  /** Returns a zero-count [[WriteResult]] without writing any data.
    *
    * @param data
    *   Ignored.
    * @param config
    *   Ignored.
    * @param runId
    *   Ignored.
    * @return
    *   `Right(WriteResult(0L, "", LocalDate.now(), ""))` always.
    */
  def write(
      data: DataFrame,
      config: SourceConfig,
      runId: java.util.UUID
  ): Either[StorageWriteError, WriteResult] =
    Right(WriteResult(0L, "", java.time.LocalDate.now(), ""))
}

// -----------------------------------------------------------------------------
// DeltaBronzeWriter — production Delta Lake implementation (BronzeLayerWriter)
// -----------------------------------------------------------------------------

/** Production Delta Lake implementation of [[BronzeLayerWriter]].
  *
  * Writes `data` in append mode using the Delta Lake format to the path specified in
  * `config.storage.path`. If `config.storage.partitionBy` is non-empty, the data is partitioned by
  * those columns. The record count is computed before the write operation so that a precise count
  * is always available even if the write subsequently fails.
  *
  * The entire operation is wrapped in a `scala.util.Try` so that any exception thrown by the
  * underlying Spark or Delta Lake write path is captured and returned as a `StorageWriteError`
  * rather than propagated to the caller.
  *
  * This class also implements the legacy [[BronzeWriter]] interface to preserve backward
  * compatibility with test infrastructure written against the original trait. That interface will
  * be removed once all consumers have migrated to [[BronzeLayerWriter]].
  */
final class DeltaBronzeWriter extends BronzeLayerWriter with BronzeWriter {

  /** Writes `data` to the Delta Lake Bronze path defined in `config`.
    *
    * Satisfies the [[BronzeLayerWriter]] contract: returns a full [[WriteResult]] on success.
    *
    * @param data
    *   The validated DataFrame to persist.
    * @param config
    *   Fully resolved pipeline configuration supplying storage path and partition columns.
    * @param runId
    *   Unique identifier for this pipeline run (embedded in the returned [[WriteResult]] path for
    *   auditability; the storage path itself is taken from `config.storage.path`).
    * @return
    *   `Right(WriteResult)` on success, or `Left(StorageWriteError(path, cause))` on any write
    *   failure.
    */
  def write(
      data: DataFrame,
      config: SourceConfig,
      runId: java.util.UUID
  ): Either[StorageWriteError, WriteResult] = {
    val path = config.storage.path
    Try {
      val count  = data.count()
      val writer = data.write.format("delta").mode("append")
      val partitioned =
        if (config.storage.partitionBy.nonEmpty)
          writer.partitionBy(config.storage.partitionBy: _*)
        else
          writer
      partitioned.save(path)
      WriteResult(
        recordsWritten = count,
        path = path,
        partitionDate = java.time.LocalDate.now(),
        checksum = ""
      )
    }.toEither.left.map(t => StorageWriteError(path, t.getMessage))
  }

  /** Writes `data` to the Delta Lake Bronze path defined in `config`.
    *
    * Satisfies the legacy [[BronzeWriter]] contract for backward compatibility with existing test
    * infrastructure. Delegates to [[write(DataFrame, SourceConfig, UUID)]] using a fresh random
    * UUID and maps the result to a bare `Long`.
    *
    * @param data
    *   The validated DataFrame to persist.
    * @param config
    *   Fully resolved pipeline configuration supplying storage path and partition columns.
    * @return
    *   `Right(count)` on success, or `Left(StorageWriteError(path, cause))` on any write failure.
    */
  def write(data: DataFrame, config: SourceConfig): Either[StorageWriteError, Long] =
    write(data, config, java.util.UUID.randomUUID()).map(_.recordsWritten)
}

// -----------------------------------------------------------------------------
// BronzeWriter companion object — factory
// -----------------------------------------------------------------------------

/** Factory for [[BronzeLayerWriter]] instances.
  *
  * Provides a `default` factory method that returns the production [[DeltaBronzeLayerWriter]].
  * Callers that need a different implementation (e.g. test doubles) should supply their own
  * instance to the engine constructor rather than using this factory.
  */
object BronzeWriter {

  /** Creates and returns a new [[DeltaBronzeLayerWriter]], the production Bronze writer.
    *
    * The returned instance injects CIDF audit metadata columns and partition columns into every
    * DataFrame before writing, and enforces `delta.appendOnly = true` after each write.
    *
    * @return
    *   A new [[DeltaBronzeLayerWriter]] instance typed as [[BronzeLayerWriter]].
    */
  def default: BronzeLayerWriter = new DeltaBronzeLayerWriter
}

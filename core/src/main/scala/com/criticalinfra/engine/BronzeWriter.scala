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
// =============================================================================

/** Contract for writing a validated DataFrame to the Bronze lakehouse layer.
  *
  * Implementations must adhere to the following contracts:
  *
  *   - Any write failure must be returned as `Left(StorageWriteError)`. Exceptions must never be
  *     allowed to propagate to the caller.
  *   - The count of records written is returned in `Right(count)` on success, enabling the engine to
  *     capture the exact number of records committed to storage.
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
    *   `Left(StorageWriteError(path, cause))` if the write fails for any reason. The `path` field of
    *   the error is set to `config.storage.path` so that operators can inspect the target location
    *   directly.
    */
  def write(data: DataFrame, config: SourceConfig): Either[StorageWriteError, Long]
}

/** Production Delta Lake implementation of [[BronzeWriter]].
  *
  * Writes `data` in append mode using the Delta Lake format to the path specified in
  * `config.storage.path`. If `config.storage.partitionBy` is non-empty, the data is partitioned by
  * those columns. The record count is computed before the write operation so that a precise count is
  * always available even if the write subsequently fails.
  *
  * The entire operation is wrapped in a `scala.util.Try` so that any exception thrown by the
  * underlying Spark or Delta Lake write path is captured and returned as a `StorageWriteError` rather
  * than propagated to the caller.
  */
final class DeltaBronzeWriter extends BronzeWriter {

  /** Writes `data` to the Delta Lake Bronze path defined in `config`.
    *
    * @param data
    *   The validated DataFrame to persist.
    * @param config
    *   Fully resolved pipeline configuration supplying storage path and partition columns.
    * @return
    *   `Right(count)` on success, or `Left(StorageWriteError(path, cause))` on any write failure.
    */
  def write(data: DataFrame, config: SourceConfig): Either[StorageWriteError, Long] = {
    val path = config.storage.path
    Try {
      val count = data.count()
      val writer = data.write.format("delta").mode("append")
      val partitioned =
        if (config.storage.partitionBy.nonEmpty)
          writer.partitionBy(config.storage.partitionBy: _*)
        else
          writer
      partitioned.save(path)
      count
    }.toEither.left.map(t => StorageWriteError(path, t.getMessage))
  }
}

/** Factory for [[BronzeWriter]] instances.
  *
  * Provides a `default` factory method that returns the production [[DeltaBronzeWriter]]. Callers
  * that need a different implementation (e.g. test doubles) should supply their own instance to the
  * engine constructor rather than using this factory.
  */
object BronzeWriter {

  /** Creates and returns a new [[DeltaBronzeWriter]], the default production Bronze writer.
    *
    * @return
    *   A new [[DeltaBronzeWriter]] instance.
    */
  def default: BronzeWriter = new DeltaBronzeWriter
}

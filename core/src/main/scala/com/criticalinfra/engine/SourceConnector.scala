package com.criticalinfra.engine

import com.criticalinfra.config.SourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

// =============================================================================
// SourceConnector — interface every source connector implementation must satisfy
//
// The ingestion engine uses ConnectorRegistry to look up a SourceConnector for
// a given ConnectionType, then calls extract to obtain a DataFrame that is
// passed to the validation and storage layers. Each connector implementation is
// responsible for exactly one transport protocol (JDBC, file, Kafka, API).
//
// Failures are returned as ConnectorError rather than thrown so that the engine
// receives a uniform Either-based result and can handle all failure modes
// through exhaustive pattern matching without exception leakage.
// =============================================================================

/** Interface every source connector implementation must satisfy.
  *
  * The ingestion engine resolves the appropriate implementation from [[ConnectorRegistry]] based on
  * the `connection.connectionType` field of the pipeline configuration, then calls `extract` to
  * obtain the raw source DataFrame. The returned DataFrame is passed to the schema-enforcement and
  * Bronze-layer storage steps without modification.
  *
  * Implementations must adhere to the following contracts:
  *
  *   - Failures during extraction must be returned as `Left(ConnectorError)` rather than thrown.
  *     The engine performs no try-catch around `extract`; an uncaught exception will propagate as an
  *     unhandled failure.
  *   - Implementations must not close or stop the supplied `SparkSession`. The session is owned by
  *     the engine and has a lifetime that extends beyond any single extraction call.
  *   - The DataFrame returned in `Right` must not be mutated by the caller after the `extract` call
  *     returns. Implementations may return a lazily evaluated DataFrame; any subsequent action that
  *     triggers evaluation may still produce a `ConnectorError` — callers must handle this by
  *     wrapping Spark actions in their own error boundary.
  *   - Implementations are expected to be safe for concurrent use by multiple pipeline runs sharing
  *     the same `SourceConnector` instance.
  */
trait SourceConnector {

  /** Extracts data from the source system described by `config` using the supplied `SparkSession`.
    *
    * @param config
    *   Fully resolved pipeline configuration for this extraction run. The `connection` section
    *   supplies all connectivity parameters; credentials have already been resolved from the secrets
    *   manager before this method is called.
    * @param spark
    *   Active `SparkSession` owned by the ingestion engine. Implementations must not stop or close
    *   it. The session may be shared across concurrent extraction calls.
    * @return
    *   `Right(dataFrame)` containing the raw extracted dataset on success, or
    *   `Left(ConnectorError)` if the extraction fails for any reason — including connection errors,
    *   authentication failures, missing tables or files, and query execution errors. The
    *   `ConnectorError.source` field must be set to `config.metadata.sourceId` so that the error can
    *   be correlated with the originating pipeline configuration in audit logs.
    */
  def extract(
      config: SourceConfig,
      spark: SparkSession
  ): Either[ConnectorError, DataFrame]
}

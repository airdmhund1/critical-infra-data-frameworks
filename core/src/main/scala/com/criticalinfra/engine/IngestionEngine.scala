package com.criticalinfra.engine

import com.criticalinfra.config.SourceConfig
import org.apache.spark.sql.SparkSession

// =============================================================================
// IngestionEngine — configuration-driven ingestion pipeline orchestrator
//
// Reads a fully resolved SourceConfig at runtime and executes the standard
// five-step pipeline: connector lookup → extraction → quality validation →
// Bronze write → lineage recording.
//
// Every failure mode is returned as a typed Left[IngestionError] so that
// callers can handle errors through exhaustive pattern matching without
// exception leakage. The single try/catch at the outermost boundary of run
// catches any unexpected Throwable not handled by connector or writer
// contracts and wraps it in UnexpectedError, preserving the full stack trace.
// =============================================================================

/** Configuration-driven ingestion pipeline that reads a [[com.criticalinfra.config.SourceConfig]]
  * at runtime and executes the five canonical pipeline steps: connector lookup, data extraction,
  * quality validation, Bronze-layer write, and lineage recording.
  *
  * All four collaborators are supplied by the caller and default to their Phase 1 no-op or default
  * production implementations, enabling straightforward substitution of test doubles without
  * modifying the engine.
  *
  * Thread safety: instances of `IngestionEngine` are safe for concurrent use only if the supplied
  * collaborators are themselves thread-safe. The production defaults are all thread-safe.
  *
  * @param registry
  *   Immutable registry mapping [[com.criticalinfra.config.ConnectionType]] values to their
  *   [[SourceConnector]] implementations. Must contain an entry for every `connectionType` value
  *   that will be submitted via [[run]]; absent entries produce a `Left(ConfigurationError)`.
  * @param validator
  *   Data quality validator applied to the raw DataFrame before Bronze storage. Defaults to
  *   [[PassThroughDataQualityValidator]], which passes all records through unchanged.
  * @param lineageRecorder
  *   Lineage catalog recorder invoked after each successful pipeline run. Defaults to
  *   [[NoOpLineageRecorder]], which discards all lineage information.
  * @param bronzeWriter
  *   Writer that persists the validated DataFrame to the Bronze lakehouse layer. Defaults to the
  *   production [[DeltaBronzeWriter]] obtained via [[BronzeWriter.default]].
  */
final class IngestionEngine(
    registry: ConnectorRegistry,
    validator: DataQualityValidator = PassThroughDataQualityValidator,
    lineageRecorder: LineageRecorder = NoOpLineageRecorder,
    bronzeWriter: BronzeLayerWriter = BronzeWriter.default
) {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[IngestionEngine])

  /** Executes a complete ingestion pipeline run for the source described by `config`.
    *
    * The pipeline is executed in exactly the following order:
    *
    *   1. Generates a fresh UUID (`runId`) that serves as the unique correlation key for this run
    *      across audit logs, lineage records, and the returned [[IngestionResult]].
    *      2. Looks up the [[SourceConnector]] for `config.connection.connectionType` in the
    *      [[ConnectorRegistry]]. Returns `Left(ConfigurationError)` if no connector is registered.
    *      3. Calls `connector.extract(config, spark)` to obtain the raw source
    *      [[org.apache.spark.sql.DataFrame]]. Returns `Left(ConnectorError)` if extraction fails.
    *      4. Counts the raw records (`recordsRead`) — this count reflects the number of records
    *      received from the source, before any quality filtering. 5. Applies the
    *      [[DataQualityValidator]] to the raw DataFrame, producing a validated DataFrame that may
    *      contain fewer records. 6. Calls `bronzeWriter.write(validated, config, runId)` to persist
    *      the validated DataFrame. Returns `Left(StorageWriteError)` if the write fails. 7. Creates
    *      an [[IngestionResult]] using the same `runId`, invokes the [[LineageRecorder]], and
    *      returns the result in `Right`.
    *
    * Any unexpected `Throwable` not handled by the collaborator contracts is caught at the
    * outermost boundary, logged at ERROR level, and returned as `Left(UnexpectedError)`. The
    * original `Throwable` is preserved in the error for diagnostic purposes.
    *
    * @param config
    *   Fully resolved pipeline configuration. All credential references must already have been
    *   resolved to their runtime values before this method is called.
    * @param spark
    *   Active `SparkSession` owned by the caller. The engine does not stop or close the session.
    * @return
    *   `Right(IngestionResult)` on success, or `Left(IngestionError)` if any pipeline step fails.
    *   The specific subtype of [[IngestionError]] identifies which step failed.
    */
  def run(config: SourceConfig, spark: SparkSession): Either[IngestionError, IngestionResult] = {
    val startMs = System.currentTimeMillis()
    val runId   = java.util.UUID.randomUUID()
    logger.info("Starting ingestion run for source: {}", config.metadata.sourceId)

    try {
      val pipeline = for {
        connector <- registry.lookup(config.connection.connectionType)
        rawDf     <- connector.extract(config, spark)
        recordsRead = rawDf.count()
        _ = logger.info(
          "Extracted {} records from source: {}",
          recordsRead.asInstanceOf[AnyRef],
          config.metadata.sourceId
        )
        validated   = validator.validate(rawDf)
        writeResult <- bronzeWriter.write(validated, config, runId)
      } yield {
        val durationMs = System.currentTimeMillis() - startMs
        val result     = IngestionResult.create(runId.toString, recordsRead, writeResult.recordsWritten, durationMs)
        lineageRecorder.record(result.runId, config, result)
        logger.info(
          "Ingestion run complete — runId: {}, recordsRead: {}, recordsWritten: {}, durationMs: {}",
          result.runId,
          result.recordsRead.asInstanceOf[AnyRef],
          result.recordsWritten.asInstanceOf[AnyRef],
          result.durationMs.asInstanceOf[AnyRef]
        )
        result
      }

      pipeline match {
        case Right(result) => Right(result)
        case Left(err)     => logAndReturn(err)
      }
    }
    catch {
      case t: Throwable =>
        logger.error(
          "Unexpected error during ingestion run for source: {}",
          config.metadata.sourceId,
          t
        )
        Left(UnexpectedError(t.getMessage, Some(t)))
    }
  }

  /** Logs the [[IngestionError]] at ERROR level and returns it wrapped in `Left`.
    *
    * @param error
    *   The error to log and propagate.
    * @return
    *   `Left(error)` — the same value passed in, allowing this helper to be used inline.
    */
  private def logAndReturn(error: IngestionError): Either[IngestionError, IngestionResult] = {
    error match {
      case ConfigurationError(field, message) =>
        logger.error("Configuration error — field: {}, message: {}", field, message)
      case ConnectorError(source, cause) =>
        logger.error("Connector error — source: {}, cause: {}", source, cause)
      case StorageWriteError(path, cause) =>
        logger.error("Storage write error — path: {}, cause: {}", path, cause)
      case UnexpectedError(message, throwable) =>
        logger.error("Unexpected error — message: {}", message, throwable.orNull)
    }
    Left(error)
  }
}

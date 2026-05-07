package com.criticalinfra.engine

// =============================================================================
// AuditEvent — data model for pipeline audit log entries
//
// Captures a complete, immutable record of a single pipeline run: the source
// that was processed, the time window, record counts, the Bronze storage path,
// a content checksum, the overall run status, and any error detail.
//
// AuditStatus encodes the three terminal outcomes a run can produce.
// AuditLogError represents failures that an audit logger implementation can
// encounter when attempting to persist an AuditEvent.
//
// No logger implementation is included here — that is introduced in Branch 2.
// =============================================================================

/** Terminal status of a completed pipeline run as recorded in the audit log.
  *
  * Implementations of `AuditEventLogger` map their write outcomes to one of these three values so
  * that downstream consumers can filter and alert on run health without parsing free-form text.
  */
sealed trait AuditStatus

/** Companion object providing all concrete [[AuditStatus]] values. */
object AuditStatus {

  /** All records were read and written without error; no records were quarantined. */
  case object Success extends AuditStatus

  /** The pipeline run encountered a terminal error; no records were written to Bronze. */
  case object Failure extends AuditStatus

  /** Some records were written to Bronze and some were quarantined; the run completed but with
    * data-quality exceptions.
    */
  case object Partial extends AuditStatus
}

/** Immutable audit-log entry capturing the full outcome of a single pipeline run.
  *
  * One [[AuditEvent]] is produced per pipeline run and persisted by the audit logger to the
  * compliance audit trail. The `runId` is the primary correlation key linking this record to
  * [[IngestionResult]], lineage catalog entries, and Prometheus metrics for the same run.
  *
  * @param runId
  *   Globally unique identifier for the pipeline run, generated as a random UUID and shared with
  *   [[IngestionResult]] and [[LineageRecorder]] for cross-system correlation.
  * @param sourceName
  *   Human-readable name of the data source as declared in the pipeline configuration
  *   (`metadata.sourceName`).
  * @param sourceType
  *   Connection type string identifying the source connector category (e.g. `"jdbc"`, `"file"`,
  *   `"stream"`), derived from `connection.connectionType`.
  * @param pipelineStartTs
  *   Wall-clock timestamp (UTC) at which the pipeline run began extraction from the source
  *   connector.
  * @param pipelineEndTs
  *   Wall-clock timestamp (UTC) at which the pipeline run completed — either after a successful
  *   Bronze write or after a terminal error was recorded.
  * @param recordsRead
  *   Number of records successfully extracted from the source connector before any quality
  *   validation was applied.
  * @param recordsWritten
  *   Number of records successfully written to the Bronze lakehouse layer after quality validation.
  *   Will be less than or equal to `recordsRead`; the difference represents records routed to
  *   quarantine.
  * @param quarantineCount
  *   Number of records that failed quality validation and were routed to the quarantine store
  *   rather than written to Bronze. `recordsRead - recordsWritten - quarantineCount` should equal
  *   zero for a well-formed run.
  * @param bronzePath
  *   URI of the Bronze storage location where records were written (e.g.
  *   `"s3://bronze/financial/trades/2026-05-07"`). Empty string when `status` is
  *   [[AuditStatus.Failure]] and no write was attempted.
  * @param checksum
  *   SHA-256 hex digest of the written data batch as produced by [[DeltaBronzeLayerWriter]]. Empty
  *   string when `status` is [[AuditStatus.Failure]] and no write was attempted.
  * @param status
  *   Terminal outcome of the pipeline run: [[AuditStatus.Success]], [[AuditStatus.Failure]], or
  *   [[AuditStatus.Partial]].
  * @param errorMessage
  *   Human-readable description of the failure or warning condition, if any. `None` for
  *   [[AuditStatus.Success]] runs; `Some(message)` for [[AuditStatus.Failure]] and
  *   [[AuditStatus.Partial]] runs.
  */
final case class AuditEvent(
    runId: String,
    sourceName: String,
    sourceType: String,
    pipelineStartTs: java.time.Instant,
    pipelineEndTs: java.time.Instant,
    recordsRead: Long,
    recordsWritten: Long,
    quarantineCount: Long,
    bronzePath: String,
    checksum: String,
    status: AuditStatus,
    errorMessage: Option[String]
)

/** Base type for all errors that can occur when an audit logger attempts to persist an
  * [[AuditEvent]].
  *
  * Implementations of `AuditEventLogger` return `Either[AuditLogError, Unit]` so that logging
  * failures are surfaced explicitly without exception propagation. A logging failure must never
  * abort an otherwise successful pipeline run.
  */
sealed trait AuditLogError

/** Companion object providing all concrete [[AuditLogError]] subtypes. */
object AuditLogError {

  /** Raised when the audit logger fails to write the event to its storage backend.
    *
    * Covers file-system permission errors, S3 write failures, database write failures, and any
    * other I/O error occurring during the persistence phase.
    *
    * @param path
    *   URI or file-system path to the audit log destination that the logger attempted to write to.
    * @param cause
    *   Human-readable description of the write failure, including any error code or message from
    *   the underlying storage layer.
    */
  final case class WriteError(path: String, cause: String) extends AuditLogError

  /** Raised when the audit logger encounters a schema incompatibility or missing field when
    * serialising or deserialising an [[AuditEvent]].
    *
    * @param message
    *   Human-readable description of the schema violation or missing field.
    */
  final case class SchemaError(message: String) extends AuditLogError
}

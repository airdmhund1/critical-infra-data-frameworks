package com.criticalinfra.engine

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.util.Try

// =============================================================================
// AuditEventLogger — contract and production implementation for audit logging
//
// The trait defines the interface that all audit-logger implementations must
// satisfy. NoOpAuditEventLogger discards events (Phase 1 default). The
// production DeltaAuditEventLogger persists events to a Delta Lake append-only
// audit table, satisfying Dodd-Frank, NERC CIP, and NIST CSF audit-trail
// requirements.
// =============================================================================

/** Contract for persisting [[AuditEvent]] records to a durable audit trail.
  *
  * Callers program to this trait. The concrete implementation is supplied at runtime so that the
  * no-op stub used in testing can be swapped for the Delta-backed implementation in production
  * without changing any call sites.
  *
  * Implementations must not throw exceptions. Any failure to persist an event must be surfaced as
  * `Left(AuditLogError)`. A logging failure must never abort an otherwise successful pipeline run.
  */
trait AuditEventLogger {

  /** Persists a single [[AuditEvent]] to the audit log.
    *
    * @param event
    *   The completed pipeline run record to persist.
    * @return
    *   `Right(())` on success, or `Left(AuditLogError)` if the event could not be persisted.
    */
  def log(event: AuditEvent): Either[AuditLogError, Unit]
}

/** No-op implementation of [[AuditEventLogger]] that discards all audit events.
  *
  * All arguments to `log` are accepted and immediately discarded. No external system is contacted
  * and no state is written.
  *
  * '''Phase 1 stub.''' This object is the default logger available in v0.1.0. Production deployments
  * replace it with [[DeltaAuditEventLogger]], which persists events to a Delta Lake append-only
  * audit table.
  */
object NoOpAuditEventLogger extends AuditEventLogger {

  /** Accepts the event and returns `Right(())` immediately without writing any audit record.
    *
    * @param event
    *   Ignored.
    * @return
    *   Always `Right(())`.
    */
  def log(event: AuditEvent): Either[AuditLogError, Unit] = Right(())
}

/** Production Delta Lake implementation of [[AuditEventLogger]].
  *
  * Writes each [[AuditEvent]] as a single row to a Delta Lake table at `auditPath`. The table is
  * created automatically on the first write. After every write the `delta.appendOnly` table
  * property is set, making the audit trail immutable by design and satisfying compliance-by-design
  * architecture requirements.
  *
  * ==Column mapping==
  * `java.time.Instant` fields (`pipelineStartTs`, `pipelineEndTs`) are serialised as ISO-8601
  * strings via `.toString`. The `AuditStatus` sealed trait is serialised to the strings
  * `"SUCCESS"`, `"FAILURE"`, or `"PARTIAL"`. `errorMessage` is stored as a nullable `StringType`
  * column; `None` maps to SQL `NULL`.
  *
  * ==Error handling==
  * The entire write operation is wrapped in `scala.util.Try` and converted to `Either`. Any Spark
  * or Delta Lake exception is captured and returned as `Left(AuditLogError.WriteError)`. Exceptions
  * are never propagated to the caller.
  *
  * ==Thread safety==
  * Instances are stateless beyond the injected `SparkSession`. A single instance may be shared
  * across pipeline runs provided the `SparkSession` is thread-safe for the target deployment.
  *
  * @param auditPath
  *   URI or file-system path of the Delta Lake audit table. The table is created on the first
  *   `log` call if it does not already exist.
  * @param spark
  *   Active [[SparkSession]] used to create DataFrames and execute SQL. Injected for testability.
  */
final class DeltaAuditEventLogger(auditPath: String, spark: SparkSession) extends AuditEventLogger {

  /** Fixed schema for the audit table.
    *
    * All timestamp and status fields are stored as strings; `error_message` is nullable.
    */
  private val schema: StructType = StructType(Seq(
    StructField("run_id",            StringType, nullable = false),
    StructField("source_name",       StringType, nullable = false),
    StructField("source_type",       StringType, nullable = false),
    StructField("pipeline_start_ts", StringType, nullable = false),
    StructField("pipeline_end_ts",   StringType, nullable = false),
    StructField("records_read",      LongType,   nullable = false),
    StructField("records_written",   LongType,   nullable = false),
    StructField("quarantine_count",  LongType,   nullable = false),
    StructField("bronze_path",       StringType, nullable = false),
    StructField("checksum",          StringType, nullable = false),
    StructField("status",            StringType, nullable = false),
    StructField("error_message",     StringType, nullable = true)
  ))

  /** Serialises an [[AuditStatus]] value to its canonical uppercase string representation.
    *
    * @param s
    *   The status to serialise.
    * @return
    *   `"SUCCESS"`, `"FAILURE"`, or `"PARTIAL"`.
    */
  private def statusToString(s: AuditStatus): String = s match {
    case AuditStatus.Success => "SUCCESS"
    case AuditStatus.Failure => "FAILURE"
    case AuditStatus.Partial => "PARTIAL"
  }

  /** Persists the [[AuditEvent]] as a single row to the Delta Lake audit table at `auditPath`.
    *
    * ==Order of operations==
    *   1. Builds a single-row `DataFrame` from the event using the fixed [[schema]].
    *   2. Appends the row to the Delta table at `auditPath` (creates the table on first write).
    *   3. Sets `delta.appendOnly = true` via `ALTER TABLE … SET TBLPROPERTIES` (idempotent).
    *
    * @param event
    *   The completed pipeline run record to persist.
    * @return
    *   `Right(())` on success, or `Left(AuditLogError.WriteError(auditPath, cause))` on any
    *   Spark or Delta Lake failure.
    */
  def log(event: AuditEvent): Either[AuditLogError, Unit] =
    Try {
      val row = Row(
        event.runId,
        event.sourceName,
        event.sourceType,
        event.pipelineStartTs.toString,
        event.pipelineEndTs.toString,
        event.recordsRead,
        event.recordsWritten,
        event.quarantineCount,
        event.bronzePath,
        event.checksum,
        statusToString(event.status),
        event.errorMessage.orNull
      )

      val df = spark.createDataFrame(
        java.util.Collections.singletonList(row),
        schema
      )

      df.write.format("delta").mode("append").save(auditPath)

      spark.sql(
        s"ALTER TABLE delta.`$auditPath` SET TBLPROPERTIES ('delta.appendOnly' = 'true')"
      )

      ()
    }.toEither.left.map(t => AuditLogError.WriteError(auditPath, t.getMessage))
}

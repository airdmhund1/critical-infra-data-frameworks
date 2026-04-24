package com.criticalinfra.engine

// =============================================================================
// IngestionResult — value type capturing the outcome of a single pipeline run
//
// Produced by the ingestion engine at the end of every run and passed to the
// LineageRecorder. The runId is the primary correlation key linking this result
// to audit log entries, lineage records, and Prometheus metrics for the same
// run.
// =============================================================================

/** Immutable summary of a completed ingestion pipeline run.
  *
  * Every field is set at run completion; no partial or in-progress result is ever exposed to
  * callers. Prefer constructing instances via `IngestionResult.create` rather than the case-class
  * constructor directly, so that the `runId` is always a freshly generated UUID.
  *
  * @param runId
  *   Globally unique identifier for this pipeline run, generated as a random UUID. Used as the
  *   primary correlation key in audit logs, lineage records, and Prometheus metric labels.
  * @param recordsRead
  *   Number of records successfully extracted from the source connector.
  * @param recordsWritten
  *   Number of records successfully written to the Bronze lakehouse layer. Will be less than or
  *   equal to `recordsRead`; the difference (if any) represents records routed to quarantine.
  * @param durationMs
  *   Wall-clock duration of the pipeline run in milliseconds, measured from extraction start to
  *   Bronze write completion.
  */
final case class IngestionResult(
    runId: String,
    recordsRead: Long,
    recordsWritten: Long,
    durationMs: Long
)

/** Factory for [[IngestionResult]] instances.
  *
  * Centralises `runId` generation so callers never supply their own UUID and risk
  * reusing an existing correlation key.
  */
object IngestionResult {

  /** Creates an [[IngestionResult]] with a freshly generated random UUID as the `runId`.
    *
    * @param recordsRead
    *   Number of records extracted from the source connector during the run.
    * @param recordsWritten
    *   Number of records written to Bronze storage during the run.
    * @param durationMs
    *   Wall-clock duration of the run in milliseconds.
    * @return
    *   A new [[IngestionResult]] whose `runId` is a randomly generated UUID string.
    */
  def create(
      recordsRead: Long,
      recordsWritten: Long,
      durationMs: Long
  ): IngestionResult =
    IngestionResult(
      runId = java.util.UUID.randomUUID().toString,
      recordsRead = recordsRead,
      recordsWritten = recordsWritten,
      durationMs = durationMs
    )
}

package com.criticalinfra.engine

// =============================================================================
// LineageRecorder — contract and Phase 1 stub for lineage catalog recording
//
// The trait defines the recording interface that all lineage-catalog
// implementations must satisfy. In Phase 1 (v0.1.0) only the no-op stub is
// provided; catalog integration is introduced in Phase 3 (v1.0.0).
// =============================================================================

/** Contract for recording source-to-Bronze lineage after each pipeline run.
  *
  * Implementations write a lineage record — correlating the source configuration, run outcome, and
  * unique run identifier — to an external lineage catalog. This directly supports Dodd-Frank, NERC
  * CIP, and NIST CSF audit-trail requirements.
  *
  * Callers should program to this trait. The concrete implementation is supplied by the engine at
  * runtime so that catalog integration can be introduced in Phase 3 without changing any call
  * sites.
  */
trait LineageRecorder {

  /** Records lineage information for a completed pipeline run.
    *
    * Implementations must not throw exceptions. Any failure to write a lineage record should be
    * surfaced through the implementation's own logging mechanism and must not propagate to the
    * caller, as a lineage recording failure must not abort an otherwise successful pipeline run.
    *
    * @param runId
    *   The unique run identifier from [[IngestionResult.runId]], used as the primary correlation
    *   key in the lineage catalog.
    * @param config
    *   The [[com.criticalinfra.config.SourceConfig]] that governed the pipeline run, providing
    *   source identity, storage destination, and audit settings.
    * @param result
    *   The [[IngestionResult]] produced at the end of the run, carrying record counts and duration.
    */
  def record(
      runId: String,
      config: com.criticalinfra.config.SourceConfig,
      result: IngestionResult
  ): Unit
}

/** Phase 1 no-op implementation of [[LineageRecorder]] that discards all lineage information.
  *
  * All arguments to `record` are accepted and immediately discarded. No external system is
  * contacted and no state is written.
  *
  * '''Phase 1 stub.''' This object is the sole recorder available in v0.1.0. Phase 3 (v1.0.0) will
  * introduce a catalog-backed implementation that writes source-to-Bronze lineage records to a
  * metadata catalog (e.g. Apache Atlas or OpenMetadata), satisfying Dodd-Frank, NERC CIP, and NIST
  * CSF lineage-tracking requirements.
  */
object NoOpLineageRecorder extends LineageRecorder {

  /** Accepts all arguments and returns immediately without writing any lineage record.
    *
    * @param runId
    *   Ignored.
    * @param config
    *   Ignored.
    * @param result
    *   Ignored.
    */
  def record(
      runId: String,
      config: com.criticalinfra.config.SourceConfig,
      result: IngestionResult
  ): Unit = ()
}

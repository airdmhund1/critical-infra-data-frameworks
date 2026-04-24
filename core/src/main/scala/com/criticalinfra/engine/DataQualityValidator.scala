package com.criticalinfra.engine

// =============================================================================
// DataQualityValidator — contract and Phase 1 stub for data quality validation
//
// The trait defines the validation interface that all quality-enforcement
// implementations must satisfy. In Phase 1 (v0.1.0) only the pass-through stub
// is provided; the rules-based engine is introduced in Phase 2 (v0.2.0).
// =============================================================================

/** Contract for data quality validation applied to a DataFrame before Bronze storage.
  *
  * Implementations inspect incoming records against a set of quality rules and return a DataFrame
  * containing only the records that pass validation. Failed records are expected to be routed to
  * quarantine by the implementation — no record is ever silently dropped.
  *
  * Callers should program to this trait. The concrete implementation is supplied by the engine at
  * runtime based on the pipeline configuration, enabling rules-based enforcement to be introduced
  * in Phase 2 without changing any call sites.
  */
trait DataQualityValidator {

  /** Applies data quality rules to `data` and returns the passing records.
    *
    * @param data
    *   The DataFrame produced by the source connector, prior to Bronze storage.
    * @return
    *   A DataFrame containing only records that satisfy all quality rules. In the Phase 1
    *   pass-through implementation this is identical to the input DataFrame.
    */
  def validate(data: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame
}

/** Phase 1 stub implementation of [[DataQualityValidator]] that passes all records through
  * unchanged.
  *
  * Returns the input DataFrame without modification. No quality rules are evaluated and no records
  * are quarantined.
  *
  * '''Phase 1 stub.''' This object is the sole validator available in v0.1.0. Phase 2 (v0.2.0) will
  * introduce a rules-based implementation that evaluates the `qualityRules` section of the pipeline
  * configuration and routes failed records to the configured quarantine path.
  */
object PassThroughDataQualityValidator extends DataQualityValidator {

  /** Returns `data` unchanged.
    *
    * @param data
    *   The input DataFrame.
    * @return
    *   The same DataFrame reference, unmodified.
    */
  def validate(data: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = data
}

package com.criticalinfra.engine

import com.criticalinfra.config.SourceConfig
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

// =============================================================================
// InMemoryTestConnector — deterministic in-memory SourceConnector for integration tests
//
// Returns a fixed eight-row DataFrame with schema (id: Long, name: String,
// amount: Double).  The rows are hardcoded so that every integration test that
// relies on this connector produces an identical, reproducible result without
// any external dependency.
//
// THIS CLASS IS TEST-ONLY.  It lives exclusively in the test source tree and
// must never be referenced from production code or packaged into the assembly JAR.
// =============================================================================

/** Test-only connector. Must never be used in production. Lives in the test source tree.
  *
  * Returns a deterministic eight-row [[org.apache.spark.sql.DataFrame]] with schema
  * `(id: Long, name: String, amount: Double)` built from explicit [[org.apache.spark.sql.Row]]
  * and [[org.apache.spark.sql.types.StructType]] values. Implicit Spark encoders and `.toDF()`
  * conversions are deliberately avoided to guarantee schema determinism across all Spark versions
  * and Scala compiler configurations.
  *
  * The connector always returns `Right(df)` — it never produces a
  * [[com.criticalinfra.engine.ConnectorError]], making it suitable for integration tests that focus
  * on downstream pipeline behaviour (Bronze write, result fields, schema propagation) rather than
  * connector failure paths.
  */
final class InMemoryTestConnector extends SourceConnector {

  /** Hardcoded schema: id (Long), name (String), amount (Double). */
  private val schema: StructType = StructType(
    Seq(
      StructField("id",     LongType,   nullable = false),
      StructField("name",   StringType, nullable = false),
      StructField("amount", DoubleType, nullable = false)
    )
  )

  /** Hardcoded eight-row dataset. Values are fixed so all assertions remain deterministic. */
  private val rows: Seq[Row] = Seq(
    Row(1L, "alpha",   100.0),
    Row(2L, "beta",    200.0),
    Row(3L, "gamma",   300.0),
    Row(4L, "delta",   400.0),
    Row(5L, "epsilon", 500.0),
    Row(6L, "zeta",    600.0),
    Row(7L, "eta",     700.0),
    Row(8L, "theta",   800.0)
  )

  /** Extracts the hardcoded eight-row dataset.
    *
    * The supplied `config` and `spark` parameters are used only to create the DataFrame via
    * `spark.createDataFrame`; no external system is contacted. The `config` parameter is accepted
    * to satisfy the [[SourceConnector]] contract and is otherwise ignored.
    *
    * @param config
    *   Pipeline configuration (ignored by this test-only implementation).
    * @param spark
    *   Active `SparkSession` used to construct the in-memory DataFrame.
    * @return
    *   `Right(df)` containing an eight-row DataFrame with schema `(id: Long, name: String,
    *   amount: Double)`. Never returns `Left`.
    */
  def extract(
      config: SourceConfig,
      spark: SparkSession
  ): Either[ConnectorError, DataFrame] = {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )
    Right(df)
  }
}

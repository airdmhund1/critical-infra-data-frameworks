package com.criticalinfra.engine

import com.criticalinfra.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.util.UUID

// =============================================================================
// BronzeLayerWriterIntegrationSpec — integration tests for DeltaBronzeLayerWriter
//
// Tests cover (all against 10 000-row DataFrames unless noted):
//   1. Write 10 000 records; read back count equals 10 000
//   2. All four _cidf_* metadata columns are non-null on every row
//   3. delta.appendOnly = true property confirmed after write
//   4. Checksum in WriteResult is a 64-character lowercase hex string
//   5. Append semantics: 10 000 + 5 000 = 15 000 total rows
// =============================================================================

/** Integration tests for [[DeltaBronzeLayerWriter]] at scale (10 000 rows).
  *
  * Each test uses an isolated temporary directory created via
  * `java.nio.file.Files.createTempDirectory` and cleaned up in a `finally` block.
  * A single [[SparkSession]] with Delta Lake extensions is shared across the suite
  * and stopped in `afterAll`.
  *
  * DataFrames are created via `spark.range` — the `spark` reference is captured as
  * a local `val` inside each test so that Scala 2.13's "stable identifier" requirement
  * for imports is satisfied when `spark` is a `var`.
  */
class BronzeLayerWriterIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // SparkSession — shared across the suite, Delta extensions registered
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("BronzeLayerWriterIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Recursively deletes a directory and all its contents. */
  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory)
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }

  /** Minimal valid [[SourceConfig]] whose Bronze storage path is `bronzePath`. */
  private def testConfig(bronzePath: String, sourceName: String = "integration-source"): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "bronze-integration-spec-source",
        sourceName  = sourceName,
        sector      = Sector.FinancialServices,
        owner       = "test-owner",
        environment = Environment.Dev,
        tags        = List.empty
      ),
      connection = Connection(
        connectionType = ConnectionType.File,
        credentialsRef = "vault://test/creds",
        filePath       = Some("/test/data"),
        fileFormat     = Some(FileFormat.Csv)
      ),
      ingestion         = Ingestion(mode = IngestionMode.Full),
      schemaEnforcement = SchemaEnforcement(enabled = false),
      qualityRules      = QualityRules(enabled = false),
      quarantine        = Quarantine(enabled = false),
      storage = Storage(
        layer  = StorageLayer.Bronze,
        format = StorageFormat.Delta,
        path   = bronzePath
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // Test 1 — Write 10 000 records; read back count equals 10 000
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter (integration)" should
    "write 10 000 records and read back the same count from the Delta table" in {

    val tempPath = Files.createTempDirectory("bronze-integration-spec-1-").toAbsolutePath.toString
    try {
      val ss     = spark
      val data   = ss.range(10000L).toDF("id")
      val config = testConfig(tempPath, "count-check-source")
      val writer = new DeltaBronzeLayerWriter()

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      val written = ss.read.format("delta").load(tempPath)
      written.count() shouldBe 10000L
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2 — All four _cidf_* metadata columns are non-null on every row
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter (integration)" should
    "produce zero null values in all four _cidf_* metadata columns across 10 000 rows" in {

    val tempPath = Files.createTempDirectory("bronze-integration-spec-2-").toAbsolutePath.toString
    try {
      val ss     = spark
      val data   = ss.range(10000L).toDF("id")
      val config = testConfig(tempPath, "metadata-null-check-source")
      val writer = new DeltaBronzeLayerWriter()

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      val written = ss.read.format("delta").load(tempPath)

      written.filter(col("_cidf_ingestion_ts").isNull).count() shouldBe 0L
      written.filter(col("_cidf_source_name").isNull).count()  shouldBe 0L
      written.filter(col("_cidf_run_id").isNull).count()       shouldBe 0L
      written.filter(col("_cidf_checksum").isNull).count()     shouldBe 0L
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3 — delta.appendOnly = true property confirmed on 10 000-record table
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter (integration)" should
    "set delta.appendOnly = true table property after writing 10 000 records" in {

    val tempPath = Files.createTempDirectory("bronze-integration-spec-3-").toAbsolutePath.toString
    try {
      val ss     = spark
      val data   = ss.range(10000L).toDF("id")
      val config = testConfig(tempPath, "append-only-prop-source")
      val writer = new DeltaBronzeLayerWriter()

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      // SHOW TBLPROPERTIES returns a DataFrame with columns "key" and "value"
      val props = ss
        .sql(s"SHOW TBLPROPERTIES delta.`$tempPath`")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap

      props.get("delta.appendOnly") shouldBe Some("true")
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4 — Checksum is a 64-character lowercase hex string
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter (integration)" should
    "return a WriteResult whose checksum is a 64-character lowercase hex SHA-256 string" in {

    val tempPath = Files.createTempDirectory("bronze-integration-spec-4-").toAbsolutePath.toString
    try {
      val ss     = spark
      val data   = ss.range(10000L).toDF("id")
      val config = testConfig(tempPath, "checksum-hex-source")
      val writer = new DeltaBronzeLayerWriter()

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      val wr = result.toOption.get
      wr.checksum should fullyMatch regex "[0-9a-f]{64}"
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5 — Append semantics: 10 000 + 5 000 = 15 000
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter (integration)" should
    "accumulate 15 000 rows after writing 10 000 then 5 000 to the same Delta path" in {

    val tempPath = Files.createTempDirectory("bronze-integration-spec-5-").toAbsolutePath.toString
    try {
      val ss     = spark
      val config = testConfig(tempPath, "append-semantics-source")
      val writer = new DeltaBronzeLayerWriter()

      // First batch: 10 000 rows
      val data1   = ss.range(10000L).toDF("id")
      val result1 = writer.write(data1, config, UUID.randomUUID())
      result1.isRight shouldBe true

      // Second batch: 5 000 rows (same path — must append, not overwrite)
      val data2   = ss.range(10000L, 15000L).toDF("id")
      val result2 = writer.write(data2, config, UUID.randomUUID())
      result2.isRight shouldBe true

      // Total rows in the table must be 15 000
      val written = ss.read.format("delta").load(tempPath)
      written.count() shouldBe 15000L
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }
}

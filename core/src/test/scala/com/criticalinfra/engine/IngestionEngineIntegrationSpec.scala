package com.criticalinfra.engine

import com.criticalinfra.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// IngestionEngineIntegrationSpec — end-to-end integration tests for IngestionEngine
//
// These tests exercise the full pipeline path using:
//   - InMemoryTestConnector   — deterministic in-memory source producing 8 rows
//   - DeltaBronzeWriter        — real Delta Lake writes to a local temp directory
//   - ConnectorRegistry        — real registry wired with File → InMemoryTestConnector
//
// Delta Lake requires SparkSession extensions to be configured; the session is
// created with the required DeltaSparkSessionExtension and DeltaCatalog settings
// in beforeAll and stopped in afterAll.
//
// Each test writes to its own sub-directory of a shared temp root so that
// concurrent test runs do not interfere with one another.
// =============================================================================

/** End-to-end integration tests for [[IngestionEngine]] using a real [[DeltaBronzeWriter]].
  *
  * Each test in this suite creates a fresh sub-directory under a shared temporary root, runs the
  * full pipeline via [[IngestionEngine.run]], and asserts on both the returned [[IngestionResult]]
  * and the data written to the local Delta table.
  *
  * Delta Lake session extensions are registered in [[BeforeAndAfterAll.beforeAll]] and the
  * `SparkSession` is stopped in [[BeforeAndAfterAll.afterAll]].
  */
class IngestionEngineIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // SparkSession and temp directory — lifecycle managed by BeforeAndAfterAll
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _
  private var tempDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("IngestionEngineIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()
    tempDir = java.nio.file.Files.createTempDirectory("integration-test-bronze-")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (tempDir != null) {
      import scala.reflect.io.Directory
      new Directory(tempDir.toFile).deleteRecursively()
    }
  }

  // ---------------------------------------------------------------------------
  // Helper — minimal valid SourceConfig pointing at the given bronzePath
  // ---------------------------------------------------------------------------

  /** Builds a minimal schema-valid [[SourceConfig]] whose storage path is `bronzePath`.
    *
    * Uses [[ConnectionType.File]] and [[StorageFormat.Delta]] so the registry entry for
    * [[ConnectionType.File]] (mapped to [[InMemoryTestConnector]]) is resolved and the
    * [[DeltaBronzeWriter]] writes a Delta table at the specified path.
    *
    * @param bronzePath
    *   Absolute path to the local directory where the Delta table will be written.
    * @return
    *   A minimal but schema-valid [[SourceConfig]] instance.
    */
  private def testConfig(bronzePath: String): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "integration-test-source",
        sourceName  = "Integration Test Source",
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
  // Helper — build the engine wired with InMemoryTestConnector
  // ---------------------------------------------------------------------------

  private def buildEngine(): IngestionEngine =
    new IngestionEngine(
      ConnectorRegistry(Map(ConnectionType.File -> new InMemoryTestConnector))
    )

  // ---------------------------------------------------------------------------
  // Test 1 — happy path end-to-end
  // ---------------------------------------------------------------------------

  "IngestionEngine integration" should
    "write 8 records to a local Delta table and return correct IngestionResult" in {

    val bronzePath = tempDir.resolve("happy-path").toAbsolutePath.toString
    val engine     = buildEngine()
    val result     = engine.run(testConfig(bronzePath), spark)

    result.isRight shouldBe true

    val r = result.toOption.get
    r.recordsRead    shouldBe 8L
    r.recordsWritten shouldBe 8L
    r.durationMs     should be >= 0L

    // runId must be a parseable UUID — fromString throws if the format is invalid
    noException should be thrownBy java.util.UUID.fromString(r.runId)

    // Verify the data actually landed in the Delta table
    val written = spark.read.format("delta").load(bronzePath)
    written.count() shouldBe 8L
  }

  // ---------------------------------------------------------------------------
  // Test 2 — Delta table schema is preserved after write
  // ---------------------------------------------------------------------------

  "IngestionEngine integration" should "write a Delta table with the expected schema" in {

    val bronzePath = tempDir.resolve("schema-check").toAbsolutePath.toString
    val engine     = buildEngine()
    val result     = engine.run(testConfig(bronzePath), spark)

    result.isRight shouldBe true

    val written = spark.read.format("delta").load(bronzePath)
    written.columns.toSet should contain("id")
    written.columns.toSet should contain("name")
    written.columns.toSet should contain("amount")
  }

  // ---------------------------------------------------------------------------
  // Test 3 — runId is a non-empty, valid UUID
  // ---------------------------------------------------------------------------

  "IngestionEngine integration" should "assign a non-empty valid UUID as runId" in {

    val bronzePath = tempDir.resolve("uuid-check").toAbsolutePath.toString
    val engine     = buildEngine()
    val result     = engine.run(testConfig(bronzePath), spark)

    result.isRight shouldBe true

    val runId = result.toOption.get.runId
    runId.nonEmpty shouldBe true
    noException should be thrownBy java.util.UUID.fromString(runId)
  }
}

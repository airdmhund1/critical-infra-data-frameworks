package com.criticalinfra.engine

import com.criticalinfra.config._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// AuditEventLoggerIntegrationSpec — end-to-end integration tests for
// DeltaAuditEventLogger
//
// These tests exercise the full audit log path using:
//   - InMemoryTestConnector   — deterministic in-memory source producing 8 rows
//   - DeltaBronzeLayerWriter   — real Delta Lake writes to a local temp directory
//   - DeltaAuditEventLogger   — real Delta Lake writes to a separate audit path
//   - ConnectorRegistry        — wired with File → InMemoryTestConnector (success)
//                                or FailingConnector (failure scenario)
//
// All audit table queries are issued via spark.sql using the delta.`<path>` syntax
// so that no framework-specific import appears in the query or assertion path.
//
// Test 1 verifies all 12 audit fields on a successful pipeline run.
// Test 2 verifies that a FAILURE audit event is written even when the pipeline fails.
// =============================================================================

/** End-to-end integration tests for [[DeltaAuditEventLogger]].
  *
  * Each test writes to isolated sub-directories of a shared temp root so that concurrent runs do not
  * interfere. The [[SparkSession]] is started once in [[BeforeAndAfterAll.beforeAll]] and stopped in
  * [[BeforeAndAfterAll.afterAll]].
  */
class AuditEventLoggerIntegrationSpec
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
      .appName("AuditEventLoggerIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    tempDir = java.nio.file.Files.createTempDirectory("integration-test-audit-")
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
  // Copied verbatim from IngestionEngineIntegrationSpec.
  // ---------------------------------------------------------------------------

  /** Builds a minimal schema-valid [[SourceConfig]] whose storage path is `bronzePath`.
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
  // Helper — build the engine wired with InMemoryTestConnector + DeltaAuditEventLogger
  // ---------------------------------------------------------------------------

  /** Constructs an [[IngestionEngine]] backed by [[InMemoryTestConnector]] and a real
    * [[DeltaAuditEventLogger]] writing to `auditPath`.
    *
    * @param auditPath
    *   Absolute path to the local directory where the Delta audit table will be written.
    * @return
    *   A fully wired [[IngestionEngine]] ready for integration testing.
    */
  private def buildEngine(auditPath: String): IngestionEngine =
    new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new InMemoryTestConnector)),
      bronzeWriter = new DeltaBronzeLayerWriter(),
      auditLogger  = new DeltaAuditEventLogger(auditPath, spark)
    )

  // ---------------------------------------------------------------------------
  // Failing connector stub — used only in the failure-scenario test
  // ---------------------------------------------------------------------------

  /** Test-only connector that always returns a [[ConnectorError]].
    *
    * Used exclusively in the failure-scenario test to verify that the audit log still receives a
    * FAILURE event when the pipeline's extraction step fails.
    */
  private class FailingConnector extends SourceConnector {
    def extract(config: com.criticalinfra.config.SourceConfig, spark: SparkSession): Either[ConnectorError, DataFrame] =
      Left(ConnectorError("integration-test-source", "simulated connector timeout"))
  }

  // ---------------------------------------------------------------------------
  // Test 1 — Success run: all 12 audit fields verified via Spark SQL
  // ---------------------------------------------------------------------------

  "DeltaAuditEventLogger integration" should
    "write a single SUCCESS audit row with all 12 fields correct after a successful pipeline run" in {

    val bronzePath = tempDir.resolve("audit-success-bronze").toAbsolutePath.toString
    val auditPath  = tempDir.resolve("audit-success-audit").toAbsolutePath.toString
    val engine     = buildEngine(auditPath)
    val config     = testConfig(bronzePath)
    val result     = engine.run(config, spark)

    result.isRight shouldBe true
    val ingestionResult = result.toOption.get

    // Query via raw Spark SQL — no framework imports in the query path
    val auditDf = spark.sql(s"SELECT * FROM delta.`$auditPath`")
    auditDf.count() shouldBe 1L

    val row = auditDf.first()

    // run_id: parseable UUID and matches ingestionResult.runId
    val runId = row.getAs[String]("run_id")
    noException should be thrownBy java.util.UUID.fromString(runId)
    runId shouldBe ingestionResult.runId

    // Timestamp fields: non-null and non-empty
    row.getAs[String]("pipeline_start_ts") should not be null
    row.getAs[String]("pipeline_start_ts") should not be empty
    row.getAs[String]("pipeline_end_ts") should not be null
    row.getAs[String]("pipeline_end_ts") should not be empty

    // Record counts
    row.getAs[Long]("records_read")      shouldBe 8L
    row.getAs[Long]("records_written")   shouldBe 8L
    row.getAs[Long]("quarantine_count")  shouldBe 0L

    // Bronze path matches the path supplied to testConfig
    row.getAs[String]("bronze_path") shouldBe bronzePath

    // Checksum: 64-character lowercase hex (SHA-256)
    val checksum = row.getAs[String]("checksum")
    checksum should fullyMatch regex "[0-9a-f]{64}"

    // Status and error message
    row.getAs[String]("status")        shouldBe "SUCCESS"
    row.getAs[String]("error_message") shouldBe null

    // Source metadata
    row.getAs[String]("source_name") shouldBe config.metadata.sourceName
    val sourceType = row.getAs[String]("source_type")
    sourceType should not be null
    sourceType should not be empty
  }

  // ---------------------------------------------------------------------------
  // Test 2 — Failure scenario: status is FAILURE, errorMessage populated
  // ---------------------------------------------------------------------------

  "DeltaAuditEventLogger integration" should
    "write a single FAILURE audit row with error_message when the connector fails" in {

    val bronzePath = tempDir.resolve("audit-failure-bronze").toAbsolutePath.toString
    val auditPath  = tempDir.resolve("audit-failure-audit").toAbsolutePath.toString
    val engine = new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new FailingConnector)),
      bronzeWriter = new DeltaBronzeLayerWriter(),
      auditLogger  = new DeltaAuditEventLogger(auditPath, spark)
    )
    val result = engine.run(testConfig(bronzePath), spark)

    // Pipeline must have failed
    result.isLeft shouldBe true

    // Audit event is still written even on failure
    val auditDf = spark.sql(s"SELECT * FROM delta.`$auditPath`")
    auditDf.count() shouldBe 1L

    val row = auditDf.first()
    row.getAs[String]("status")        shouldBe "FAILURE"
    row.getAs[String]("error_message") should not be null
    row.getAs[String]("error_message") should include("timeout")
    row.getAs[String]("run_id")        should not be empty
  }
}

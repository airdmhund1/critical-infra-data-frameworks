package com.criticalinfra.engine

import com.criticalinfra.config._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// IngestionEngineAuditSpec — tests for AuditEventLogger wiring in IngestionEngine
//
// Verifies that:
//   1. A successful run produces one AuditStatus.Success event with correct fields.
//   2. A failing run produces one AuditStatus.Failure event with an error message.
//   3. An AuditEventLogger failure is swallowed and does not mask the pipeline result.
//   4. The default NoOpAuditEventLogger is wired in transparently (existing callers
//      that do not supply auditLogger continue to work unchanged).
//
// Delta Lake is not used. BronzeLayerWriter and SourceConnector implementations
// are all supplied as injectable test stubs.
// =============================================================================

/** Tests for the [[AuditEventLogger]] integration in [[IngestionEngine]].
  *
  * All Spark operations use a local `master("local[*]")` session created once in
  * [[BeforeAndAfterAll.beforeAll]] and stopped in [[BeforeAndAfterAll.afterAll]].
  */
class IngestionEngineAuditSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // SparkSession — shared across all tests in this suite
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("IngestionEngineAuditSpec")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ---------------------------------------------------------------------------
  // Test helpers — minimal SourceConfig and stub collaborators
  // ---------------------------------------------------------------------------

  /** Builds a minimal schema-valid [[SourceConfig]] for the given connection type.
    *
    * All optional fields not required for the test are omitted. The `storagePath` defaults to
    * `/tmp/test-bronze` and is never actually written to (the [[BronzeLayerWriter]] is always a stub).
    */
  private def testConfig(
      connectionType: ConnectionType,
      storagePath: String = "/tmp/test-bronze"
  ): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId = "test-source",
        sourceName = "Test Source",
        sector = Sector.FinancialServices,
        owner = "test-owner",
        environment = Environment.Dev,
        tags = List.empty
      ),
      connection = Connection(
        connectionType = connectionType,
        credentialsRef = "vault://test/creds",
        filePath = Some("/test/data"),
        fileFormat = Some(FileFormat.Csv)
      ),
      ingestion = Ingestion(mode = IngestionMode.Full),
      schemaEnforcement = SchemaEnforcement(enabled = false),
      qualityRules = QualityRules(enabled = false),
      quarantine = Quarantine(enabled = false),
      storage = Storage(
        layer = StorageLayer.Bronze,
        format = StorageFormat.Delta,
        path = storagePath
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit = Audit(enabled = false)
    )

  // -- Stub SourceConnectors ---------------------------------------------------

  /** Always returns `Right(df)`. */
  private class SucceedingConnector(df: DataFrame) extends SourceConnector {
    def extract(config: SourceConfig, spark: SparkSession): Either[ConnectorError, DataFrame] =
      Right(df)
  }

  /** Always returns `Left(error)`. */
  private class FailingConnector(error: ConnectorError) extends SourceConnector {
    def extract(config: SourceConfig, spark: SparkSession): Either[ConnectorError, DataFrame] =
      Left(error)
  }

  // -- Stub BronzeLayerWriters -------------------------------------------------

  /** Always returns `Right(WriteResult(count, ...))`. */
  private class SucceedingBronzeWriter(count: Long) extends BronzeLayerWriter {
    def write(
        data: DataFrame,
        config: SourceConfig,
        runId: java.util.UUID
    ): Either[StorageWriteError, WriteResult] =
      Right(WriteResult(count, "", java.time.LocalDate.now(), ""))
  }

  // -- Stub AuditEventLoggers --------------------------------------------------

  /** Records all logged events for assertion. */
  private class RecordingAuditLogger extends AuditEventLogger {
    val events: scala.collection.mutable.ArrayBuffer[AuditEvent] =
      scala.collection.mutable.ArrayBuffer.empty
    def log(event: AuditEvent): Either[AuditLogError, Unit] = {
      events += event; Right(())
    }
  }

  /** Always returns Left(WriteError) to test that logger failure is swallowed. */
  private object FailingAuditLogger extends AuditEventLogger {
    def log(event: AuditEvent): Either[AuditLogError, Unit] =
      Left(AuditLogError.WriteError("/audit", "disk full"))
  }

  // ---------------------------------------------------------------------------
  // Test 1 — success run produces one AuditStatus.Success event with correct fields
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "emit one AuditStatus.Success event with correct fields on a successful run" in {
    val df          = spark.range(10).toDF()
    val auditLogger = new RecordingAuditLogger
    val config      = testConfig(ConnectionType.File)
    val engine = new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new SucceedingConnector(df))),
      bronzeWriter = new SucceedingBronzeWriter(10),
      auditLogger  = auditLogger
    )

    val result = engine.run(config, spark)

    result.isRight shouldBe true

    auditLogger.events should have size 1
    val event = auditLogger.events.head

    event.status        shouldBe AuditStatus.Success
    event.runId         shouldBe result.toOption.get.runId
    event.recordsRead   shouldBe 10L
    event.recordsWritten shouldBe 10L
    event.errorMessage  shouldBe None
    event.sourceName    shouldBe config.metadata.sourceName
  }

  // ---------------------------------------------------------------------------
  // Test 2 — connector failure produces one AuditStatus.Failure event with errorMessage
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "emit one AuditStatus.Failure event with errorMessage on connector failure" in {
    val auditLogger = new RecordingAuditLogger
    val engine = new IngestionEngine(
      registry    = ConnectorRegistry(Map(ConnectionType.File -> new FailingConnector(ConnectorError("src", "timeout")))),
      auditLogger = auditLogger
    )

    val result = engine.run(testConfig(ConnectionType.File), spark)

    result.isLeft shouldBe true

    auditLogger.events should have size 1
    val event = auditLogger.events.head

    event.status             shouldBe AuditStatus.Failure
    event.errorMessage.isDefined shouldBe true
    event.errorMessage.get   should include("timeout")
  }

  // ---------------------------------------------------------------------------
  // Test 3 — audit logger failure does NOT mask or change the pipeline result
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "not mask a successful pipeline result when the audit logger fails" in {
    val df = spark.range(5).toDF()
    val engine = new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new SucceedingConnector(df))),
      bronzeWriter = new SucceedingBronzeWriter(5),
      auditLogger  = FailingAuditLogger
    )

    val result = engine.run(testConfig(ConnectionType.File), spark)

    result.isRight                        shouldBe true
    result.toOption.get.recordsWritten    shouldBe 5L
  }

  // ---------------------------------------------------------------------------
  // Test 4 — default NoOpAuditEventLogger is wired in transparently
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "work correctly when no auditLogger parameter is supplied (default NoOp)" in {
    val df = spark.range(1).toDF()
    val engine = new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new SucceedingConnector(df))),
      bronzeWriter = new SucceedingBronzeWriter(1)
    )

    val result = engine.run(testConfig(ConnectionType.File), spark)

    result.isRight shouldBe true
  }
}

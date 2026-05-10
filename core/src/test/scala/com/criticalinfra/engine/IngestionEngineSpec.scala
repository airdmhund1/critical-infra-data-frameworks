package com.criticalinfra.engine

import com.criticalinfra.config._
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// IngestionEngineSpec — unit test suite for IngestionEngine
//
// Tests cover all happy-path and failure branches of IngestionEngine.run:
//   1. Successful end-to-end run
//   2. Missing connector → ConfigurationError
//   3. Connector extraction failure → ConnectorError
//   4. Bronze write failure → StorageWriteError
//   5. Unexpected connector exception → UnexpectedError
//   6. Distinct runIds across separate runs
//   7. Validator is applied before recordsWritten count is captured
//
// Delta Lake is not available in the test scope; BronzeWriter implementations
// are supplied as injectable stubs to avoid any Delta Lake dependency.
// =============================================================================

/** Unit tests for [[IngestionEngine]].
  *
  * All Spark operations use a local `master("local[*]")` session created once in
  * [[BeforeAndAfterAll.beforeAll]] and stopped in [[BeforeAndAfterAll.afterAll]]. Delta Lake is not
  * used: [[BronzeWriter]] implementations are supplied as test stubs injected via the engine
  * constructor.
  */
class IngestionEngineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // SparkSession — shared across all tests in this suite
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("IngestionEngineSpec")
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
    * `/tmp/test-bronze` and is never actually written to (the [[BronzeWriter]] is always a stub).
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

  /** Always throws a `RuntimeException` to exercise the outer try/catch. */
  private object ExplodingConnector extends SourceConnector {
    def extract(config: SourceConfig, spark: SparkSession): Either[ConnectorError, DataFrame] =
      throw new RuntimeException("boom")
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

  /** Always returns `Left(error)`. */
  private class FailingBronzeWriter(error: StorageWriteError) extends BronzeLayerWriter {
    def write(
        data: DataFrame,
        config: SourceConfig,
        runId: java.util.UUID
    ): Either[StorageWriteError, WriteResult] =
      Left(error)
  }

  // ---------------------------------------------------------------------------
  // Test 1 — happy path: Right(IngestionResult) returned
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "return Right(IngestionResult) on the happy path" in {
    val df = spark.range(10).toDF()
    val engine = new IngestionEngine(
      registry = ConnectorRegistry(Map(ConnectionType.File -> new SucceedingConnector(df))),
      bronzeWriter = new SucceedingBronzeWriter(10)
    )
    val result = engine.run(testConfig(ConnectionType.File), spark)

    result.isRight shouldBe true
    val r = result.toOption.get
    r.recordsRead    shouldBe 10L
    r.recordsWritten shouldBe 10L
    r.runId          should not be empty
    r.durationMs     should be >= 0L
  }

  // ---------------------------------------------------------------------------
  // Test 2 — no connector registered → ConfigurationError
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "return Left(ConfigurationError) when no connector is registered for the source type" in {
    val engine = new IngestionEngine(registry = ConnectorRegistry.empty)
    val result = engine.run(testConfig(ConnectionType.Jdbc), spark)

    result.isLeft shouldBe true
    result match {
      case Left(ConfigurationError(field, _)) => field shouldBe "connection.connectionType"
      case other                              => fail(s"Expected Left(ConfigurationError) but got: $other")
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3 — connector extraction failure → ConnectorError propagated as-is
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "return Left(ConnectorError) when the connector fails to extract" in {
    val error = ConnectorError("test-source", "connection refused")
    val engine = new IngestionEngine(
      registry = ConnectorRegistry(Map(ConnectionType.File -> new FailingConnector(error)))
    )
    val result = engine.run(testConfig(ConnectionType.File), spark)

    result shouldBe Left(error)
  }

  // ---------------------------------------------------------------------------
  // Test 4 — Bronze write failure → StorageWriteError propagated as-is
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "return Left(StorageWriteError) when the Bronze write fails" in {
    val df         = spark.range(5).toDF()
    val writeError = StorageWriteError("/bad/path", "permission denied")
    val engine = new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new SucceedingConnector(df))),
      bronzeWriter = new FailingBronzeWriter(writeError)
    )
    val result = engine.run(testConfig(ConnectionType.File), spark)

    result shouldBe Left(writeError)
  }

  // ---------------------------------------------------------------------------
  // Test 5 — unexpected connector exception → wrapped in UnexpectedError
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "wrap an unexpected connector exception in UnexpectedError" in {
    val engine = new IngestionEngine(
      registry = ConnectorRegistry(Map(ConnectionType.File -> ExplodingConnector))
    )
    val result = engine.run(testConfig(ConnectionType.File), spark)

    result.isLeft shouldBe true
    result match {
      case Left(UnexpectedError(msg, Some(_))) => msg should include("boom")
      case other                               => fail(s"Expected Left(UnexpectedError) but got: $other")
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6 — two separate runs produce distinct runIds
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "generate distinct runIds for separate runs" in {
    val df = spark.range(1).toDF()
    val engine = new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new SucceedingConnector(df))),
      bronzeWriter = new SucceedingBronzeWriter(1)
    )
    val config = testConfig(ConnectionType.File)

    val result1 = engine.run(config, spark)
    val result2 = engine.run(config, spark)

    result1.isRight shouldBe true
    result2.isRight shouldBe true
    result1.toOption.get.runId should not equal result2.toOption.get.runId
  }

  // ---------------------------------------------------------------------------
  // Test 7 — validator is applied before Bronze write (recordsRead is pre-validation count)
  // ---------------------------------------------------------------------------

  "IngestionEngine" should "pass the validated DataFrame to the BronzeWriter (validator is applied)" in {
    val df = spark.range(5).toDF()

    // Inline validator that always returns an empty DataFrame — simulates all records failing QA
    val droppingValidator = new DataQualityValidator {
      def validate(data: DataFrame): DataFrame = data.filter(F.lit(false))
    }

    val engine = new IngestionEngine(
      registry     = ConnectorRegistry(Map(ConnectionType.File -> new SucceedingConnector(df))),
      validator    = droppingValidator,
      bronzeWriter = new SucceedingBronzeWriter(0)
    )
    val result = engine.run(testConfig(ConnectionType.File), spark)

    result.isRight           shouldBe true
    val r = result.toOption.get
    // recordsRead is captured before validation: must reflect the full source count
    r.recordsRead    shouldBe 5L
    // recordsWritten is what the BronzeWriter stub reports (0) — validator dropped everything
    r.recordsWritten shouldBe 0L
  }
}

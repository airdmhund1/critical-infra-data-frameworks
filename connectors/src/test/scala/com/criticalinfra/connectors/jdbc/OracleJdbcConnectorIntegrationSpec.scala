package com.criticalinfra.connectors.jdbc

import com.criticalinfra.config.{
  Audit,
  Connection,
  ConnectionType,
  Environment,
  Ingestion,
  IngestionMode,
  JdbcDriver,
  Metadata,
  Monitoring,
  QualityRules,
  Quarantine,
  SchemaEnforcement,
  Sector,
  SourceConfig,
  Storage,
  StorageFormat,
  StorageLayer
}
import com.criticalinfra.connectors.watermark.{IntegerWatermark, Watermark, WatermarkStore}
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager
import scala.collection.mutable

// =============================================================================
// OracleJdbcConnectorIntegrationSpec — Oracle integration test suite
//
// All tests are gated behind ORA_INTEGRATION=true via two mechanisms:
//   1. The sbt testOptions filter in build.sbt excludes the OraIntegration tag.
//   2. Each test body calls assume() as a belt-and-suspenders guard.
//
// Requires a running Oracle Database instance (19c+) and ojdbc11 on the JVM
// classpath. No Docker container is used. Connection parameters are read
// entirely from environment variables — no hardcoded values.
//
// Connection environment variables:
//   ORA_HOST         — Oracle DB hostname (required)
//   ORA_PORT         — Oracle listener port (default: 1521)
//   ORA_SERVICE      — Oracle service name (required)
//   ORA_USER         — DB username (required)
//   ORA_PASSWORD     — DB password (required)
//   ORA_WALLET_PATH  — Wallet directory path (optional; enables wallet-auth test)
//
// Test scenarios:
//   1. Full refresh returns all 5 rows.
//   2. Incremental with IntegerWatermark(2L) returns rows 3, 4, 5.
//   3. Parallel reads with partition options return correct row count.
//   4. Watermark persistence across two sequential incremental runs.
//   5. Wallet authentication (skipped when ORA_WALLET_PATH is absent).
// =============================================================================

/** Integration test suite for [[OracleJdbcConnector]] against a real Oracle Database instance.
  *
  * All tests are tagged [[OraIntegration]] and skipped during default CI runs. Set
  * `ORA_INTEGRATION=true` and the required Oracle connection variables to execute them.
  *
  * The `PIPELINE_TEST` table is created in `beforeAll()` with 5 initial rows and dropped in
  * `afterAll()`. Scenario 4 inserts rows 6 and 7 mid-suite to verify watermark persistence.
  */
class OracleJdbcConnectorIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Oracle connection parameters (all from environment variables)
  // ---------------------------------------------------------------------------

  private def oraHost: String    = sys.env.getOrElse("ORA_HOST", "localhost")
  private def oraPort: String    = sys.env.getOrElse("ORA_PORT", "1521")
  private def oraService: String = sys.env.getOrElse("ORA_SERVICE", "ORCL")
  private def oraUser: String    = sys.env.getOrElse("ORA_USER", "")
  private def oraPassword: String = sys.env.getOrElse("ORA_PASSWORD", "")

  private def jdbcUrl: String =
    s"jdbc:oracle:thin:@//$oraHost:$oraPort/$oraService"

  // ---------------------------------------------------------------------------
  // SparkSession state
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  // ---------------------------------------------------------------------------
  // JDBC connection used for DDL/DML in beforeAll/afterAll
  // ---------------------------------------------------------------------------

  private var adminConn: java.sql.Connection = _

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Guard: only perform setup when integration flag is set.
    // If ORA_INTEGRATION is not set the tests will individually assume() out, but
    // we must not attempt real DB connections here either.
    if (sys.env.getOrElse("ORA_INTEGRATION", "false") != "true") return

    // Load Oracle driver and open admin connection for DDL.
    Class.forName("oracle.jdbc.OracleDriver")
    adminConn = DriverManager.getConnection(jdbcUrl, oraUser, oraPassword)

    // Create PIPELINE_TEST table and insert 5 initial rows.
    seedDatabase()

    // Start SparkSession.
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("OracleJdbcConnectorIntegrationSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (sys.env.getOrElse("ORA_INTEGRATION", "false") == "true") {
        dropTable()
        if (adminConn != null && !adminConn.isClosed) adminConn.close()
      }
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Creates PIPELINE_TEST and inserts 5 rows with IDs 1–5. */
  private def seedDatabase(): Unit = {
    val stmt = adminConn.createStatement()
    try {
      stmt.execute("""
        CREATE TABLE PIPELINE_TEST (
          id         NUMBER(19)     NOT NULL PRIMARY KEY,
          name       VARCHAR2(50)   NOT NULL,
          amount     NUMBER(15,4)   NOT NULL,
          created_at TIMESTAMP      NOT NULL
        )
      """)
      stmt.execute("INSERT INTO PIPELINE_TEST VALUES (1, 'Alice',   1000.0000, TIMESTAMP '2024-01-15 09:00:00')")
      stmt.execute("INSERT INTO PIPELINE_TEST VALUES (2, 'Bob',     2000.0000, TIMESTAMP '2024-02-20 10:00:00')")
      stmt.execute("INSERT INTO PIPELINE_TEST VALUES (3, 'Carol',   3000.0000, TIMESTAMP '2024-03-25 11:00:00')")
      stmt.execute("INSERT INTO PIPELINE_TEST VALUES (4, 'Dave',    4000.0000, TIMESTAMP '2024-04-10 12:00:00')")
      stmt.execute("INSERT INTO PIPELINE_TEST VALUES (5, 'Eve',     5000.0000, TIMESTAMP '2024-05-05 13:00:00')")
      adminConn.commit()
    } finally stmt.close()
  }

  /** Inserts rows 6 and 7 for the watermark-persistence scenario. */
  private def insertExtraRows(): Unit = {
    val stmt = adminConn.createStatement()
    try {
      stmt.execute("INSERT INTO PIPELINE_TEST VALUES (6, 'Frank',   6000.0000, TIMESTAMP '2024-06-01 08:00:00')")
      stmt.execute("INSERT INTO PIPELINE_TEST VALUES (7, 'Grace',   7000.0000, TIMESTAMP '2024-07-04 09:00:00')")
      adminConn.commit()
    } finally stmt.close()
  }

  /** Drops the PIPELINE_TEST table, ignoring ORA-00942 (table does not exist). */
  private def dropTable(): Unit = {
    val stmt = adminConn.createStatement()
    try {
      stmt.execute("DROP TABLE PIPELINE_TEST")
    } catch {
      case ex: java.sql.SQLException if ex.getErrorCode == 942 => // ORA-00942: table or view does not exist
    } finally stmt.close()
  }

  // ---------------------------------------------------------------------------
  // In-memory WatermarkStore
  // ---------------------------------------------------------------------------

  private class InMemoryWatermarkStore extends WatermarkStore {
    private val store = mutable.Map.empty[String, Watermark]

    def read(sourceId: String): Either[ConnectorError, Option[Watermark]] =
      Right(store.get(sourceId))

    def write(sourceId: String, watermark: Watermark): Either[ConnectorError, Unit] = {
      store.put(sourceId, watermark)
      Right(())
    }

    /** Test-only helper: returns the currently stored watermark for `sourceId`. */
    def get(sourceId: String): Option[Watermark] = store.get(sourceId)
  }

  // ---------------------------------------------------------------------------
  // OracleTestConnector — overrides testConnection to supply credentials
  // ---------------------------------------------------------------------------

  /** Subclass of [[OracleJdbcConnector]] that overrides `testConnection` to supply
    * username and password via DriverManager.  The base-class `testConnection` calls
    * `DriverManager.getConnection(url)` with no credentials, which Oracle rejects.
    */
  private class OracleTestConnector(
      store: WatermarkStore,
      user: String,
      password: String,
      walletPath: Option[String] = None,
      partitionCol: Option[String] = None,
      partitionLower: Option[Long] = None,
      partitionUpper: Option[Long] = None
  ) extends OracleJdbcConnector(
        "PIPELINE_TEST",
        store,
        maxAttempts = 1,
        retryDelayFn = _ => (),
        walletPath = walletPath,
        partitionCol = partitionCol,
        partitionLower = partitionLower,
        partitionUpper = partitionUpper
      ) {
    override def testConnection(config: SourceConfig): Either[ConnectorError, Unit] =
      this.jdbcUrl(config).flatMap { url =>
        scala.util.Try {
          Class.forName(driverClass)
          val conn = java.sql.DriverManager.getConnection(url, user, password)
          conn.close()
        }.fold(
          ex => Left(ConnectorError(config.metadata.sourceId, ex.getMessage)),
          _  => Right(())
        )
      }
  }

  // ---------------------------------------------------------------------------
  // Helper: build SourceConfig
  // ---------------------------------------------------------------------------

  private def testConfig(
      mode: IngestionMode,
      incrementalColumn: Option[String] = None,
      watermarkStorage: Option[String] = None
  ): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "ora-integration-test",
        sourceName  = "Oracle Integration Test",
        sector      = Sector.FinancialServices,
        owner       = "test-team",
        environment = Environment.Dev
      ),
      connection = Connection(
        connectionType = ConnectionType.Jdbc,
        credentialsRef = "local://test",
        host           = sys.env.get("ORA_HOST"),
        port           = sys.env.get("ORA_PORT").map(_.toInt),
        database       = sys.env.get("ORA_SERVICE"),
        jdbcDriver     = Some(JdbcDriver.Oracle)
      ),
      ingestion = Ingestion(
        mode              = mode,
        incrementalColumn = incrementalColumn,
        watermarkStorage  = watermarkStorage,
        batchSize         = 1000,
        parallelism       = 2,
        timeout           = 30
      ),
      schemaEnforcement = SchemaEnforcement(enabled = false),
      qualityRules      = QualityRules(enabled = false),
      quarantine        = Quarantine(enabled = false),
      storage = Storage(
        layer  = StorageLayer.Bronze,
        format = StorageFormat.Delta,
        path   = "/tmp/test-bronze-ora"
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // Scenario 1 — Full refresh returns all 5 rows
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector integration" should
    "return all 5 rows in Full refresh mode against a real Oracle instance" taggedAs OraIntegration in {
    assume(sys.env.getOrElse("ORA_INTEGRATION", "false") == "true")

    val store     = new InMemoryWatermarkStore
    val connector = new OracleTestConnector(store, oraUser, oraPassword)
    val config    = testConfig(mode = IngestionMode.Full)

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(5L)
  }

  // ---------------------------------------------------------------------------
  // Scenario 2 — Incremental with IntegerWatermark(2L) returns rows 3, 4, 5
  // ---------------------------------------------------------------------------

  it should "return only rows with id > 2 when IntegerWatermark(2L) is present" taggedAs OraIntegration in {
    assume(sys.env.getOrElse("ORA_INTEGRATION", "false") == "true")

    val store = new InMemoryWatermarkStore
    store.write("ora-integration-test", IntegerWatermark(2L))

    val connector = new OracleTestConnector(store, oraUser, oraPassword)
    val config = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/wm-ora")
    )

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(3L)
  }

  // ---------------------------------------------------------------------------
  // Scenario 3 — Parallel reads with partition options return correct row count
  // ---------------------------------------------------------------------------

  it should "return all 5 rows when parallel-read partition options are provided" taggedAs OraIntegration in {
    assume(sys.env.getOrElse("ORA_INTEGRATION", "false") == "true")

    val store = new InMemoryWatermarkStore
    val connector = new OracleTestConnector(
      store,
      oraUser,
      oraPassword,
      partitionCol   = Some("id"),
      partitionLower = Some(1L),
      partitionUpper = Some(5L)
    )
    val config = testConfig(mode = IngestionMode.Full)

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(5L)
  }

  // ---------------------------------------------------------------------------
  // Scenario 4 — Watermark persistence across two sequential incremental runs
  // ---------------------------------------------------------------------------

  it should "persist watermarks correctly across two incremental runs" taggedAs OraIntegration in {
    assume(sys.env.getOrElse("ORA_INTEGRATION", "false") == "true")

    val store     = new InMemoryWatermarkStore
    val connector = new OracleTestConnector(store, oraUser, oraPassword)
    val config = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/wm-ora")
    )

    // --- Run 1: store is empty so all 5 rows are read --------------------------
    val df1 = connector.extract(config, spark).toOption.get
    df1.count() shouldBe 5L
    connector.persistWatermark(config, df1)
    store.get("ora-integration-test") shouldBe Some(IntegerWatermark(5L))

    // --- Insert rows 6 and 7 mid-test ------------------------------------------
    insertExtraRows()

    // --- Run 2: watermark is IntegerWatermark(5L) so only rows 6 and 7 appear --
    val df2 = connector.extract(config, spark).toOption.get
    df2.count() shouldBe 2L
    val ids = df2.select("id").collect().map(_.getLong(0)).toSet
    ids shouldBe Set(6L, 7L)
    connector.persistWatermark(config, df2)
    store.get("ora-integration-test") shouldBe Some(IntegerWatermark(7L))
  }

  // ---------------------------------------------------------------------------
  // Scenario 5 — Wallet authentication (skipped when ORA_WALLET_PATH is absent)
  // ---------------------------------------------------------------------------

  it should "successfully connect and extract using Oracle Wallet authentication" taggedAs OraIntegration in {
    assume(sys.env.getOrElse("ORA_INTEGRATION", "false") == "true")
    assume(sys.env.contains("ORA_WALLET_PATH"))

    val store = new InMemoryWatermarkStore
    val connector = new OracleTestConnector(
      store,
      oraUser,
      oraPassword,
      walletPath = sys.env.get("ORA_WALLET_PATH")
    )
    val config = testConfig(mode = IngestionMode.Full)

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
  }
}

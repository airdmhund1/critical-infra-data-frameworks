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
import org.apache.spark.sql.functions.min
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.sql.DriverManager
import java.time.Duration
import scala.collection.mutable

// =============================================================================
// JdbcConnectorBaseIntegrationSpec — Testcontainers Postgres integration tests
//
// Exercises JdbcConnectorBase against a real PostgreSQL instance managed by
// Testcontainers. The container is started once in beforeAll() and shared
// across all four test scenarios. Uses the raw Java Testcontainers API
// (GenericContainer) exactly as established in VaultSecretsResolverIntegrationSpec.
//
// Test scenarios:
//   1. Full refresh returns all 5 rows.
//   2. Incremental mode filters by existing IntegerWatermark.
//   3. Parallel reads with partition options return correct row count.
//   4. Watermark persistence across two incremental runs.
// =============================================================================

/** Testcontainers Postgres integration test suite for [[JdbcConnectorBase]].
  *
  * Requires Docker to be running and the `postgres:16` image to be pullable from Docker Hub.
  *
  * The container is started once in `beforeAll()` and stopped in `afterAll()`. All test scenarios
  * share the same container and SparkSession. Additional rows are inserted mid-suite in scenario 4
  * to verify watermark-based filtering across two sequential runs.
  */
class JdbcConnectorBaseIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // PostgreSQL container state
  // ---------------------------------------------------------------------------

  private val pgUser     = "testuser"
  private val pgPassword = "testpass"
  private val pgDatabase = "testdb"

  private var pgContainer: GenericContainer[_] = _
  private var baseJdbcUrl: String              = _
  private var jdbcUrlWithCreds: String         = _

  // ---------------------------------------------------------------------------
  // SparkSession state
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  override def beforeAll(): Unit = {
    super.beforeAll()

    // -- Start Postgres container ------------------------------------------------
    pgContainer = new GenericContainer("postgres:16")
    pgContainer.withEnv("POSTGRES_USER", pgUser)
    pgContainer.withEnv("POSTGRES_PASSWORD", pgPassword)
    pgContainer.withEnv("POSTGRES_DB", pgDatabase)
    pgContainer.withExposedPorts(5432)
    pgContainer.waitingFor(
      Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60))
    )
    pgContainer.start()

    baseJdbcUrl = s"jdbc:postgresql://${pgContainer.getHost}:${pgContainer.getMappedPort(5432)}/$pgDatabase"
    jdbcUrlWithCreds = s"$baseJdbcUrl?user=$pgUser&password=$pgPassword"

    // -- Create SparkSession ----------------------------------------------------
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("JdbcConnectorBaseIntegrationSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // -- Seed the database ------------------------------------------------------
    seedDatabase()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      if (pgContainer != null) pgContainer.stop()
      super.afterAll()
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Seeds the `trades` table with 5 initial rows via DriverManager. */
  private def seedDatabase(): Unit = {
    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(jdbcUrlWithCreds)
    try {
      val stmt = conn.createStatement()
      stmt.execute("""
        CREATE TABLE trades (
          id        BIGINT PRIMARY KEY,
          symbol    VARCHAR(10) NOT NULL,
          amount    DOUBLE PRECISION NOT NULL,
          traded_at TIMESTAMP NOT NULL
        )
      """)
      stmt.execute("INSERT INTO trades VALUES (1, 'AAPL', 100.0, '2024-01-01 10:00:00')")
      stmt.execute("INSERT INTO trades VALUES (2, 'MSFT', 200.0, '2024-02-01 10:00:00')")
      stmt.execute("INSERT INTO trades VALUES (3, 'GOOG', 300.0, '2024-03-01 10:00:00')")
      stmt.execute("INSERT INTO trades VALUES (4, 'AMZN', 400.0, '2024-04-01 10:00:00')")
      stmt.execute("INSERT INTO trades VALUES (5, 'TSLA', 500.0, '2024-05-01 10:00:00')")
      stmt.close()
    } finally conn.close()
  }

  /** Inserts two additional rows needed for the watermark-persistence scenario. */
  private def insertExtraRows(): Unit = {
    val conn = DriverManager.getConnection(jdbcUrlWithCreds)
    try {
      val stmt = conn.createStatement()
      stmt.execute("INSERT INTO trades VALUES (6, 'NVDA', 600.0, '2024-06-01 10:00:00')")
      stmt.execute("INSERT INTO trades VALUES (7, 'META', 700.0, '2024-07-01 10:00:00')")
      stmt.close()
    } finally conn.close()
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
  // Concrete PostgreSQL test connector subclasses
  // ---------------------------------------------------------------------------

  /** Base Postgres connector. Embeds credentials in the JDBC URL so that both
    * [[JdbcConnectorBase.testConnection]] (DriverManager) and Spark JDBC reads
    * authenticate successfully without requiring changes to the base class.
    */
  private class PostgresTestConnector(
      store: WatermarkStore,
      url: String,
      user: String,
      password: String
  ) extends JdbcConnectorBase(store, maxAttempts = 1, retryDelayFn = _ => ()) {
    override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] =
      Right(s"$url?user=$user&password=$password")
    override protected def driverClass: String                     = "org.postgresql.Driver"
    override protected def tableName(config: SourceConfig): String = "trades"
  }

  /** Postgres connector that enables parallel reads partitioned on the `id` column. */
  private class PostgresPartitionedConnector(
      store: WatermarkStore,
      url: String,
      user: String,
      password: String
  ) extends PostgresTestConnector(store, url, user, password) {
    override protected def partitionColumn(config: SourceConfig): Option[String] = Some("id")
    override protected def partitionLowerBound(config: SourceConfig): Option[Long] = Some(1L)
    override protected def partitionUpperBound(config: SourceConfig): Option[Long] = Some(5L)
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
        sourceId    = "integration-test-trades",
        sourceName  = "Integration Test Trades",
        sector      = Sector.FinancialServices,
        owner       = "test-team",
        environment = Environment.Dev
      ),
      connection = Connection(
        connectionType = ConnectionType.Jdbc,
        credentialsRef = "local://test",
        host           = Some(pgContainer.getHost),
        port           = Some(pgContainer.getMappedPort(5432)),
        database       = Some(pgDatabase),
        jdbcDriver     = Some(JdbcDriver.Postgres)
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
        path   = "/tmp/test-bronze"
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // Scenario 1 — Full refresh returns all 5 rows
  // ---------------------------------------------------------------------------

  "JdbcConnectorBase integration" should
    "return all 5 rows in Full refresh mode against a real Postgres instance" in {
    val store     = new InMemoryWatermarkStore
    val connector = new PostgresTestConnector(store, baseJdbcUrl, pgUser, pgPassword)
    val config    = testConfig(mode = IngestionMode.Full)

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(5L)
  }

  // ---------------------------------------------------------------------------
  // Scenario 2 — Incremental mode filters by integer watermark
  // ---------------------------------------------------------------------------

  it should "return only rows with id > 2 when IntegerWatermark(2L) is present" in {
    val store = new InMemoryWatermarkStore
    store.write("integration-test-trades", IntegerWatermark(2L))

    val connector = new PostgresTestConnector(store, baseJdbcUrl, pgUser, pgPassword)
    val config = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/wm")
    )

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(3L)
    result.map(df => df.agg(min("id")).first().getLong(0)) shouldBe Right(3L)
  }

  // ---------------------------------------------------------------------------
  // Scenario 3 — Parallel reads with partition options return correct row count
  // ---------------------------------------------------------------------------

  it should "return all 5 rows when parallel-read partition options are provided" in {
    val store     = new InMemoryWatermarkStore
    val connector = new PostgresPartitionedConnector(store, baseJdbcUrl, pgUser, pgPassword)
    val config    = testConfig(mode = IngestionMode.Full)

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(5L)
  }

  // ---------------------------------------------------------------------------
  // Scenario 4 — Watermark persistence across two sequential runs
  // ---------------------------------------------------------------------------

  it should "persist watermarks correctly across two incremental runs" in {
    val store     = new InMemoryWatermarkStore
    val connector = new PostgresTestConnector(store, baseJdbcUrl, pgUser, pgPassword)
    val config = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/wm")
    )

    // --- Run 1: store is empty so all 5 rows are read --------------------------
    val df1 = connector.extract(config, spark).toOption.get
    df1.count() shouldBe 5L
    connector.persistWatermark(config, df1)
    store.get("integration-test-trades") shouldBe Some(IntegerWatermark(5L))

    // --- Insert rows 6 and 7 mid-test ------------------------------------------
    insertExtraRows()

    // --- Run 2: watermark is IntegerWatermark(5L) so only rows 6 and 7 appear --
    val df2 = connector.extract(config, spark).toOption.get
    df2.count() shouldBe 2L
    val ids = df2.select("id").collect().map(_.getLong(0)).toSet
    ids shouldBe Set(6L, 7L)
    connector.persistWatermark(config, df2)
    store.get("integration-test-trades") shouldBe Some(IntegerWatermark(7L))
  }
}

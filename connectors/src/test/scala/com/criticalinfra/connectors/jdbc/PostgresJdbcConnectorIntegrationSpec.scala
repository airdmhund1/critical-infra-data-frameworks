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
// PostgresJdbcConnectorIntegrationSpec — Testcontainers integration tests
//
// Exercises PostgresJdbcConnector against a real PostgreSQL instance managed by
// Testcontainers. The container is started once in beforeAll() and shared
// across all four test scenarios. Uses the raw Java Testcontainers API
// (GenericContainer) exactly as established in JdbcConnectorBaseIntegrationSpec.
//
// Test scenarios:
//   1. Full refresh returns all 5 rows.
//   2. Incremental mode filters by existing IntegerWatermark.
//   3. Parallel reads with partition options return correct row count.
//   4. Watermark persistence across two sequential incremental runs.
// =============================================================================

/** Testcontainers integration test suite for [[PostgresJdbcConnector]].
  *
  * Requires Docker to be running and the `postgres:16` image to be pullable from Docker Hub.
  *
  * The container is started once in `beforeAll()` and stopped in `afterAll()`. All test scenarios
  * share the same container and SparkSession. Additional rows are inserted mid-suite in scenario 4
  * to verify watermark-based filtering across two sequential runs.
  *
  * Runs unconditionally in standard CI — no environment flags or `assume()` guards.
  */
class PostgresJdbcConnectorIntegrationSpec
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

    baseJdbcUrl    = s"jdbc:postgresql://${pgContainer.getHost}:${pgContainer.getMappedPort(5432)}/$pgDatabase"
    jdbcUrlWithCreds = s"$baseJdbcUrl?user=$pgUser&password=$pgPassword"

    // -- Create SparkSession ----------------------------------------------------
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresJdbcConnectorIntegrationSpec")
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

  /** Seeds the `meter_readings` table with 5 initial rows via DriverManager. */
  private def seedDatabase(): Unit = {
    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(jdbcUrlWithCreds)
    try {
      val stmt = conn.createStatement()
      stmt.execute("""
        CREATE TABLE meter_readings (
          id          BIGINT PRIMARY KEY,
          meter_id    VARCHAR(20) NOT NULL,
          reading_kwh DOUBLE PRECISION NOT NULL,
          recorded_at TIMESTAMP NOT NULL
        )
      """)
      stmt.execute("INSERT INTO meter_readings VALUES (1, 'METER-001', 10.5, '2024-01-01 08:00:00')")
      stmt.execute("INSERT INTO meter_readings VALUES (2, 'METER-002', 20.5, '2024-02-01 08:00:00')")
      stmt.execute("INSERT INTO meter_readings VALUES (3, 'METER-003', 30.5, '2024-03-01 08:00:00')")
      stmt.execute("INSERT INTO meter_readings VALUES (4, 'METER-004', 40.5, '2024-04-01 08:00:00')")
      stmt.execute("INSERT INTO meter_readings VALUES (5, 'METER-005', 50.5, '2024-05-01 08:00:00')")
      stmt.close()
    } finally conn.close()
  }

  /** Inserts two additional rows needed for the watermark-persistence scenario. */
  private def insertExtraRows(): Unit = {
    val conn = DriverManager.getConnection(jdbcUrlWithCreds)
    try {
      val stmt = conn.createStatement()
      stmt.execute("INSERT INTO meter_readings VALUES (6, 'METER-006', 60.5, '2024-06-01 08:00:00')")
      stmt.execute("INSERT INTO meter_readings VALUES (7, 'METER-007', 70.5, '2024-07-01 08:00:00')")
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
  // Concrete PostgresJdbcConnector test subclasses
  // ---------------------------------------------------------------------------

  /** Test connector that overrides `jdbcUrl` to embed credentials as query params.
    *
    * [[PostgresJdbcConnector.testConnection]] calls `DriverManager.getConnection(url)` with a
    * single-arg form, which requires credentials to be present in the URL itself. Embedding
    * `?user=...&password=...` satisfies both DriverManager and Spark's JDBC reader without
    * modifying the base connector.
    */
  private class PostgresTestConnector(
      store: WatermarkStore,
      url: String,
      user: String,
      password: String,
      sslMode: PostgresJdbcConnector.SslMode = PostgresJdbcConnector.SslMode.Disable,
      partitionCol: Option[String] = None,
      partitionLower: Option[Long] = None,
      partitionUpper: Option[Long] = None
  ) extends PostgresJdbcConnector(
        "meter_readings",
        store,
        maxAttempts = 1,
        retryDelayFn = _ => (),
        sslMode = sslMode,
        partitionCol = partitionCol,
        partitionLower = partitionLower,
        partitionUpper = partitionUpper
      ) {
    override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] =
      Right(s"$url?user=$user&password=$password")
  }

  /** Connector that enables parallel reads partitioned on the `id` column. */
  private class PostgresPartitionedConnector(
      store: WatermarkStore,
      url: String,
      user: String,
      password: String
  ) extends PostgresTestConnector(
        store,
        url,
        user,
        password,
        partitionCol = Some("id"),
        partitionLower = Some(1L),
        partitionUpper = Some(5L)
      )

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
        sourceId    = "integration-test-meter-readings",
        sourceName  = "Integration Test Meter Readings",
        sector      = Sector.Energy,
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
        path   = "/tmp/test-bronze-pg"
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // Scenario 1 — Full refresh returns all 5 rows
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector integration" should
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
    store.write("integration-test-meter-readings", IntegerWatermark(2L))

    val connector = new PostgresTestConnector(store, baseJdbcUrl, pgUser, pgPassword)
    val config = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/wm-pg")
    )

    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(3L)
    result.map(df => df.agg(min("id")).first().getLong(0)) shouldBe Right(3L)
  }

  // ---------------------------------------------------------------------------
  // Scenario 3 — Parallel reads return correct row count
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
  // Scenario 4 — Watermark persistence across two incremental runs
  // ---------------------------------------------------------------------------

  it should "persist watermarks correctly across two incremental runs" in {
    val store     = new InMemoryWatermarkStore
    val connector = new PostgresTestConnector(store, baseJdbcUrl, pgUser, pgPassword)
    val config = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/wm-pg")
    )

    // --- Run 1: store is empty so all 5 rows are read --------------------------
    val df1 = connector.extract(config, spark).toOption.get
    df1.count() shouldBe 5L
    connector.persistWatermark(config, df1)
    store.get("integration-test-meter-readings") shouldBe Some(IntegerWatermark(5L))

    // --- Insert rows 6 and 7 mid-test ------------------------------------------
    insertExtraRows()

    // --- Run 2: watermark is IntegerWatermark(5L) so only rows 6 and 7 appear --
    val df2 = connector.extract(config, spark).toOption.get
    df2.count() shouldBe 2L
    val ids = df2.select("id").collect().map(_.getLong(0)).toSet
    ids shouldBe Set(6L, 7L)
    connector.persistWatermark(config, df2)
    store.get("integration-test-meter-readings") shouldBe Some(IntegerWatermark(7L))
  }
}

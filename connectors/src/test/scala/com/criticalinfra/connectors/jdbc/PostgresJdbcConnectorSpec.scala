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
import com.criticalinfra.connectors.watermark.{Watermark, WatermarkStore}
import com.criticalinfra.connectors.watermark.IntegerWatermark
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager
import scala.collection.mutable

// =============================================================================
// PostgresJdbcConnectorSpec — unit tests for PostgresJdbcConnector
//
// All tests pass without a running PostgreSQL instance.
// Driver-loading and real network calls are bypassed via TestablePostgresConnector
// which overrides testConnection to return Right(()) unconditionally.
//
// H2-backed extraction tests (tests 13–15) use a second test double that
// overrides jdbcUrl and driverClass to route Spark JDBC reads through H2.
// =============================================================================

class PostgresJdbcConnectorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  import PostgresJdbcConnector.SslMode

  // ---------------------------------------------------------------------------
  // H2 constants
  // ---------------------------------------------------------------------------

  private val H2Url    = "jdbc:h2:mem:pgtest;DB_CLOSE_DELAY=-1"
  private val H2Driver = "org.h2.Driver"

  // ---------------------------------------------------------------------------
  // In-memory WatermarkStore test double
  // ---------------------------------------------------------------------------

  private class InMemoryWatermarkStore extends WatermarkStore {
    private val store = mutable.Map.empty[String, Watermark]

    def read(sourceId: String): Either[ConnectorError, Option[Watermark]] =
      Right(store.get(sourceId))

    def write(sourceId: String, watermark: Watermark): Either[ConnectorError, Unit] = {
      store.put(sourceId, watermark)
      Right(())
    }
  }

  // ---------------------------------------------------------------------------
  // Test doubles
  // ---------------------------------------------------------------------------

  /** Subclass of PostgresJdbcConnector that bypasses driver loading.
    *
    * Overrides `testConnection` to return `Right(())` immediately so no real
    * PostgreSQL instance is required. Protected methods are exposed via thin
    * wrappers for direct assertion in tests.
    */
  private class TestablePostgresConnector(
      table: String,
      store: WatermarkStore,
      sslMode: SslMode = SslMode.Require,
      partitionCol: Option[String] = None,
      partitionLower: Option[Long] = None,
      partitionUpper: Option[Long] = None
  ) extends PostgresJdbcConnector(
        table,
        store,
        maxAttempts = 1,
        retryDelayFn = _ => (),
        sslMode = sslMode,
        partitionCol = partitionCol,
        partitionLower = partitionLower,
        partitionUpper = partitionUpper
      ) {

    // Skip PostgreSQL driver loading entirely
    override def testConnection(config: SourceConfig): Either[ConnectorError, Unit] = Right(())

    // Expose protected methods for direct testing
    def testJdbcUrl(config: SourceConfig): Either[ConnectorError, String]  = jdbcUrl(config)
    def testDriverClass: String                                             = driverClass
    def testTableName(config: SourceConfig): String                        = tableName(config)
    def testExtraOpts(config: SourceConfig): Map[String, String]           = extraJdbcOptions(config)
    def testPartitionColumn(config: SourceConfig): Option[String]          = partitionColumn(config)
    def testPartitionLower(config: SourceConfig): Option[Long]             = partitionLowerBound(config)
    def testPartitionUpper(config: SourceConfig): Option[Long]             = partitionUpperBound(config)
  }

  /** H2-backed test double that routes Spark JDBC reads through H2 instead of PostgreSQL.
    *
    * Overrides `testConnection` (to bypass driver loading) and `jdbcUrl` / `driverClass`
    * (to redirect reads to the local H2 in-memory database created in `beforeAll`).
    */
  private class H2BackedPostgresConnector(store: WatermarkStore)
      extends TestablePostgresConnector("pg_test_table", store) {
    override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] =
      Right(H2Url)
    override protected def driverClass: String = H2Driver
  }

  // ---------------------------------------------------------------------------
  // SparkSession lifecycle
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresJdbcConnectorSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Create and populate H2 table used by the H2-backed extraction tests
    Class.forName(H2Driver)
    val conn = DriverManager.getConnection(H2Url)
    try {
      val stmt = conn.createStatement()
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS pg_test_table (
          id     BIGINT PRIMARY KEY,
          name   VARCHAR(100),
          amount DOUBLE
        )
      """)
      stmt.execute("DELETE FROM pg_test_table")
      stmt.execute("INSERT INTO pg_test_table VALUES (1, 'Alpha',   1000.0)")
      stmt.execute("INSERT INTO pg_test_table VALUES (2, 'Beta',    2000.0)")
      stmt.execute("INSERT INTO pg_test_table VALUES (3, 'Gamma',   3000.0)")
      stmt.execute("INSERT INTO pg_test_table VALUES (4, 'Delta',   4000.0)")
      stmt.execute("INSERT INTO pg_test_table VALUES (5, 'Epsilon', 5000.0)")
      stmt.close()
    }
    finally conn.close()
  }

  override def afterAll(): Unit =
    try {
      val conn = DriverManager.getConnection(H2Url)
      try {
        val stmt = conn.createStatement()
        stmt.execute("DROP TABLE IF EXISTS pg_test_table")
        stmt.close()
      }
      finally conn.close()
    }
    finally {
      if (spark != null) spark.stop()
      super.afterAll()
    }

  // ---------------------------------------------------------------------------
  // Helper: build a minimal SourceConfig for tests
  // ---------------------------------------------------------------------------

  private def testConfig(
      host: Option[String] = Some("localhost"),
      port: Option[Int] = Some(5432),
      database: Option[String] = Some("mydb"),
      mode: IngestionMode = IngestionMode.Full,
      incrementalColumn: Option[String] = None,
      watermarkStorage: Option[String] = None
  ): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "pg-source",
        sourceName  = "Postgres Test Source",
        sector      = Sector.FinancialServices,
        owner       = "test-team",
        environment = Environment.Dev
      ),
      connection = Connection(
        connectionType = ConnectionType.Jdbc,
        credentialsRef = "vault/test/postgres",
        host           = host,
        port           = port,
        database       = database,
        jdbcDriver     = Some(JdbcDriver.Postgres)
      ),
      ingestion = Ingestion(
        mode              = mode,
        incrementalColumn = incrementalColumn,
        watermarkStorage  = watermarkStorage,
        batchSize         = 500,
        parallelism       = 2,
        timeout           = 60
      ),
      schemaEnforcement = SchemaEnforcement(enabled = false),
      qualityRules      = QualityRules(enabled = false),
      quarantine        = Quarantine(enabled = false),
      storage = Storage(
        layer  = StorageLayer.Bronze,
        format = StorageFormat.Delta,
        path   = "/tmp/test-pg-bronze"
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // 1. jdbcUrl — happy path
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector.jdbcUrl" should
    "return the correct URL when host, port, and database are present" in {
      val store     = new InMemoryWatermarkStore
      val connector = new TestablePostgresConnector("public.trades", store)
      val config    = testConfig()
      connector.testJdbcUrl(config) shouldBe Right("jdbc:postgresql://localhost:5432/mydb")
    }

  // ---------------------------------------------------------------------------
  // 2. jdbcUrl — missing host
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when host is absent" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("public.trades", store)
    val config    = testConfig(host = None)
    val result    = connector.testJdbcUrl(config)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("pg-source")
  }

  // ---------------------------------------------------------------------------
  // 3. jdbcUrl — missing port
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when port is absent" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("public.trades", store)
    val config    = testConfig(port = None)
    val result    = connector.testJdbcUrl(config)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("pg-source")
  }

  // ---------------------------------------------------------------------------
  // 4. jdbcUrl — missing database
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when database is absent" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("public.trades", store)
    val config    = testConfig(database = None)
    val result    = connector.testJdbcUrl(config)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("pg-source")
  }

  // ---------------------------------------------------------------------------
  // 5. driverClass
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector.driverClass" should "return exactly org.postgresql.Driver" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("public.trades", store)
    connector.testDriverClass shouldBe "org.postgresql.Driver"
  }

  // ---------------------------------------------------------------------------
  // 6. tableName
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector.tableName" should "return the table name supplied at construction" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("energy.meter_readings", store)
    val config    = testConfig()
    connector.testTableName(config) shouldBe "energy.meter_readings"
  }

  // ---------------------------------------------------------------------------
  // 7. extraJdbcOptions — SslMode.Disable
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector.extraJdbcOptions" should
    "return sslmode=disable for SslMode.Disable" in {
      val store     = new InMemoryWatermarkStore
      val connector = new TestablePostgresConnector("t", store, sslMode = SslMode.Disable)
      val config    = testConfig()
      connector.testExtraOpts(config) shouldBe Map("sslmode" -> "disable")
    }

  // ---------------------------------------------------------------------------
  // 8. extraJdbcOptions — SslMode.Require (default)
  // ---------------------------------------------------------------------------

  it should "return sslmode=require for SslMode.Require (the default)" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("t", store, sslMode = SslMode.Require)
    val config    = testConfig()
    connector.testExtraOpts(config) shouldBe Map("sslmode" -> "require")
  }

  // ---------------------------------------------------------------------------
  // 9. extraJdbcOptions — SslMode.VerifyCa
  // ---------------------------------------------------------------------------

  it should "return sslmode=verify-ca for SslMode.VerifyCa" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("t", store, sslMode = SslMode.VerifyCa)
    val config    = testConfig()
    connector.testExtraOpts(config) shouldBe Map("sslmode" -> "verify-ca")
  }

  // ---------------------------------------------------------------------------
  // 10. extraJdbcOptions — SslMode.VerifyFull
  // ---------------------------------------------------------------------------

  it should "return sslmode=verify-full for SslMode.VerifyFull" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("t", store, sslMode = SslMode.VerifyFull)
    val config    = testConfig()
    connector.testExtraOpts(config) shouldBe Map("sslmode" -> "verify-full")
  }

  // ---------------------------------------------------------------------------
  // 11. partitionColumn / partitionLower / partitionUpper — all None (defaults)
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector.partitionColumn" should "return None when no partitionCol is set" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("t", store)
    val config    = testConfig()
    connector.testPartitionColumn(config) shouldBe None
    connector.testPartitionLower(config) shouldBe None
    connector.testPartitionUpper(config) shouldBe None
  }

  // ---------------------------------------------------------------------------
  // 12. partitionColumn / partitionLower / partitionUpper — constructor params set
  // ---------------------------------------------------------------------------

  it should "return Some values when constructor partition params are supplied" in {
    val store = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector(
      "t",
      store,
      partitionCol   = Some("id"),
      partitionLower = Some(1L),
      partitionUpper = Some(999L)
    )
    val config = testConfig()
    connector.testPartitionColumn(config) shouldBe Some("id")
    connector.testPartitionLower(config) shouldBe Some(1L)
    connector.testPartitionUpper(config) shouldBe Some(999L)
  }

  // ---------------------------------------------------------------------------
  // 13. partitionLowerBound and partitionUpperBound delegate correctly
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector.partitionLowerBound" should "return the constructor-supplied lower bound" in {
    val store = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector(
      "t",
      store,
      partitionCol   = Some("amount_id"),
      partitionLower = Some(100L),
      partitionUpper = Some(5000L)
    )
    val config = testConfig()
    connector.testPartitionLower(config) shouldBe Some(100L)
    connector.testPartitionUpper(config) shouldBe Some(5000L)
  }

  // ---------------------------------------------------------------------------
  // 14. extract full-refresh — succeeds with H2 backing, correct row count
  // ---------------------------------------------------------------------------

  "PostgresJdbcConnector.extract" should
    "return Right(DataFrame) with correct row count using H2 backing" in {
      val store     = new InMemoryWatermarkStore
      val connector = new H2BackedPostgresConnector(store)
      val config    = testConfig()
      val result    = connector.extract(config, spark)
      result shouldBe a[Right[_, _]]
      result.map(_.count()) shouldBe Right(5L)
    }

  // ---------------------------------------------------------------------------
  // 15. extract incremental — watermark filters correctly, returns only newer rows
  // ---------------------------------------------------------------------------

  it should "return only rows newer than the pre-seeded IntegerWatermark in incremental mode" in {
    val store = new InMemoryWatermarkStore
    // Pre-seed: rows with id > 3 should be returned
    store.write("pg-source", IntegerWatermark(3L))

    val connector = new H2BackedPostgresConnector(store)
    val config = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/pg-watermarks")
    )
    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(2L)
  }

  // ---------------------------------------------------------------------------
  // 16. extract — Left when jdbcUrl returns Left (missing host)
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when jdbcUrl returns Left due to missing host" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestablePostgresConnector("public.trades", store)
    val config    = testConfig(host = None)
    val result    = connector.extract(config, spark)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("pg-source")
  }
}

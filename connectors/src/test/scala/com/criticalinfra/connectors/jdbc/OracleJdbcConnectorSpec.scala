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
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager
import scala.collection.mutable

// =============================================================================
// OracleJdbcConnectorSpec — unit tests for OracleJdbcConnector
//
// All tests pass without oracle.jdbc.OracleDriver on the classpath.
// Driver-loading is bypassed via TestableOracleConnector which overrides
// testConnection to return Right(()) unconditionally.
//
// The H2-backed extraction test (test 12) uses a second test double that also
// overrides jdbcUrl and driverClass to route Spark JDBC reads through H2.
// =============================================================================

class OracleJdbcConnectorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // H2 constants
  // ---------------------------------------------------------------------------

  private val H2Url    = "jdbc:h2:mem:oracletest;DB_CLOSE_DELAY=-1"
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

  /** Subclass of OracleJdbcConnector that bypasses Oracle driver loading.
    *
    * Overrides `testConnection` to return `Right(())` immediately — the Oracle
    * driver jar (ojdbc11) is not required on the test classpath. Protected methods
    * are exposed via thin wrappers for direct assertion in tests.
    */
  private class TestableOracleConnector(
      table: String,
      store: WatermarkStore,
      walletPath: Option[String] = None,
      partitionCol: Option[String] = None,
      partitionLower: Option[Long] = None,
      partitionUpper: Option[Long] = None
  ) extends OracleJdbcConnector(
        table,
        store,
        maxAttempts = 1,
        retryDelayFn = _ => (),
        walletPath = walletPath,
        partitionCol = partitionCol,
        partitionLower = partitionLower,
        partitionUpper = partitionUpper
      ) {

    // Skip Oracle driver loading entirely — no ojdbc11 on classpath
    override def testConnection(config: SourceConfig): Either[ConnectorError, Unit] = Right(())

    // Expose protected methods for direct testing
    def exposedJdbcUrl(config: SourceConfig): Either[ConnectorError, String] = jdbcUrl(config)
    def exposedTableName(config: SourceConfig): String                       = tableName(config)
    def exposedDriverClass: String                                            = driverClass
    def exposedExtraJdbcOptions(config: SourceConfig): Map[String, String]   = extraJdbcOptions(config)
    def exposedPartitionColumn(config: SourceConfig): Option[String]         = partitionColumn(config)
    def exposedPartitionLowerBound(config: SourceConfig): Option[Long]       = partitionLowerBound(config)
    def exposedPartitionUpperBound(config: SourceConfig): Option[Long]       = partitionUpperBound(config)
  }

  /** H2-backed test double that routes Spark JDBC reads through H2 instead of Oracle.
    *
    * Overrides both `testConnection` (to bypass driver loading) and `jdbcUrl` / `driverClass`
    * (to redirect reads to the local H2 in-memory database created in `beforeAll`).
    */
  private class H2BackedOracleConnector(store: WatermarkStore)
      extends OracleJdbcConnector(
        "oracle_test_table",
        store,
        maxAttempts = 1,
        retryDelayFn = _ => ()
      ) {
    override def testConnection(config: SourceConfig): Either[ConnectorError, Unit] = Right(())
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
      .appName("OracleJdbcConnectorSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Create and populate H2 table used by the H2-backed extraction test
    Class.forName(H2Driver)
    val conn = DriverManager.getConnection(H2Url)
    try {
      val stmt = conn.createStatement()
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS oracle_test_table (
          id     BIGINT PRIMARY KEY,
          name   VARCHAR(100),
          amount DOUBLE
        )
      """)
      stmt.execute("DELETE FROM oracle_test_table")
      stmt.execute("INSERT INTO oracle_test_table VALUES (1, 'Alpha', 1000.0)")
      stmt.execute("INSERT INTO oracle_test_table VALUES (2, 'Beta',  2000.0)")
      stmt.execute("INSERT INTO oracle_test_table VALUES (3, 'Gamma', 3000.0)")
      stmt.execute("INSERT INTO oracle_test_table VALUES (4, 'Delta', 4000.0)")
      stmt.execute("INSERT INTO oracle_test_table VALUES (5, 'Epsilon', 5000.0)")
      stmt.close()
    }
    finally conn.close()
  }

  override def afterAll(): Unit =
    try {
      val conn = DriverManager.getConnection(H2Url)
      try {
        val stmt = conn.createStatement()
        stmt.execute("DROP TABLE IF EXISTS oracle_test_table")
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
      host: Option[String] = Some("dbhost"),
      port: Option[Int] = Some(1521),
      database: Option[String] = Some("ORCL"),
      mode: IngestionMode = IngestionMode.Full,
      incrementalColumn: Option[String] = None,
      watermarkStorage: Option[String] = None
  ): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "oracle-source",
        sourceName  = "Oracle Test Source",
        sector      = Sector.FinancialServices,
        owner       = "test-team",
        environment = Environment.Dev
      ),
      connection = Connection(
        connectionType = ConnectionType.Jdbc,
        credentialsRef = "vault/test/oracle",
        host           = host,
        port           = port,
        database       = database,
        jdbcDriver     = Some(JdbcDriver.Oracle)
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
        path   = "/tmp/test-oracle-bronze"
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // 1. jdbcUrl — with host, port, and service name
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector.jdbcUrl" should "return the correct thin URL when host, port, and database are present" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    val config    = testConfig()
    connector.exposedJdbcUrl(config) shouldBe Right("jdbc:oracle:thin:@//dbhost:1521/ORCL")
  }

  // ---------------------------------------------------------------------------
  // 2. jdbcUrl — missing host
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when host is absent" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    val config    = testConfig(host = None)
    val result    = connector.exposedJdbcUrl(config)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("oracle-source")
    result.left.map(_.cause) shouldBe Left(
      "Oracle connector requires host, port, and database (service name) in the connection configuration"
    )
  }

  // ---------------------------------------------------------------------------
  // 3. jdbcUrl — missing port
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when port is absent" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    val config    = testConfig(port = None)
    val result    = connector.exposedJdbcUrl(config)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("oracle-source")
  }

  // ---------------------------------------------------------------------------
  // 4. jdbcUrl — missing database (service name)
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when database (service name) is absent" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    val config    = testConfig(database = None)
    val result    = connector.exposedJdbcUrl(config)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("oracle-source")
  }

  // ---------------------------------------------------------------------------
  // 5. driverClass — string constant, no Class.forName
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector.driverClass" should "return the Oracle driver class name string" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    connector.exposedDriverClass shouldBe "oracle.jdbc.OracleDriver"
  }

  // ---------------------------------------------------------------------------
  // 6. tableName — returns constructor-passed table name
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector.tableName" should "return the table name supplied at construction" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("ENERGY.METER_READINGS", store)
    val config    = testConfig()
    connector.exposedTableName(config) shouldBe "ENERGY.METER_READINGS"
  }

  // ---------------------------------------------------------------------------
  // 7. extraJdbcOptions — no wallet path
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector.extraJdbcOptions" should "return an empty map when walletPath is None" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store, walletPath = None)
    val config    = testConfig()
    connector.exposedExtraJdbcOptions(config) shouldBe Map.empty
  }

  // ---------------------------------------------------------------------------
  // 8. extraJdbcOptions — with wallet path
  // ---------------------------------------------------------------------------

  it should "return a map with oracle.net.wallet_location in SOURCE descriptor format when walletPath is set" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector(
      "FINANCE.TRADES",
      store,
      walletPath = Some("/opt/oracle/wallet")
    )
    val config  = testConfig()
    val options = connector.exposedExtraJdbcOptions(config)
    options should contain key "oracle.net.wallet_location"
    options("oracle.net.wallet_location") shouldBe
      "(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/opt/oracle/wallet)))"
  }

  // ---------------------------------------------------------------------------
  // 9. partitionColumn — None (default)
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector.partitionColumn" should "return None when no partitionCol is set" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    val config    = testConfig()
    connector.exposedPartitionColumn(config) shouldBe None
  }

  // ---------------------------------------------------------------------------
  // 10. partitionColumn — ROWID
  // ---------------------------------------------------------------------------

  it should "return Some(\"ROWID\") when ROWID partitioning is configured" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector(
      "FINANCE.TRADES",
      store,
      partitionCol   = Some("ROWID"),
      partitionLower = Some(1000L),
      partitionUpper = Some(9999L)
    )
    val config = testConfig()
    connector.exposedPartitionColumn(config) shouldBe Some("ROWID")
  }

  // ---------------------------------------------------------------------------
  // 11. partitionLowerBound and partitionUpperBound
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector.partitionLowerBound" should "return the constructor-supplied lower bound" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector(
      "FINANCE.TRADES",
      store,
      partitionCol   = Some("id"),
      partitionLower = Some(1L),
      partitionUpper = Some(100L)
    )
    val config = testConfig()
    connector.exposedPartitionLowerBound(config) shouldBe Some(1L)
    connector.exposedPartitionUpperBound(config) shouldBe Some(100L)
  }

  "OracleJdbcConnector.partitionUpperBound" should "return None when no upper bound is configured" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    val config    = testConfig()
    connector.exposedPartitionUpperBound(config) shouldBe None
  }

  // ---------------------------------------------------------------------------
  // 12. extract full-refresh — succeeds with H2 backing
  // ---------------------------------------------------------------------------

  "OracleJdbcConnector.extract" should "return Right(DataFrame) with correct row count using H2 backing" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2BackedOracleConnector(store)
    val config    = testConfig()
    val result    = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(5L)
  }

  // ---------------------------------------------------------------------------
  // 13. extract — Left when jdbcUrl returns Left (missing host)
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when jdbcUrl returns Left due to missing host" in {
    val store     = new InMemoryWatermarkStore
    val connector = new TestableOracleConnector("FINANCE.TRADES", store)
    val config    = testConfig(host = None)
    val result    = connector.extract(config, spark)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("oracle-source")
    result.left.map(_.cause) shouldBe Left(
      "Oracle connector requires host, port, and database (service name) in the connection configuration"
    )
  }
}

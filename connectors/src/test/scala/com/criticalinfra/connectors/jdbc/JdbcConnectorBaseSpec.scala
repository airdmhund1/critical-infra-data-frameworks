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
import com.criticalinfra.connectors.watermark.{IntegerWatermark, TimestampWatermark, Watermark, WatermarkStore}
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{DriverManager, Timestamp}
import scala.collection.mutable

// =============================================================================
// JdbcConnectorBaseSpec — unit tests for JdbcConnectorBase
//
// Uses an H2 in-process database so no external JDBC server is required.
// The SparkSession is created once in beforeAll and stopped in afterAll.
// All 14 cases are deterministic — no Thread.sleep, no time-based assertions.
// =============================================================================

class JdbcConnectorBaseSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // H2 constants
  // ---------------------------------------------------------------------------

  private val H2Url    = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
  private val H2Driver = "org.h2.Driver"

  // ---------------------------------------------------------------------------
  // Test connector subclasses
  // ---------------------------------------------------------------------------

  /** Minimal H2-backed connector with no partition opts. */
  private class H2TestConnector(
      store: WatermarkStore,
      attempts: Int = 1,
      delayFn: Long => Unit = _ => ()
  ) extends JdbcConnectorBase(store, attempts, delayFn) {
    override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] =
      Right(H2Url)
    override protected def driverClass: String                = H2Driver
    override protected def tableName(config: SourceConfig): String = "test_records"
  }

  /** H2 connector that overrides all three partition methods. */
  private class H2PartitionedConnector(store: WatermarkStore) extends H2TestConnector(store) {
    override protected def partitionColumn(config: SourceConfig): Option[String] = Some("id")
    override protected def partitionLowerBound(config: SourceConfig): Option[Long] = Some(1L)
    override protected def partitionUpperBound(config: SourceConfig): Option[Long] = Some(3L)
  }

  /** Connector whose jdbcUrl always returns Left — simulates a misconfigured URL. */
  private class BadUrlConnector(store: WatermarkStore)
      extends JdbcConnectorBase(store, maxAttempts = 1, retryDelayFn = _ => ()) {
    override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] =
      Left(ConnectorError(config.metadata.sourceId, "bad url"))
    override protected def driverClass: String                     = H2Driver
    override protected def tableName(config: SourceConfig): String = "test_records"
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

    /** Returns the currently stored watermark for `sourceId` — test-only helper. */
    def get(sourceId: String): Option[Watermark] = store.get(sourceId)
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
      .appName("JdbcConnectorBaseSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Create and populate H2 table via DriverManager
    Class.forName(H2Driver)
    val conn = DriverManager.getConnection(H2Url)
    try {
      val stmt = conn.createStatement()
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS test_records (
          id         BIGINT PRIMARY KEY,
          name       VARCHAR(100),
          amount     DOUBLE,
          updated_at TIMESTAMP
        )
      """)
      stmt.execute("DELETE FROM test_records")
      stmt.execute("INSERT INTO test_records VALUES (1, 'Alice', 100.0, TIMESTAMP '2024-01-01 10:00:00')")
      stmt.execute("INSERT INTO test_records VALUES (2, 'Bob',   200.0, TIMESTAMP '2024-02-01 10:00:00')")
      stmt.execute("INSERT INTO test_records VALUES (3, 'Carol', 300.0, TIMESTAMP '2024-03-01 10:00:00')")
      stmt.close()
    }
    finally conn.close()
  }

  override def afterAll(): Unit =
    try {
      val conn = DriverManager.getConnection(H2Url)
      try {
        val stmt = conn.createStatement()
        stmt.execute("DROP TABLE IF EXISTS test_records")
        stmt.close()
      }
      finally conn.close()
    }
    finally {
      if (spark != null) spark.stop()
      super.afterAll()
    }

  // ---------------------------------------------------------------------------
  // Helper: build a SourceConfig for tests
  // ---------------------------------------------------------------------------

  private def testConfig(
      mode: IngestionMode,
      incrementalColumn: Option[String] = None,
      watermarkStorage: Option[String] = None
  ): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "test-source",
        sourceName  = "Test Source",
        sector      = Sector.FinancialServices,
        owner       = "test-team",
        environment = Environment.Dev
      ),
      connection = Connection(
        connectionType = ConnectionType.Jdbc,
        credentialsRef = "vault/test/jdbc",
        host           = Some("localhost"),
        port           = Some(5432),
        database       = Some("testdb"),
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
  // 1. testConnection succeeds for H2 URL
  // ---------------------------------------------------------------------------

  "JdbcConnectorBase.testConnection" should "return Right(()) for a valid H2 URL" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(IngestionMode.Full)
    connector.testConnection(config) shouldBe Right(())
  }

  // ---------------------------------------------------------------------------
  // 2. testConnection fails with a bad URL
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) for a garbage JDBC URL" in {
    val store = new InMemoryWatermarkStore
    val connector = new JdbcConnectorBase(store, maxAttempts = 1, retryDelayFn = _ => ()) {
      override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] =
        Right("jdbc:garbage://no-such-host/db")
      override protected def driverClass: String                     = H2Driver
      override protected def tableName(config: SourceConfig): String = "test_records"
    }
    val config = testConfig(IngestionMode.Full)
    val result = connector.testConnection(config)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("test-source")
  }

  // ---------------------------------------------------------------------------
  // 3. testConnection retries — fails N times then succeeds
  // ---------------------------------------------------------------------------

  it should "retry the configured number of times before succeeding" in {
    val store    = new InMemoryWatermarkStore
    var attempts = 0
    val connector = new JdbcConnectorBase(store, maxAttempts = 3, retryDelayFn = _ => ()) {
      override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] = {
        attempts += 1
        if (attempts < 3) Right("jdbc:garbage://fail/db")
        else Right(H2Url)
      }
      override protected def driverClass: String                     = H2Driver
      override protected def tableName(config: SourceConfig): String = "test_records"
    }
    val config = testConfig(IngestionMode.Full)
    val result = connector.testConnection(config)
    result shouldBe Right(())
    attempts shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // 4. extract full-refresh — returns all 3 rows
  // ---------------------------------------------------------------------------

  "JdbcConnectorBase.extract" should "return all rows in Full refresh mode" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(IngestionMode.Full)
    val result    = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(3L)
  }

  // ---------------------------------------------------------------------------
  // 5. extract incremental — first run (no prior watermark) returns all rows
  // ---------------------------------------------------------------------------

  it should "return all rows on first incremental run when no watermark exists" in {
    val store     = new InMemoryWatermarkStore // empty
    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/test-wm")
    )
    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(3L)
  }

  // ---------------------------------------------------------------------------
  // 6. extract incremental — with IntegerWatermark(1L) returns rows with id > 1
  // ---------------------------------------------------------------------------

  it should "return only rows newer than the existing IntegerWatermark" in {
    val store = new InMemoryWatermarkStore
    store.write("test-source", IntegerWatermark(1L))

    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/test-wm")
    )
    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(2L)
    result.map(_.select("id").collect().map(_.getLong(0)).toSet) shouldBe Right(Set(2L, 3L))
  }

  // ---------------------------------------------------------------------------
  // 7. extract incremental — with TimestampWatermark returns rows newer than timestamp
  // ---------------------------------------------------------------------------

  it should "return only rows newer than the existing TimestampWatermark" in {
    val store = new InMemoryWatermarkStore
    // Watermark at 2024-01-15 — rows 2 and 3 have updated_at after this
    store.write("test-source", TimestampWatermark(Timestamp.valueOf("2024-01-15 00:00:00")))

    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("updated_at"),
      watermarkStorage  = Some("/tmp/test-wm")
    )
    val result = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(2L)
  }

  // ---------------------------------------------------------------------------
  // 8. extract fails when incrementalColumn absent in Incremental mode
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when incrementalColumn is absent in Incremental mode" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = None,
      watermarkStorage  = Some("/tmp/test-wm")
    )
    val result = connector.extract(config, spark)
    result shouldBe Left(
      ConnectorError("test-source", "incrementalColumn is required for Incremental mode")
    )
  }

  // ---------------------------------------------------------------------------
  // 9. extract fails when watermarkStorage absent in Incremental mode
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) when watermarkStorage is absent in Incremental mode" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = None
    )
    val result = connector.extract(config, spark)
    result shouldBe Left(
      ConnectorError("test-source", "watermarkStorage is required for Incremental mode")
    )
  }

  // ---------------------------------------------------------------------------
  // 10. Parallel-read options included when all three partition fields provided
  // ---------------------------------------------------------------------------

  it should "succeed when parallel-read partition options are provided" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2PartitionedConnector(store)
    val config    = testConfig(IngestionMode.Full)
    val result    = connector.extract(config, spark)
    result shouldBe a[Right[_, _]]
    result.map(_.count()) shouldBe Right(3L)
  }

  // ---------------------------------------------------------------------------
  // 11. persistWatermark Full mode — no-op, store unchanged
  // ---------------------------------------------------------------------------

  "JdbcConnectorBase.persistWatermark" should "be a no-op for Full refresh mode" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(IngestionMode.Full)
    val df        = spark.read.format("jdbc").options(Map(
      "url"    -> H2Url,
      "dbtable" -> "test_records",
      "driver" -> H2Driver
    )).load()

    val result = connector.persistWatermark(config, df)
    result shouldBe Right(())
    store.get("test-source") shouldBe None
  }

  // ---------------------------------------------------------------------------
  // 12. persistWatermark Incremental with integer column — updates store with max(id)
  // ---------------------------------------------------------------------------

  it should "update the store with IntegerWatermark of max(id) for Incremental mode" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/test-wm")
    )
    val df = spark.read.format("jdbc").options(Map(
      "url"    -> H2Url,
      "dbtable" -> "test_records",
      "driver" -> H2Driver
    )).load()

    val result = connector.persistWatermark(config, df)
    result shouldBe Right(())
    store.get("test-source") shouldBe Some(IntegerWatermark(3L))
  }

  // ---------------------------------------------------------------------------
  // 13. persistWatermark Incremental with timestamp column — updates store with max(updated_at)
  // ---------------------------------------------------------------------------

  it should "update the store with TimestampWatermark of max(updated_at) for Incremental mode" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("updated_at"),
      watermarkStorage  = Some("/tmp/test-wm")
    )
    val df = spark.read.format("jdbc").options(Map(
      "url"    -> H2Url,
      "dbtable" -> "test_records",
      "driver" -> H2Driver
    )).load()

    val result = connector.persistWatermark(config, df)
    result shouldBe Right(())
    store.get("test-source") shouldBe Some(
      TimestampWatermark(Timestamp.valueOf("2024-03-01 10:00:00.0"))
    )
  }

  // ---------------------------------------------------------------------------
  // 14. persistWatermark on empty DataFrame — no watermark update, store unchanged
  // ---------------------------------------------------------------------------

  it should "return Right(()) and leave store unchanged when the DataFrame is empty" in {
    val store     = new InMemoryWatermarkStore
    val connector = new H2TestConnector(store)
    val config    = testConfig(
      mode              = IngestionMode.Incremental,
      incrementalColumn = Some("id"),
      watermarkStorage  = Some("/tmp/test-wm")
    )

    // Empty DataFrame with the correct schema
    val df = spark.read.format("jdbc").options(Map(
      "url"     -> H2Url,
      "dbtable" -> "(SELECT * FROM test_records WHERE id < 0) t",
      "driver"  -> H2Driver
    )).load()

    val result = connector.persistWatermark(config, df)
    result shouldBe Right(())
    store.get("test-source") shouldBe None
  }

  // ---------------------------------------------------------------------------
  // 15. extract propagates Left from jdbcUrl
  // ---------------------------------------------------------------------------

  it should "propagate Left(ConnectorError) when jdbcUrl returns Left" in {
    val store     = new InMemoryWatermarkStore
    val connector = new BadUrlConnector(store)
    val config    = testConfig(IngestionMode.Full)
    val result    = connector.extract(config, spark)
    result shouldBe Left(ConnectorError("test-source", "bad url"))
  }
}

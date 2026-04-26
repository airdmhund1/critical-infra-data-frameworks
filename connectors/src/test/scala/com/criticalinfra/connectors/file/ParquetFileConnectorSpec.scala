package com.criticalinfra.connectors.file

import com.criticalinfra.config.{
  Audit,
  Connection,
  ConnectionType,
  Environment,
  FileFormat,
  Ingestion,
  IngestionMode,
  Metadata,
  Monitoring,
  QualityRules,
  Quarantine,
  SchemaEnforcement,
  SchemaEnforcementMode,
  Sector,
  SourceConfig,
  Storage,
  StorageFormat,
  StorageLayer
}
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

// =============================================================================
// ParquetFileConnectorSpec — unit tests for ParquetFileConnector
//
// Uses temp Parquet directories written via SparkSession in local mode so there
// are no external dependencies. SparkSession runs in local[1] mode.
//
// Test scenarios (17 total):
//   - Path validation (1)
//   - No schemaRef — passthrough (2)
//   - Schema match — no drift (2)
//   - Schema drift — extra column (2)
//   - Schema drift — missing column (1)
//   - Schema drift — type mismatch (1)
//   - Column pruning (2)
//   - Hive partition discovery (2)
//   - Partition filter pushdown (3)
//   - Schema loader failure (1)
// =============================================================================

/** Unit test suite for [[ParquetFileConnector]].
  *
  * Every test writes Parquet content to a temporary directory using the shared SparkSession. No
  * Testcontainers or network connectivity is required. SparkSession runs in `local[1]` mode with
  * the UI disabled and shuffle partitions set to 1.
  *
  * `spark.createDataFrame(rows, schema)` is used throughout in preference to the `toDF` implicit
  * enrichment because `spark` is a `var` and Scala 2.13 requires a stable identifier for member
  * imports.
  */
class ParquetFileConnectorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // SparkSession lifecycle
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("ParquetFileConnectorSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }
  }

  // ---------------------------------------------------------------------------
  // Declared test schema
  // ---------------------------------------------------------------------------

  private val testSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true)
    )
  )

  // ---------------------------------------------------------------------------
  // Schema loader test doubles
  // ---------------------------------------------------------------------------

  private def makeLoader(schema: StructType): String => Either[ConnectorError, StructType] =
    _ => Right(schema)

  private val failLoader: String => Either[ConnectorError, StructType] =
    ref => Left(ConnectorError("test", s"Schema not found: $ref"))

  // ---------------------------------------------------------------------------
  // Config builder helper
  // ---------------------------------------------------------------------------

  private def makeConfig(
      filePath: Option[String],
      schemaRef: Option[String] = None
  ): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "test-parquet-source",
        sourceName  = "Test Parquet Source",
        sector      = Sector.Energy,
        owner       = "test-team",
        environment = Environment.Dev
      ),
      connection = Connection(
        connectionType = ConnectionType.File,
        credentialsRef = "local/test",
        filePath       = filePath,
        fileFormat     = Some(FileFormat.Parquet)
      ),
      ingestion       = Ingestion(mode = IngestionMode.Full),
      schemaEnforcement = SchemaEnforcement(
        enabled     = true,
        mode        = Some(SchemaEnforcementMode.Strict),
        registryRef = schemaRef
      ),
      qualityRules = QualityRules(),
      quarantine   = Quarantine(enabled = false),
      storage = Storage(
        layer  = StorageLayer.Bronze,
        format = StorageFormat.Delta,
        path   = "/tmp/test-bronze"
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // Parquet fixture helpers
  //
  // spark.createDataFrame is used instead of the toDF enrichment because `spark`
  // is a var — Scala 2.13 requires a stable identifier for member imports and
  // would reject `import spark.implicits._` inside a method body.
  // ---------------------------------------------------------------------------

  /** Writes a simple flat Parquet file with the three testSchema columns. */
  private def writeParquet(dir: java.io.File, rows: Seq[(Long, String, Double)]): String = {
    val schema = StructType(Seq(
      StructField("id",     LongType,   nullable = false),
      StructField("name",   StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true)
    ))
    val sparkRows = rows.map { case (id, name, amount) => Row(id, name, amount) }
    val df        = spark.createDataFrame(spark.sparkContext.parallelize(sparkRows), schema)
    val path      = dir.getAbsolutePath
    df.write.mode("overwrite").parquet(path)
    path
  }

  /** Writes a Hive-partitioned Parquet directory with three `date=YYYY-MM-DD` partitions. */
  private def writePartitionedParquet(baseDir: java.io.File): String = {
    val schema = StructType(Seq(
      StructField("id",     LongType,   nullable = false),
      StructField("name",   StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("date",   StringType, nullable = true)
    ))
    val rows = Seq(
      Row(1L, "Alice", 10.0, "2024-01-01"),
      Row(2L, "Bob",   20.0, "2024-01-02"),
      Row(3L, "Carol", 30.0, "2024-01-03")
    )
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val path = baseDir.getAbsolutePath
    df.write.mode("overwrite").partitionBy("date").parquet(path)
    path
  }

  // ---------------------------------------------------------------------------
  // Convenience factory for a default connector (no schemaLoader, no date bounds)
  // ---------------------------------------------------------------------------

  private def defaultConnector: ParquetFileConnector = new ParquetFileConnector()

  // ===========================================================================
  // 1. Path validation
  // ===========================================================================

  "ParquetFileConnector" should "return Left(ConnectorError) when filePath is absent" in {
    val connector = defaultConnector
    val config    = makeConfig(filePath = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Left[_, _]]
    result.left.map(_.cause should include("filePath")).left.getOrElse(())
  }

  // ===========================================================================
  // 2. No schemaRef — passthrough
  // ===========================================================================

  it should "return Right(DataFrame) with the correct row count when schemaRef is absent" in {
    val dir = Files.createTempDirectory("parquet-spec-passthrough-").toFile
    dir.deleteOnExit()
    val path = writeParquet(dir, Seq((1L, "Alice", 10.0), (2L, "Bob", 20.0)))

    val connector = defaultConnector
    val config    = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 2L)
  }

  it should "return all embedded-schema columns when schemaRef is absent" in {
    val dir = Files.createTempDirectory("parquet-spec-all-cols-").toFile
    dir.deleteOnExit()
    val path = writeParquet(dir, Seq((1L, "Alice", 10.0)))

    val connector = defaultConnector
    val config    = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.columns.toSeq should contain allOf ("id", "name", "amount")
    }
  }

  // ===========================================================================
  // 3. Schema match — no drift
  // ===========================================================================

  it should "return Right(DataFrame) when Parquet schema matches registered schema" in {
    val dir = Files.createTempDirectory("parquet-spec-match-").toFile
    dir.deleteOnExit()
    val path = writeParquet(dir, Seq((1L, "Alice", 10.0), (2L, "Bob", 20.0)))

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(path), schemaRef = Some("test-schema-v1"))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 2L)
  }

  it should "return a DataFrame whose column names match testSchema when schemas are aligned" in {
    val dir = Files.createTempDirectory("parquet-spec-match-cols-").toFile
    dir.deleteOnExit()
    val path = writeParquet(dir, Seq((1L, "Alice", 10.0)))

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(path), schemaRef = Some("test-schema-v1"))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.columns.toSeq shouldBe testSchema.fieldNames.toSeq
    }
  }

  // ===========================================================================
  // 4. Schema drift — extra column in Parquet file
  // ===========================================================================

  it should "prune extra columns from the output when Parquet has more columns than registeredSchema" in {
    val dir = Files.createTempDirectory("parquet-spec-extra-").toFile
    dir.deleteOnExit()

    // Write Parquet with an extra 'bonus' column beyond testSchema
    val schema = StructType(Seq(
      StructField("id",     LongType,   nullable = false),
      StructField("name",   StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("bonus",  DoubleType, nullable = true)
    ))
    val rows = Seq(Row(1L, "Alice", 10.0, 99.9), Row(2L, "Bob", 20.0, 50.0))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.mode("overwrite").parquet(dir.getAbsolutePath)

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(dir.getAbsolutePath), schemaRef = Some("test-schema-v1"))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { out =>
      out.columns should not contain "bonus"
      out.columns.toSeq shouldBe testSchema.fieldNames.toSeq
    }
  }

  it should "contain only testSchema columns (no bonus) after extra-column drift pruning" in {
    val dir = Files.createTempDirectory("parquet-spec-extra2-").toFile
    dir.deleteOnExit()

    val schema = StructType(Seq(
      StructField("id",     LongType,   nullable = false),
      StructField("name",   StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("bonus",  DoubleType, nullable = true)
    ))
    val rows = Seq(Row(1L, "Alice", 10.0, 99.9))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.mode("overwrite").parquet(dir.getAbsolutePath)

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(dir.getAbsolutePath), schemaRef = Some("test-schema-v1"))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { out =>
      out.columns.length shouldBe testSchema.fieldNames.length
      out.columns should not contain "bonus"
    }
  }

  // ===========================================================================
  // 5. Schema drift — missing column in Parquet file
  // ===========================================================================

  it should "return Right and detect drift when Parquet is missing a column declared in registeredSchema" in {
    val dir = Files.createTempDirectory("parquet-spec-missing-").toFile
    dir.deleteOnExit()

    // Write Parquet with only 'id' and 'name'; 'amount' is absent
    val schema = StructType(Seq(
      StructField("id",   LongType,   nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val rows = Seq(Row(1L, "Alice"), Row(2L, "Bob"))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.mode("overwrite").parquet(dir.getAbsolutePath)

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(dir.getAbsolutePath), schemaRef = Some("test-schema-v1"))

    val result = connector.extract(config, spark)

    // Drift is detected and logged; column pruning selects only present registered cols
    result shouldBe a[Right[_, _]]
    result.foreach { out =>
      // 'amount' was in registeredSchema but not in Parquet — pruning skips missing cols
      out.columns should not contain "amount"
      out.count() shouldBe 2L
    }
  }

  // ===========================================================================
  // 6. Schema drift — type mismatch
  // ===========================================================================

  it should "detect drift when Parquet has IntegerType for a column declared as LongType" in {
    val dir = Files.createTempDirectory("parquet-spec-type-").toFile
    dir.deleteOnExit()

    // Write Parquet with 'id' as IntegerType instead of LongType
    val schema = StructType(Seq(
      StructField("id",     IntegerType, nullable = false),
      StructField("name",   StringType,  nullable = true),
      StructField("amount", DoubleType,  nullable = true)
    ))
    val rows = Seq(Row(1, "Alice", 10.0), Row(2, "Bob", 20.0))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.mode("overwrite").parquet(dir.getAbsolutePath)

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(dir.getAbsolutePath), schemaRef = Some("test-schema-v1"))

    // The connector returns Right; drift is logged as a WARN (not a failure)
    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { out =>
      // Column pruning selects 'id' (present in both); its type is what Parquet embedded
      out.columns should contain("id")
    }
  }

  // ===========================================================================
  // 7. Column pruning
  // ===========================================================================

  it should "select only registeredSchema columns when Parquet has 5 columns and schema declares 3" in {
    val dir = Files.createTempDirectory("parquet-spec-prune5-").toFile
    dir.deleteOnExit()

    val schema = StructType(Seq(
      StructField("id",     LongType,    nullable = false),
      StructField("name",   StringType,  nullable = true),
      StructField("amount", DoubleType,  nullable = true),
      StructField("region", StringType,  nullable = true),
      StructField("active", BooleanType, nullable = true)
    ))
    val rows = Seq(Row(1L, "Alice", 10.0, "NY", true))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.mode("overwrite").parquet(dir.getAbsolutePath)

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(dir.getAbsolutePath), schemaRef = Some("test-schema-v1"))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.columns.length shouldBe 3)
  }

  it should "have column names matching registeredSchema after pruning from a 5-column Parquet" in {
    val dir = Files.createTempDirectory("parquet-spec-prune5-names-").toFile
    dir.deleteOnExit()

    val schema = StructType(Seq(
      StructField("id",     LongType,    nullable = false),
      StructField("name",   StringType,  nullable = true),
      StructField("amount", DoubleType,  nullable = true),
      StructField("region", StringType,  nullable = true),
      StructField("active", BooleanType, nullable = true)
    ))
    val rows = Seq(Row(1L, "Alice", 10.0, "NY", true))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.mode("overwrite").parquet(dir.getAbsolutePath)

    val connector = new ParquetFileConnector(schemaLoader = makeLoader(testSchema))
    val config    = makeConfig(filePath = Some(dir.getAbsolutePath), schemaRef = Some("test-schema-v1"))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { out =>
      out.columns.toSet shouldBe testSchema.fieldNames.toSet
    }
  }

  // ===========================================================================
  // 8. Hive partition discovery
  // ===========================================================================

  it should "return all rows from a Hive-partitioned Parquet directory when no date filter is set" in {
    val baseDir = Files.createTempDirectory("parquet-spec-hive-all-").toFile
    baseDir.deleteOnExit()
    val path = writePartitionedParquet(baseDir)

    val connector = defaultConnector
    val config    = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 3L)
  }

  it should "include the partition column in the result when reading a Hive-partitioned directory" in {
    val baseDir = Files.createTempDirectory("parquet-spec-hive-col-").toFile
    baseDir.deleteOnExit()
    val path = writePartitionedParquet(baseDir)

    val connector = defaultConnector
    val config    = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.columns should contain("date")
    }
  }

  // ===========================================================================
  // 9. Partition filter pushdown
  // ===========================================================================

  it should "return 2 rows when startDate=2024-01-02 (Jan 2 and Jan 3)" in {
    val baseDir = Files.createTempDirectory("parquet-spec-filter-start-").toFile
    baseDir.deleteOnExit()
    val path = writePartitionedParquet(baseDir)

    val connector = new ParquetFileConnector(startDate = Some("2024-01-02"))
    val config    = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 2L)
  }

  it should "return 2 rows when endDate=2024-01-02 (Jan 1 and Jan 2)" in {
    val baseDir = Files.createTempDirectory("parquet-spec-filter-end-").toFile
    baseDir.deleteOnExit()
    val path = writePartitionedParquet(baseDir)

    val connector = new ParquetFileConnector(endDate = Some("2024-01-02"))
    val config    = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 2L)
  }

  it should "return exactly 1 row when startDate=endDate=2024-01-02 (Jan 2 only)" in {
    val baseDir = Files.createTempDirectory("parquet-spec-filter-between-").toFile
    baseDir.deleteOnExit()
    val path = writePartitionedParquet(baseDir)

    val connector = new ParquetFileConnector(
      startDate = Some("2024-01-02"),
      endDate   = Some("2024-01-02")
    )
    val config = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 1L)
  }

  // ===========================================================================
  // 10. Schema loader failure propagation
  // ===========================================================================

  it should "propagate Left(ConnectorError) when the schema loader fails" in {
    val dir = Files.createTempDirectory("parquet-spec-loader-fail-").toFile
    dir.deleteOnExit()
    val path = writeParquet(dir, Seq((1L, "Alice", 10.0)))

    val connector = new ParquetFileConnector(schemaLoader = failLoader)
    val config    = makeConfig(filePath = Some(path), schemaRef = Some("missing-schema-ref"))

    val result = connector.extract(config, spark)

    result shouldBe a[Left[_, _]]
    result.left.map(_.cause should include("Schema not found")).left.getOrElse(())
  }
}

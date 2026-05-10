package com.criticalinfra.engine

import com.criticalinfra.config._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.time.LocalDate
import java.util.UUID
import scala.util.Try

// =============================================================================
// DeltaBronzeLayerWriterSpec — unit tests for DeltaBronzeLayerWriter
//
// Tests cover:
//   1. Partition directories exist on disk after write
//   2. All four _cidf_* metadata columns present and non-null
//   3. delta.appendOnly = true property set after write
//   4. Second append succeeds and total count is cumulative
//   5. Delete attempt fails with Delta exception when appendOnly is set
//   6. WriteResult fields are correctly populated
// =============================================================================

/** Unit tests for [[DeltaBronzeLayerWriter]].
  *
  * Each test uses an isolated temp directory created via `java.nio.file.Files.createTempDirectory`
  * and cleaned up in a finally block. A single [[SparkSession]] with Delta Lake extensions is
  * shared across the suite and stopped in `afterAll`. DataFrames are created via
  * `spark.createDataFrame` with explicit Row/StructType rather than implicits, because `spark` is a
  * `var` and Scala 2.13 requires a stable identifier for import.
  */
class DeltaBronzeLayerWriterSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // SparkSession — shared across the suite, Delta extensions registered
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DeltaBronzeLayerWriterSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Recursively deletes a directory and all its contents. */
  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory)
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }

  /** Schema for the test DataFrames: id (Int), name (String), amount (Double). */
  private val testSchema: StructType = StructType(Seq(
    StructField("id",     IntegerType, nullable = false),
    StructField("name",   StringType,  nullable = false),
    StructField("amount", DoubleType,  nullable = false)
  ))

  /** Creates a test DataFrame from a list of (Int, String, Double) tuples. */
  private def makeDataFrame(rows: Seq[(Int, String, Double)]): org.apache.spark.sql.DataFrame = {
    val ss = spark // capture stable reference for Row construction
    ss.createDataFrame(
      ss.sparkContext.parallelize(rows.map { case (id, name, amt) => Row(id, name, amt) }),
      testSchema
    )
  }

  /** Minimal valid [[SourceConfig]] whose Bronze storage path is `bronzePath`. */
  private def testConfig(bronzePath: String, sourceName: String = "test-source"): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "delta-writer-spec-source",
        sourceName  = sourceName,
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
  // Test 1 — Partition directories exist on disk after write
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "create ingestion_date and source_name partition directories on disk" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-1-").toAbsolutePath.toString
    try {
      val data   = makeDataFrame(Seq((1, "alice", 100.0), (2, "bob", 200.0)))
      val config = testConfig(tempPath, "partition-check-source")
      val writer = new DeltaBronzeLayerWriter

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      val today = LocalDate.now().toString

      // The partition directory for ingestion_date must exist
      val ingestionDateDir = new java.io.File(tempPath, s"ingestion_date=$today")
      ingestionDateDir.exists()      shouldBe true
      ingestionDateDir.isDirectory   shouldBe true

      // Inside it, the source_name sub-directory must exist
      val sourceNameDir = new java.io.File(ingestionDateDir, "source_name=partition-check-source")
      sourceNameDir.exists()    shouldBe true
      sourceNameDir.isDirectory shouldBe true
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2 — All four _cidf_* metadata columns present and non-null
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "inject _cidf_ingestion_ts, _cidf_source_name, _cidf_run_id, and _cidf_checksum columns" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-2-").toAbsolutePath.toString
    try {
      val data   = makeDataFrame(Seq((10, "carol", 300.0), (20, "dave", 400.0)))
      val config = testConfig(tempPath, "metadata-columns-source")
      val writer = new DeltaBronzeLayerWriter

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      val ss      = spark
      val written = ss.read.format("delta").load(tempPath)
      val cols    = written.columns.toSet

      // All four metadata columns must be present
      cols should contain("_cidf_ingestion_ts")
      cols should contain("_cidf_source_name")
      cols should contain("_cidf_run_id")
      cols should contain("_cidf_checksum")

      // No row may have a null value in any of these columns
      import org.apache.spark.sql.functions.col
      written.filter(col("_cidf_ingestion_ts").isNull).count() shouldBe 0L
      written.filter(col("_cidf_source_name").isNull).count()  shouldBe 0L
      written.filter(col("_cidf_run_id").isNull).count()       shouldBe 0L
      written.filter(col("_cidf_checksum").isNull).count()     shouldBe 0L
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3 — delta.appendOnly = true property set after write
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "set delta.appendOnly = true table property after write" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-3-").toAbsolutePath.toString
    try {
      val data   = makeDataFrame(Seq((1, "eve", 500.0)))
      val config = testConfig(tempPath, "append-only-source")
      val writer = new DeltaBronzeLayerWriter

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      // SHOW TBLPROPERTIES returns a DataFrame with columns "key" and "value"
      val ss    = spark
      val props = ss
        .sql(s"SHOW TBLPROPERTIES delta.`$tempPath`")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap

      props.get("delta.appendOnly") shouldBe Some("true")
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4 — Second append succeeds and total count is cumulative
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "accumulate rows across two consecutive appends" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-4-").toAbsolutePath.toString
    try {
      val config = testConfig(tempPath, "accumulate-source")
      val writer = new DeltaBronzeLayerWriter

      // First write: 5 rows
      val data1   = makeDataFrame((1 to 5).map(i => (i, s"row-$i", i * 10.0)))
      val result1 = writer.write(data1, config, UUID.randomUUID())
      result1.isRight shouldBe true
      result1.toOption.get.recordsWritten shouldBe 5L

      // Second write: 3 rows
      val data2   = makeDataFrame((6 to 8).map(i => (i, s"row-$i", i * 10.0)))
      val result2 = writer.write(data2, config, UUID.randomUUID())
      result2.isRight shouldBe true
      result2.toOption.get.recordsWritten shouldBe 3L

      // Total rows in the table must be 8
      val ss      = spark
      val written = ss.read.format("delta").load(tempPath)
      written.count() shouldBe 8L
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5 — Delete attempt fails with Delta exception when appendOnly is set
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "prevent DeltaTable.delete() after appendOnly is enforced" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-5-").toAbsolutePath.toString
    try {
      val data   = makeDataFrame(Seq((1, "frank", 99.0)))
      val config = testConfig(tempPath, "delete-guard-source")
      val writer = new DeltaBronzeLayerWriter

      val writeResult = writer.write(data, config, UUID.randomUUID())
      writeResult.isRight shouldBe true

      // Confirm appendOnly is in place before attempting delete
      val ss    = spark
      val props = ss
        .sql(s"SHOW TBLPROPERTIES delta.`$tempPath`")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap
      props.get("delta.appendOnly") shouldBe Some("true")

      // A delete on an appendOnly table must fail
      val deleteAttempt = Try(DeltaTable.forPath(ss, tempPath).delete())
      deleteAttempt.isFailure shouldBe true
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6 — WriteResult fields are correctly populated
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "return a WriteResult with the correct path, partitionDate, recordsWritten, and checksum" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-6-").toAbsolutePath.toString
    try {
      val rowCount = 7
      val data     = makeDataFrame((1 to rowCount).map(i => (i, s"name-$i", i * 5.0)))
      val config   = testConfig(tempPath, "write-result-source")
      val writer   = new DeltaBronzeLayerWriter

      val before = LocalDate.now()
      val result = writer.write(data, config, UUID.randomUUID())
      val after  = LocalDate.now()

      result.isRight shouldBe true
      val wr = result.toOption.get

      wr.path           shouldBe tempPath
      wr.recordsWritten shouldBe rowCount.toLong
      wr.checksum       should not be empty // SHA-256 hex string; blank placeholder replaced in Branch 3

      // partitionDate must be today
      wr.partitionDate.isBefore(before) shouldBe false
      wr.partitionDate.isAfter(after)   shouldBe false
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 7 — Checksum is a valid 64-character hex string and matches written column
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "produce a 64-character lowercase hex SHA-256 checksum that matches the _cidf_checksum column" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-7-").toAbsolutePath.toString
    try {
      val data   = makeDataFrame((1 to 5).map(i => (i, s"checksum-row-$i", i * 7.0)))
      val config = testConfig(tempPath, "checksum-verify-source")
      val writer = new DeltaBronzeLayerWriter

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      val wr = result.toOption.get

      // Checksum must be exactly 64 lowercase hex characters (SHA-256)
      wr.checksum should fullyMatch regex "[0-9a-f]{64}"

      // Every written row must carry the same checksum in the _cidf_checksum column
      val ss      = spark
      val written = ss.read.format("delta").load(tempPath)

      import org.apache.spark.sql.functions.col
      val distinctChecksums = written.select(col("_cidf_checksum")).distinct().collect()
      distinctChecksums.length               shouldBe 1
      distinctChecksums.head.getString(0)    shouldBe wr.checksum
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 8 — Count mismatch above threshold returns Left(StorageWriteError)
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "return Left(StorageWriteError) when mismatchThreshold forces a count mismatch failure" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-8-").toAbsolutePath.toString
    try {
      val data = makeDataFrame(Seq((1, "mismatch-test", 1.0)))
      val config = testConfig(tempPath, "mismatch-source")

      // mismatchThreshold = -1L: math.abs(sourceCount - writtenCount) == 0, and 0 > -1L is true,
      // so the validation always fails regardless of actual counts.
      val writer = new DeltaBronzeLayerWriter(mismatchThreshold = -1L)

      val result = writer.write(data, config, UUID.randomUUID())
      result.isLeft shouldBe true

      val storageWriteError = result.left.toOption.get
      storageWriteError.cause.toLowerCase should include("count mismatch")
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 9 — Count match with mismatchThreshold = 0 returns Right(WriteResult)
  //          with correct recordsWritten
  // ---------------------------------------------------------------------------

  "DeltaBronzeLayerWriter" should
    "return Right(WriteResult) with recordsWritten == 10 when mismatchThreshold = 0 and counts match" in {

    val tempPath = Files.createTempDirectory("delta-writer-spec-9-").toAbsolutePath.toString
    try {
      val data   = makeDataFrame((1 to 10).map(i => (i, s"threshold-row-$i", i * 3.0)))
      val config = testConfig(tempPath, "threshold-zero-source")
      val writer = new DeltaBronzeLayerWriter(mismatchThreshold = 0L)

      val result = writer.write(data, config, UUID.randomUUID())
      result.isRight shouldBe true

      val wr = result.toOption.get
      wr.recordsWritten shouldBe 10L
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }
}

package com.criticalinfra.connectors.watermark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.sql.Timestamp

// =============================================================================
// DeltaWatermarkStoreSpec — integration tests for DeltaWatermarkStore
//
// Each test uses a fresh temp-directory sub-path so tests are fully isolated.
// The shared SparkSession is created once in beforeAll and stopped in afterAll.
// The top-level temp directory is deleted (recursively) in afterAll.
// =============================================================================

class DeltaWatermarkStoreSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession  = _
  private var tempDir: Path        = _

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("delta-watermark-spec-")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DeltaWatermarkStoreSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    }
    finally {
      if (tempDir != null) deleteRecursively(tempDir)
      super.afterAll()
    }
  }

  /** Deletes a directory tree using the Java NIO2 file visitor API (no external dependencies). */
  private def deleteRecursively(root: Path): Unit =
    if (Files.exists(root)) {
      Files.walkFileTree(
        root,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }
          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
      ()
    }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Returns a unique sub-path under tempDir for each test case. */
  private def freshPath(name: String): String =
    tempDir.resolve(name).toAbsolutePath.toString

  private val sampleTs: Timestamp = Timestamp.valueOf("2024-06-01 08:00:00.0")

  // ---------------------------------------------------------------------------
  // 1. read on a non-existent path returns Right(None)
  // ---------------------------------------------------------------------------

  "DeltaWatermarkStore.read" should "return Right(None) when the checkpoint table does not exist" in {
    val store  = DeltaWatermarkStore(freshPath("test-1-nonexistent"), spark)
    val result = store.read("src-1")
    result shouldBe Right(None)
  }

  // ---------------------------------------------------------------------------
  // 2. write then read round-trips a TimestampWatermark
  // ---------------------------------------------------------------------------

  "DeltaWatermarkStore" should "round-trip a TimestampWatermark via write then read" in {
    val path      = freshPath("test-2-timestamp")
    val store     = DeltaWatermarkStore(path, spark)
    val watermark = TimestampWatermark(sampleTs)

    val writeResult = store.write("src-ts", watermark)
    writeResult shouldBe Right(())

    val readResult = store.read("src-ts")
    readResult shouldBe Right(Some(watermark))
  }

  // ---------------------------------------------------------------------------
  // 3. write then read round-trips an IntegerWatermark
  // ---------------------------------------------------------------------------

  it should "round-trip an IntegerWatermark via write then read" in {
    val path      = freshPath("test-3-integer")
    val store     = DeltaWatermarkStore(path, spark)
    val watermark = IntegerWatermark(1234567890L)

    val writeResult = store.write("src-int", watermark)
    writeResult shouldBe Right(())

    val readResult = store.read("src-int")
    readResult shouldBe Right(Some(watermark))
  }

  // ---------------------------------------------------------------------------
  // 4. Calling write twice for the same sourceId upserts rather than duplicating
  // ---------------------------------------------------------------------------

  it should "upsert on a second write for the same sourceId" in {
    val path  = freshPath("test-4-upsert")
    val store = DeltaWatermarkStore(path, spark)

    val first  = TimestampWatermark(Timestamp.valueOf("2024-01-01 00:00:00.0"))
    val second = TimestampWatermark(Timestamp.valueOf("2024-06-15 12:00:00.0"))

    store.write("src-upsert", first) shouldBe Right(())
    store.write("src-upsert", second) shouldBe Right(())

    // Only the latest value should be visible
    val readResult = store.read("src-upsert")
    readResult shouldBe Right(Some(second))

    // Exactly one row should exist in the table
    val df = spark.read.format("delta").load(path).filter("source_id = 'src-upsert'")
    df.count() shouldBe 1L
  }

  // ---------------------------------------------------------------------------
  // 5. Multiple sourceIds coexist independently
  // ---------------------------------------------------------------------------

  it should "store multiple sourceIds independently in the same checkpoint table" in {
    val path  = freshPath("test-5-multi")
    val store = DeltaWatermarkStore(path, spark)

    val wmA = TimestampWatermark(Timestamp.valueOf("2024-03-01 00:00:00.0"))
    val wmB = IntegerWatermark(999L)

    store.write("src-A", wmA) shouldBe Right(())
    store.write("src-B", wmB) shouldBe Right(())

    store.read("src-A") shouldBe Right(Some(wmA))
    store.read("src-B") shouldBe Right(Some(wmB))
  }

  // ---------------------------------------------------------------------------
  // 6. read for an unknown sourceId in an existing table returns Right(None)
  // ---------------------------------------------------------------------------

  it should "return Right(None) for an unknown sourceId in an existing table" in {
    val path  = freshPath("test-6-unknown-source")
    val store = DeltaWatermarkStore(path, spark)

    // Populate the table with one row so the table exists
    store.write("src-known", IntegerWatermark(1L)) shouldBe Right(())

    val result = store.read("src-unknown")
    result shouldBe Right(None)
  }
}

package com.criticalinfra.engine

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.time.Instant
import scala.util.Try

// =============================================================================
// AuditEventLoggerSpec — unit tests for AuditEventLogger, NoOpAuditEventLogger,
// and DeltaAuditEventLogger
//
// Tests cover:
//   1. Success event logged with all fields populated correctly
//   2. Failure event with errorMessage populated
//   3. Append-only enforcement confirmed via delete attempt
//   4. NoOpAuditEventLogger always returns Right(())
// =============================================================================

/** Unit tests for [[DeltaAuditEventLogger]] and [[NoOpAuditEventLogger]].
  *
  * Each Delta test uses an isolated temp directory created via
  * `java.nio.file.Files.createTempDirectory` and cleaned up in a finally block. A single
  * [[SparkSession]] with Delta Lake extensions is shared across the suite and stopped in
  * `afterAll`. DataFrames are read back via `spark.read.format("delta")`.
  */
class AuditEventLoggerSpec
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
      .appName("AuditEventLoggerSpec")
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

  /** Builds a minimal [[AuditEvent]] with configurable status and errorMessage. */
  private def makeEvent(
      status: AuditStatus,
      errorMessage: Option[String] = None
  ): AuditEvent =
    AuditEvent(
      runId           = "run-test-001",
      sourceName      = "financial-trades-source",
      sourceType      = "jdbc",
      pipelineStartTs = Instant.parse("2026-05-07T08:00:00Z"),
      pipelineEndTs   = Instant.parse("2026-05-07T08:05:30Z"),
      recordsRead     = 1000L,
      recordsWritten  = 980L,
      quarantineCount = 20L,
      bronzePath      = "s3://bronze/financial/trades/2026-05-07",
      checksum        = "a" * 64,
      status          = status,
      errorMessage    = errorMessage
    )

  // ---------------------------------------------------------------------------
  // Test 1 — Success event logged with all fields populated correctly
  // ---------------------------------------------------------------------------

  "DeltaAuditEventLogger" should
    "write a Success event to the audit Delta table with all fields correct" in {

    val tempPath = Files.createTempDirectory("audit-logger-spec-1-").toAbsolutePath.toString
    try {
      val logger = new DeltaAuditEventLogger(tempPath, spark)
      val event  = makeEvent(AuditStatus.Success)

      val result = logger.log(event)
      result shouldBe Right(())

      val ss      = spark
      val written = ss.read.format("delta").load(tempPath)

      written.count() shouldBe 1L

      val row = written.collect().head

      row.getAs[String]("run_id")       shouldBe event.runId
      row.getAs[String]("source_name")  shouldBe event.sourceName
      row.getAs[String]("status")       shouldBe "SUCCESS"
      row.getAs[String]("error_message") shouldBe null
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2 — Failure event with errorMessage populated
  // ---------------------------------------------------------------------------

  "DeltaAuditEventLogger" should
    "write a Failure event with error_message populated" in {

    val tempPath = Files.createTempDirectory("audit-logger-spec-2-").toAbsolutePath.toString
    try {
      val logger = new DeltaAuditEventLogger(tempPath, spark)
      val event  = makeEvent(AuditStatus.Failure, Some("connector timed out"))

      val result = logger.log(event)
      result shouldBe Right(())

      val ss      = spark
      val written = ss.read.format("delta").load(tempPath)

      written.count() shouldBe 1L

      val row = written.collect().head
      row.getAs[String]("status")        shouldBe "FAILURE"
      row.getAs[String]("error_message") shouldBe "connector timed out"
    } finally {
      deleteRecursively(new java.io.File(tempPath))
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3 — Append-only enforcement confirmed via delete attempt
  // ---------------------------------------------------------------------------

  "DeltaAuditEventLogger" should
    "set delta.appendOnly = true and prevent DeltaTable.delete()" in {

    val tempPath = Files.createTempDirectory("audit-logger-spec-3-").toAbsolutePath.toString
    try {
      val logger = new DeltaAuditEventLogger(tempPath, spark)
      val event  = makeEvent(AuditStatus.Success)

      val result = logger.log(event)
      result shouldBe Right(())

      // Confirm appendOnly property is set
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
  // Test 4 — NoOpAuditEventLogger always returns Right(())
  // ---------------------------------------------------------------------------

  "NoOpAuditEventLogger" should
    "return Right(()) for any AuditEvent without requiring a SparkSession" in {

    val event = makeEvent(AuditStatus.Partial, Some("some records quarantined"))

    NoOpAuditEventLogger.log(event) shouldBe Right(())
  }
}

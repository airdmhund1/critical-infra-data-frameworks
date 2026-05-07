package com.criticalinfra.engine

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.time.Instant

// =============================================================================
// AuditEventSpec — unit + Spark round-trip tests for AuditEvent data model
//
// Tests cover:
//   1. Success event construction — all 12 fields accessible and correct
//   2. Failure event construction — status and errorMessage correct
//   3. Partial event construction — quarantineCount > 0, errorMessage = None
//   4. AuditLogError.WriteError fields — path and cause accessible
//   5. AuditLogError.SchemaError fields — message accessible
//   6. Spark schema round-trip — single-row Parquet write-read, rowCount == 1
//      and run_id string column matches
// =============================================================================

/** Unit tests and a Spark Parquet round-trip test for the [[AuditEvent]] data model.
  *
  * Tests 1–5 require no Spark; they exercise the case-class and sealed-trait constructors
  * using plain ScalaTest assertions.  Test 6 starts a local `SparkSession` (created once
  * in `beforeAll` and stopped in `afterAll`) to verify that all [[AuditEvent]] fields can
  * be projected into a flat Spark schema and round-tripped through Parquet without error.
  */
class AuditEventSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Shared fixtures — instants and a canonical AuditEvent builder
  // ---------------------------------------------------------------------------

  private val startTs: Instant = Instant.parse("2026-05-07T08:00:00Z")
  private val endTs:   Instant = Instant.parse("2026-05-07T08:05:00Z")

  /** Builds a minimal valid [[AuditEvent]] with the supplied overrides. */
  private def baseEvent(
      status:         AuditStatus       = AuditStatus.Success,
      quarantineCount: Long             = 0L,
      errorMessage:   Option[String]    = None
  ): AuditEvent =
    AuditEvent(
      runId            = "run-abc-001",
      sourceName       = "trade-feed",
      sourceType       = "jdbc",
      pipelineStartTs  = startTs,
      pipelineEndTs    = endTs,
      recordsRead      = 1000L,
      recordsWritten   = 1000L - quarantineCount,
      quarantineCount  = quarantineCount,
      bronzePath       = "s3://bronze/financial/trades/2026-05-07",
      checksum         = "sha256:deadbeefdeadbeefdeadbeefdeadbeef" +
                           "deadbeefdeadbeefdeadbeefdeadbeef",
      status           = status,
      errorMessage     = errorMessage
    )

  // ---------------------------------------------------------------------------
  // SparkSession — used only by Test 6; created lazily and stopped in afterAll
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("AuditEventSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ---------------------------------------------------------------------------
  // Test 1 — Success event: all 12 fields accessible and equal to set values
  // ---------------------------------------------------------------------------

  "AuditEvent" should "expose all 12 fields when constructed with AuditStatus.Success" in {
    val event = baseEvent()

    event.runId            shouldBe "run-abc-001"
    event.sourceName       shouldBe "trade-feed"
    event.sourceType       shouldBe "jdbc"
    event.pipelineStartTs  shouldBe startTs
    event.pipelineEndTs    shouldBe endTs
    event.recordsRead      shouldBe 1000L
    event.recordsWritten   shouldBe 1000L
    event.quarantineCount  shouldBe 0L
    event.bronzePath       shouldBe "s3://bronze/financial/trades/2026-05-07"
    event.checksum         should have length 71   // "sha256:" prefix + 64 hex chars
    event.status           shouldBe AuditStatus.Success
    event.errorMessage     shouldBe None
  }

  // ---------------------------------------------------------------------------
  // Test 2 — Failure event: status and errorMessage are correct
  // ---------------------------------------------------------------------------

  "AuditEvent" should "record AuditStatus.Failure and a non-empty errorMessage" in {
    val event = baseEvent(
      status       = AuditStatus.Failure,
      errorMessage = Some("connector timed out")
    )

    event.status        shouldBe AuditStatus.Failure
    event.errorMessage  shouldBe Some("connector timed out")
  }

  // ---------------------------------------------------------------------------
  // Test 3 — Partial event: quarantineCount > 0, errorMessage = None
  // ---------------------------------------------------------------------------

  "AuditEvent" should "record AuditStatus.Partial with quarantineCount > 0 and no errorMessage" in {
    val event = baseEvent(
      status          = AuditStatus.Partial,
      quarantineCount = 42L
    )

    event.status          shouldBe AuditStatus.Partial
    event.quarantineCount should be > 0L
    event.errorMessage    shouldBe None
    // recordsWritten reflects the remainder after quarantine
    event.recordsWritten  shouldBe 958L
  }

  // ---------------------------------------------------------------------------
  // Test 4 — AuditLogError.WriteError: path and cause are accessible
  // ---------------------------------------------------------------------------

  "AuditLogError.WriteError" should "expose path and cause fields" in {
    val err = AuditLogError.WriteError("s3://audit-log/2026-05-07", "disk full")

    err.path  shouldBe "s3://audit-log/2026-05-07"
    err.cause shouldBe "disk full"
  }

  // ---------------------------------------------------------------------------
  // Test 5 — AuditLogError.SchemaError: message is accessible
  // ---------------------------------------------------------------------------

  "AuditLogError.SchemaError" should "expose the message field" in {
    val err = AuditLogError.SchemaError("missing column: run_id")

    err.message shouldBe "missing column: run_id"
  }

  // ---------------------------------------------------------------------------
  // Test 6 — Spark schema round-trip: write to Parquet, read back, assert row count
  //          and run_id column value
  //
  // AuditEvent cannot be directly encoded by Spark because it contains
  // java.time.Instant fields and a sealed-trait AuditStatus field.  We project
  // it to a flat tuple of primitives and use an explicit StructType schema so
  // the round-trip validates that every field can be serialised without error.
  // ---------------------------------------------------------------------------

  /** Flat Spark schema mirroring all 12 [[AuditEvent]] fields using only Spark-native types.
    *
    * `java.time.Instant` is stored as a `StringType` ISO-8601 string.
    * `AuditStatus` is stored as its `toString` value.
    * `Option[String]` is stored as a nullable `StringType`.
    */
  private val auditEventSchema: StructType = StructType(Seq(
    StructField("run_id",             StringType,  nullable = false),
    StructField("source_name",        StringType,  nullable = false),
    StructField("source_type",        StringType,  nullable = false),
    StructField("pipeline_start_ts",  StringType,  nullable = false),
    StructField("pipeline_end_ts",    StringType,  nullable = false),
    StructField("records_read",       LongType,    nullable = false),
    StructField("records_written",    LongType,    nullable = false),
    StructField("quarantine_count",   LongType,    nullable = false),
    StructField("bronze_path",        StringType,  nullable = false),
    StructField("checksum",           StringType,  nullable = false),
    StructField("status",             StringType,  nullable = false),
    StructField("error_message",      StringType,  nullable = true)
  ))

  /** Converts an [[AuditEvent]] to a Spark [[Row]] using only Spark-native types. */
  private def toRow(e: AuditEvent): Row = Row(
    e.runId,
    e.sourceName,
    e.sourceType,
    e.pipelineStartTs.toString,
    e.pipelineEndTs.toString,
    e.recordsRead,
    e.recordsWritten,
    e.quarantineCount,
    e.bronzePath,
    e.checksum,
    e.status.toString,
    e.errorMessage.orNull
  )

  "AuditEvent" should "round-trip through Parquet (write 1 row, read back 1 row, run_id matches)" in {
    val event   = baseEvent()
    val row     = toRow(event)
    val tempDir = Files.createTempDirectory("audit-event-spec-parquet-").toAbsolutePath.toString

    try {
      val ss = spark   // local val — stable identifier required for import in Scala 2.13

      // Write a single-row DataFrame to Parquet
      val df = ss.createDataFrame(
        ss.sparkContext.parallelize(Seq(row)),
        auditEventSchema
      )
      df.write.mode("overwrite").parquet(tempDir)

      // Read back and assert
      val read = ss.read.schema(auditEventSchema).parquet(tempDir)

      read.count() shouldBe 1L

      val firstRow = read.collect().head
      firstRow.getAs[String]("run_id") shouldBe event.runId
    } finally {
      // Clean up the temp directory
      def deleteRecursively(f: java.io.File): Unit = {
        if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
        f.delete()
        ()
      }
      deleteRecursively(new java.io.File(tempDir))
    }
  }
}

package com.criticalinfra.engine

import com.criticalinfra.config.SourceConfig
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate
import java.util.UUID

// =============================================================================
// BronzeLayerWriterSpec — unit tests for WriteResult and BronzeLayerWriter
//
// Tests cover:
//   1. WriteResult fields are accessible and structural equality works correctly
//   2. A test double implementing BronzeLayerWriter can be instantiated and
//      write() called, returning Right(WriteResult(...))
//   3. WriteResult.recordsWritten, path, partitionDate, and checksum are all
//      populated and accessible correctly
//   4. DefaultBronzeLayerWriter stub returns Right(WriteResult) without error
// =============================================================================

/** Unit tests for [[WriteResult]] and [[BronzeLayerWriter]].
  *
  * No SparkSession is required — all tests use null DataFrames (never inspected by the stub) and
  * verify the type-level and value-level contracts of [[WriteResult]] and [[BronzeLayerWriter]]
  * implementations without any Spark execution.
  */
class BronzeLayerWriterSpec extends AnyFlatSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Test 1 — WriteResult fields are accessible and equality works correctly
  // ---------------------------------------------------------------------------

  "WriteResult" should "expose all fields and satisfy structural equality" in {
    val date   = LocalDate.of(2026, 4, 29)
    val result = WriteResult(
      recordsWritten = 42L,
      path           = "s3://bronze/financial/trades/2026-04-29",
      partitionDate  = date,
      checksum       = "sha256:abc123"
    )

    result.recordsWritten shouldBe 42L
    result.path           shouldBe "s3://bronze/financial/trades/2026-04-29"
    result.partitionDate  shouldBe date
    result.checksum       shouldBe "sha256:abc123"
  }

  "WriteResult" should "support case-class copy and structural equality" in {
    val date    = LocalDate.of(2026, 4, 29)
    val original = WriteResult(10L, "/bronze/path", date, "checksum-xyz")
    val copy     = original.copy(recordsWritten = 20L)

    copy.recordsWritten shouldBe 20L
    copy.path           shouldBe original.path
    copy.partitionDate  shouldBe original.partitionDate
    copy.checksum       shouldBe original.checksum
    copy                should not equal original
  }

  // ---------------------------------------------------------------------------
  // Test 2 — A test double implementing BronzeLayerWriter returns Right(WriteResult)
  // ---------------------------------------------------------------------------

  "BronzeLayerWriter" should "allow a test double to be instantiated and write() called" in {
    val expectedDate   = LocalDate.of(2026, 4, 29)
    val expectedResult = WriteResult(100L, "/bronze/test", expectedDate, "abc")

    val testDouble = new BronzeLayerWriter {
      def write(
          data: DataFrame,
          config: SourceConfig,
          runId: UUID
      ): Either[StorageWriteError, WriteResult] =
        Right(expectedResult)
    }

    val result = testDouble.write(null, null, UUID.randomUUID())

    result.isRight shouldBe true
    result shouldBe Right(expectedResult)
  }

  "BronzeLayerWriter" should "allow a test double to return Left(StorageWriteError)" in {
    val expectedError = StorageWriteError("/bad/path", "permission denied")

    val failingDouble = new BronzeLayerWriter {
      def write(
          data: DataFrame,
          config: SourceConfig,
          runId: UUID
      ): Either[StorageWriteError, WriteResult] =
        Left(expectedError)
    }

    val result = failingDouble.write(null, null, UUID.randomUUID())

    result.isLeft shouldBe true
    result shouldBe Left(expectedError)
  }

  // ---------------------------------------------------------------------------
  // Test 3 — WriteResult fields are individually accessible and correctly typed
  // ---------------------------------------------------------------------------

  "WriteResult" should "have recordsWritten, path, partitionDate, and checksum all populated" in {
    val date   = LocalDate.now()
    val runId  = UUID.randomUUID()
    val result = WriteResult(
      recordsWritten = 8L,
      path           = s"s3://bronze/run-$runId",
      partitionDate  = date,
      checksum       = "sha256:deadbeef"
    )

    // All four fields are accessible and hold the expected values
    result.recordsWritten shouldBe 8L
    result.path           should startWith("s3://bronze/run-")
    result.partitionDate  shouldBe date
    result.checksum       shouldBe "sha256:deadbeef"

    // Types are correct at compile time — these are runtime confirmations
    result.recordsWritten shouldBe a[java.lang.Long]
    result.partitionDate  shouldBe a[LocalDate]
  }

  // ---------------------------------------------------------------------------
  // Test 4 — DefaultBronzeLayerWriter stub returns Right without error
  // ---------------------------------------------------------------------------

  "DefaultBronzeLayerWriter" should "return Right(WriteResult) with zero record count" in {
    val stub   = new DefaultBronzeLayerWriter
    val result = stub.write(null, null, UUID.randomUUID())

    result.isRight             shouldBe true
    result.toOption.get.recordsWritten shouldBe 0L
    result.toOption.get.path           shouldBe ""
    result.toOption.get.checksum       shouldBe ""
  }

  "DefaultBronzeLayerWriter" should "return a partitionDate of today" in {
    val stub   = new DefaultBronzeLayerWriter
    val before = LocalDate.now()
    val result = stub.write(null, null, UUID.randomUUID())
    val after  = LocalDate.now()

    result.isRight shouldBe true
    val partitionDate = result.toOption.get.partitionDate
    // partitionDate must be today (between before and after inclusive)
    partitionDate.isBefore(before) shouldBe false
    partitionDate.isAfter(after)   shouldBe false
  }
}

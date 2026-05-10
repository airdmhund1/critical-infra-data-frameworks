package com.criticalinfra.connectors.watermark

import com.criticalinfra.engine.ConnectorError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

// =============================================================================
// WatermarkSpec — unit tests for the Watermark sealed hierarchy
//
// No Spark dependency.  Tests cover:
//   - SQL filter generation for each concrete type
//   - Round-trip deserialisation via Watermark.fromRow
//   - Error cases for unknown type tags and absent value columns
// =============================================================================

class WatermarkSpec extends AnyFlatSpec with Matchers {

  private val sampleTs: Timestamp = Timestamp.valueOf("2024-01-15 12:30:00.0")

  // ---------------------------------------------------------------------------
  // 1. TimestampWatermark.toSqlFilter
  // ---------------------------------------------------------------------------

  "TimestampWatermark.toSqlFilter" should "produce a TIMESTAMP SQL predicate" in {
    val wm     = TimestampWatermark(sampleTs)
    val filter = wm.toSqlFilter("updated_at")
    filter shouldBe s"updated_at > TIMESTAMP '$sampleTs'"
  }

  // ---------------------------------------------------------------------------
  // 2. IntegerWatermark.toSqlFilter
  // ---------------------------------------------------------------------------

  "IntegerWatermark.toSqlFilter" should "produce an integer comparison SQL predicate" in {
    val wm     = IntegerWatermark(42L)
    val filter = wm.toSqlFilter("record_id")
    filter shouldBe "record_id > 42"
  }

  // ---------------------------------------------------------------------------
  // 3. fromRow — TIMESTAMP type with a present ts value
  // ---------------------------------------------------------------------------

  "Watermark.fromRow" should "return Right(TimestampWatermark) for type TIMESTAMP with a ts value" in {
    val result = Watermark.fromRow("TIMESTAMP", Some(sampleTs), None)
    result shouldBe Right(TimestampWatermark(sampleTs))
  }

  // ---------------------------------------------------------------------------
  // 4. fromRow — INTEGER type with a present int value
  // ---------------------------------------------------------------------------

  it should "return Right(IntegerWatermark) for type INTEGER with an int value" in {
    val result = Watermark.fromRow("INTEGER", None, Some(99L))
    result shouldBe Right(IntegerWatermark(99L))
  }

  // ---------------------------------------------------------------------------
  // 5. fromRow — unknown type tag
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) for an unknown type tag" in {
    val result = Watermark.fromRow("UNKNOWN", None, None)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.cause) shouldBe Left("Unknown watermark type: UNKNOWN")
  }

  // ---------------------------------------------------------------------------
  // 6. fromRow — TIMESTAMP type with absent ts value
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) for type TIMESTAMP when ts value is absent" in {
    val result = Watermark.fromRow("TIMESTAMP", None, None)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("WatermarkCheckpoint")
    result.left.map(e => e.cause.contains("TIMESTAMP")) shouldBe Left(true)
  }

  // ---------------------------------------------------------------------------
  // 7. fromRow — INTEGER type with absent int value
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) for type INTEGER when int value is absent" in {
    val result = Watermark.fromRow("INTEGER", None, None)
    result shouldBe a[Left[ConnectorError, _]]
    result.left.map(_.source) shouldBe Left("WatermarkCheckpoint")
    result.left.map(e => e.cause.contains("INTEGER")) shouldBe Left(true)
  }
}

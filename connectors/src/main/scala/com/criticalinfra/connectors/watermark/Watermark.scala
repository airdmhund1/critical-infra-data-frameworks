package com.criticalinfra.connectors.watermark

import com.criticalinfra.engine.ConnectorError

// =============================================================================
// Watermark — sealed hierarchy representing incremental-extraction high-water marks
//
// A Watermark captures the furthest-processed position in a source dataset so
// that subsequent pipeline runs can request only new or updated records.  Two
// concrete types are supported:
//
//   TimestampWatermark — for sources that expose a TIMESTAMP change column
//   IntegerWatermark   — for sources that expose a monotonically-increasing
//                        integer sequence / surrogate key column
//
// Both types project themselves into a SQL WHERE-clause fragment via toSqlFilter,
// which is injected into the source connector's extraction query.
// =============================================================================

/** Base type for all incremental-extraction watermarks.
  *
  * A [[Watermark]] represents the high-water mark of the last successful pipeline run for a given
  * source. It is persisted by [[WatermarkStore]] between runs and consumed by source connectors to
  * construct WHERE-clause predicates that restrict extraction to new or updated records.
  */
sealed trait Watermark {

  /** Returns a SQL fragment for use in a WHERE clause.
    *
    * Examples:
    *   - `updated_at > TIMESTAMP '2024-01-01 00:00:00.0'`
    *   - `record_id > 42`
    *
    * @param column
    *   The column name from `SourceConfig.ingestion.incrementalColumn`.
    * @return
    *   A SQL predicate string, suitable for embedding in a WHERE clause.
    */
  def toSqlFilter(column: String): String
}

/** Watermark backed by a [[java.sql.Timestamp]] value.
  *
  * Used when the source exposes a TIMESTAMP column (e.g. `updated_at`, `event_time`) as the
  * incremental extraction key.
  *
  * @param value
  *   The timestamp of the last-processed record.
  */
final case class TimestampWatermark(value: java.sql.Timestamp) extends Watermark {
  def toSqlFilter(column: String): String = s"$column > TIMESTAMP '$value'"
}

/** Watermark backed by a monotonically-increasing [[Long]] value.
  *
  * Used when the source exposes an integer sequence or surrogate key column (e.g. `id`, `seq_num`)
  * as the incremental extraction key.
  *
  * @param value
  *   The integer value of the last-processed record's key column.
  */
final case class IntegerWatermark(value: Long) extends Watermark {
  def toSqlFilter(column: String): String = s"$column > $value"
}

/** Companion object providing deserialisation from checkpoint table rows. */
object Watermark {

  private val TypeTimestamp = "TIMESTAMP"
  private val TypeInteger   = "INTEGER"

  /** Deserialises a [[Watermark]] from a checkpoint table row.
    *
    * The checkpoint table stores watermarks in a type-tagged, nullable-column format so that a
    * single table can hold both timestamp and integer watermarks without schema duplication.
    *
    * @param watermarkType
    *   The discriminator string stored in the `watermark_type` column: `"TIMESTAMP"` or
    *   `"INTEGER"`.
    * @param tsValue
    *   The value from the `watermark_ts` column; must be `Some` when `watermarkType` is
    *   `"TIMESTAMP"`.
    * @param intValue
    *   The value from the `watermark_int` column; must be `Some` when `watermarkType` is
    *   `"INTEGER"`.
    * @return
    *   `Right(watermark)` on success; `Left(ConnectorError)` when the type tag is unknown or the
    *   required value column is `None`.
    */
  def fromRow(
      watermarkType: String,
      tsValue: Option[java.sql.Timestamp],
      intValue: Option[Long]
  ): Either[ConnectorError, Watermark] =
    watermarkType match {
      case TypeTimestamp =>
        tsValue match {
          case Some(ts) => Right(TimestampWatermark(ts))
          case None =>
            Left(
              ConnectorError(
                "WatermarkCheckpoint",
                s"Watermark type TIMESTAMP requires a non-null timestamp value"
              )
            )
        }
      case TypeInteger =>
        intValue match {
          case Some(v) => Right(IntegerWatermark(v))
          case None =>
            Left(
              ConnectorError(
                "WatermarkCheckpoint",
                s"Watermark type INTEGER requires a non-null integer value"
              )
            )
        }
      case unknown =>
        Left(ConnectorError("WatermarkCheckpoint", s"Unknown watermark type: $unknown"))
    }
}

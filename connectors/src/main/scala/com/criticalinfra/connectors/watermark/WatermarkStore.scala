package com.criticalinfra.connectors.watermark

import com.criticalinfra.engine.ConnectorError
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Timestamp
import java.time.Instant
import scala.util.Try

// =============================================================================
// WatermarkStore — persistence abstraction for incremental-extraction watermarks
//
// The trait separates the watermark read/write contract from the Delta Lake
// implementation so that unit tests can inject an in-memory stub without
// requiring a live SparkSession.
//
// DeltaWatermarkStore is the production implementation.  It persists watermarks
// to a Delta table at a configurable path (SourceConfig.ingestion.watermarkStorage)
// and uses a MERGE operation so that repeated writes for the same sourceId are
// idempotent upserts rather than duplicating rows.
// =============================================================================

/** Persistence contract for incremental-extraction watermarks.
  *
  * Implementations must be safe to call from Spark driver code. The `read` and `write` operations
  * may interact with remote storage (Delta Lake, S3, ADLS, GCS) and should therefore wrap all I/O
  * in `scala.util.Try` and return `Left(ConnectorError)` on failure rather than propagating
  * exceptions.
  */
trait WatermarkStore {

  /** Reads the current watermark for `sourceId`.
    *
    * @param sourceId
    *   Unique identifier of the source system (typically `metadata.sourceId` from pipeline config).
    * @return
    *   `Right(Some(watermark))` if a checkpoint exists; `Right(None)` if the source has never been
    *   checkpointed; `Left(ConnectorError)` on storage or deserialisation failure.
    */
  def read(sourceId: String): Either[ConnectorError, Option[Watermark]]

  /** Persists (upserts) the watermark for `sourceId`.
    *
    * If a row already exists for `sourceId` it is updated in place. If no row exists a new row is
    * inserted. The operation must be atomic from the caller's perspective.
    *
    * @param sourceId
    *   Unique identifier of the source system.
    * @param watermark
    *   The new high-water mark to persist.
    * @return
    *   `Right(())` on success; `Left(ConnectorError)` on storage failure.
    */
  def write(sourceId: String, watermark: Watermark): Either[ConnectorError, Unit]
}

/** Delta Lake-backed [[WatermarkStore]].
  *
  * Stores watermarks in a Delta table at `checkpointPath`. The table is created automatically on
  * the first write if it does not already exist.
  *
  * ==Checkpoint table schema==
  * {{{
  *   source_id      STRING    NOT NULL
  *   watermark_type STRING    NOT NULL
  *   watermark_ts   TIMESTAMP           -- non-null for TIMESTAMP watermarks
  *   watermark_int  BIGINT              -- non-null for INTEGER watermarks
  *   updated_at     TIMESTAMP NOT NULL
  * }}}
  *
  * @param checkpointPath
  *   Delta table URI (e.g. `s3a://bucket/watermarks` or a local path for tests).
  * @param spark
  *   Active [[SparkSession]] configured with Delta Lake extensions.
  */
final class DeltaWatermarkStore(checkpointPath: String, spark: SparkSession)
    extends WatermarkStore {

  // ---------------------------------------------------------------------------
  // read
  // ---------------------------------------------------------------------------

  override def read(sourceId: String): Either[ConnectorError, Option[Watermark]] =
    Try {
      if (!DeltaTable.isDeltaTable(spark, checkpointPath)) {
        Right(None): Either[ConnectorError, Option[Watermark]]
      }
      else {
        val df = spark.read
          .format("delta")
          .load(checkpointPath)
          .filter(col("source_id") === sourceId)
          .orderBy(col("updated_at").desc)
          .limit(1)

        if (df.isEmpty) {
          Right(None): Either[ConnectorError, Option[Watermark]]
        }
        else {
          val row         = df.first()
          val wmType      = row.getString(row.fieldIndex("watermark_type"))
          val tsColIdx    = row.fieldIndex("watermark_ts")
          val intColIdx   = row.fieldIndex("watermark_int")
          val tsValueOpt  = if (row.isNullAt(tsColIdx)) None else Some(row.getTimestamp(tsColIdx))
          val intValueOpt = if (row.isNullAt(intColIdx)) None else Some(row.getLong(intColIdx))
          Watermark.fromRow(wmType, tsValueOpt, intValueOpt).map(Some(_))
        }
      }
    }.fold(
      ex => Left(ConnectorError("WatermarkCheckpoint", ex.getMessage)),
      identity
    )

  // ---------------------------------------------------------------------------
  // write
  // ---------------------------------------------------------------------------

  // Checkpoint table schema — used when building the single-row source DataFrame for MERGE.
  private val checkpointSchema: StructType = StructType(
    Seq(
      StructField("source_id", StringType, nullable = false),
      StructField("watermark_type", StringType, nullable = false),
      StructField("watermark_ts", TimestampType, nullable = true),
      StructField("watermark_int", LongType, nullable = true),
      StructField("updated_at", TimestampType, nullable = false)
    )
  )

  override def write(sourceId: String, watermark: Watermark): Either[ConnectorError, Unit] =
    Try {
      val now = Timestamp.from(Instant.now())

      val (wmType, tsValue, intValue): (String, Timestamp, java.lang.Long) = watermark match {
        case TimestampWatermark(ts) => ("TIMESTAMP", ts, null)
        case IntegerWatermark(v)    => ("INTEGER", null, v: java.lang.Long)
      }

      val sourceDF = spark.createDataFrame(
        java.util.Arrays.asList(Row(sourceId, wmType, tsValue, intValue, now)),
        checkpointSchema
      )

      if (!DeltaTable.isDeltaTable(spark, checkpointPath)) {
        // First write: create the table by writing the row as a Delta table
        sourceDF.write
          .format("delta")
          .mode("append")
          .option("path", checkpointPath)
          .save()
      }
      else {
        val target = DeltaTable.forPath(spark, checkpointPath)
        target
          .as("target")
          .merge(sourceDF.as("source"), "target.source_id = source.source_id")
          .whenMatched()
          .updateExpr(
            Map(
              "watermark_type" -> "source.watermark_type",
              "watermark_ts"   -> "source.watermark_ts",
              "watermark_int"  -> "source.watermark_int",
              "updated_at"     -> "source.updated_at"
            )
          )
          .whenNotMatched()
          .insertAll()
          .execute()
      }
    }.fold(
      ex => Left(ConnectorError("WatermarkCheckpoint", ex.getMessage)),
      _ => Right(())
    )
}

/** Companion object for [[DeltaWatermarkStore]]. */
object DeltaWatermarkStore {

  /** Constructs a [[DeltaWatermarkStore]] for the given `checkpointPath`.
    *
    * @param checkpointPath
    *   Delta table URI for watermark persistence.
    * @param spark
    *   Active [[SparkSession]] with Delta Lake extensions enabled.
    * @return
    *   A new [[DeltaWatermarkStore]] instance.
    */
  def apply(checkpointPath: String, spark: SparkSession): DeltaWatermarkStore =
    new DeltaWatermarkStore(checkpointPath, spark)
}

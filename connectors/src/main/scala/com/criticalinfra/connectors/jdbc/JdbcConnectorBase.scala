package com.criticalinfra.connectors.jdbc

import com.criticalinfra.config.{IngestionMode, SourceConfig}
import com.criticalinfra.connectors.RetryPolicy
import com.criticalinfra.connectors.watermark.{
  IntegerWatermark,
  TimestampWatermark,
  Watermark,
  WatermarkStore
}
import com.criticalinfra.engine.{ConnectorError, SourceConnector}
import org.apache.spark.sql.functions.{col, max => sparkMax}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

// =============================================================================
// JdbcConnectorBase — abstract JDBC connector wiring RetryPolicy and WatermarkStore
//
// Implements the full JDBC extraction lifecycle:
//   1. Validate configuration (incremental-mode required fields)
//   2. Test connectivity via DriverManager (with retry)
//   3. Resolve last watermark from WatermarkStore (incremental only)
//   4. Build Spark JDBC options map (dbtable, fetchsize, queryTimeout, partition opts)
//   5. Load DataFrame via spark.read.format("jdbc")
//
// Subclasses supply the driver-specific URL, driver class name, and table name.
// Parallel-read partitioning options default to None/disabled and may be
// overridden by subclasses that know a numeric partition column.
// =============================================================================

/** Abstract JDBC connector that wires [[RetryPolicy]] and [[WatermarkStore]] into a complete
  * extraction lifecycle.
  *
  * Subclasses must implement the three abstract methods that are driver-specific: [[jdbcUrl]],
  * [[driverClass]], and [[tableName]]. All other extraction logic — watermark resolution, query
  * construction, Spark JDBC option assembly, and watermark persistence — is implemented here and
  * shared across all JDBC connector implementations.
  *
  * @param watermarkStore
  *   Store used to read and write incremental-extraction high-water marks.
  * @param maxAttempts
  *   Maximum number of connection attempts via [[RetryPolicy]]. Default: 3.
  * @param retryDelayFn
  *   Function that receives a delay in milliseconds and performs the actual sleep. Inject a no-op
  *   (`_ => ()`) in unit tests to avoid real delays. Default: [[Thread.sleep]].
  */
abstract class JdbcConnectorBase(
    protected val watermarkStore: WatermarkStore,
    protected val maxAttempts: Int = 3,
    protected val retryDelayFn: Long => Unit = Thread.sleep
) extends SourceConnector {

  // ---------------------------------------------------------------------------
  // Abstract — subclasses must implement
  // ---------------------------------------------------------------------------

  /** Builds the full JDBC connection URL for this driver.
    *
    * Examples: `"jdbc:postgresql://host:5432/db"`, `"jdbc:h2:mem:testdb"`.
    *
    * @param config
    *   Pipeline configuration from which host, port, and database are read.
    * @return
    *   `Right(url)` on success; `Left(ConnectorError)` if required connection fields are absent.
    */
  protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String]

  /** Fully-qualified JDBC driver class name.
    *
    * Example: `"org.postgresql.Driver"`, `"org.h2.Driver"`.
    */
  protected def driverClass: String

  /** Source table or view to query.
    *
    * Example: `"public.trades"`, `"test_records"`.
    *
    * @param config
    *   Pipeline configuration; may be used to derive schema-qualified table names.
    */
  protected def tableName(config: SourceConfig): String

  // ---------------------------------------------------------------------------
  // Protected overridable — parallel-read partitioning (default: disabled)
  // ---------------------------------------------------------------------------

  /** Column name used to split the JDBC read into parallel partitions.
    *
    * Must be a numeric column. When `None` (the default), parallel partitioning is disabled and
    * Spark reads the table with a single JDBC task.
    */
  protected def partitionColumn(config: SourceConfig): Option[String] = None

  /** Inclusive lower bound on [[partitionColumn]] for parallel partitioning. */
  protected def partitionLowerBound(config: SourceConfig): Option[Long] = None

  /** Inclusive upper bound on [[partitionColumn]] for parallel partitioning. */
  protected def partitionUpperBound(config: SourceConfig): Option[Long] = None

  /** Driver-specific JDBC options merged into the options map after the base options.
    *
    * Subclasses override this to inject driver-specific connection properties such as Oracle Wallet
    * location or SSL certificate paths. The default implementation returns an empty map.
    *
    * @param config
    *   Pipeline configuration; may be inspected to select options conditionally.
    * @return
    *   Map of extra JDBC option key-value pairs to merge into the final options map.
    */
  protected def extraJdbcOptions(config: SourceConfig): Map[String, String] = Map.empty

  // ---------------------------------------------------------------------------
  // SourceConnector implementation
  // ---------------------------------------------------------------------------

  /** Extracts data from the JDBC source described by `config`.
    *
    * Full extraction flow:
    *   1. Validates configuration (incremental-mode required fields). 2. Calls [[testConnection]]
    *      to verify connectivity. 3. Resolves the last watermark from [[watermarkStore]]
    *      (incremental only). 4. Builds `dbtable` expression and Spark JDBC options. 5. Loads a
    *      DataFrame via `spark.read.format("jdbc")`.
    *
    * @param config
    *   Fully resolved pipeline configuration.
    * @param spark
    *   Active [[SparkSession]] owned by the ingestion engine.
    * @return
    *   `Right(dataFrame)` on success; `Left(ConnectorError)` on any failure.
    */
  override def extract(
      config: SourceConfig,
      spark: SparkSession
  ): Either[ConnectorError, DataFrame] =
    for {
      _          <- validateConfig(config)
      _          <- testConnection(config)
      url        <- jdbcUrl(config)
      dbtableVal <- resolveDbtable(config)
      opts = buildJdbcOptions(url, dbtableVal, config)
      df <- loadDataFrame(spark, opts, config.metadata.sourceId)
    } yield df

  /** Persists a new watermark after a successful Bronze write.
    *
    * For Full-refresh mode this is a no-op. For Incremental mode the maximum value of
    * [[SourceConfig.ingestion.incrementalColumn]] is computed from `data` and written to
    * [[watermarkStore]].
    *
    * @param config
    *   Pipeline configuration identifying the incremental column and storage path.
    * @param data
    *   The DataFrame that was successfully written to Bronze storage.
    * @return
    *   `Right(())` on success or when no watermark update is needed; `Left(ConnectorError)` on
    *   watermark write failure.
    */
  def persistWatermark(
      config: SourceConfig,
      data: DataFrame
  ): Either[ConnectorError, Unit] =
    config.ingestion.mode match {
      case IngestionMode.Full => Right(())
      case IngestionMode.Incremental =>
        val incrementalColumnOpt = config.ingestion.incrementalColumn
        val watermarkStorageOpt  = config.ingestion.watermarkStorage

        (incrementalColumnOpt, watermarkStorageOpt) match {
          case (None, _) => Right(())
          case (_, None) => Right(())
          case (Some(incrementalCol), Some(_)) =>
            computeNewWatermark(data, incrementalCol, config.metadata.sourceId).flatMap {
              case None          => Right(())
              case Some(newMark) => watermarkStore.write(config.metadata.sourceId, newMark)
            }
        }
    }

  // ---------------------------------------------------------------------------
  // testConnection — verifies connectivity via DriverManager with retry
  // ---------------------------------------------------------------------------

  /** Tests JDBC connectivity by opening and immediately closing a [[java.sql.Connection]].
    *
    * Uses no credentials — suitable for H2 in-memory databases and any JDBC URL that accepts
    * anonymous connections. Connection attempts are retried according to [[maxAttempts]] and
    * [[retryDelayFn]].
    *
    * @param config
    *   Pipeline configuration; [[jdbcUrl]] is called to obtain the connection URL.
    * @return
    *   `Right(())` on success; `Left(ConnectorError)` if all attempts fail.
    */
  def testConnection(config: SourceConfig): Either[ConnectorError, Unit] =
    RetryPolicy.retry(
      attempt = jdbcUrl(config).flatMap { url =>
        Try {
          Class.forName(driverClass)
          val conn = java.sql.DriverManager.getConnection(url)
          conn.close()
        }.fold(
          ex =>
            Left(
              ConnectorError(
                config.metadata.sourceId,
                s"Connection test failed: ${ex.getMessage}"
              )
            ),
          _ => Right(())
        )
      },
      maxAttempts = maxAttempts,
      delayFn = retryDelayFn
    )

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Validates configuration fields required for the chosen [[IngestionMode]]. */
  private def validateConfig(config: SourceConfig): Either[ConnectorError, Unit] =
    config.ingestion.mode match {
      case IngestionMode.Incremental =>
        if (config.ingestion.incrementalColumn.isEmpty)
          Left(
            ConnectorError(
              config.metadata.sourceId,
              "incrementalColumn is required for Incremental mode"
            )
          )
        else if (config.ingestion.watermarkStorage.isEmpty)
          Left(
            ConnectorError(
              config.metadata.sourceId,
              "watermarkStorage is required for Incremental mode"
            )
          )
        else Right(())
      case IngestionMode.Full => Right(())
    }

  /** Resolves the `dbtable` value: a subquery for incremental runs with a prior watermark, or the
    * plain table name for full-refresh and first-run incremental.
    */
  private def resolveDbtable(config: SourceConfig): Either[ConnectorError, String] =
    config.ingestion.mode match {
      case IngestionMode.Full        => Right(tableName(config))
      case IngestionMode.Incremental =>
        // incrementalColumn and watermarkStorage are guaranteed present by validateConfig
        val col = config.ingestion.incrementalColumn.get
        watermarkStore.read(config.metadata.sourceId).map {
          case None =>
            // First run — no prior watermark; read the whole table
            tableName(config)
          case Some(watermark) =>
            // Subsequent run — filter to records newer than the watermark
            s"(SELECT * FROM ${tableName(config)} WHERE ${watermark.toSqlFilter(col)}) t"
        }
    }

  /** Builds the complete Spark JDBC options map, including optional partition options. */
  private def buildJdbcOptions(
      url: String,
      dbtableVal: String,
      config: SourceConfig
  ): Map[String, String] = {
    val base: Map[String, String] = Map(
      "url"          -> url,
      "dbtable"      -> dbtableVal,
      "driver"       -> driverClass,
      "fetchsize"    -> config.ingestion.batchSize.toString,
      "queryTimeout" -> config.ingestion.timeout.toString
    )
    base ++ partitionOpts(config) ++ extraJdbcOptions(config)
  }

  /** Returns parallel-read partition options when all three partition fields are defined. Returns
    * an empty map when any of the three is [[None]] (Spark requires all four together).
    */
  private def partitionOpts(config: SourceConfig): Map[String, String] =
    (partitionColumn(config), partitionLowerBound(config), partitionUpperBound(config)) match {
      case (Some(pc), Some(lb), Some(ub)) =>
        Map(
          "partitionColumn" -> pc,
          "lowerBound"      -> lb.toString,
          "upperBound"      -> ub.toString,
          "numPartitions"   -> config.ingestion.parallelism.toString
        )
      case _ => Map.empty
    }

  /** Loads a [[DataFrame]] from the JDBC source using the assembled options map. */
  private def loadDataFrame(
      spark: SparkSession,
      opts: Map[String, String],
      sourceId: String
  ): Either[ConnectorError, DataFrame] =
    Try(spark.read.format("jdbc").options(opts).load())
      .fold(
        ex => Left(ConnectorError(sourceId, ex.getMessage)),
        df => Right(df)
      )

  /** Computes the new [[Watermark]] from the maximum value of `incrementalCol` in `data`.
    *
    * Returns `Right(None)` when `data` is empty (the agg result is null), so the caller can skip
    * the watermark write without error.
    */
  private def computeNewWatermark(
      data: DataFrame,
      incrementalCol: String,
      sourceId: String
  ): Either[ConnectorError, Option[Watermark]] =
    Try {
      val row = data.agg(sparkMax(col(incrementalCol))).first()
      if (row.isNullAt(0)) {
        Right(None): Either[ConnectorError, Option[Watermark]]
      }
      else {
        row.get(0) match {
          case ts: java.sql.Timestamp => Right(Some(TimestampWatermark(ts)))
          case l: Long                => Right(Some(IntegerWatermark(l)))
          case i: Int                 => Right(Some(IntegerWatermark(i.toLong)))
          case other =>
            Left(
              ConnectorError(
                sourceId,
                s"Unsupported watermark column type: ${other.getClass.getName}"
              )
            )
        }
      }
    }.fold(
      ex => Left(ConnectorError(sourceId, s"Failed to compute new watermark: ${ex.getMessage}")),
      identity
    )
}

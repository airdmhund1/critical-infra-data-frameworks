package com.criticalinfra.connectors.jdbc

import com.criticalinfra.config.SourceConfig
import com.criticalinfra.connectors.watermark.WatermarkStore
import com.criticalinfra.engine.ConnectorError

// =============================================================================
// PostgresJdbcConnector — PostgreSQL JDBC connector
//
// Extends JdbcConnectorBase with PostgreSQL-specific URL construction, four
// SSL/TLS enforcement modes, and JSONB column transparency notes for operators.
//
// The PostgreSQL driver (org.postgresql:postgresql) is on the compile classpath
// (BSD-2-Clause licensed, safe to bundle). Unlike OracleJdbcConnector, no
// runtime jar-loading guard is required.
// =============================================================================

/** PostgreSQL JDBC connector.
  *
  * Generates connection URLs in the standard PostgreSQL JDBC format:
  * {{{
  *   jdbc:postgresql://host:port/database
  * }}}
  *
  * ==SSL/TLS modes==
  *
  * The `sslmode` JDBC property is always injected into the Spark options map via
  * [[extraJdbcOptions]]. Choose the mode that matches your security posture:
  *
  *   - '''`Disable`''' — no encryption; data is transmitted in plaintext. Use only on trusted
  *     private networks where the threat model does not require transport-level security. Never
  *     appropriate for regulated environments.
  *   - '''`Require`''' (default) — an encrypted TLS channel is established, but the server's
  *     certificate is not validated against a CA. Protects against passive eavesdropping; does not
  *     protect against an active man-in-the-middle attacker who can intercept the TCP connection.
  *   - '''`VerifyCa`''' — encrypted + the server's certificate is validated against the CA bundle
  *     specified in the `sslrootcert` JDBC property. Prevents impersonation by servers with
  *     certificates signed by untrusted CAs. Requires the CA cert path to be supplied (see Known
  *     limitations below).
  *   - '''`VerifyFull`''' — encrypted + CA validated + the server certificate's Common Name (CN) or
  *     Subject Alternative Name (SAN) is verified against the hostname in the JDBC URL. Provides
  *     the strongest guarantee and is recommended for all production deployments in regulated
  *     sectors.
  *
  * ==JSONB column handling==
  *
  * The PostgreSQL JDBC driver maps `JSONB` columns to `java.lang.String` by calling
  * `PGobject.getValue()` internally. Spark's JDBC reader therefore infers `StringType` for `JSONB`
  * columns automatically — no custom type mapping or deserializer is required on the connector
  * side. Applications can parse the returned JSON string downstream using Spark SQL's `from_json`
  * function or equivalent:
  * {{{
  *   df.withColumn("payload_parsed", from_json(col("payload"), schema))
  * }}}
  * `JSON` columns (non-binary text JSON) behave identically to `JSONB` at the JDBC layer.
  *
  * ==Known limitations==
  *
  *   - '''SSL client certificates''' (`sslcert`, `sslkey`) are not configurable through this
  *     connector's constructor. If mutual TLS (mTLS) is required, pass the relevant JDBC properties
  *     via the connection properties map resolved from the `credentialsRef`-backed secrets manager
  *     entry.
  *   - '''`VerifyCa` and `VerifyFull` require `sslrootcert`.''' These modes require the path to the
  *     CA certificate bundle to be set as the `sslrootcert` JDBC property. This property is not
  *     currently wired into [[extraJdbcOptions]]. To supply it, extend `PostgresJdbcConnector` and
  *     override `extraJdbcOptions` to merge in the `sslrootcert` path resolved from the secrets
  *     manager, or pass it via the JDBC connection properties through `credentialsRef`.
  *
  * @param table
  *   Source table or view name (schema-qualified if necessary, e.g. `"public.trades"`).
  * @param watermarkStore
  *   Store used to read and write incremental-extraction high-water marks.
  * @param maxAttempts
  *   Maximum number of connection attempts via [[com.criticalinfra.connectors.RetryPolicy]].
  *   Default: 3.
  * @param retryDelayFn
  *   Function that receives a delay in milliseconds and performs the actual sleep. Inject a no-op
  *   (`_ => ()`) in unit tests to avoid real delays. Default: [[Thread.sleep]].
  * @param sslMode
  *   SSL/TLS enforcement mode injected as the `sslmode` JDBC property. See [[SslMode]] for the four
  *   available options and their security implications. Default:
  *   [[PostgresJdbcConnector.SslMode.Require]].
  * @param partitionCol
  *   Column name used to split the JDBC read into parallel Spark partitions. Must be a numeric
  *   column. When `None` (the default), parallel partitioning is disabled and Spark reads the table
  *   with a single JDBC task.
  * @param partitionLower
  *   Inclusive lower bound for the `partitionCol` range. Must be set when `partitionCol` is `Some`.
  * @param partitionUpper
  *   Inclusive upper bound for the `partitionCol` range. Must be set when `partitionCol` is `Some`.
  */
class PostgresJdbcConnector(
    private val table: String,
    watermarkStore: WatermarkStore,
    maxAttempts: Int = 3,
    retryDelayFn: Long => Unit = Thread.sleep,
    private val sslMode: PostgresJdbcConnector.SslMode = PostgresJdbcConnector.SslMode.Require,
    private val partitionCol: Option[String] = None,
    private val partitionLower: Option[Long] = None,
    private val partitionUpper: Option[Long] = None
) extends JdbcConnectorBase(watermarkStore, maxAttempts, retryDelayFn) {

  // ---------------------------------------------------------------------------
  // Abstract method implementations
  // ---------------------------------------------------------------------------

  /** Fully-qualified JDBC driver class name for the PostgreSQL driver.
    *
    * The string constant `"org.postgresql.Driver"` is returned without loading the class — the
    * class is registered automatically by the PostgreSQL driver's `java.sql.DriverManager`
    * service-loader mechanism when the jar is on the classpath.
    */
  override protected def driverClass: String = "org.postgresql.Driver"

  /** Source table or view name, returned directly from the constructor parameter.
    *
    * @param config
    *   Pipeline configuration (not used; the table name is set at construction time).
    * @return
    *   The table name passed to the constructor.
    */
  override protected def tableName(config: SourceConfig): String = table

  /** Builds the PostgreSQL JDBC connection URL.
    *
    * Requires `host`, `port`, and `database` to be present in `config.connection`. Returns
    * `Left(ConnectorError)` if any of the three fields is absent.
    *
    * @param config
    *   Pipeline configuration from which host, port, and database are read.
    * @return
    *   `Right("jdbc:postgresql://host:port/database")` on success; `Left(ConnectorError)` if host,
    *   port, or database is absent.
    */
  override protected def jdbcUrl(config: SourceConfig): Either[ConnectorError, String] =
    (config.connection.host, config.connection.port, config.connection.database) match {
      case (Some(host), Some(port), Some(database)) =>
        Right(s"jdbc:postgresql://$host:$port/$database")
      case _ =>
        Left(
          ConnectorError(
            config.metadata.sourceId,
            "Postgres connector requires host, port, and database in the connection configuration"
          )
        )
    }

  // ---------------------------------------------------------------------------
  // Parallel-read partitioning overrides
  // ---------------------------------------------------------------------------

  /** Column name used to split the JDBC read into parallel Spark partitions.
    *
    * Must be a numeric column. When `None` (the default), parallel partitioning is disabled.
    *
    * @param config
    *   Pipeline configuration (not used; the value is set at construction time).
    */
  override protected def partitionColumn(config: SourceConfig): Option[String] = partitionCol

  /** Inclusive lower bound on [[partitionColumn]] for parallel partitioning.
    *
    * @param config
    *   Pipeline configuration (not used; the value is set at construction time).
    */
  override protected def partitionLowerBound(config: SourceConfig): Option[Long] = partitionLower

  /** Inclusive upper bound on [[partitionColumn]] for parallel partitioning.
    *
    * @param config
    *   Pipeline configuration (not used; the value is set at construction time).
    */
  override protected def partitionUpperBound(config: SourceConfig): Option[Long] = partitionUpper

  // ---------------------------------------------------------------------------
  // SSL/TLS — driver-specific JDBC options
  // ---------------------------------------------------------------------------

  /** Injects the `sslmode` JDBC property for every connection.
    *
    * The `sslmode` property is always present in the returned map — there is no "absent" case. The
    * default [[SslMode.Require]] enforces an encrypted channel unless the constructor caller
    * explicitly opts down to [[SslMode.Disable]] or opts up to [[SslMode.VerifyCa]] /
    * [[SslMode.VerifyFull]].
    *
    * @param config
    *   Pipeline configuration (not used; the SSL mode is set at construction time).
    * @return
    *   Map containing the single key `"sslmode"` with the JDBC string value for the configured
    *   mode.
    */
  override protected def extraJdbcOptions(config: SourceConfig): Map[String, String] =
    Map("sslmode" -> sslMode.jdbcValue)
}

object PostgresJdbcConnector {

  /** SSL/TLS enforcement mode for the PostgreSQL JDBC connection.
    *
    * Maps directly to the PostgreSQL JDBC driver's `sslmode` connection property. See the
    * class-level ScalaDoc on [[PostgresJdbcConnector]] for full security implications of each mode.
    */
  sealed abstract class SslMode(val jdbcValue: String)

  object SslMode {

    /** No encryption — plaintext only. Use only on trusted private networks. */
    case object Disable extends SslMode("disable")

    /** Encrypted channel; server certificate is not validated. Default. */
    case object Require extends SslMode("require")

    /** Encrypted + server CA certificate validated against `sslrootcert`. */
    case object VerifyCa extends SslMode("verify-ca")

    /** Encrypted + CA validated + hostname verified. Recommended for production. */
    case object VerifyFull extends SslMode("verify-full")
  }
}

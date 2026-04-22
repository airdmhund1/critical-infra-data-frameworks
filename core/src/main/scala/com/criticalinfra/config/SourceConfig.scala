package com.criticalinfra.config

// =============================================================================
// SourceConfig — domain model mirroring schemas/source-config-v1.json
//
// Every section of the JSON Schema is represented as a final case class.
// Enum-valued schema fields are represented as sealed trait hierarchies so
// that exhaustiveness checking is enforced at compile time.
//
// Optional JSON Schema fields map to Option[T].
// Fields that carry a schema-level default are given a matching default
// parameter value here so callers need not supply them explicitly.
// =============================================================================

// -----------------------------------------------------------------------------
// Sector enum
// -----------------------------------------------------------------------------

/** Regulated sector the data source belongs to, as defined in the schema. */
sealed trait Sector

object Sector {

  /** U.S. financial services sector (Dodd-Frank applicable). */
  case object FinancialServices extends Sector

  /** U.S. energy infrastructure sector (NERC CIP applicable). */
  case object Energy extends Sector

  /** Healthcare sector. */
  case object Healthcare extends Sector

  /** Government sector. */
  case object Government extends Sector
}

// -----------------------------------------------------------------------------
// Environment enum
// -----------------------------------------------------------------------------

/** Deployment environment controlling retention policies and alert severity. */
sealed trait Environment

object Environment {

  /** Development environment; relaxed compliance enforcement. */
  case object Dev extends Environment

  /** Staging environment; compliance enforcement required. */
  case object Staging extends Environment

  /** Production environment; full compliance enforcement required. */
  case object Prod extends Environment
}

// -----------------------------------------------------------------------------
// ConnectionType enum
// -----------------------------------------------------------------------------

/** Source system type that determines which connector class the engine loads. */
sealed trait ConnectionType

object ConnectionType {

  /** JDBC-connected relational database source. */
  case object Jdbc extends ConnectionType

  /** File system or object-store source. */
  case object File extends ConnectionType

  /** Apache Kafka streaming source. */
  case object Kafka extends ConnectionType

  /** HTTP API source. */
  case object Api extends ConnectionType
}

// -----------------------------------------------------------------------------
// JdbcDriver enum
// -----------------------------------------------------------------------------

/** JDBC driver variant to load when the connection type is jdbc. */
sealed trait JdbcDriver

object JdbcDriver {

  /** Oracle Database JDBC driver. */
  case object Oracle extends JdbcDriver

  /** PostgreSQL JDBC driver. */
  case object Postgres extends JdbcDriver

  /** Teradata JDBC driver. */
  case object Teradata extends JdbcDriver
}

// -----------------------------------------------------------------------------
// FileFormat enum
// -----------------------------------------------------------------------------

/** File format to parse when the connection type is file. */
sealed trait FileFormat

object FileFormat {

  /** Comma-separated values text format. */
  case object Csv extends FileFormat

  /** Apache Parquet columnar binary format. */
  case object Parquet extends FileFormat

  /** JSON text format. */
  case object Json extends FileFormat

  /** Apache Avro binary serialisation format. */
  case object Avro extends FileFormat
}

// -----------------------------------------------------------------------------
// IngestionMode enum
// -----------------------------------------------------------------------------

/** Extraction strategy for a pipeline run. */
sealed trait IngestionMode

object IngestionMode {

  /** Full load: all available data is extracted on every run. */
  case object Full extends IngestionMode

  /** Incremental load: only records added or modified since the last watermark are extracted. */
  case object Incremental extends IngestionMode
}

// -----------------------------------------------------------------------------
// SchemaEnforcementMode enum
// -----------------------------------------------------------------------------

/** Strictness level applied when schema enforcement is enabled. */
sealed trait SchemaEnforcementMode

object SchemaEnforcementMode {

  /** Reject any record with an unexpected column or type mismatch; pipeline aborts on violation. */
  case object Strict extends SchemaEnforcementMode

  /** Route records with schema violations to quarantine and continue processing the remainder. */
  case object Permissive extends SchemaEnforcementMode
}

// -----------------------------------------------------------------------------
// StorageLayer enum
// -----------------------------------------------------------------------------

/** Target lakehouse layer for pipeline output. */
sealed trait StorageLayer

object StorageLayer {

  /** Raw, immutable Bronze layer; the only layer supported by the ingestion engine in v0.1.0. */
  case object Bronze extends StorageLayer

  /** Validated and conformed Silver layer; written by downstream processes. */
  case object Silver extends StorageLayer

  /** Consumption-optimised Gold layer; written by downstream processes. */
  case object Gold extends StorageLayer
}

// -----------------------------------------------------------------------------
// StorageFormat enum
// -----------------------------------------------------------------------------

/** Lakehouse storage format providing ACID transactions, time travel, and schema evolution. */
sealed trait StorageFormat

object StorageFormat {

  /** Delta Lake format. */
  case object Delta extends StorageFormat

  /** Apache Iceberg format. */
  case object Iceberg extends StorageFormat
}

// =============================================================================
// Section case classes
// =============================================================================

/** Identity and classification of a data source.
  *
  * Written to every audit log entry and the lineage catalog for every pipeline run. The `sourceId`
  * is the primary key in the lineage catalog.
  *
  * @param sourceId
  *   Machine-readable unique identifier (lowercase, hyphens only).
  * @param sourceName
  *   Human-readable display name shown in dashboards and alerts.
  * @param sector
  *   Regulated sector this source belongs to.
  * @param owner
  *   Team or individual accountable for this source; used for alert routing.
  * @param environment
  *   Deployment environment controlling retention and alert severity.
  * @param tags
  *   Arbitrary labels for grouping sources in dashboards.
  */
final case class Metadata(
    sourceId: String,
    sourceName: String,
    sector: Sector,
    owner: String,
    environment: Environment,
    tags: List[String] = List.empty
)

/** Connectivity details for a source system.
  *
  * All credential material is resolved at runtime via `credentialsRef` — no literal secret is ever
  * present in this model. Conditional fields are present as `Option` because their applicability
  * depends on the `connectionType` value; the loader validates that required conditionals are
  * populated.
  *
  * @param connectionType
  *   Source system type determining which connector class the engine loads.
  * @param credentialsRef
  *   Path to credentials in the external secrets manager (Vault or AWS KMS).
  * @param host
  *   Hostname or IP of the source server (required for jdbc and api types).
  * @param port
  *   Port number of the source server (required for jdbc and api types).
  * @param database
  *   Database or catalog name on the source server (required for jdbc type).
  * @param jdbcDriver
  *   JDBC driver to load (required for jdbc type).
  * @param filePath
  *   Absolute path or URI to the source file or directory (required for file type).
  * @param fileFormat
  *   File format to parse (required for file type).
  * @param kafkaTopic
  *   Kafka topic name to consume from (required for kafka type).
  * @param kafkaBootstrapServers
  *   Comma-separated list of Kafka broker addresses (required for kafka type).
  */
final case class Connection(
    connectionType: ConnectionType,
    credentialsRef: String,
    host: Option[String] = None,
    port: Option[Int] = None,
    database: Option[String] = None,
    jdbcDriver: Option[JdbcDriver] = None,
    filePath: Option[String] = None,
    fileFormat: Option[FileFormat] = None,
    kafkaTopic: Option[String] = None,
    kafkaBootstrapServers: Option[String] = None
)

/** Extraction strategy, parallelism, scheduling, and timeout for a pipeline run.
  *
  * @param mode
  *   Extraction strategy: full reload or incremental watermark-based pull.
  * @param incrementalColumn
  *   Column used to determine new or changed records (required when mode is Incremental).
  * @param watermarkStorage
  *   URI where the engine persists the last-seen watermark value (required when mode is
  *   Incremental).
  * @param batchSize
  *   Number of rows fetched per JDBC round-trip or file chunk. Default: 10000.
  * @param parallelism
  *   Number of concurrent Spark tasks for extraction. Default: 4.
  * @param schedule
  *   Cron expression defining automatic trigger schedule; absent means external orchestrator
  *   trigger.
  * @param timeout
  *   Maximum wall-clock seconds before the engine aborts extraction. Default: 3600.
  */
final case class Ingestion(
    mode: IngestionMode,
    incrementalColumn: Option[String] = None,
    watermarkStorage: Option[String] = None,
    batchSize: Int = 10000,
    parallelism: Int = 4,
    schedule: Option[String] = None,
    timeout: Int = 3600
)

/** Controls whether extracted records are validated against a declared schema before storage.
  *
  * When `enabled` is true, both `mode` and `registryRef` must be supplied; the loader enforces this
  * constraint and returns a `SchemaValidationError` if either is absent.
  *
  * @param enabled
  *   If false, schema validation is skipped and all records proceed to storage.
  * @param mode
  *   Enforcement strictness (required when enabled is true).
  * @param registryRef
  *   Path to the JSON Schema document in the schema registry (required when enabled is true).
  */
final case class SchemaEnforcement(
    enabled: Boolean,
    mode: Option[SchemaEnforcementMode] = None,
    registryRef: Option[String] = None
)

/** Phase 2 placeholder for data quality rule configuration.
  *
  * In v0.1.0 the only valid configuration is `enabled = false`. The engine ignores this section
  * entirely. Rule definitions will be added in the v0.2.0 schema update.
  *
  * @param enabled
  *   Must be false in v0.1.0; setting to true produces a validator error until the v0.2.0 engine is
  *   deployed. Default: false.
  */
final case class QualityRules(
    enabled: Boolean = false
)

/** Defines where failed records are written and how errors are classified.
  *
  * No record is ever silently dropped — quarantined records carry their failure reason so they can
  * be reviewed and replayed. When `enabled` is true, `path` must be supplied and must differ from
  * the Bronze storage path.
  *
  * @param enabled
  *   If false, failed records are discarded rather than quarantined.
  * @param path
  *   URI of the quarantine storage location (required when enabled is true).
  * @param retentionDays
  *   Days quarantined records are retained before deletion. Default: 90.
  * @param errorClassification
  *   If true, each quarantined record is tagged with a structured error code. Default: true.
  */
final case class Quarantine(
    enabled: Boolean,
    path: Option[String] = None,
    retentionDays: Int = 90,
    errorClassification: Boolean = true
)

/** Defines where and how successfully validated records are written to the lakehouse.
  *
  * In v0.1.0 the ingestion engine writes to the Bronze layer only.
  *
  * @param layer
  *   Target lakehouse layer.
  * @param format
  *   Storage format (Delta Lake or Apache Iceberg).
  * @param path
  *   URI of the target storage location.
  * @param partitionBy
  *   Column names used to partition the output dataset. Default: empty list.
  * @param compactionEnabled
  *   If true, the engine runs a compaction step after each write. Default: false.
  */
final case class Storage(
    layer: StorageLayer,
    format: StorageFormat,
    path: String,
    partitionBy: List[String] = List.empty,
    compactionEnabled: Boolean = false
)

/** Controls the Prometheus metrics endpoint and SLA alerting emitted during a pipeline run.
  *
  * @param metricsEnabled
  *   If true, the engine exposes a Prometheus metrics endpoint.
  * @param prometheusPort
  *   Port for the Prometheus endpoint (required when metricsEnabled is true). Default: 9090.
  * @param slaThresholdSeconds
  *   Maximum acceptable wall-clock duration in seconds; exceeding this emits an SLA breach event.
  * @param alertOnFailure
  *   If true, the engine emits an alert when the pipeline terminates with failure status. Default:
  *   true.
  */
final case class Monitoring(
    metricsEnabled: Boolean,
    prometheusPort: Int = 9090,
    slaThresholdSeconds: Option[Int] = None,
    alertOnFailure: Boolean = true
)

/** Controls the audit trail written to the lineage catalog per pipeline run.
  *
  * Directly supports Dodd-Frank, NERC CIP, and NIST CSF compliance requirements. Must be enabled in
  * staging and production environments.
  *
  * @param enabled
  *   If false, no audit records are written.
  * @param lineageTracking
  *   If true, records source-to-target lineage for every run. Default: true.
  * @param immutableRawEnabled
  *   If true, the Bronze layer path is enforced as write-once. Default: true.
  * @param retentionDays
  *   Days audit records are retained. Default: 2555 (7 years, matching U.S. financial services
  *   requirements).
  */
final case class Audit(
    enabled: Boolean,
    lineageTracking: Boolean = true,
    immutableRawEnabled: Boolean = true,
    retentionDays: Int = 2555
)

// =============================================================================
// Top-level SourceConfig
// =============================================================================

/** Root domain model for a YAML-driven source configuration file.
  *
  * Mirrors the structure of `schemas/source-config-v1.json`. The ingestion engine reads a validated
  * instance of this class at pipeline startup to drive all subsequent extraction, validation,
  * storage, and audit behaviour — no source-specific code is required.
  *
  * @param schemaVersion
  *   Version of the schema specification this config conforms to (must be "1.0").
  * @param metadata
  *   Identity and classification of the data source.
  * @param connection
  *   Connectivity details for the source system.
  * @param ingestion
  *   Extraction strategy, parallelism, scheduling, and timeout.
  * @param schemaEnforcement
  *   Schema validation configuration.
  * @param qualityRules
  *   Phase 2 quality rules placeholder (must have enabled = false in v0.1.0).
  * @param quarantine
  *   Quarantine storage and error classification configuration.
  * @param storage
  *   Lakehouse storage destination and write settings.
  * @param monitoring
  *   Prometheus metrics endpoint and SLA alerting settings.
  * @param audit
  *   Audit trail and lineage tracking settings.
  */
final case class SourceConfig(
    schemaVersion: String,
    metadata: Metadata,
    connection: Connection,
    ingestion: Ingestion,
    schemaEnforcement: SchemaEnforcement,
    qualityRules: QualityRules,
    quarantine: Quarantine,
    storage: Storage,
    monitoring: Monitoring,
    audit: Audit
)

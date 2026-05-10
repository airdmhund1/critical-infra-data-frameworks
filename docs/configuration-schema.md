# Configuration Schema Specification

**Schema version**: 1.0  
**Document status**: Authoritative  
**Applies to**: v0.1.0 and later

---

## Who this is for

Engineers implementing a source connector, the ingestion engine, or a config validator. This document is the contract: every field listed here is either read by the engine at runtime or reserved for a future engine version. Do not add fields outside this schema — the validator will reject unknown keys.

## What you will find here

Field-level reference for all ten top-level schema sections, with a YAML snippet per section and a complete skeleton at the end.

---

## Security note

Config files are safe to commit to version control because they contain **no credentials**. Every secret — database passwords, API tokens, TLS private keys — is resolved at runtime from an external secrets manager. Use the `credentialsRef` field to point at the path in HashiCorp Vault or AWS KMS where the credentials live. If you find a literal password, token, or private key anywhere in a config file, treat it as a security incident.

---

## 1. `schemaVersion`

The version of this schema specification the config file conforms to. The runtime validator checks this value against its own supported version list and rejects configs that do not match.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `schemaVersion` | string | Yes | — | Must be `"1.0"`. Quoted string, not a number. |

```yaml
schemaVersion: "1.0"
```

---

## 2. `metadata`

Identity and classification of the data source being configured. These fields populate the lineage catalog and are written to every audit log entry produced by this pipeline run.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `sourceId` | string | Yes | — | Machine-readable unique identifier for this source; used as the primary key in the lineage catalog. Use lowercase, hyphens only (e.g., `fin-trade-oracle-prod`). |
| `sourceName` | string | Yes | — | Human-readable display name shown in dashboards and alerts. |
| `sector` | enum | Yes | — | Regulated sector this source belongs to. One of: `financial-services`, `energy`, `healthcare`, `government`. |
| `owner` | string | Yes | — | Team or individual accountable for this source (e.g., `risk-data-team`). Appears in alert routing. |
| `environment` | enum | Yes | — | Deployment environment. One of: `dev`, `staging`, `prod`. Controls default retention policies and alert severity. |
| `tags` | list[string] | No | `[]` | Arbitrary labels for grouping sources in dashboards (e.g., `[dodd-frank, tier-1]`). |

```yaml
metadata:
  sourceId: "fin-trade-oracle-prod"
  sourceName: "Daily Trade Reporting - Oracle"
  sector: "financial-services"
  owner: "risk-data-team"
  environment: "prod"
  tags:
    - "dodd-frank"
    - "tier-1"
```

---

## 3. `connection`

Connectivity details for the source system. Fields are conditional on `type` — the validator enforces that JDBC-required fields are present when `type: jdbc` and raises an error if JDBC fields appear under a `file` config.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `type` | enum | Yes | — | Source system type. One of: `jdbc`, `file`, `kafka`, `api`. Determines which connector class the engine loads. |
| `credentialsRef` | string | Yes | — | Path to credentials in the external secrets manager (e.g., `vault://secret/data/tradedb` or `aws-kms://arn:aws:secretsmanager:us-east-1:123456789012:secret:tradedb`). Never a literal credential. |
| `host` | string | Conditional | — | Hostname or IP of the source server. Required when `type` is `jdbc` or `api`. |
| `port` | integer | Conditional | — | Port number of the source server. Required when `type` is `jdbc` or `api`. |
| `database` | string | Conditional | — | Database or catalog name on the source server. Required when `type` is `jdbc`. |
| `jdbcDriver` | enum | Conditional | — | JDBC driver to load. Required when `type` is `jdbc`. One of: `oracle`, `postgres`, `teradata`. |
| `filePath` | string | Conditional | — | Absolute path or URI to the source file or directory. Required when `type` is `file`. Supports `s3://`, `gs://`, `abfss://`, and local paths. |
| `fileFormat` | enum | Conditional | — | File format to parse. Required when `type` is `file`. One of: `csv`, `parquet`, `json`, `avro`. |
| `kafkaTopic` | string | Conditional | — | Kafka topic name to consume from. Required when `type` is `kafka`. |
| `kafkaBootstrapServers` | string | Conditional | — | Comma-separated list of Kafka broker addresses (e.g., `broker1:9092,broker2:9092`). Required when `type` is `kafka`. |

```yaml
# JDBC example
connection:
  type: "jdbc"
  credentialsRef: "vault://secret/data/oracle-tradedb-prod"
  host: "oracle-prod.internal.example.com"
  port: 1521
  database: "TRADEDB"
  jdbcDriver: "oracle"

# File example
connection:
  type: "file"
  credentialsRef: "aws-kms://arn:aws:secretsmanager:us-east-1:123456789012:secret:s3-readonly"
  filePath: "s3://datalake-raw/energy/meter-readings/"
  fileFormat: "parquet"
```

---

## 4. `ingestion`

Controls how data is extracted from the source: whether a full reload or an incremental pull, how parallelism is managed, and when the pipeline runs.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `mode` | enum | Yes | — | Extraction strategy. `full` loads all available data; `incremental` loads only records added or modified since the last watermark. |
| `incrementalColumn` | string | Conditional | — | Column used to determine new or changed records. Required when `mode` is `incremental`. Must be a monotonically increasing timestamp or integer (e.g., `last_modified_ts`, `trade_id`). |
| `watermarkStorage` | string | Conditional | — | URI where the engine persists the last-seen watermark value between runs (e.g., `s3://pipeline-state/watermarks/fin-trade-oracle-prod`). Required when `mode` is `incremental`. |
| `batchSize` | integer | No | `10000` | Number of rows fetched per JDBC round-trip or file chunk. Tune based on source server capacity and network bandwidth. |
| `parallelism` | integer | No | `4` | Number of concurrent Spark tasks for extraction. Increase for large sources with partition-friendly schemas; decrease if source server becomes a bottleneck. |
| `schedule` | string | No | — | Cron expression defining when the pipeline runs automatically (e.g., `0 2 * * *` for 02:00 UTC daily). If absent, the pipeline is triggered externally by the orchestrator. |
| `timeout` | integer | No | `3600` | Maximum wall-clock seconds the engine will wait for extraction to complete before aborting and raising an alert. |

```yaml
ingestion:
  mode: "incremental"
  incrementalColumn: "last_modified_ts"
  watermarkStorage: "s3://pipeline-state/watermarks/fin-trade-oracle-prod"
  batchSize: 10000
  parallelism: 8
  schedule: "0 2 * * *"
  timeout: 7200
```

---

## 5. `schemaEnforcement`

Controls whether the engine validates extracted records against a declared schema before writing to storage. When enabled, records that violate the schema are routed to quarantine, not written to the Bronze layer.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `enabled` | boolean | Yes | — | If `false`, schema validation is skipped and all records proceed to storage regardless of structure. Set to `true` in all non-development environments. |
| `mode` | enum | Conditional | — | Enforcement strictness. Required when `enabled` is `true`. `strict` rejects any record with an unexpected column or type mismatch; `permissive` writes records with schema violations to quarantine but continues processing the rest of the batch. |
| `registryRef` | string | Conditional | — | Path to the JSON Schema document in the schema registry (e.g., `schemas/financial/trade-data-v3.json`). Required when `enabled` is `true`. The engine fetches this schema at pipeline startup. |

```yaml
schemaEnforcement:
  enabled: true
  mode: "strict"
  registryRef: "schemas/financial/trade-data-v3.json"
```

---

## 6. `qualityRules`

**Reserved — Phase 2 (v0.2.0)**

This section is defined in the schema now to reserve the key and prevent naming conflicts when quality rules are implemented. In v0.1.0, the only valid configuration is `enabled: false`. The engine ignores this section entirely when `enabled` is `false`.

Do not add rule definitions here yet — the quality rules DSL will be specified in full in the v0.2.0 schema update.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `enabled` | boolean | Yes | — | Must be `false` in v0.1.0. Setting to `true` will cause a validator error until the v0.2.0 engine is deployed. |

```yaml
qualityRules:
  enabled: false
  # Rule definitions added in v0.2.0
```

---

## 7. `quarantine`

Defines where failed records are written and how errors are classified. A record fails if it violates schema enforcement rules. Quarantined records are never silently dropped — each record is written with its failure reason so it can be reviewed and replayed.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `enabled` | boolean | Yes | — | If `false`, failed records are discarded rather than quarantined. Only acceptable in development environments with explicit justification. |
| `path` | string | Conditional | — | URI of the quarantine storage location (e.g., `s3://datalake-quarantine/fin-trade-oracle-prod/`). Required when `enabled` is `true`. Must be a different location from Bronze storage. |
| `retentionDays` | integer | No | `90` | Number of days quarantined records are retained before deletion. Set in alignment with your data retention policy. Regulatory environments typically require a minimum of 90 days. |
| `errorClassification` | boolean | No | `true` | If `true`, each quarantined record is tagged with a structured error code identifying which validation rule it failed (e.g., `SCHEMA_TYPE_MISMATCH`, `NULL_IN_REQUIRED_COLUMN`). Strongly recommended — enables targeted recovery. |

```yaml
quarantine:
  enabled: true
  path: "s3://datalake-quarantine/fin-trade-oracle-prod/"
  retentionDays: 90
  errorClassification: true
```

---

## 8. `storage`

Defines where and how successfully validated records are written to the lakehouse. In v0.1.0, the engine writes to the Bronze layer only. Silver and Gold layer writes are handled by downstream processes and are out of scope for this config.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `layer` | enum | Yes | — | Target lakehouse layer. One of: `bronze`, `silver`, `gold`. In v0.1.0, only `bronze` is supported by the ingestion engine. |
| `format` | enum | Yes | — | Storage format. One of: `delta`, `iceberg`. Both formats provide ACID transactions, time travel, and schema evolution — required properties for audit compliance. |
| `path` | string | Yes | — | URI of the target storage location (e.g., `s3://datalake-bronze/financial/trade-data/`). |
| `partitionBy` | list[string] | No | `[]` | List of column names used to partition the output dataset. Partitioning on date or time columns is strongly recommended for Bronze to enable efficient time-range queries and regulatory replay. |
| `compactionEnabled` | boolean | No | `false` | If `true`, the engine runs a compaction step after each write to merge small files. Useful for high-frequency incremental loads. Adds overhead to each pipeline run. |

```yaml
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/financial/trade-data/"
  partitionBy:
    - "ingestion_date"
    - "currency"
  compactionEnabled: false
```

---

## 9. `monitoring`

Controls what the engine emits during a pipeline run. All metrics are exposed in Prometheus format and scraped by the monitoring stack. Dashboards and alert rules are defined separately in `monitoring/grafana/`.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `metricsEnabled` | boolean | Yes | — | If `true`, the engine exposes a Prometheus metrics endpoint on `prometheusPort` for the duration of the pipeline run. |
| `prometheusPort` | integer | Conditional | `9090` | Port on which the Prometheus metrics endpoint is exposed. Required when `metricsEnabled` is `true`. Must not conflict with other services on the same host. |
| `slaThresholdSeconds` | integer | No | — | Maximum acceptable wall-clock duration for a complete pipeline run, in seconds. If the run exceeds this value, an SLA breach event is emitted to the alert channel. |
| `alertOnFailure` | boolean | No | `true` | If `true`, the engine emits an alert when the pipeline terminates with a failure status. Alert routing is configured in the monitoring stack, not here. |

```yaml
monitoring:
  metricsEnabled: true
  prometheusPort: 9090
  slaThresholdSeconds: 3600
  alertOnFailure: true
```

---

## 10. `audit`

Controls the audit trail written by the engine for each pipeline run. Audit records are written to the lineage catalog and are independent of the application log stream. These fields directly support compliance requirements under Dodd-Frank, NERC CIP, and NIST CSF — see `docs/compliance-mapping.md` for the mapping.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `enabled` | boolean | Yes | — | If `false`, no audit records are written. Must be `true` in `staging` and `prod` environments. |
| `lineageTracking` | boolean | No | `true` | If `true`, the engine records source-to-target lineage for every pipeline run: source system, extraction timestamp, record counts, target path, and schema version used. |
| `immutableRawEnabled` | boolean | No | `true` | If `true`, the engine enforces that the Bronze layer path is write-once: no overwrites or deletes are permitted after initial write. Implemented via storage-level object lock (S3 Object Lock or equivalent). |
| `retentionDays` | integer | No | `2555` | Number of days audit records are retained. Default is 2555 days (7 years), matching common U.S. financial services retention requirements. Adjust to match your regulatory obligation. |

```yaml
audit:
  enabled: true
  lineageTracking: true
  immutableRawEnabled: true
  retentionDays: 2555
```

---

## Schema versioning

The `schemaVersion` field ties a config file to a specific version of the schema specification. The runtime validator maintains an explicit list of supported versions and rejects any config whose `schemaVersion` it does not recognise.

Version progression:

| Schema version | Introduced in | Breaking changes |
|---|---|---|
| `"1.0"` | v0.1.0 | — (initial version) |

When a future engine release adds a new required field or removes a field, the schema version will increment. Configs written for an older schema version will continue to be parsed by the version they were written for; they will not be silently upgraded. Migration tooling will be provided in the release notes for any breaking schema change.

---

## Complete example

The skeleton below covers all ten sections with minimal but valid values. It is not a production-ready config — it is a starting point for copying and filling in. Fully worked domain examples (financial services trade data, energy meter readings) live in `examples/configs/`.

```yaml
schemaVersion: "1.0"

metadata:
  sourceId: "example-source-prod"
  sourceName: "Example Source"
  sector: "financial-services"
  owner: "platform-team"
  environment: "prod"
  tags:
    - "example"

connection:
  type: "jdbc"
  credentialsRef: "vault://secret/data/example-source-prod"
  host: "db.internal.example.com"
  port: 5432
  database: "exampledb"
  jdbcDriver: "postgres"

ingestion:
  mode: "incremental"
  incrementalColumn: "updated_at"
  watermarkStorage: "s3://pipeline-state/watermarks/example-source-prod"
  batchSize: 10000
  parallelism: 4
  schedule: "0 3 * * *"
  timeout: 3600

schemaEnforcement:
  enabled: true
  mode: "strict"
  registryRef: "schemas/example/source-v1.json"

qualityRules:
  enabled: false

quarantine:
  enabled: true
  path: "s3://datalake-quarantine/example-source-prod/"
  retentionDays: 90
  errorClassification: true

storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/example/source/"
  partitionBy:
    - "ingestion_date"
  compactionEnabled: false

monitoring:
  metricsEnabled: true
  prometheusPort: 9090
  slaThresholdSeconds: 3600
  alertOnFailure: true

audit:
  enabled: true
  lineageTracking: true
  immutableRawEnabled: true
  retentionDays: 2555
```

---

## Related documents

- `docs/reference-architecture.md` — system architecture overview showing where config is consumed
- `docs/architecture-decisions.md` — ADR-001 explaining the configuration-driven design decision
- `docs/compliance-mapping.md` — how individual fields map to Dodd-Frank, NERC CIP, NIST CSF, and NCS 2023 requirements
- `examples/configs/` — complete worked examples for each supported sector

---

## Annotated Examples

These two examples show a complete pipeline configuration for each sector. Every field carries an inline comment explaining why it is set to that value — not just what it does.

---

### Example 1 — Financial Services: Oracle JDBC incremental extraction (Dodd-Frank)

```yaml
# Source:      Trade Execution Records — Oracle
# Sector:      Financial Services
# Description: Ingests trade execution records from the firm's Oracle trading
#              database (TRADEDB) using watermark-based incremental extraction
#              on TRADE_TIMESTAMP. Runs every four hours during market hours.
#              Raw records land in the Bronze Delta table partitioned by trade
#              date and asset class. Write-once enforcement on the Bronze path
#              supports Dodd-Frank immutable recordkeeping obligations.
# Compliance:  Dodd-Frank Act (17 CFR Part 45), NIST CSF 2.0, NCS 2023
#
# Driver note: ojdbc11.jar must be supplied by the operator; it is not bundled
#              with this framework (OTN License). See docs/connectors/oracle-connector.md.

schemaVersion: "1.0"   # The only value the v0.1.0 engine accepts; a different value
                        # causes the config loader to return a SchemaValidationError
                        # before any extraction begins.

metadata:
  sourceId: "fs-oracle-trades-001"          # Primary key in the lineage catalog; must be globally
                                            # unique across all sources. Lowercase, hyphens only —
                                            # this value is used in S3 path construction and metric
                                            # labels, so special characters will break downstream tools.
  sourceName: "Trade Execution Records — Oracle"  # Shown in Grafana dashboards and alert notifications;
                                                  # descriptive enough that an on-call engineer
                                                  # understands the source without opening this file.
  sector: "financial-services"              # Activates Dodd-Frank compliance enforcement paths in the
                                            # engine. Setting "energy" here would apply NERC CIP
                                            # enforcement instead, which has different retention
                                            # and audit rules. Must match the actual regulatory scope.
  owner: "trade-data-engineering"           # Team that receives failure alerts and is attributed in
                                            # lineage records. A wrong value here means the wrong
                                            # team gets paged at 2am.
  environment: "prod"                       # Triggers full compliance enforcement: 7-year retention
                                            # defaults, strict schema validation, and immediate
                                            # on-call alerting. Dev/staging environments apply
                                            # relaxed enforcement and shorter defaults.
  tags:
    - "dodd-frank"                          # Links this source to Dodd-Frank 17 CFR Part 45 swap
                                            # data reporting obligations in the lineage catalog.
    - "tier-1"                              # Tier-1 SLA: any pipeline failure pages on-call
                                            # immediately rather than routing to the next-business-day
                                            # queue. Remove this tag only if you reclassify the SLA.
    - "trade-reporting"                     # Groups this source with other trade-reporting pipelines
                                            # in dashboards; no engine behaviour is driven by this tag.

connection:
  type: "jdbc"                              # Selects the JDBC extraction path in the connector
                                            # registry. The engine will instantiate OracleJdbcConnector
                                            # because jdbcDriver is "oracle". Changing this to "file"
                                            # or "kafka" would load a completely different connector class.
  credentialsRef: "vault://secret/prod/oracle-trades/jdbc-credentials"
                                            # Vault path resolved at runtime to username + password.
                                            # The engine never reads literal credentials from config —
                                            # this file is safe to commit. If Vault is unavailable at
                                            # startup, the engine fails fast rather than connecting
                                            # without credentials.
  host: "oracle-trading-prod.internal.example.com"
                                            # Internal DNS name rather than an IP address so that
                                            # Oracle RAC failover to a new node does not require a
                                            # config change. Never use an IP here for production RAC.
  port: 1521                               # Standard Oracle listener port. Change only if your DBA
                                            # has configured a non-standard listener; verify with
                                            # `tnsping` before changing.
  database: "TRADEDB"                      # Oracle SERVICE NAME (not SID). The JDBC URL the engine
                                            # builds uses the thin driver // syntax which expects a
                                            # service name. If you supply a SID here the connection
                                            # will fail — see docs/connectors/oracle-connector.md.
  jdbcDriver: "oracle"                     # Loads ojdbc11.jar and constructs a
                                            # jdbc:oracle:thin:@//host:port/service URL. The driver
                                            # jar must be present on the executor classpath; the
                                            # framework does not bundle it due to OTN license terms.

ingestion:
  mode: "incremental"                      # Only records with TRADE_TIMESTAMP greater than the stored
                                            # watermark are extracted on each run. Full refresh is not
                                            # viable for a high-volume trade database — a full scan
                                            # would take hours and exceed the 2-hour timeout below.
  incrementalColumn: "TRADE_TIMESTAMP"     # Monotonically increasing Oracle TIMESTAMP column. The
                                            # engine issues WHERE TRADE_TIMESTAMP > :last_watermark.
                                            # Do not use a TIMESTAMP WITH TIME ZONE column unless
                                            # oracle.jdbc.J2EE13Compliant=true is set; the JDBC thin
                                            # driver returns TZ offsets inconsistently otherwise.
                                            # See docs/connectors/oracle-connector.md.
  watermarkStorage: "s3://pipeline-state/watermarks/fs-oracle-trades-001"
                                            # After each successful Bronze write, the engine persists
                                            # MAX(TRADE_TIMESTAMP) from the extracted batch here. A
                                            # failed write leaves this file unchanged, so the next run
                                            # automatically re-covers the missed window. Losing this
                                            # file causes the next run to do a full scan — keep S3
                                            # versioning enabled on this bucket.
  batchSize: 25000                         # JDBC fetchsize: rows fetched per round-trip to Oracle.
                                            # Trade rows are wide (50+ columns, LOB fields) so 25000
                                            # sits at the memory-safe ceiling for the executor heap
                                            # size used here. Narrower rows could use 50000–100000.
                                            # Too large → executor OOM; too small → excessive
                                            # round-trips and slower extraction.
  parallelism: 8                           # Spark tasks spawned for extraction, also controls
                                            # numPartitions when partition column options are set.
                                            # Set to the number of executor cores available on the
                                            # cluster for this pipeline. Higher values increase
                                            # extraction throughput but also increase Oracle connection
                                            # pool pressure — verify the pool limit before raising.
  schedule: "0 */4 * * *"                  # Fires every 4 hours at minute 0 (00:00, 04:00, 08:00…).
                                            # Chosen to align with intra-day risk reporting windows.
                                            # A 2-hour extraction window fits comfortably between runs.
                                            # If extraction starts to exceed 2 hours, narrow the
                                            # interval rather than increasing the timeout.
  timeout: 7200                            # Hard abort after 2 hours. Without this, a slow Oracle
                                            # query could block the executor indefinitely, causing the
                                            # next scheduled run to queue behind it. The 4-hour
                                            # schedule gives a 2-hour buffer before the next run
                                            # would be delayed.

schemaEnforcement:
  enabled: true                            # Reject records that do not conform to the declared schema.
                                            # Disabling this risks type mismatches landing silently in
                                            # Bronze where they would corrupt downstream Silver transforms.
  mode: "strict"                           # Abort the pipeline on the first schema violation rather
                                            # than routing bad records to quarantine and continuing.
                                            # "strict" is appropriate for trade data where a schema
                                            # change (e.g. a new Oracle column) should halt ingestion
                                            # until the schema registry document is updated — not
                                            # silently pass through mismatched records.
  registryRef: "schemas/financial-services/trade-execution-v2.json"
                                            # Path to the JSON Schema document that defines expected
                                            # column names, types, and nullability for TRADE_EXECUTION
                                            # records. "v2" — this schema has been through one revision;
                                            # do not point to v1 unless you are intentionally running
                                            # against the old schema version.

qualityRules:
  enabled: false                           # Quality rules are a Phase 2 feature targeted for the
                                            # v0.2.0 release. Setting this to true in v0.1.0 causes
                                            # the config validator to reject the entire config file
                                            # at startup — the pipeline will not run at all.

quarantine:
  enabled: true                            # Failed records are written to the quarantine path rather
                                            # than being silently dropped. This is a non-negotiable
                                            # architectural requirement — silent record loss creates
                                            # an undetectable gap in Dodd-Frank reporting data.
  path: "s3://datalake-quarantine/financial-services/oracle-trades/"
                                            # Must be a different bucket or prefix from storage.path.
                                            # The engine validates this at startup; using the same path
                                            # risks overwriting valid Bronze records with quarantine
                                            # entries. Prefixed by sector to isolate access policies.
  retentionDays: 365                       # Quarantine records kept for 1 year to allow investigation
                                            # and replay within the same fiscal year. Shorter periods
                                            # risk deleting records that are still under regulatory
                                            # scrutiny. Note: the audit trail (audit.retentionDays)
                                            # runs for 7 years regardless of this setting.
  errorClassification: true               # Tags each quarantined record with a structured error code
                                            # (schema_violation, type_mismatch, null_violation, etc.)
                                            # so that the cause of failure can be determined without
                                            # replaying the record manually. Disabling this makes
                                            # quarantine investigation substantially harder.

storage:
  layer: "bronze"                          # The v0.1.0 ingestion engine only writes to Bronze. Silver
                                            # and Gold layers are produced by separate downstream
                                            # processes. Changing this value to "silver" or "gold"
                                            # will be rejected at startup in v0.1.0.
  format: "delta"                          # Delta Lake provides ACID transactions, time travel, and
                                            # schema evolution — all required for regulated audit
                                            # compliance. "iceberg" is the alternative if your lakehouse
                                            # catalog requires it; both are supported by the engine.
  path: "s3://datalake-bronze/financial-services/oracle-trades/"
                                            # Target S3 prefix for Bronze Delta table data files.
                                            # Must not overlap with quarantine.path. The engine
                                            # enforces write-once on this path when
                                            # audit.immutableRawEnabled is true.
  partitionBy:
    - "trade_date"                         # Partition by calendar date so that time-range queries
                                            # required by Dodd-Frank reporting skip entire partitions
                                            # rather than full-table scanning.
    - "asset_class"                        # Secondary partition reduces scan cost for asset-class-
                                            # specific regulatory queries (e.g. rates vs credit vs FX
                                            # reporting). Only add a partition column if queries
                                            # regularly filter on it — over-partitioning creates
                                            # excessive small files.
  compactionEnabled: true                  # Run Delta small-file compaction (OPTIMIZE) after each
                                            # write. Every 4-hour run appends a new batch of files;
                                            # without compaction, read amplification on the Bronze
                                            # table grows linearly over weeks. Enabled here because
                                            # 4-hourly runs produce enough files to justify the
                                            # compaction overhead.

monitoring:
  metricsEnabled: true                     # Expose a Prometheus /metrics endpoint during pipeline
                                            # execution. Required for SLA visibility in Grafana.
                                            # Disabling this means the SLA threshold below has no
                                            # effect — there are no metrics to evaluate.
  prometheusPort: 9091                     # Non-default port (9090 is used by another pipeline on
                                            # the same executor host). Each pipeline on a shared
                                            # executor must use a distinct port; conflicts cause the
                                            # metrics endpoint to fail silently on one of the pipelines.
  slaThresholdSeconds: 3600                # Emit an SLA breach event if extraction + write exceeds
                                            # 1 hour. Chosen to leave a 1-hour buffer before the next
                                            # 4-hourly run and provide early warning that extraction is
                                            # growing toward the timeout.
  alertOnFailure: true                     # Emit an alert to the owner team on any terminal pipeline
                                            # failure. Combined with the "tier-1" tag this routes
                                            # to the immediate on-call queue rather than the
                                            # next-business-day ticketing flow.

audit:
  enabled: true                            # Write audit records to the lineage catalog for every run.
                                            # Must be true in production. Disabling audit in a
                                            # Dodd-Frank-tagged pipeline would create a gap in the
                                            # traceable chain of custody that regulators require.
  lineageTracking: true                    # Record source-to-Bronze lineage per run: source table,
                                            # target S3 path, run timestamp, row counts extracted and
                                            # written. This record is what a regulator or auditor
                                            # reviews to confirm data was ingested correctly and on time.
  immutableRawEnabled: true               # Enforce write-once on the Bronze path. The engine sets
                                            # Delta table properties to deny updates and deletes on
                                            # the Bronze layer. This is a direct control for Dodd-Frank
                                            # 17 CFR Part 45 immutable recordkeeping obligations.
  retentionDays: 2555                      # 7-year retention (365 × 7 = 2555 days). This is the
                                            # U.S. financial services regulatory minimum for swap data
                                            # records under 17 CFR Part 45. Shortening this value
                                            # would put the firm out of compliance; lengthening it
                                            # is conservative and acceptable.
```

---

### Example 2 — Energy: PostgreSQL JDBC full refresh (NERC CIP)

```yaml
# Source:      Smart Meter Registry — PostgreSQL
# Sector:      Energy Infrastructure
# Description: Daily full extract of smart meter registry data (meter IDs,
#              installation dates, service addresses) from a PostgreSQL
#              operational database. Full refresh is chosen because the registry
#              is small (< 500K rows) and the operational complexity of
#              watermark-based incremental extraction is not justified at this
#              size. A daily run at 03:00 completes well before business hours.
#              Write-once Bronze enforcement and 5-year audit retention satisfy
#              NERC CIP-007 R5 and CIP-008 R3 requirements.
# Compliance:  NERC CIP-007 R5 (Security Patch Management logging),
#              NERC CIP-008 R3 (incident reporting audit trail),
#              NIST CSF 2.0, NCS 2023

schemaVersion: "1.0"   # The only value the v0.1.0 engine accepts; a different value
                        # causes the config loader to return a SchemaValidationError
                        # before any extraction begins.

metadata:
  sourceId: "energy-pg-meter-registry-001" # Primary key in the lineage catalog. Must be unique
                                            # across all sources. The "energy" prefix groups this
                                            # source with other energy-sector pipelines in dashboards
                                            # and makes sector visible in metric labels without
                                            # opening the config file.
  sourceName: "Smart Meter Registry — PostgreSQL"
                                            # Shown in Grafana alerts and the lineage catalog UI.
                                            # "Smart Meter Registry" tells an on-call engineer exactly
                                            # which operational dataset is affected — avoids the
                                            # ambiguity of a generic name like "postgres-source-1".
  sector: "energy"                         # Activates NERC CIP compliance enforcement paths in the
                                            # engine (retention policy defaults, audit trail format,
                                            # and alert routing). Using "financial-services" here
                                            # would apply Dodd-Frank rules, which have different
                                            # retention minimums and would miscategorise lineage.
  owner: "grid-data-engineering"           # Team accountable for this pipeline. Receives failure
                                            # alerts and is attributed in every lineage catalog entry.
                                            # This must map to a real team in your alerting system —
                                            # an incorrect value silently routes pages to the wrong queue.
  environment: "prod"                      # Enables full compliance enforcement: NERC CIP retention
                                            # defaults, strict schema validation, and immediate
                                            # on-call alerting. Never run a production NERC CIP source
                                            # with environment set to "dev" or "staging" — enforcement
                                            # is relaxed and retention periods are shortened.

connection:
  type: "jdbc"                             # Selects the JDBC extraction path. The engine will
                                            # instantiate PostgresJdbcConnector because jdbcDriver
                                            # is "postgres". No custom connector code is required.
  credentialsRef: "vault://secret/prod/pg-meter-registry/jdbc-credentials"
                                            # Vault path resolved at runtime to PostgreSQL username
                                            # and password. The engine fails fast if Vault is
                                            # unreachable rather than falling back to an insecure
                                            # alternative. This file is safe to commit — no literal
                                            # credentials appear anywhere in it.
  host: "pg-meter-ops.grid.example.com"   # Internal DNS name for the PostgreSQL operational host.
                                            # Using DNS rather than an IP means a database failover
                                            # or host migration does not require a config change —
                                            # only a DNS record update.
  port: 5432                               # Standard PostgreSQL port. Change only if the DBA has
                                            # configured a non-standard listener. Verify with
                                            # `psql -h pg-meter-ops.grid.example.com -p 5432` before
                                            # deploying.
  database: "meter_registry"              # The PostgreSQL database containing the meter registry
                                            # tables. PostgreSQL connections are scoped to a single
                                            # database; the engine cannot cross-database query in a
                                            # single connection.
  jdbcDriver: "postgres"                   # Loads the PostgreSQL JDBC driver (postgresql-42.x.jar)
                                            # and constructs a jdbc:postgresql://host:port/database
                                            # URL. The driver jar is bundled with the framework for
                                            # PostgreSQL (unlike Oracle, which requires a separate
                                            # OTN-licensed download).
  sslMode: "verify-full"                   # Encrypted connection with CA validation and server
                                            # hostname verification against the CN/SAN in the
                                            # server certificate. NERC CIP-007 R5 requires encrypted
                                            # transport for access to BES Cyber System data. Using
                                            # "disable" or "require" (no CA verification) would leave
                                            # the connection vulnerable to man-in-the-middle attack
                                            # and would not satisfy CIP-007 R5. The CA certificate
                                            # and client certificate must be supplied via credentialsRef.

ingestion:
  mode: "full"                             # Extract all rows from the meter registry on every run.
                                            # Full refresh is the right choice here: the registry is
                                            # < 500K rows (fits in a single JDBC batch with parallelism 4),
                                            # the change detection complexity of incremental extraction
                                            # (choosing and maintaining a watermark column) is not
                                            # justified at this size, and a daily full snapshot
                                            # provides a complete point-in-time record per partition.
  batchSize: 10000                         # JDBC fetchsize per round-trip. Meter registry rows are
                                            # narrow (meter ID, date, address — ~10 columns), so 10000
                                            # rows per fetch is conservative. The full extract of
                                            # 500K rows completes in ~50 fetches. Raising to 50000
                                            # would also be safe for this row width.
  parallelism: 4                           # Spark tasks for extraction. With < 500K rows and a
                                            # small operational database, 4 tasks avoids overloading
                                            # the PostgreSQL connection pool while still allowing
                                            # parallel fetch. Raising parallelism on a small dataset
                                            # adds coordination overhead without meaningful throughput gain.
  schedule: "0 3 * * *"                   # Daily at 03:00. Chosen to run after nightly database
                                            # maintenance windows (typically 01:00–02:30) and complete
                                            # before business-hours reporting queries begin at 06:00.
                                            # The 30-minute SLA threshold below means a failure is
                                            # detected by 03:30 — well before any operational dependency.
  timeout: 1800                            # Hard abort after 30 minutes. A full extract of 500K
                                            # narrow rows should complete in under 5 minutes under
                                            # normal conditions. A 30-minute timeout catches runaway
                                            # queries (e.g. a missing index after a schema change)
                                            # without blocking the executor for hours.

schemaEnforcement:
  enabled: true                            # Validate every extracted row against the declared schema.
                                            # Meter registry data feeds asset-tracking and incident-
                                            # response workflows under NERC CIP-008. A type mismatch
                                            # on a meter ID column landing silently in Bronze would
                                            # corrupt those downstream lookups.
  mode: "strict"                           # Abort on the first schema violation. A schema change in
                                            # the operational PostgreSQL database (e.g. a new column
                                            # or a type change to a meter ID field) should halt
                                            # ingestion and require an explicit schema registry update
                                            # before proceeding — not silently pass through mismatched
                                            # records to the Bronze layer.
  registryRef: "schemas/energy/meter-registry-v1.json"
                                            # Path to the JSON Schema document in the registry that
                                            # defines expected columns, types, and nullability for
                                            # the meter registry export. "v1" — this is the initial
                                            # schema version; increment to v2 when the operational
                                            # database schema changes and update this reference
                                            # before the next pipeline run.

qualityRules:
  enabled: false                           # Quality rules are a Phase 2 feature targeted for the
                                            # v0.2.0 release. Setting this to true in v0.1.0 causes
                                            # the config validator to reject the entire config file
                                            # at startup — the pipeline will not run at all.

quarantine:
  enabled: true                            # Route failed records to the quarantine path rather than
                                            # dropping them. Even in a full-refresh pipeline, quarantined
                                            # records represent rows that failed schema validation —
                                            # they must be preserved for investigation, not silently
                                            # discarded. Under NERC CIP-008 R3, evidence of data
                                            # handling decisions must be retainable for incident review.
  path: "s3://datalake-quarantine/energy/meter-registry/"
                                            # Must differ from storage.path. Prefixed by sector to
                                            # allow separate IAM policies for energy vs financial-services
                                            # quarantine buckets. The engine validates that this path
                                            # does not overlap with the Bronze storage path at startup.
  retentionDays: 1825                      # 5-year retention (365 × 5 = 1825 days). NERC CIP-007 R5
                                            # and CIP-008 R3 require security event and incident records
                                            # to be retained for at least 3 years; 5 years is the
                                            # NERC CIP audit lookback window and matches the audit
                                            # retention below. Aligning both avoids a scenario where
                                            # quarantine records expire before the audit trail they
                                            # are referenced from.
  errorClassification: true               # Tag each quarantined record with a structured error code
                                            # so that the failure cause (schema_violation, null_violation,
                                            # type_mismatch) is immediately visible without replaying
                                            # the record. In a NERC CIP context this classification
                                            # supports incident triage documentation.

storage:
  layer: "bronze"                          # The v0.1.0 ingestion engine writes to Bronze only.
                                            # Bronze is the raw, immutable landing layer. Silver
                                            # (conformed) and Gold (consumption-optimised) layers
                                            # are produced by separate downstream processes not yet
                                            # in scope for v0.1.0.
  format: "delta"                          # Delta Lake provides ACID transactions and time travel.
                                            # Time travel is important here: a daily full-refresh
                                            # Delta table retains each day's snapshot via the Delta
                                            # log, enabling point-in-time queries for incident
                                            # investigations under NERC CIP-008 without requiring
                                            # a separate backup process.
  path: "s3://datalake-bronze/energy/meter-registry/"
                                            # Target S3 prefix for Bronze Delta table data files.
                                            # The "energy/" prefix isolates energy-sector data in
                                            # a path that can receive a separate S3 bucket policy
                                            # from financial-services data.
  partitionBy:
    - "ingestion_date"                     # Each daily full-refresh run lands in its own partition.
                                            # This means yesterday's snapshot is not overwritten by
                                            # today's — Delta time travel is available, and each
                                            # partition is independently queryable for point-in-time
                                            # incident investigation. Without this partition, each
                                            # full refresh would require Delta REPLACE TABLE semantics
                                            # and would eliminate historical snapshots.
  compactionEnabled: false                 # Full-refresh runs produce one clean batch of files per
                                            # partition per day. There is no file fragmentation from
                                            # multiple small appends, so compaction adds overhead
                                            # with no read-performance benefit. Re-evaluate if the
                                            # batch size is reduced and parallelism is raised, which
                                            # would produce more small files per partition.

monitoring:
  metricsEnabled: true                     # Expose a Prometheus /metrics endpoint during pipeline
                                            # execution. Required for the Grafana SLA dashboard and
                                            # for the slaThresholdSeconds check below to have any effect.
  prometheusPort: 9093                     # Non-default port; 9090 and 9091 are used by other
                                            # pipelines on the same executor host. Each pipeline on
                                            # a shared executor must bind a distinct port. Port 9093
                                            # is also the default Kafka broker metrics port — verify
                                            # there is no Kafka broker on this host before using it.
  slaThresholdSeconds: 1800                # Emit an SLA breach event if extraction + write exceeds
                                            # 30 minutes. This matches the timeout value, meaning
                                            # an SLA breach fires at the same moment the pipeline
                                            # would be aborted. In practice, a healthy run takes
                                            # under 5 minutes — an SLA breach at this threshold
                                            # indicates a serious performance regression.
  alertOnFailure: true                     # Emit an alert to the owner team on any terminal pipeline
                                            # failure. Grid operations depend on the meter registry
                                            # being current; a silent failure would propagate stale
                                            # data downstream without any notification.

audit:
  enabled: true                            # Write audit records to the lineage catalog for every run.
                                            # NERC CIP-007 R5 requires that access to and processing of
                                            # BES Cyber System Information (BCSI) be logged and
                                            # auditable. Disabling audit here would create a gap
                                            # in that mandatory log.
  lineageTracking: true                    # Record source-to-Bronze lineage per run: PostgreSQL host,
                                            # target S3 path, run timestamp, and row counts extracted
                                            # and written. This record is what a NERC CIP auditor
                                            # reviews to confirm data was ingested correctly and on schedule.
  immutableRawEnabled: true               # Enforce write-once on the Bronze path. The engine sets
                                            # Delta table properties to deny updates and deletes on
                                            # the Bronze layer. Combined with partitionBy ingestion_date,
                                            # each daily snapshot becomes a permanent, tamper-evident
                                            # record — supporting NERC CIP-008 R3 incident evidence
                                            # retention requirements.
  retentionDays: 1825                      # 5-year retention (365 × 5 = 1825 days). NERC CIP-007 R5
                                            # and CIP-008 R3 require security log and incident record
                                            # retention for a minimum of 3 years; 5 years aligns with
                                            # the NERC CIP audit lookback period and matches the
                                            # quarantine retention above. Mismatching these two values
                                            # risks audit records outliving the quarantine records
                                            # they reference, or vice versa.
```

---

## Onboarding a New Source

This tutorial walks through building a pipeline configuration file from scratch for a new JDBC data source. By the end, you will have a complete config that runs against the local Docker Compose stack and writes to the Bronze Delta table.

**Prerequisite:** The local dev stack is running (`docker compose ... up -d`). See [docs/getting-started.md](getting-started.md) for setup instructions.

---

### Step 1 — Create the file and declare the schema version

Create `examples/configs/my-source.yaml`. Every config file must begin with:

```yaml
schemaVersion: "1.0"
```

This is the only valid value for v0.1.0. The config loader rejects any other value at startup, before attempting to connect to the source.

---

### Step 2 — Add metadata

`metadata` identifies the source in audit logs, lineage records, and alerts. Fill in each field:

```yaml
schemaVersion: "1.0"

metadata:
  sourceId: "my-source-001"           # Must be lowercase, hyphens only — this is the primary key in the lineage catalog
  sourceName: "My Source — JDBC"      # Human-readable; shown in Grafana dashboards and Prometheus metric labels
  sector: "financial-services"        # financial-services | energy | healthcare | government
                                      # Wrong sector activates the wrong compliance enforcement path
  owner: "platform-data-engineering"  # The team that receives failure alerts
  environment: "dev"                  # dev | staging | prod
                                      # dev: relaxed enforcement, shorter retention defaults
                                      # prod: full compliance enforcement, immutable Bronze, audit trail required
  tags:
    - "onboarding"
```

**Verify:** run `python -m scripts.validate_config examples/configs/my-source.yaml`. If `schemaVersion` and `metadata` are valid, the validator proceeds past this section.

---

### Step 3 — Configure the connection

Add the `connection` section. For a JDBC source:

```yaml
connection:
  type: "jdbc"                                        # jdbc | file | kafka | api
  credentialsRef: "vault://secret/dev/my-source/jdbc-credentials"
                                                      # Vault path seeded manually via vault-init.sh
                                                      # Never put literal credentials here
  host: "postgres"                                    # Docker Compose service name; use real FQDN in staging/prod
  port: 5432
  database: "cidf_dev"
  jdbcDriver: "postgres"                              # postgres | oracle | teradata
```

**Create the Vault secret before running the pipeline:**

```bash
# Source your .env so VAULT_ADDR and VAULT_DEV_ROOT_TOKEN are set
set -a; source deployment/docker/.env; set +a

vault kv put secret/dev/my-source/jdbc-credentials \
  username=<db-user> \
  password=<db-password>
```

The engine resolves `credentialsRef` at startup. If the path is not found in Vault, the pipeline fails immediately with a `SecretsResolutionError` before attempting any database connection.

---

### Step 4 — Set the ingestion strategy

Choose `full` or `incremental`:

**Full refresh** (small tables, < ~1M rows, or tables without a reliable watermark column):

```yaml
ingestion:
  mode: "full"
  batchSize: 10000      # Rows per JDBC round-trip; increase for narrow rows, decrease for wide rows
  parallelism: 4
  schedule: "0 3 * * *" # Daily at 03:00 UTC
  timeout: 3600
```

**Watermark-based incremental** (large tables, append-heavy, with a monotonically increasing timestamp or sequence column):

```yaml
ingestion:
  mode: "incremental"
  incrementalColumn: "updated_at"                          # Must be monotonically increasing and indexed
  watermarkStorage: "s3://bronze/watermarks/my-source-001" # Local dev: s3://bronze/watermarks/my-source-001 (MinIO)
  batchSize: 10000
  parallelism: 4
  schedule: "*/15 * * * *"
  timeout: 900
```

The watermark file is read at pipeline start (to get the last-seen value) and written after a successful Bronze write (to advance the cursor). A failed write leaves the watermark unchanged — the next run re-covers the window.

---

### Step 5 — Enable schema enforcement

`schemaEnforcement` determines how the engine handles records that do not match the declared schema. For a new source, start with `discovered-and-log` to learn the schema, then switch to `strict` for production.

**Phase 1 — discover the schema:**

```yaml
schemaEnforcement:
  enabled: true
  mode: "discovered-and-log"            # Infers schema from data; logs any drift vs. registryRef as WARN
  registryRef: "schemas/my-source-v1.json"  # Must exist in the schema registry; create it first
```

**Phase 2 — enforce strictly once the schema is stable:**

```yaml
schemaEnforcement:
  enabled: true
  mode: "strict"                        # Aborts the pipeline on any schema violation; no silent pass-through
  registryRef: "schemas/my-source-v1.json"
```

Per ADR-005: `registryRef` is required for all file and JDBC sources. The engine rejects configs where `schemaEnforcement.enabled: true` but `registryRef` is absent.

---

### Step 6 — Configure quarantine

Quarantine stores records that fail schema or quality validation, so they can be reviewed and replayed without re-running the full extraction.

```yaml
qualityRules:
  enabled: false   # Phase 2 feature — must be false in v0.1.0

quarantine:
  enabled: true
  path: "s3://bronze/quarantine/my-source/"  # Local dev: MinIO 'bronze' bucket, quarantine/ prefix
                                             # Must differ from storage.path
  retentionDays: 90
  errorClassification: true                  # Tags each quarantined record with a structured error code
```

---

### Step 7 — Configure storage

Specify the Bronze Delta table path:

```yaml
storage:
  layer: "bronze"         # v0.1.0 writes to Bronze only
  format: "delta"
  path: "s3://bronze/my-source/"  # Local dev: MinIO 'bronze' bucket
  partitionBy:
    - "ingestion_date"    # Recommended for all sources; enables efficient time-range queries
  compactionEnabled: false
```

In local dev, `s3://bronze/...` resolves to MinIO via the `AWS_S3_ENDPOINT` and `AWS_S3_PATH_STYLE_ACCESS` environment variables set in the Docker Compose stack.

---

### Step 8 — Add monitoring and audit

```yaml
monitoring:
  metricsEnabled: true
  prometheusPort: 9094   # Use a port not already taken by other pipelines in the stack
  slaThresholdSeconds: 3600
  alertOnFailure: true

audit:
  enabled: true
  lineageTracking: true
  immutableRawEnabled: false  # Set true in prod; enforces write-once on the Bronze path
  retentionDays: 90           # Extend to 2555 (7 years) for financial services, 1825 (5 years) for energy
```

---

### Step 9 — Run and verify

**Validate the config file:**

```bash
python -m scripts.validate_config examples/configs/my-source.yaml
```

A clean validation output confirms the config is schema-valid. It does not test connectivity.

**Run the pipeline (once a Main class entry point is available in v0.1.0):**

```bash
docker compose -f deployment/docker/docker-compose.yml run --rm ingestion-engine \
  --config /app/configs/my-source.yaml
```

**Verify the Bronze output in MinIO:**

Open http://localhost:9001, navigate to the `bronze` bucket, and confirm a directory at `my-source/ingestion_date=<today>/` exists with `.parquet` files.

**Check the audit log:**

```bash
docker compose -f deployment/docker/docker-compose.yml exec minio \
  mc ls local/bronze/audit/
```

A row in the audit table confirms the pipeline run was recorded. The `status` column will be `SUCCESS` if the Bronze write completed.

---

### Promoting to staging or production

Once the pipeline runs successfully in dev, update the following fields before deploying to staging or production:

- `metadata.environment` → `staging` or `prod`
- `connection.host` → real database FQDN
- `connection.sslMode` → `verify-full` (or at minimum `verify-ca`) for any regulated source
- `audit.immutableRawEnabled` → `true`
- `audit.retentionDays` → match regulatory requirement for the sector
- `quarantine.retentionDays` → align with `audit.retentionDays`
- `schemaEnforcement.mode` → `strict` once the schema is stable

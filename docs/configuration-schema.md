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

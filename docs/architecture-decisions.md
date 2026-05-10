# Architecture Decision Record

## ADR-001: Configuration-Driven Ingestion Pattern

### Status
Accepted

### Context
Organizations in regulated critical-infrastructure sectors (financial services, energy, insurance) repeatedly build custom data-ingestion pipelines from scratch for each new data source. This produces fragmented, inconsistent systems that are expensive to maintain and create inconsistent security postures across the organization.

The core observation driving this architecture is that the *requirements* across regulated-sector data sources are substantially similar — high-volume ingestion under latency constraints, schema enforcement, audit-trail generation, quality validation — even though the *sources themselves* vary. This means the ingestion logic can be standardized and parameterized, with source-specific details captured in configuration rather than code.

### Decision
The ingestion framework uses a **configuration-driven** approach where:

1. **Source definitions** are expressed as structured configuration files (YAML/HOCON), not as custom pipeline code
2. **A shared processing engine** reads source configurations and executes standardized ingestion, validation, and loading steps
3. **Schema enforcement, quality rules, and partitioning strategies** are declared per-source in configuration
4. **New data sources are onboarded by adding a configuration file**, not by writing a new pipeline

### Rationale
This pattern was validated in production across three independent regulated environments:

- **Financial services**: Configuration-driven Spark/Scala framework processing regulatory datasets, achieving ~30-65% runtime reduction vs. prior fragmented approach
- **Insurance**: Automated ETL pipelines with parameterized transformations, achieving ~40-50% throughput improvement
- **Energy**: Backend data systems with configurable protocol integration, achieving ~35% API efficiency improvement

In each case, the configuration-driven approach produced:
- Faster onboarding of new data sources (days instead of weeks)
- Consistent security and validation practices across all sources
- Reduced operational burden through standardized monitoring
- Lower error rates from elimination of copy-paste pipeline code

### Consequences
- Requires upfront investment in framework design before first source can be onboarded
- Configuration schema must be carefully designed to be expressive enough for diverse sources without becoming overly complex
- Framework changes affect all sources simultaneously (benefit for security updates, risk for regressions — mitigated by comprehensive testing)

---

## ADR-002: Lakehouse Storage Architecture (Bronze/Silver/Gold)

### Status
Accepted

### Context
Regulated environments require both raw data preservation (for audit and replay) and progressively refined data (for analytics and reporting). A single-layer storage approach forces a tradeoff between auditability and usability.

### Decision
The framework implements a three-layer lakehouse architecture:

- **Bronze layer**: Raw, immutable, time-partitioned copies of ingested data. No transformations applied. Serves as the audit-trail and replay source.
- **Silver layer**: Cleansed, conformed data with standardized schemas, business keys, and quality validations applied. Failed records quarantined with error classification.
- **Gold layer**: Subject-domain-specific views optimized for consumption by analytics, reporting, and downstream systems.

### Rationale
This pattern provides:
- Full auditability (Bronze preserves raw data exactly as received)
- Progressive quality improvement (each layer adds validation and refinement)
- Clear separation of concerns (ingestion vs. cleansing vs. consumption)
- Regulatory compliance (lineage from raw source to final output is traceable)

This pattern was validated in production at a globally systemically important financial institution, where it supported regulatory reporting workflows requiring complete data lineage.

### Consequences
- Increased storage requirements (data exists in multiple refined states)
- Requires clear governance over which layer downstream consumers should access
- Schema evolution must be managed across layers

---

## ADR-003: Compliance-Grade Observability

### Status
Accepted

### Context
In regulated environments, it is not sufficient to know that a pipeline ran successfully. Operators must be able to demonstrate *when* data arrived, *what* validations were applied, *how many* records passed or failed, and *whether* service-level agreements were met — often to external auditors or regulators.

### Decision
The monitoring stack is designed for **compliance-grade observability**, meaning:

1. **Structured logging** across all framework components with consistent format and correlation IDs
2. **Metrics collection** for throughput, duration, error rates, and SLA adherence at pipeline and source level
3. **Real-time alerting** routed to operational support with severity classification
4. **Audit-ready dashboards** showing historical performance, quality trends, and SLA compliance over time
5. **Data quality metrics store** enabling trend analysis and reporting on validation outcomes

### Rationale
Standard application monitoring (uptime, CPU, memory) is insufficient for regulated data systems. The framework's observability layer was designed based on operational requirements observed across financial services and insurance environments, where:
- Regulators require evidence that data was processed within defined timeframes
- Internal audit teams require quality trend reporting
- Operations teams need early warning of degradation before it becomes a compliance issue

Production deployment of this observability approach reduced manual operational interventions by approximately 80% and improved SLA adherence by approximately 20-25 percentage points.

### Consequences
- Monitoring infrastructure adds operational overhead (Grafana, Prometheus, log aggregation)
- Dashboard design requires collaboration with compliance and operations stakeholders
- Metrics retention policies must be aligned with regulatory record-keeping requirements

---

## ADR-005: Explicit Schema Modes for File-Based Source Connectors

### Status
Accepted

### Context
File-based sources — CSV, JSON, TSV, fixed-width — are common in regulated sectors: meter readings, trade reports, and regulatory submissions are routinely delivered as flat files. Apache Spark's default behaviour for CSV and JSON reads is **schema inference**: it samples records and guesses column types, behaviour that is invisible to the pipeline operator.

In regulated environments, silent schema inference creates three categories of risk:

1. **Data integrity risk**: a type widening (e.g. an integer column inferred as string) silently corrupts downstream aggregations without raising an error.
2. **Compliance risk**: pipelines must be reproducible and auditable; schema inference is non-deterministic across Spark versions and sample sizes, meaning the same source file can produce different schemas on different runs.
3. **Drift blindness**: when a file producer changes column names or types, an inference-based pipeline silently adapts — the change goes undetected until downstream consumers surface incorrect results, which may be after regulatory reporting has occurred.

### Decision
All file-based source connectors in this framework operate in one of exactly two explicit schema modes. There is no third "auto" or "inferred" mode.

- **`strict` mode**: the operator declares a `schemaRef` in the pipeline configuration pointing to a registered JSON Schema document. The connector reads the file with the declared schema applied. Any record that does not conform raises a `ConnectorError` and the pipeline halts (or routes to quarantine, depending on the corrupt record mode setting). The schema is never inferred from the data.
- **`discovered-and-log` mode**: the connector allows Spark to infer the schema from the file and compares the inferred schema against the registered schema for the source. Any structural difference — new columns, missing columns, type changes — is emitted as a structured WARN log entry with the full diff. The inferred schema is **never automatically applied** to downstream processing; the pipeline still applies the registered schema. This mode is intended for source-onboarding and monitoring, not for production ingestion.

Silent schema inference (`inferSchema=true` with no comparison or logging) is explicitly prohibited and must never be used in a connector implementation.

### Rationale
- Maps to **NERC CIP-007** (data integrity for energy sector) and **Dodd-Frank Act** data-quality requirements (financial services): both require that data ingestion is auditable and reproducible.
- **NIST CSF 2.0 Identify function**: asset management requires that the schema of data flowing through the system is known and governed, not discovered at runtime.
- Production incident pattern: in financial services environments, silent schema changes in upstream flat-file feeds have caused incorrect regulatory capital calculations that were not detected until next-day reconciliation — after the reporting deadline.
- The `discovered-and-log` mode provides a safe path for schema change detection without creating a pipeline failure that blocks time-sensitive ingestion; operators are notified of drift and can update the registered schema after review.

### Consequences
- Every file-based source must have a registered schema (`schemaRef`) — there is no zero-configuration onboarding path for file sources.
- Operators must update the registered schema explicitly when a source producer makes a breaking change; the pipeline will not self-adapt.
- `discovered-and-log` mode produces WARN log entries that must be monitored; recommended: alert on any schema drift event in production pipelines.
- Spark's `inferSchema=true` option may still be used internally by the framework in `discovered-and-log` mode for comparison purposes, but the inferred schema is never surfaced to the DataFrame that leaves the connector — this distinction must be enforced in code review.
- This ADR supersedes any earlier framework behaviour that permitted implicit inference; all file connectors written before this ADR was accepted must be updated to conform.

---

## ADR-004: Watermark-Based Incremental Extraction

### Status
Accepted

### Context
Regulated data sources — JDBC databases, file drops, streaming feeds — are rarely amenable to full-table extraction on every pipeline run. A 200M-row regulatory database extracted in full every 15 minutes is impractical: it saturates network bandwidth, consumes excessive Spark resources, and produces unnecessarily large Bronze writes.

Two main patterns exist for extracting only new or changed records:
1. **Watermark-based polling**: query `WHERE updated_at > last_known_watermark`, store the max value seen, advance the watermark on success.
2. **Change Data Capture (CDC) / log-based replication**: read the database's binary replication log (Postgres WAL, Oracle LogMiner, MySQL binlog) and stream only changed rows.

### Decision
The framework uses watermark-based incremental extraction as the primary pattern for JDBC and file sources. The `incrementalColumn` and `watermarkStorage` fields in the pipeline configuration define the column to track and the URI where the last watermark is persisted. Watermarks are advanced only on successful Bronze writes — a failed write leaves the watermark unchanged so the next run re-covers the window without data loss.

### Rationale
Watermark-based extraction was chosen because:
- It requires no database-side configuration (no replication slots, no supplemental logging, no binlog retention policy)
- It works across all supported JDBC drivers (Oracle, Postgres, Teradata) without driver-specific CDC adapters
- The failure recovery model is simple: if a run fails, the watermark is not advanced, and the next run re-extracts from the last good point
- It is auditable: the watermark value is a first-class artefact stored in a durable location and recorded in the audit log alongside each run
- Watermark columns (`updated_at`, `recorded_at`, `event_timestamp`) are already present in most regulated-sector operational databases as a compliance requirement

Production validation: watermark-based extraction was used in financial services regulatory reporting pipelines processing 50M+ rows/day, achieving consistent sub-60-second extraction latency for incremental runs vs. 45-minute full extracts.

### Alternatives Considered
- **CDC/log-based replication (Debezium, Oracle GoldenGate, AWS DMS):** Captures all changes including deletes and multiple updates to the same row within a window. Rejected for Phase 1 because it requires database-side configuration (WAL level, replication slots, supplemental logging), creates operational dependency on the source database's log retention settings, introduces DDL sensitivity (schema changes can break the CDC stream), and adds infrastructure complexity (Kafka, Debezium connectors) that is disproportionate to Phase 1 scope. CDC is documented as a Phase 3 capability for sources that require it.
- **Full extraction with deduplication:** Extract the entire table and deduplicate against what is already in Bronze. Rejected: impractical at scale and produces unnecessarily large Bronze writes, violating the storage efficiency principle.
- **Source-side triggers / outbox pattern:** Application-level change tracking where the source system writes changed records to a staging table. Rejected: requires source system modification, which is outside the framework's scope for read-only integration.

### Consequences
- Sources must have a monotonically increasing column suitable for use as a watermark (`TIMESTAMP`, `BIGINT` sequence). Sources without such a column require a full load strategy.
- Watermark-based extraction does not capture hard deletes. If a source record is deleted, the deletion is not reflected in Bronze unless the source uses soft deletes with an `updated_at` column update.
- Clock skew between the source database server and the pipeline runner can cause records to be missed if the watermark advances past records that were written with a slightly earlier timestamp. Mitigated by configuring a safe lag margin (documented in connector configuration).
- Late-arriving records — records with a past timestamp inserted after the watermark has advanced — are not captured. Acceptable in the regulated-sector use cases validated, where source systems write records in near-real-time.

---

## ADR-006: External Secrets Management

### Status
Accepted

### Context
Every data ingestion pipeline requires credentials to connect to source systems: database passwords, API keys, storage access keys, TLS certificates. The question is not whether credentials are needed, but where they live and how the framework accesses them at runtime.

Three naive approaches appear in practice:
1. **Environment variables**: credentials set in the container or process environment at launch time.
2. **Config file encryption**: credentials stored in the pipeline configuration file, encrypted with a key managed by the operator.
3. **External secrets manager**: credentials stored in a dedicated secrets management system (HashiCorp Vault, AWS KMS/Secrets Manager, GCP Secret Manager) and resolved at runtime via authenticated API call.

### Decision
All credentials are resolved at runtime from an external secrets manager. Pipeline configuration files reference credentials using `vault://`-scheme URIs (e.g. `vault://secret/prod/oracle-tradedb/jdbc-credentials`). The `SecretsResolver` interface is resolved at engine startup to the appropriate implementation (`VaultSecretsResolver`, `AwsKmsSecretsResolver`, or `LocalDevSecretsResolver` for testing). No credential value — not even an encrypted one — is ever present in a pipeline configuration file or version-controlled artefact.

### Rationale
- **NIST CSF 2.0 Protect function (PR.AC)**: access to credentials must be managed, audited, and revocable. Environment variables and config files provide none of these.
- **Dodd-Frank / NERC CIP audit requirements**: credential access must be logged. Vault and AWS KMS provide a complete audit trail of every credential read, including who read it, when, and from which system.
- **Rotation without redeployment**: Vault and KMS support credential rotation that takes effect on the next resolution call without restarting the pipeline. Environment variables require container restarts; config file encryption requires re-encryption and redeployment.
- **Blast radius containment**: a compromised pipeline configuration file reveals only a `vault://` URI — not a usable credential. An attacker also needs valid Vault authentication to resolve the secret.
- **Developer safety**: the `vault://` URI scheme makes it syntactically impossible to accidentally commit a real credential. A committed `vault://...` string is harmless; a committed password is not.

### Alternatives Considered
- **Environment variables**: Simple and universally supported, but credentials appear in process listings (`/proc/<pid>/environ`), are inherited by child processes, are not audited, and require container restarts to rotate. Rejected for production use. Retained for `LocalDevSecretsResolver` in test environments only, where these risks are acceptable.
- **Config file encryption (Ansible Vault, SOPS, git-crypt)**: Moves the problem rather than solving it — the decryption key must now be protected and distributed. Encrypted ciphertext committed to version control is still an attack surface (cryptanalysis, key compromise). Rotation requires re-encrypting every affected config file. Rejected.
- **Kubernetes Secrets**: Base64-encoded values stored in etcd, readable by any pod in the namespace with default RBAC. Not encrypted at rest without additional configuration. Acceptable as a distribution mechanism from an external secrets manager (External Secrets Operator pattern), but not as the authoritative secret store. Documented as an acceptable intermediary in production K8s deployments; see `deployment/k8s/README.md`.
- **Hardcoded credentials in source**: Mentioned for completeness — explicitly prohibited by all applicable regulatory frameworks and this project's architecture principles.

### Consequences
- Every deployment environment must have a running secrets manager instance accessible to the pipeline engine. `LocalDevSecretsResolver` is provided for local development and CI environments where Vault/KMS is unavailable.
- The `SecretsResolver` interface is injectable, making it straightforward to add new secrets backends (e.g. GCP Secret Manager) without modifying the engine.
- Credential resolution adds a network round-trip at pipeline startup. In practice this is negligible (<100ms) relative to pipeline execution time.
- The `vault://` URI scheme is a framework convention — it is not a standard and must be documented for operators unfamiliar with the pattern.

---

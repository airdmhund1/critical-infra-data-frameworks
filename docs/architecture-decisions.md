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

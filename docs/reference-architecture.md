# Reference Architecture

## High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │  RDBMS   │  │  Files   │  │ Streams  │  │   APIs   │               │
│  │(Oracle,  │  │(CSV,     │  │(Kafka,   │  │(REST,    │               │
│  │Teradata, │  │Parquet,  │  │Event     │  │GraphQL,  │               │
│  │Postgres) │  │JSON)     │  │Hubs)     │  │OCPP)     │               │
│  └─────┬────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘               │
└────────┼────────────┼─────────────┼──────────────┼──────────────────────┘
         │            │             │              │
         ▼            ▼             ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    SOURCE CONNECTORS                                    │
│  Configuration-driven connector selection based on source type          │
│  Credentials resolved from external secrets manager                     │
│  Schema discovery and validation at connection time                     │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               CONFIGURATION & METADATA SERVICE                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐      │
│  │ Source Configs    │  │ Schema Registry  │  │ Quality Rules    │      │
│  │ (YAML/HOCON)     │  │ (JSON Schema)    │  │ (Per-Source)     │      │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘      │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              SPARK/SCALA INGESTION ENGINE                                │
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  │
│  │ Schema      │  │ Partition   │  │ Transform   │  │ Standardize  │  │
│  │ Enforcement │  │ Strategy    │  │ Engine      │  │ & Conform    │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬───────┘  │
│         └────────────────┼────────────────┼────────────────┘           │
└──────────────────────────┼────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              DATA QUALITY & VALIDATION ENGINE                            │
│                                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │Complete- │  │Referential│  │ Range &  │  │Timeliness│               │
│  │ness      │  │Integrity │  │ Pattern  │  │& SLA     │               │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘               │
│       └──────────────┼────────────┼──────────────┘                     │
│                      ▼                                                  │
│              ┌──────────────┐     ┌──────────────┐                     │
│              │   PASSED     │     │ QUARANTINED  │                     │
│              │   Records    │     │ Records +    │                     │
│              │              │     │ Error Class  │                     │
│              └──────┬───────┘     └──────┬───────┘                     │
└─────────────────────┼────────────────────┼──────────────────────────────┘
                      │                    │
                      ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    LAKEHOUSE STORAGE                                     │
│                                                                         │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐            │
│  │  BRONZE LAYER  │  │  SILVER LAYER  │  │   GOLD LAYER   │            │
│  │                │  │                │  │                │            │
│  │ Raw, immutable │  │ Cleansed,      │  │ Domain-specific│            │
│  │ Time-partitioned│ │ conformed,     │  │ optimized for  │            │
│  │ Audit source   │  │ validated      │  │ consumption    │            │
│  └────────────────┘  └────────────────┘  └────────────────┘            │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│              ORCHESTRATION & MONITORING                                  │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │ Enterprise   │  │ Monitoring & │  │ Metadata &   │                  │
│  │ Scheduler    │  │ Alerting     │  │ Lineage      │                  │
│  │ Integration  │  │ (Grafana/    │  │ Catalog      │                  │
│  │              │  │ Prometheus)  │  │              │                  │
│  └──────────────┘  └──────────────┘  └──────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Component Descriptions

### Source Connectors
Standardized connectors for common data-source types. Each connector implements a consistent interface, enabling the ingestion engine to process any source type through the same pipeline logic. Source-specific details (connection parameters, credentials, fetch strategies) are defined in configuration.

### Configuration & Metadata Service
Central configuration store defining data contracts, schema rules, quality policies, and processing parameters for each source. The ingestion engine reads this configuration at runtime, eliminating the need for source-specific pipeline code.

### Spark/Scala Ingestion Engine
The core processing engine. Reads source configurations, executes standardized ingestion steps (extract, enforce schema, partition, transform, standardize), and writes to the appropriate lakehouse layer. All processing is configuration-driven and reusable across sources.

### Data Quality & Validation Engine
Rules-based validation layer executing quality checks defined in per-source configuration. Records that pass validation proceed to the Silver layer; records that fail are routed to quarantine with error classification for review and recovery.

### Lakehouse Storage
Three-layer storage architecture providing progressive refinement from raw data (Bronze) through validated, conformed data (Silver) to consumption-optimized views (Gold). This pattern supports both operational needs (fast access to current data) and compliance needs (full audit trail from source to output).

### Orchestration & Monitoring
Enterprise scheduler integration for job orchestration, real-time monitoring dashboards for pipeline health and SLA tracking, and a metadata catalog providing source-to-target lineage for audit and compliance purposes.

## Design Principles Reflected in This Architecture

1. **Configuration over code**: The architecture separates *what* to process (configuration) from *how* to process it (engine), enabling new sources to be onboarded without new code.

2. **Quality as a first-class concern**: Validation is embedded in the pipeline, not bolted on afterward. Failed records are quarantined with classification rather than silently dropped.

3. **Compliance by design**: Audit trails, lineage tracking, and immutable raw data preservation are architectural requirements, not optional features.

4. **Observable by default**: Every pipeline component emits metrics, logs, and status information consumed by the monitoring layer.

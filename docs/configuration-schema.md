# Ingestion Configuration Schema Specification

## Overview

This document defines the configuration schema used to onboard new data sources into the ingestion framework. Each data source is defined by a single configuration file — no custom pipeline code is required.

## Schema Version
`1.0.0-draft`

## Source Configuration Structure

```yaml
# source-config.yaml — Example configuration for a single data source
---
schema_version: "1.0"

source:
  name: "regulatory_trade_data"
  description: "Daily trade reporting dataset for regulatory submissions"
  owner: "risk-data-team"
  classification: "confidential"
  
connection:
  type: "jdbc"                    # jdbc | file | stream
  driver: "oracle"                # oracle | postgres | teradata | mysql
  host: "${ENV_DB_HOST}"          # Environment variable reference
  port: 1521
  database: "TRADEDB"
  schema: "RISK_REPORTING"
  table: "DAILY_TRADES"
  credentials_ref: "vault://secrets/tradedb"  # External secrets manager reference

ingestion:
  mode: "incremental"             # full | incremental | snapshot
  partition_column: "trade_date"
  partition_type: "date"          # date | numeric | hash
  fetch_size: 10000
  parallelism: 8
  watermark:
    column: "last_modified_ts"
    format: "yyyy-MM-dd HH:mm:ss"

schema_enforcement:
  mode: "strict"                  # strict | permissive | evolve
  schema_ref: "schemas/trade_data_v3.json"
  nullable_columns: ["counterparty_region", "settlement_notes"]
  
quality_rules:
  - rule: "completeness"
    columns: ["trade_id", "trade_date", "amount", "currency"]
    threshold: 0.99               # 99% completeness required
    
  - rule: "referential_integrity"
    column: "counterparty_id"
    reference_table: "dim_counterparty"
    reference_column: "id"
    
  - rule: "range_check"
    column: "amount"
    min: 0
    max: 999999999
    
  - rule: "timeliness"
    column: "trade_date"
    max_staleness_hours: 24

quarantine:
  enabled: true
  destination: "bronze.quarantine.regulatory_trade_data"
  error_classification: true
  alert_threshold: 0.05           # Alert if >5% of records quarantined

target:
  bronze:
    path: "bronze/regulatory/trade_data"
    format: "parquet"
    partition_by: ["trade_date"]
    retention_days: 2555          # 7 years for regulatory compliance
    
  silver:
    path: "silver/regulatory/trade_data"
    format: "delta"               # delta | iceberg
    partition_by: ["trade_date", "currency"]
    merge_key: ["trade_id"]
    
monitoring:
  sla:
    completion_deadline: "06:00"  # Must complete by 6 AM
    alert_channels: ["ops-team", "risk-reporting"]
  metrics:
    - throughput_rows_per_minute
    - processing_duration_seconds
    - quality_pass_rate
    - quarantine_rate

audit:
  lineage_tracking: true
  record_counts: true
  checksum_validation: true
```

## Configuration Fields Reference

### `source` — Source Metadata
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique identifier for this data source |
| `description` | string | Yes | Human-readable description |
| `owner` | string | Yes | Responsible team or individual |
| `classification` | enum | Yes | Data classification: `public`, `internal`, `confidential`, `restricted` |

### `connection` — Source Connection Details
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | enum | Yes | Connection type: `jdbc`, `file`, `stream` |
| `driver` | string | Conditional | Database driver (required for jdbc) |
| `credentials_ref` | string | Yes | Reference to external secrets manager |

### `ingestion` — Processing Parameters
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mode` | enum | Yes | Ingestion mode: `full`, `incremental`, `snapshot` |
| `partition_column` | string | Conditional | Column used for partitioning (required for incremental) |
| `parallelism` | integer | No | Number of parallel processing threads (default: 4) |

### `quality_rules` — Validation Configuration
| Rule Type | Description |
|-----------|-------------|
| `completeness` | Checks that specified columns contain non-null values above threshold |
| `referential_integrity` | Validates foreign key relationships against reference tables |
| `range_check` | Validates numeric values fall within specified bounds |
| `timeliness` | Checks that data is not staler than specified threshold |
| `uniqueness` | Validates uniqueness constraints on specified columns |
| `regex_pattern` | Validates string values match specified patterns |

### `quarantine` — Failed Record Handling
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | boolean | No | Enable quarantine for failed records (default: true) |
| `destination` | string | Conditional | Target location for quarantined records |
| `error_classification` | boolean | No | Classify errors by rule type (default: true) |
| `alert_threshold` | float | No | Alert if quarantine rate exceeds this fraction |

## Environment Variable Substitution

Configuration values can reference environment variables using `${ENV_VARIABLE}` syntax. This enables the same configuration to be deployed across development, testing, and production environments without modification — a key requirement for CI/CD automation in regulated environments.

## Design Rationale

This schema was designed based on patterns validated in production across financial services, insurance, and energy environments. Key design principles:

1. **Declarative over imperative**: Source characteristics are declared, not coded
2. **Security by default**: Credentials always reference external secrets managers, never inline
3. **Compliance-aware**: Quality rules, quarantine, and audit trails are first-class configuration elements
4. **Environment-agnostic**: Variable substitution enables consistent deployment across environments

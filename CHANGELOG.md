# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-04-26

### Added

- sbt multi-module project structure (core, connectors, validation, monitoring) with scalafmt, scalafix, and sbt-coverage configured
- GitHub Actions CI pipeline running compile, test, scalafmt check, scalafix, and 80% coverage gate on every push
- YAML source configuration schema (source-config-v1.json) with JSON Schema validation covering all ten pipeline sections (metadata, connection, ingestion, schemaEnforcement, qualityRules, quarantine, storage, monitoring, audit)
- Configuration loader mapping YAML pipeline configs to typed Scala case classes (SourceConfig) with secrets resolution interface (VaultSecretsResolver, AwsKmsSecretsResolver, LocalDevSecretsResolver)
- IngestionEngine pipeline orchestrator wiring connector lookup → extraction → quality validation → Bronze write → lineage recording, returning Either[IngestionError, IngestionResult] with full correlation ID
- HashiCorp Vault (AppRole auth) and AWS KMS secrets resolver implementations resolving vault:// and kms:// URI references at pipeline startup
- JdbcConnectorBase abstract class with watermark-based incremental extraction, Spark parallel JDBC reads, configurable retry with exponential backoff, and WatermarkStore interface
- OracleJdbcConnector supporting Oracle Wallet authentication via extraJdbcOptions hook; ojdbc11 not bundled (OTN License constraint documented); integration test suite gated behind ORA_INTEGRATION=true
- PostgresJdbcConnector with TLS SSL mode configuration (disable/require/verify-ca/verify-full); PostgreSQL JDBC driver bundled (BSD-2-Clause); Testcontainers integration test suite
- CsvFileConnector implementing ADR-005 explicit schema modes (strict and discovered-and-log); silent schema inference prohibited; six configurable CSV options; three corrupt record modes
- ParquetFileConnector with embedded schema drift detection (WARN log with field-level diff), Hive-style date partition directory auto-discovery, date-range filter pushdown, and column pruning
- JsonFileConnector supporting JSON Lines and MultilineArray formats, configurable flattenDepth for nested struct flattening, three corrupt record modes, and ADR-005 schema enforcement
- DeltaBronzeLayerWriter writing append-only Delta Lake Bronze tables with four _cidf_* audit metadata columns, SHA-256 batch checksum, record count mismatch validation, and delta.appendOnly = true enforcement
- DeltaAuditEventLogger writing a structured AuditEvent (12 fields including runId, timestamps, record counts, checksum, status, errorMessage) to an append-only Delta table after every pipeline run (success or failure); wired into IngestionEngine as an injectable dependency
- Multi-stage Dockerfile (eclipse-temurin:11-jdk builder → eclipse-temurin:11-jre runtime) and docker-compose stack (ingestion-engine, postgres, minio, vault) with Postgres seed data, Vault credential init script, MinIO bucket bootstrap, and smoke-test.sh acceptance script
- Kubernetes dev manifests (Namespace, ServiceAccount, ConfigMap, Secret reference skeleton, Deployment with resource limits and exec probes, ClusterIP Service) compatible with minikube and kind; deployment/k8s/README.md with local setup and production guidance
- Six ADRs in docs/architecture-decisions.md: configuration-driven design, lakehouse storage, compliance-grade observability, watermark-based extraction, explicit schema modes (ADR-005), and external secrets management — each with Status/Context/Decision/Rationale/Alternatives Considered/Consequences
- Production-quality README.md with Mermaid eight-layer architecture diagram, annotated sector examples (Dodd-Frank and NERC CIP), compliance mapping links, and Getting Started in 15 Minutes quick start
- Complete docs/configuration-schema.md reference with field tables for all ten sections, two fully-annotated example configs, step-by-step onboarding tutorial, common patterns (incremental, full refresh, parallel reads, multi-file glob), and troubleshooting for the top five config errors

[Unreleased]: https://github.com/airdmhund1/critical-infra-data-frameworks/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/airdmhund1/critical-infra-data-frameworks/releases/tag/v0.1.0

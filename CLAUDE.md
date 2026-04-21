# Critical Infrastructure Data Frameworks

## Project Overview

This is an open-source project to develop standardised reference architectures and supporting tooling for secure, reliable data systems in regulated critical-infrastructure sectors — specifically U.S. financial services and energy infrastructure.

The project addresses a documented national problem: organisations in regulated sectors repeatedly build fragmented, one-off data-processing systems from scratch despite facing substantially similar underlying requirements. This fragmentation produces inconsistent security postures across sectors that are vital to national security and economic stability (National Cybersecurity Strategy 2023, DOE AI Strategy 2025).

The core output is a set of configuration-driven, production-validated reference architectures released under Apache License 2.0, freely available for any U.S. regulated-sector organisation to evaluate and adapt.

**Repository**: https://github.com/airdmhund1/critical-infra-data-frameworks
**License**: Apache 2.0
**Maintainer**: Edmund Adu Asamoah (airdmhund1)

## Architecture Principles (Non-Negotiable)

1. **Configuration over code** — New data sources are onboarded through YAML configuration files, not custom pipeline development. The ingestion engine reads config at runtime and executes standardised steps.

2. **Quality as a first-class concern** — Data validation is embedded in the ingestion pipeline, not bolted on afterward. Failed records are quarantined with error classification, never silently dropped.

3. **Compliance by design** — Audit trails, lineage tracking, and immutable raw data preservation are architectural requirements built into every layer.

4. **Observable by default** — Every pipeline component emits metrics, logs, and status information. Full visibility into system health, throughput, and SLA adherence.

5. **Secure by design** — Security controls (encryption, authentication, access control, audit logging) are embedded in the architecture. No credentials in code or config files.

6. **Sector-adaptable** — Core patterns are consistent across sectors. Sector-specific requirements are handled through configurable modules.

## System Architecture (Eight Layers)

1. **Data Sources** — External systems (RDBMS, Files, Streams, APIs)
2. **Source Connectors** — Standardised extraction with config-driven connector selection
3. **Configuration & Metadata Service** — Central control plane (YAML/HOCON configs, JSON Schema registry, quality rules)
4. **Ingestion Engine** — Spark/Scala core that reads config and executes standardised pipeline steps
5. **Data Quality & Validation Engine** — Rules-based validation with quarantine and recovery
6. **Lakehouse Storage** — Bronze (raw/immutable) / Silver (validated/conformed) / Gold (consumption-optimised)
7. **Orchestration & Monitoring** — Scheduler integration, Grafana/Prometheus, metadata/lineage catalog
8. **Security Layer** — TLS/mTLS, certificate management, secrets manager, RBAC, encryption, audit logging

## Technology Stack

| Category | Technology |
|----------|-----------|
| Core Processing | Apache Spark 3.x (Scala 2.12/2.13) |
| Build System | sbt |
| Storage | Delta Lake / Apache Iceberg |
| Orchestration | Apache Airflow / Dagster |
| Monitoring | Grafana + Prometheus |
| Streaming | Apache Kafka |
| Configuration | YAML / HOCON / Typesafe Config |
| Schema | JSON Schema |
| Containerisation | Docker + Kubernetes |
| CI/CD | GitHub Actions |
| Secrets | HashiCorp Vault / AWS KMS |
| Languages | Scala (core), Python (utilities/testing), SQL (transformations) |
| Cloud | AWS (primary), GCP / Azure (compatibility) |

## Code Standards

### Scala
- Functional style preferred. Use `Either`, `Try`, `Option` for error handling.
- No `var` unless absolutely necessary.
- Pattern matching over if/else chains.
- Comprehensive ScalaDoc on all public APIs.
- Never swallow exceptions.
- 80%+ test coverage required for new code.

### Python
- Type hints on all function signatures.
- Black for formatting. Pylint for linting.
- pytest for testing.
- Click or Typer for CLI tools.

### Testing
- Unit tests: ScalaTest (Scala) / pytest (Python)
- Integration tests: Testcontainers for database/Kafka
- Performance tests: Benchmarking for ingestion throughput
- All tests must be deterministic. No time-based flakiness.

### Git / Commits
- Conventional Commits format: `feat(scope): description`, `fix(scope): description`
- Semantic Versioning for releases
- Feature branches, PRs required, CI must pass before merge
- Squash and merge to main

## Repository Structure

```
critical-infra-data-frameworks/
├── .github/
│   └── workflows/          # GitHub Actions CI/CD pipelines
├── docs/                   # Architecture docs, ADRs, compliance mapping
│   ├── architecture-decisions.md
│   ├── compliance-mapping.md
│   ├── configuration-schema.md
│   └── reference-architecture.md
├── core/                   # Scala core ingestion engine
│   └── src/
│       ├── main/scala/
│       └── test/scala/
├── connectors/             # Source connector implementations
├── validation/             # Data quality validation engine
├── monitoring/             # Grafana dashboards, Prometheus configs
├── deployment/             # Docker, Kubernetes manifests, Helm charts
├── examples/               # Sample configurations per sector
│   └── configs/
├── scripts/                # Python utilities (validation, testing, automation)
├── tests/                  # Integration test suites
├── CLAUDE.md              # This file — project context for Claude Code
├── CONTRIBUTING.md
├── README.md
├── ROADMAP.md
├── LICENSE                 # Apache 2.0
├── build.sbt
└── .gitignore
```

## Common Commands

```bash
# Build
sbt compile

# Run tests
sbt test

# Run a specific test
sbt "testOnly *OracleJdbcConnectorSpec"

# Package
sbt assembly

# Format code
sbt scalafmtAll

# Run static analysis
sbt scalafix

# Build Docker image
docker build -t critical-infra-data-frameworks:latest .

# Run locally
docker-compose up -d

# Deploy to Kubernetes (dev)
kubectl apply -f deployment/k8s/dev/

# Python utilities
python -m scripts.validate_config examples/configs/financial-services-trade-data.yaml
pytest scripts/tests/
```

## Current Phase

**Phase 1: Core Ingestion Framework (Months 1-2)**

Objectives:
- Spark/Scala ingestion engine with configuration-driven pipeline execution
- JDBC source connectors (Oracle, Teradata, Postgres)
- File source connectors (CSV, Parquet, JSON)
- Configuration schema specification (YAML/HOCON)
- Bronze-layer storage writer with time-partitioning
- Unit and integration test suites (80%+ coverage)
- Deployment guide for Docker/Kubernetes
- Architecture Decision Records (ADRs)
- v0.1.0 release

## Phased Roadmap

- **Phase 1 (Months 1-2)**: Core Ingestion Framework → v0.1.0
- **Phase 2 (Months 3-4)**: Quality Validation & Monitoring → v0.2.0
- **Phase 3 (Months 5-6)**: Compliance Mapping & Community → v1.0.0 STABLE
- **Phase 4 (Months 7-12+)**: Expansion & Stewardship

## Compliance Framework Targets

The framework maps to four U.S. regulatory frameworks:
- **Dodd-Frank Act** — Enhanced prudential standards (financial services)
- **NERC CIP** — Critical Infrastructure Protection standards (energy)
- **NIST CSF 2.0** — Cybersecurity Framework (cross-sector)
- **NCS 2023** — National Cybersecurity Strategy Pillar One

Compliance mapping documentation connects specific framework capabilities to specific regulatory requirements. This is NOT certification — the framework supports compliance; it does not certify it.

## Key Design Decisions

1. **Why Apache 2.0?** Maximises adoption across U.S. regulated-sector organisations including federal agencies with procurement constraints. Permissive licensing allows commercial, government, and academic use without restriction.

2. **Why Spark/Scala for the core engine?** Production-proven at scale in regulated environments. Strong type system catches errors at compile time. JVM deployment aligns with enterprise infrastructure.

3. **Why configuration-driven?** Eliminates the primary source of bespoke pipeline development. Enables hours-not-weeks onboarding of new sources. Produces consistent error handling, monitoring, and quality enforcement across all sources.

4. **Why Delta Lake / Iceberg?** ACID transactions, time travel, and schema evolution are essential for audit compliance in regulated environments.

5. **Why no credentials in config?** All secrets resolved at runtime from external secrets managers (Vault/KMS). Configuration files are safe to commit to version control.

## Style of Communication

When working on this project, prioritise:
- **Specificity over abstraction** — "Implement the JDBC connector's watermark-based incremental extraction" not "work on connectors"
- **Production-readiness** — Every line of code should be safe to run against production data
- **Compliance awareness** — Consider audit, lineage, and regulatory implications in every design decision
- **Documentation alongside code** — Update docs in the same PR as code changes

## Anti-Patterns (Things We Never Do)

- Silent failure (records dropped without quarantine entry)
- Credentials in config files or environment variables committed to Git
- Source-specific pipeline code (onboarding must be config-driven)
- Breaking changes without major version bump
- Undocumented architectural decisions
- Tests skipped or marked `@Ignore` in production code
- Dependencies on proprietary or vendor-locked tooling

## References

- Reference Architecture: docs/reference-architecture.md
- Compliance Mapping: docs/compliance-mapping.md
- Configuration Schema: docs/configuration-schema.md
- Architecture Decisions: docs/architecture-decisions.md
- Project Roadmap: ROADMAP.md
- Contribution Guidelines: CONTRIBUTING.md

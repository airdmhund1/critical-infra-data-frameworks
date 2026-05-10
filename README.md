# Critical Infrastructure Data Frameworks

**Standardized, open-source reference architectures for secure, reliable data systems in regulated critical-infrastructure sectors.**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/ci.yml/badge.svg)](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/ci.yml)
[![Dependency Check](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/dependency-check.yml/badge.svg)](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/dependency-check.yml)
[![Version](https://img.shields.io/badge/version-0.1.0--SNAPSHOT-orange.svg)](https://github.com/airdmhund1/critical-infra-data-frameworks/releases)

## Project Summary

This framework delivers a configuration-driven Spark/Scala ingestion engine for regulated critical-infrastructure environments. Phase 1 (v0.1.0) includes: JDBC source connectors for Oracle, Teradata, and Postgres; file source connectors for CSV, Parquet, and JSON; a Bronze-layer writer using Delta Lake with time-partitioned, append-only writes; an audit event log; and Kubernetes and Docker packaging. New data sources are onboarded by adding a YAML configuration file — no pipeline code is written per source.

Design constraints that make the framework usable in regulated environments: schema enforcement via ADR-005 (strict or discovered-and-log modes — no silent Spark inference), watermark-based incremental extraction with failure-safe watermark advancement, external secrets management via HashiCorp Vault or AWS KMS (no credentials in configuration files), SHA-256 batch checksum computed from raw input rows before Bronze write, append-only Delta table properties enforced after each write, and an audit event log written on every pipeline run regardless of success or failure. Technology stack: Apache Spark 3.5.3, Delta Lake 3.2.1, Scala 2.13, Kubernetes-deployable, Apache 2.0.

## Problem Statement

The National Cybersecurity Strategy (2023) Pillar One identifies persistent data infrastructure fragmentation as a systemic risk to critical-infrastructure sectors — organisations building one-off systems for each data source produce inconsistent security postures with no standardised baseline for regulators to evaluate. The DOE Artificial Intelligence Strategy (2025) names "persistent data silos" and "legacy infrastructure" as specific barriers to secure AI adoption in the energy sector, and calls for standardised data infrastructure.

The concrete operational consequence: each organisation bears the full burden of selecting, integrating, securing, and validating Apache Spark, Delta Lake, Vault, Kubernetes, and Prometheus — individually, against its own regulatory requirements. Teams building from scratch for each source incur weeks of development per source, produce inconsistent schema enforcement and audit trails, and must re-solve credential management, watermark tracking, and quarantine design independently. This framework provides a tested, documented, and compliance-mapped starting point that reduces that duplication.

## Architecture Overview

See [docs/reference-architecture.md](docs/reference-architecture.md) for the full architecture description. The embedded diagram below (Branch 2) will depict the eight-layer system: Data Sources → Source Connectors → Configuration & Metadata Service → Ingestion Engine → Quality & Validation Engine → Lakehouse Storage → Orchestration & Monitoring → Security Layer.

## Quick Start

Get the local development environment running in 15 minutes — no Spark or Java installation required.

**Prerequisites:** Docker Engine 24+ (includes Compose v2), Git.

```bash
git clone https://github.com/airdmhund1/critical-infra-data-frameworks.git
cd critical-infra-data-frameworks
cp deployment/docker/.env.example deployment/docker/.env
# Edit deployment/docker/.env — replace all `replace-me` placeholders
docker compose -f deployment/docker/docker-compose.yml --env-file deployment/docker/.env up -d
```

See [docs/getting-started.md](docs/getting-started.md) for the full step-by-step guide including Vault credential seeding, MinIO bucket creation, and running the smoke test.

## Configuration Guide

Pipeline sources are defined in YAML configuration files — no custom code required for new source onboarding. The configuration schema covers connectivity (JDBC, file, Kafka, API), schema enforcement mode (strict or discovered-and-log), ingestion strategy (full or watermark-based incremental), quarantine settings, and audit configuration.

See [docs/configuration-schema.md](docs/configuration-schema.md) for the full schema reference.

## Sector Examples

Annotated pipeline configuration files for financial services and energy are in [`examples/configs/`](examples/configs/). Each file documents which configuration fields address which regulatory requirement (Dodd-Frank, NERC CIP-007, NIST CSF 2.0).

Branch 3 will expand this section with embedded annotated YAML snippets.

## Compliance Mapping

The framework maps to four U.S. regulatory frameworks: Dodd-Frank Act enhanced prudential standards (financial services), NERC CIP-007 and CIP-008 (energy critical infrastructure protection), NIST Cybersecurity Framework 2.0 (cross-sector), and the National Cybersecurity Strategy 2023 (Pillar One).

See [docs/compliance-mapping.md](docs/compliance-mapping.md) for the field-level mapping between framework capabilities and specific regulatory controls.

Note: the compliance mapping documents where framework capabilities *support* regulatory requirements. This is not a certification or legal opinion.

## Architecture Decision Records

The [Architecture Decision Records](docs/architecture-decisions.md) document the significant technical choices made during Phase 1 development — including the selection of Apache Spark/Scala, the configuration-driven pipeline design, Delta Lake storage, watermark-based incremental extraction, explicit schema enforcement, and external secrets management.

Each ADR captures: the context that made a decision necessary, the decision taken, the rationale and evidence behind it, the alternatives considered and rejected, and the consequences for operators and contributors.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the detailed phased release plan.

**Phase 1** (Months 1–2): Core ingestion framework and documentation  
**Phase 2** (Months 3–4): Quality validation engine and monitoring templates  
**Phase 3** (Months 5–6): Compliance mapping, deployment guides, and community engagement

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

## License

This project is licensed under the Apache License 2.0 — see [LICENSE](LICENSE) for details.

## Author

**Edmund Adu Asamoah**  
MSc Big Data Technologies | Data & Software Engineer  
[LinkedIn](https://www.linkedin.com/in/eaduasamoah) | [Portfolio](https://easolutionz.com)

# Critical Infrastructure Data Frameworks

**Standardized, open-source reference architectures for secure, reliable data systems in regulated critical-infrastructure sectors.**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/ci.yml/badge.svg)](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/ci.yml)
[![Dependency Check](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/dependency-check.yml/badge.svg)](https://github.com/airdmhund1/critical-infra-data-frameworks/actions/workflows/dependency-check.yml)

## Overview

This project provides production-proven reference architectures and supporting tooling designed for organizations operating in regulated critical-infrastructure environments — including financial services, energy, and insurance.

These frameworks address a documented challenge: organizations in regulated sectors repeatedly build fragmented, one-off data-processing systems from scratch, despite facing substantially similar underlying requirements for secure ingestion, automated quality validation, real-time monitoring, and compliance-grade observability. This fragmentation creates inconsistent security postures across sectors that are vital to national security and economic stability.

The reference architectures in this repository are generalized from patterns designed, built, and validated in production across three critical-infrastructure sectors, achieving documented improvements in processing performance, system reliability, and operational efficiency.

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

## Problem Statement

Critical-infrastructure operators face a common set of data-engineering challenges:

- **High-volume ingestion** under strict latency and reliability constraints
- **Automated data-quality enforcement** with full audit trails for regulatory compliance
- **Real-time monitoring** with compliance-grade observability and alerting
- **Secure protocol handling** across distributed infrastructure components

While individual open-source tools address isolated parts of this problem (e.g., Apache Spark for processing, Great Expectations for validation, Grafana for monitoring), each organization currently bears the full burden of selecting, integrating, securing, and validating these components against its own regulatory requirements.

This project provides an integrated, tested, and documented starting point — reducing duplication, improving baseline security, and accelerating modernization.

## Core Components

| Component | Description | Status |
|-----------|-------------|--------|
| **Ingestion Framework** | Configuration-driven data ingestion enabling new sources to be onboarded through config files rather than custom code | Planned — Phase 1 |
| **Quality Validation Engine** | Rules-based validation with schema conformance, referential integrity, completeness checks, and automated quarantine | Planned — Phase 2 |
| **Monitoring & Observability** | Pre-configured dashboards and alerting for pipeline throughput, failure detection, and SLA adherence | Planned — Phase 2 |
| **Compliance Mapping** | Documentation connecting framework capabilities to sector-specific regulatory requirements | Planned — Phase 3 |
| **Deployment Guides** | Step-by-step documentation for evaluation, testing, and adaptation | Planned — Phase 3 |

## Design Principles

- **Configuration over code**: New data sources onboarded through metadata configuration, not bespoke pipeline development
- **Secure by design**: Security, encryption, and audit-trail capabilities built into the architecture, not added as afterthoughts
- **Sector-adaptable**: Core patterns are consistent; sector-specific requirements are handled through configurable compliance modules
- **Observable**: Built-in monitoring, alerting, and operational visibility at every layer
- **Open and permissive**: Released under Apache 2.0 — free for commercial, government, and academic use

## Technology Foundation

The reference architectures are built on widely adopted, production-grade open-source technologies:

- **Processing**: Apache Spark (Scala/Python)
- **Orchestration**: Configuration-driven job management with enterprise scheduler integration
- **Storage**: Lakehouse architecture (Bronze/Silver/Gold layers)
- **Monitoring**: Grafana, Prometheus
- **Deployment**: Docker, Kubernetes, CI/CD automation
- **Security**: Certificate-based authentication, encrypted communication, audit logging

## Project Roadmap

See [ROADMAP.md](ROADMAP.md) for the detailed phased release plan.

**Phase 1** (Months 1–2): Core ingestion framework and documentation  
**Phase 2** (Months 3–4): Quality validation engine and monitoring templates  
**Phase 3** (Months 5–6): Compliance mapping, deployment guides, and community engagement

## Architecture Decision Records

The [Architecture Decision Records](docs/architecture-decisions.md) document the significant technical choices made during Phase 1 development — including the selection of Apache Spark/Scala, the configuration-driven pipeline design, Delta Lake storage, watermark-based incremental extraction, explicit schema enforcement, and external secrets management.

Each ADR captures: the context that made a decision necessary, the decision taken, the rationale and evidence behind it, the alternatives considered and rejected, and the consequences for operators and contributors.

## Alignment with Federal Priorities

This project supports objectives identified in:

- **National Cybersecurity Strategy (2023)** — Pillar One: Defend Critical Infrastructure, calling for "new and innovative capabilities" and "secure-by-design" technologies
- **DOE Artificial Intelligence Strategy (2025)** — identifying "persistent data silos" and "legacy infrastructure" as barriers to secure AI adoption, and calling for standardized data infrastructure
- **Federal Data Strategy (2020)** — principles of interoperability, data governance, and standardized data management

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

## License

This project is licensed under the Apache License 2.0 — see [LICENSE](LICENSE) for details.

## Author

**Edmund Adu Asamoah**  
MSc Big Data Technologies | Data & Software Engineer  
[LinkedIn](https://www.linkedin.com/in/eaduasamoah) | [Portfolio](https://easolutionz.com)

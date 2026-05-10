# Project Roadmap

## Phased Release Plan

This document outlines the planned development and release phases for the
Critical Infrastructure Data Frameworks project.

---

## Phase 1: Core Ingestion Framework (Months 1–2) — COMPLETE (2026-05-26)

Target release: v0.1.0

### Sprint 1 (Weeks 1–2) — COMPLETE (2026-04-22)

- [x] Issue #1: Repository scaffold and sbt multi-module build
- [x] Issue #2: GitHub Actions CI pipeline, OWASP security scan, repo governance
- [x] Issue #3: Source configuration schema, JSON Schema validator, Python validator CLI, example configs

### Sprint 2 (Weeks 3–4) — COMPLETE (2026-04-24)

- [x] Issue #4: Configuration loader — YAML/HOCON parsing, typed case classes, secrets resolution interface
- [x] Issue #5: Ingestion engine — pipeline orchestrator with configurable step execution
- [x] Issue #6: Secrets resolver — HashiCorp Vault and AWS KMS implementations

### Sprint 3 (Weeks 5–6) — COMPLETE (2026-04-26)

- [x] Issue #7: JDBC connector base — watermark incremental extraction, parallel reads, retry logic
- [x] Issue #8: Oracle JDBC connector — enterprise-grade with Oracle-specific partition strategies
- [x] Issue #9: PostgreSQL JDBC connector
- [x] Issue #10: CSV file connector — schema-enforced with controlled inference modes
- [x] Issue #11: Parquet file connector — schema-aware with Hive partition discovery
- [x] Issue #12: JSON file connector — schema-enforced with nested structure handling

### Sprint 4 (Weeks 7–8) — COMPLETE (2026-05-26)

- [x] Issue #13: Bronze layer writer — Delta Lake, immutable, time-partitioned, checksum-validated
- [x] Issue #14: Audit event log — structured, append-only compliance audit trail
- [x] Issue #15: Dockerfile and docker-compose — local development environment
- [x] Issue #16: Kubernetes manifests — dev environment
- [x] Issue #17: Architecture Decision Records — Phase 1 foundational decisions
- [x] Issue #18: README — production-quality project documentation
- [x] Issue #19: Configuration schema reference — complete field documentation
- [x] Issue #20: v0.1.0 release — Core Ingestion Framework

---

## Phase 2: Quality Validation and Monitoring (Months 3–4) — PLANNED

Target release: v0.2.0  
Target start: 2026-06-01  
Target completion: 2026-08-01

- [ ] Rules-based validation engine with configurable checks (completeness, referential integrity, range/pattern, timeliness)
- [ ] Quarantine system with error classification, audit trail, and recovery workflows
- [ ] Silver-layer writer with validated, conformed data output
- [ ] Pre-configured Grafana dashboards — pipeline throughput, failure rates, SLA adherence, quality metrics
- [ ] Prometheus metrics exporters for all pipeline components
- [ ] Alerting templates — PagerDuty, Slack, email integration
- [ ] SLA monitoring with configurable thresholds and escalation
- [ ] Integration test suite covering end-to-end ingestion-to-monitoring flow
- [ ] v0.2.0 release

---

## Phase 3: Compliance Mapping and Community (Months 5–6) — NOT STARTED

Target release: v1.0.0 STABLE

- [ ] Compliance mapping documentation: Dodd-Frank Act, NERC CIP, NIST CSF 2.0, NCS 2023 Pillar One
- [ ] Gold-layer writer with consumption-optimised views
- [ ] Cross-sector adaptation guides — financial services and energy
- [ ] Sector-specific sample configurations with annotated examples
- [ ] Performance tuning guide with benchmarking methodology
- [ ] Community engagement: conference presentation proposals, blog posts, forum setup
- [ ] Contribution guidelines, code of conduct, issue templates, PR templates
- [ ] v1.0.0 stable release with full documentation

---

## Phase 4: Expansion and Stewardship (Months 7–12+) — NOT STARTED

- [ ] Additional sector modules: insurance, healthcare, federal agencies
- [ ] Advanced modules: CDC and streaming ingestion, ML feature store integration, data mesh patterns
- [ ] Performance optimisation based on production feedback
- [ ] Regulatory update tracking: Dodd-Frank revisions, NERC CIP updates, NIST framework changes
- [ ] Conference presentations, technical blog posts, community office hours
- [ ] Security vulnerability monitoring and patch management
- [ ] Version management and backward compatibility maintenance

---

## Timeline Notes

Phase and sprint status is updated at the end of each sprint as development progresses.

**Current status:** Phase 1 complete (v0.1.0 released 2026-05-26) — Phase 2 planned for 2026-06-01.

## How to Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on how to contribute
to any phase of this project.

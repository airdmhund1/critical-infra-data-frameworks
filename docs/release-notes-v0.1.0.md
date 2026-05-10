# v0.1.0 Release Notes — Critical Infrastructure Data Frameworks

**Release date:** 2026-05-26  
**Tag:** `v0.1.0`  
**Docker image:** `ghcr.io/airdmhund1/critical-infra-data-frameworks:v0.1.0`

---

## What is included in v0.1.0

v0.1.0 delivers the Phase 1 core ingestion framework: a configuration-driven Spark/Scala pipeline engine that reads YAML source configurations and executes extract → validate → Bronze write → audit pipelines without source-specific code.

### Ingestion engine

- Configuration-driven `IngestionEngine` executing the five-step pipeline: connector lookup → JDBC/file extraction → schema validation → Delta Lake Bronze write → audit log
- `SourceConfig` domain model covering all ten pipeline configuration sections
- `Either`-based error handling throughout — no uncaught exceptions at pipeline boundaries
- Structured `AuditEvent` written to a dedicated Delta Lake audit table after every run (success or failure), including `runId`, timestamps, record counts, SHA-256 checksum, and status

### Source connectors

| Connector | Type | Notes |
|-----------|------|-------|
| `OracleJdbcConnector` | JDBC | Oracle Wallet authentication; ojdbc11 not bundled (OTN License) |
| `PostgresJdbcConnector` | JDBC | TLS SSL modes (disable/require/verify-ca/verify-full); driver bundled (BSD-2-Clause) |
| `CsvFileConnector` | File | ADR-005 strict and discovered-and-log schema modes; six configurable CSV options |
| `ParquetFileConnector` | File | Embedded schema drift detection; Hive partition discovery; date-range filter pushdown |
| `JsonFileConnector` | File | JSON Lines and MultilineArray formats; configurable struct flattening; three corrupt record modes |
| `JdbcConnectorBase` | Base | Watermark-based incremental extraction; Spark parallel reads; exponential backoff retry |

### Bronze layer storage

- `DeltaBronzeLayerWriter` writing append-only Delta Lake tables with four `_cidf_*` audit metadata columns (`_cidf_ingestion_ts`, `_cidf_source_name`, `_cidf_run_id`, `_cidf_checksum`)
- SHA-256 batch checksum computed pre-write and stored as a constant column per batch
- `delta.appendOnly = true` enforced on every write (idempotent ALTER TABLE)
- Record count mismatch validation with configurable threshold

### Secrets management

- `VaultSecretsResolver` resolving `vault://` URI references via HashiCorp Vault AppRole authentication
- `AwsKmsSecretsResolver` for AWS KMS / Secrets Manager
- `LocalDevSecretsResolver` for local development without a running secrets manager

### Deployment

- Multi-stage Dockerfile (eclipse-temurin:11-jdk builder → eclipse-temurin:11-jre runtime, < 600MB)
- `docker-compose.yml` local dev stack: ingestion-engine, PostgreSQL 15, MinIO, HashiCorp Vault 1.17
- Kubernetes manifests for dev clusters (minikube, kind) with resource limits, security context, and exec probes
- Smoke test script verifying all four services healthy and Bronze storage accessible

### Documentation

- Six Architecture Decision Records (ADRs 001–006) covering all foundational design decisions
- Complete `docs/configuration-schema.md` field reference with annotated examples, onboarding tutorial, common patterns, and troubleshooting
- `docs/getting-started.md` — Getting Started in 15 Minutes guide tested against the Docker Compose stack

---

## Compliance foundations delivered

The following capabilities directly support regulated-sector requirements:

| Capability | Regulation addressed |
|------------|---------------------|
| Append-only Bronze Delta tables with write-once enforcement | Dodd-Frank 17 CFR Part 45 (immutable records), NERC CIP-007 R5 (data integrity) |
| Structured audit event log (every run, success or failure) | Dodd-Frank, NERC CIP-008 R3 (incident records), NIST CSF 2.0 PR.AC |
| SHA-256 batch checksum on Bronze writes | NERC CIP-007 R5 (data integrity verification) |
| Explicit schema enforcement (ADR-005) — no silent inference | Dodd-Frank reproducibility requirements, NIST CSF 2.0 ID.AM |
| External secrets management (Vault / KMS) — no credentials in configs | NIST CSF 2.0 PR.AC, NCS 2023 Pillar One |
| Configurable 7-year / 5-year retention (2555 / 1825 days) | Dodd-Frank 7-year minimum, NERC CIP-007 R5 / CIP-008 R3 5-year minimum |

Note: these capabilities *support* regulatory requirements. This is not a certification or legal opinion.

---

## Known limitations in v0.1.0

- **No `Main` class entry point**: `java -jar engine.jar` will fail with "no main manifest attribute". Running the pipeline requires calling `IngestionEngine.run()` programmatically. A CLI entry point is planned for v0.2.0.
- **Bronze layer only**: the ingestion engine writes to Bronze only. Silver and Gold layers are Phase 2.
- **Quality rules disabled**: `qualityRules.enabled` must be `false` in v0.1.0. Rules-based validation is Phase 2.
- **Teradata JDBC connector**: `TDJdbcConnector` is scaffolded but not implemented. Planned for Phase 2.
- **Kafka and API connectors**: connector interfaces exist but no production implementations are provided in v0.1.0.

---

## What is coming in v0.2.0

- Rules-based data quality validation engine (completeness, referential integrity, range/pattern, timeliness)
- Silver-layer writer with conformed, validated data output
- Pre-configured Grafana dashboards and Prometheus metrics exporters
- Alerting templates (PagerDuty, Slack, email)
- SLA monitoring with configurable thresholds
- CLI entry point for `java -jar engine.jar --config path/to/config.yaml`
- Teradata JDBC connector

---

## Docker image

```bash
# Pull the v0.1.0 image
docker pull ghcr.io/airdmhund1/critical-infra-data-frameworks:v0.1.0

# Or use the latest tag (always points to the most recent stable release)
docker pull ghcr.io/airdmhund1/critical-infra-data-frameworks:latest
```

The image is built from `deployment/docker/Dockerfile` using the multi-stage eclipse-temurin:11 build.

---

## SHA-256 checksums

| Artefact | SHA-256 |
|----------|---------|
| `critical-infra-core-assembly-0.1.0.jar` | `<populate after running sbt assembly — see maintainer steps below>` |

To verify after download:
```bash
sha256sum critical-infra-core-assembly-0.1.0.jar
```

---

## Maintainer release steps

These steps are performed manually by the project maintainer after all Phase 1 branches are merged to `main` and CI is green.

### 1. Verify CI is green on `main`

```bash
gh run list --branch main --limit 5
```

All runs must show `completed` / `success`.

### 2. Build and verify the fat JAR

```bash
sbt clean assembly
sha256sum core/target/scala-2.13/critical-infra-core-assembly-0.1.0.jar
```

Populate the SHA-256 value in the table above before creating the GitHub Release.

### 3. Create and push the git tag

```bash
git tag -a v0.1.0 -m "v0.1.0 — Core Ingestion Framework"
git push origin v0.1.0
```

### 4. Create the GitHub Release

```bash
gh release create v0.1.0 \
  --title "v0.1.0 — Core Ingestion Framework" \
  --notes-file docs/release-notes-v0.1.0.md \
  core/target/scala-2.13/critical-infra-core-assembly-0.1.0.jar
```

### 5. Build and push the Docker image

```bash
docker build -t ghcr.io/airdmhund1/critical-infra-data-frameworks:v0.1.0 \
             -t ghcr.io/airdmhund1/critical-infra-data-frameworks:latest \
             -f deployment/docker/Dockerfile .

docker push ghcr.io/airdmhund1/critical-infra-data-frameworks:v0.1.0
docker push ghcr.io/airdmhund1/critical-infra-data-frameworks:latest
```

### 6. Close Phase 1 issues on GitHub

Close Issues #1–#20 on GitHub once the release is published. The maintainer does this manually via the GitHub UI or:

```bash
for issue in $(seq 1 20); do
  gh issue close $issue --repo airdmhund1/critical-infra-data-frameworks \
    --comment "Closed as part of v0.1.0 release."
done
```

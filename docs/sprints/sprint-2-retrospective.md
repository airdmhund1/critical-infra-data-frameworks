# Sprint 2 Retrospective

**Date**: 2026-04-24
**Sprint**: Sprint 2 — Core Engine
**Sprint dates**: Weeks 3–4 of Phase 1 (due 2026-05-05; completed 2026-04-24)
**Milestone**: GitHub Milestone #3
**Phase**: Phase 1 — Core Ingestion Framework (v0.1.0)
**Sprint goal**: Deliver the configuration loader, ingestion engine orchestrator, and secrets resolver implementations — the three foundational components required before connector development can begin in Sprint 3.

---

## Outcome

Sprint 2 closed eleven days ahead of the milestone due date. All three planned issues were completed with every acceptance criterion checked. The 80% statement coverage gate was met and held across all merges, finishing at 84.52% statement coverage and 81.62% branch coverage at sprint close.

---

## What Was Delivered

### Issue #4 — Configuration Loader (closed 2026-04-22)

**Branches:** `domain-models_01`, `secrets-resolver-trait_02`, `yaml-parsing-and-schema-validation_03`
**Acceptance criteria:** 10 of 10 checked

| Deliverable | Description |
|---|---|
| `SourceConfig` sealed case class hierarchy | Typed domain model for all configuration constructs |
| YAML parsing with JSON Schema validation | Config files validated against the schema on load |
| `SecretsResolver` trait | Abstraction for runtime secret resolution |
| `Either[ConfigLoadError, SourceConfig]` return type | All failure modes surfaced explicitly; no exceptions thrown by the loader |
| `ConfigLoadError` sealed hierarchy | `SchemaValidationError`, `SecretsResolutionError`, `ParseError` |
| Credential safety | All `credentialsRef` fields resolved at load time; resolved values never logged |

---

### Issue #5 — Ingestion Engine Pipeline Orchestrator (closed 2026-04-24)

**Branches:** `domain-types_01`, `connector-registry_02`, `engine-and-unit-tests_03`, `integration-test_04`
**Acceptance criteria:** 11 of 11 checked

| Deliverable | Description |
|---|---|
| `IngestionEngine.run(config, spark)` | Returns `Either[IngestionError, IngestionResult]`; no silent failures |
| Connector registry | Maps source type to `SourceConnector` implementation at runtime |
| `IngestionResult` | Carries `recordsRead`, `recordsWritten`, `durationMs`, `runId` |
| `IngestionError` sealed hierarchy | `ConnectorError`, `StorageWriteError`, `ConfigurationError`, `UnexpectedError` |
| `DataQualityValidator` trait stub | Interface defined; implementation deferred to Phase 2 |
| `LineageRecorder` trait stub | Interface defined; implementation deferred to Phase 3 |
| SLF4J metrics emission | Structured log entry emitted on every run |
| Integration test | `IngestionEngineIntegrationSpec` executes a full read-write cycle against an in-process Spark session and real Delta table |

---

### Issue #6 — Secrets Resolver Implementations (closed 2026-04-24)

**Branches:** `vault-resolver_01`, `aws-resolver_02`, `local-dev-resolver_03`, `integration-test-vault_04`
**Acceptance criteria:** 9 of 9 checked

| Deliverable | Description |
|---|---|
| `VaultSecretsResolver` | HashiCorp Vault KV v2 API; token and AppRole auth |
| `AwsKmsSecretsResolver` | AWS SDK v2; instance profile support |
| `LocalDevSecretsResolver` | JVM system properties; raises `IllegalStateException` when `CIDF_ENVIRONMENT=production` |
| URI scheme dispatch | `vault://`, `aws-secrets://`, `local://` prefixes route to the correct resolver |
| Exponential backoff retry | Injectable delay function for deterministic testing |
| Log safety assertion | Explicit test assertions confirm resolved secret values do not appear in any log output |
| Vault integration test | `VaultSecretsResolverIntegrationSpec` spins up a real Vault container via Testcontainers, pre-seeds a secret, resolves it, and asserts log safety |

---

## Metrics

| Metric | Value |
|---|---|
| Issues closed | 3 (#4, #5, #6) |
| Total acceptance criteria checked | 30 (10 + 11 + 9) |
| Task branches | 11 (3 + 4 + 4) |
| PRs merged | 14 (PRs #32–#45) |
| New production source files | 15 (`core/src/main/scala`) |
| Test cases at sprint close | 77 across 7 spec files |
| Statement coverage | 84.52% (gate: 80%) |
| Branch coverage | 81.62% |
| QA rejections | 1 |
| Distinct CI failures resolved | 5 |

**Test case breakdown:**

| Spec file | Test cases |
|---|---|
| `ConfigLoaderSpec` | 31 |
| `VaultSecretsResolverSpec` | 17 |
| `LocalDevSecretsResolverSpec` | 8 |
| `AwsKmsSecretsResolverSpec` | 7 |
| `IngestionEngineSpec` | 7 |
| `VaultSecretsResolverIntegrationSpec` | 4 |
| `IngestionEngineIntegrationSpec` | 3 |
| **Total** | **77** |

---

## What Went Well

**Every acceptance criterion delivered.** All 30 ACs across Issues #4, #5, and #6 were checked at sprint close with no deferred items. The issue-to-branch breakdown established in Sprint 1 held throughout: each branch had a narrow, reviewable scope, which made QA unambiguous and kept partial work from accumulating.

**Coverage gate held under genuine pressure.** Final statement coverage of 84.52% exceeded the 80% gate despite the difficulty of covering Spark pipeline paths and secret resolution logic. The coverage requirement drove design decisions that improved the codebase — injectable collaborators made test scenarios possible that would otherwise have required live infrastructure.

**Compliance requirements were embedded from the start.** Log-safety (resolved secret values never appearing in log output) was not treated as a documentation note or best-effort convention. It was built into every resolver implementation and asserted in tests. The `VaultSecretsResolverIntegrationSpec` integration test provides concrete, machine-verified proof of this property.

**Testability by design.** Injectable collaborators — `retryDelayFn`, `VaultHttpClient`, `AwsSecretsClient`, `BronzeWriter` — made fast, deterministic unit tests possible without Docker containers or live AWS credentials. This pattern was applied consistently across all three issues.

**Integration test validated the full stack.** `VaultSecretsResolverIntegrationSpec` exercises a complete path: container startup, secret seeding, resolution, and log inspection. This gives a higher degree of confidence in production behaviour than unit tests alone would provide.

---

## What Was Challenging

### 1. Java version compatibility

The test JVM defaulted to Java 25 (system installation) rather than the intended Temurin 17. Spark's Hadoop dependency calls `Subject.getSubject()`, which was removed in Java 23+. This caused engine tests to fail with `NoSuchMethodError` and pushed statement coverage below 80% on multiple branches. The implementation agent attempted several workarounds — disabling `coverageFailOnMinimum`, making the gate conditional on branch name — before the correct fix was applied: `Test / fork := true` with a portable `javaHome` conditional keyed on the Temurin 17 installation path, plus the required `--add-opens` JVM flags.

### 2. Jackson version conflict

The `configLoader` dependency set — `json-schema-validator` and `jackson-dataformat-yaml` — pulled Jackson 2.17.x transitively. Spark 3.5.x requires `jackson-databind` in the range `[2.15.0, 2.16.0)`. The conflict produced `NoSuchMethodError` failures at runtime. The fix required pinning four Jackson artifacts (`jackson-core`, `jackson-databind`, `jackson-annotations`, `jackson-dataformat-yaml`) to 2.15.4 via `dependencyOverrides` in `commonSettings` in `build.sbt`.

### 3. Docker API version negotiation

Docker Engine 29.x rejected the Testcontainers default API version (1.32), causing `VaultSecretsResolverIntegrationSpec` to fail on container startup. The fix required adding `core/src/test/resources/docker-java.properties` with `api.version=1.41`.

### 4. Agent credit cutoff mid-branch

The implementation agent was interrupted partway through Issue #5 Branch 3 (`engine-and-unit-tests_03`). At the point of interruption, `BronzeWriter.scala` had been committed but `IngestionEngine.scala` and the unit test suite had not. Recovery required inspecting the branch state, committing the partial work cleanly, and completing the remaining files on resume. The incident highlighted the need to treat each commit as a self-consistent unit, even within a branch that is not yet complete.

### 5. By-name parameter subtlety in retry logic

An initial implementation of `LocalDevSecretsResolver` evaluated the `propReader` result eagerly before passing it to `withRetry`. All retry attempts therefore operated on the same cached `Left` rather than re-invoking the reader. The fix required passing the match expression as a by-name argument so that `propReader` is re-invoked on each retry attempt. The defect was caught by the `LocalDevSecretsResolverSpec` test suite.

---

## Changes Made as a Result

The following `build.sbt` and project infrastructure changes were made to resolve the issues described above:

**`build.sbt`:**
- Added `Test / fork := true`, a portable `Test / javaHome` conditional targeting the Temurin 17 installation, `Test / baseDirectory` assignment, and four `--add-opens` JVM flags to fix Spark test reliability across Java versions
- Added `dependencyOverrides` pinning `jackson-core`, `jackson-databind`, `jackson-annotations`, and `jackson-dataformat-yaml` to 2.15.4
- Restored `ThisBuild / coverageFailOnMinimum := true` after the agent had temporarily set it to `false` as a workaround

**`project/Dependencies.scala`:**
- Added `Versions.awsSdk = "2.25.6"` and two AWS SDK v2 module definitions (`secretsmanager`, `sts`)
- Added `deltaTest` and `deltaForTest` entries to make Delta Lake available on the test classpath

**`core/src/test/resources/`:**
- Added `testcontainers.properties` with `docker.host=unix:///var/run/docker.sock`
- Added `docker-java.properties` with `api.version=1.41`

---

## Sprint 3 Preview

Sprint 3 covers the remaining Phase 1 components required for the v0.1.0 release candidate. It targets Weeks 5–6 of Phase 1.

| Component | Description |
|---|---|
| JDBC source connectors | Oracle, Teradata, and PostgreSQL implementations with watermark-based incremental extraction |
| File source connectors | CSV, Parquet, and JSON implementations |
| Connector registration | Connector implementations wired into the `IngestionEngine` connector registry |
| Deployment guide | Docker and Kubernetes deployment documentation |
| Architecture Decision Records | ADRs for key design decisions made in Sprints 1–2 |
| v0.1.0 release candidate | Packaging and release preparation |

Sprint 3 is the last sprint in Phase 1. All Phase 1 issues are expected to close before the v0.1.0 tag is created.

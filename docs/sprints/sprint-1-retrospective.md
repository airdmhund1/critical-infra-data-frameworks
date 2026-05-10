# Sprint 1 Retrospective

**Date**: 2026-04-22
**Sprint**: Sprint 1 - Foundation
**Sprint dates**: 2026-04-20 to 2026-04-22
**Phase**: Phase 1 - Core Ingestion Framework (v0.1.0)
**Sprint goal**: Establish the foundational layer - build system, CI pipeline, and configuration schema - so that connector and engine implementation can begin in Sprint 2.

---

## Issues Completed

All 3 planned issues closed.

| Issue | Title | Status |
|-------|-------|--------|
| #1 | Repository scaffold and sbt multi-module build | Completed |
| #2 | GitHub Actions CI pipeline | Completed |
| #3 | Source configuration schema | Completed |

---

## What Was Delivered

### Issue #1 - Repository scaffold and sbt multi-module build

- sbt 1.9.9 multi-module project with four modules: `core`, `connectors`, `validation`, `monitoring`
- Scala 2.13.14, Spark 3.5.3, Delta Lake 3.2.1
- `project/Dependencies.scala`, `project/plugins.sbt`, `project/build.properties`
- `.scalafmt.conf` and `.scalafix.conf` for code quality enforcement
- sbt-scoverage 2.1.1 with 80% minimum statement coverage threshold enforced
- `sbt compile`, `sbt scalafmtCheck`, and `sbt test` all pass cleanly

### Issue #2 - GitHub Actions CI pipeline

Three branches delivered:

**ci-pipeline-core** - `.github/workflows/ci.yml`
- Triggers on push to `main` and all pull requests
- Full step chain: checkout → Java 11 → cache → compile → scalafmtCheck → scalafix → coverage test → coverageReport → coverageAggregate
- sbt/Coursier cache keyed on `build.sbt` and `project/Dependencies.scala`
- Coverage gate enforced at 80%

**security-scan** - `.github/workflows/dependency-check.yml`
- OWASP Dependency-Check with `--failOnCVSS 7`
- HTML report artifact uploaded on every run

**repo-governance**
- CI status badges added to `README.md`
- `.github/branch-protection.md` documenting required settings for `main`: 1 approving review required, `build-test` and `owasp-dependency-check` status checks required before merge

### Issue #3 - Source configuration schema

Four branches delivered:

**yaml-schema-spec** - `docs/configuration-schema.md`
- 346-line specification covering all 10 schema sections
- Field tables, YAML snippets, complete skeleton, schema versioning policy

**json-schema-file** - `schemas/source-config-v1.json`
- JSON Schema Draft 2019-09, 374 lines
- All 10 sections covered, enums enforced
- `qualityRules` locked to Phase 2 placeholder
- `not` rule blocking literal credentials in the `connection` block

**example-configs**
- `examples/configs/financial-services-oracle-trades.yaml` - Oracle JDBC, incremental extraction, Dodd-Frank 7-year retention
- `examples/configs/energy-ev-telemetry-csv.yaml` - CSV file, nightly full load, NERC CIP 5-year retention
- Both configs validated programmatically against the JSON Schema

**validator-cli-and-tests** - `scripts/validate_config.py`
- CLI with exit 0/1, field-level error output, literal credential detection
- 13 pytest tests covering: valid configs, missing required fields, invalid enums, literal credential flagging
- All 13 tests pass

---

## Metrics

| Metric | Value |
|--------|-------|
| Issues closed | 3 (#1, #2, #3) |
| Branches created | 10 |
| Total QA review passes run | 8 |
| Fixes applied post-QA | 3 |
| Tests delivered | 13 pytest tests, all passing |

**QA review cycles per issue:**

- Issue #1: 1 cycle, approved first pass
- Issue #2: ci-pipeline-core - 1 cycle, approved; security-scan - 2 cycles (approved with mandatory pre-merge fix applied); repo-governance - 2 cycles (failed first pass on job name mismatch, approved after fix)
- Issue #3: All 4 branches approved first pass (Branch 2 ran programmatic JSON schema validation; Branch 3 ran live pytest suite)

---

## What Went Well

**The implement-commit-QA-fix-approve loop held.** No branch reached the next task with unresolved issues carried over from the prior branch. The sequential dependency management - merging each branch into the next before implementation - kept branches reviewable in isolation while ensuring accumulated context was always available to subsequent branches.

**Issue #3 was the cleanest segment of the sprint.** All four branches passed first-pass QA. Programmatic validation (JSON Schema + pytest) gave objective PASS/FAIL signals that removed ambiguity from the review process. When a validator can run against a file and produce a clear exit code, QA is unambiguous.

**Compliance-aware defaults were baked in at the schema layer.** 7-year Dodd-Frank retention, 5-year NERC CIP retention, `immutableRawEnabled: true` as the default, and the `not` rule blocking literal credentials in connection config were all enforced in the JSON Schema. These are architectural decisions locked at the contract level, not guidelines left to individual implementors.

---

## What Was Challenging

**OWASP Dependency-Check action (Issue #2).** The initial implementation passed the NVD API key via `env:` at the step level rather than via the action's `with: nvdApiKey:` input parameter - the key was silently ignored in that position. A second fix commit was required: moving from `@v3.0.0` to `@main` and restructuring the step. This was the only issue that required user-directed correction after QA had already approved a version.

**Duplicate `env:` blocks in the OWASP workflow.** When the OWASP step was restructured, a new `env:` block was inserted before `with:` but the original `env: JAVA_OPTS` block after `with:` was left in place, producing two `env:` keys in the same YAML mapping. In YAML, the second key silently overwrites the first. This was caught and fixed before the final commit but should have been caught earlier.

**Job name mismatch in `branch-protection.md` (Issue #2).** The governance document was written before the CI workflow file was available on the same branch. The documented required status check was `build` but the actual `jobs:` key in `ci.yml` was `build-test`. Any branch protection rule referencing `build` would have been non-functional. Caught by QA via cross-branch `git show` verification.

**Technical-writer subagent lacks shell access.** Cannot execute `git commit`. The orchestrator must commit after file creation. Minor workflow friction but consistent across both technical-writer invocations in the sprint.

---

## What We Are Doing Differently in Sprint 2

**Cross-branch name verification.** Where a branch's documentation references names or values defined in a prior branch (job names, field names, file paths), verify those names by reading the source file directly via `git show <branch>:<path>` before committing the referencing branch. Do not rely on memory or prior summaries.

**YAML duplicate key check.** For all devops-engineer YAML work, add an explicit instruction to scan for duplicate keys at the same mapping level before committing. This is not caught by standard YAML parsers that implement last-key-wins semantics silently.

**Subagent capability disclosure.** For any subagent that lacks shell access (e.g., technical-writer), include an explicit note in the prompt so there is no expectation mismatch between the subagent's output and the orchestrator's next step.

---

## Sprint 2 Preview

Sprint 2 begins connector and engine implementation - the first Scala production code in the repository. The configuration schema locked in Sprint 1 is the contract all Sprint 2 Scala code must implement against.

Planned issues in likely execution order:

| Issue | Component | Description |
|-------|-----------|-------------|
| #4 | CORE | Configuration loader: YAML/HOCON parsing, typed Scala case classes, secrets resolution interface |
| #5 | CORE | Ingestion engine: pipeline orchestrator with configurable step execution |
| #6 | CORE | Secrets resolver: HashiCorp Vault and AWS KMS implementations |
| #7 | CONNECTOR | JDBC connector base: watermark incremental extraction, parallel reads, retry logic |
| #8 | CONNECTOR | Oracle JDBC connector |
| #9 | CONNECTOR | PostgreSQL JDBC connector |
| #10 | CONNECTOR | CSV file connector |
| #11 | CONNECTOR | Parquet file connector |
| #12 | CONNECTOR | JSON file connector |
| #13 | STORAGE | Bronze layer writer: Delta Lake, immutable, time-partitioned, checksum-validated |
| #14 | STORAGE | Audit event log: structured, append-only compliance audit trail |

Sprint 2 introduces the first test coverage requirement enforced by CI at the 80% statement coverage gate established in Sprint 1.

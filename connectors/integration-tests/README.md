# Oracle JDBC Connector — Integration Tests

Integration tests for `OracleJdbcConnector` that exercise a real Oracle Database instance.
These tests are **excluded from default CI runs** and must be enabled explicitly via an
environment variable.

---

## Why these tests are excluded from default CI

- **Oracle Database is not freely containerisable for automated CI.** The official Oracle
  Docker image (`container-registry.oracle.com/database/...`) requires accepting the Oracle
  Technology Network (OTN) License interactively — it cannot be pulled in unattended GitHub
  Actions runs without a pre-authenticated token registered against an oracle.com account.

- **ojdbc11 cannot be bundled with this project.** The OTN License prohibits redistribution
  of the Oracle JDBC driver jar. Adding ojdbc11 to `build.sbt` or `project/Dependencies.scala`
  would make the project non-distributable under its Apache 2.0 License. The driver must be
  supplied separately at runtime by the engineer running these tests.

- **Default CI (`sbt test`) runs only freely-licensed tests.** Unit tests use in-process stubs
  and test doubles. Testcontainers-based integration tests use freely-licensed database images
  (H2 in-process, PostgreSQL 16 via Docker Hub). No Oracle-specific infrastructure is required.

---

## Prerequisites

1. **Access to a running Oracle Database instance** — version 19c or later; 21c or 23c is
   recommended. The instance must be reachable from the machine running the tests.

2. **ojdbc11 on the local JVM classpath** — version 23.4.0.24.05 is recommended.

   Download options:
   - Direct download: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
   - Oracle Maven repository (requires oracle.com account): https://maven.oracle.com

   Runtime classpath options (choose one):
   - Place the jar in `$SPARK_HOME/jars/` so Spark picks it up automatically.
   - Pass it via the sbt `javaOptions` or as a Spark `--jars` argument.
   - Place it in the JVM boot classpath if running tests outside Spark.

3. **Docker is NOT required.** These tests connect to a real Oracle instance — no container
   is started by the test suite.

---

## Running the integration tests

### Set required environment variables

```bash
export ORA_INTEGRATION=true
export ORA_HOST=oracle-dev.example.com
export ORA_PORT=1521
export ORA_SERVICE=ORCL
export ORA_USER=testuser
export ORA_PASSWORD=testpass
```

### Run Oracle integration tests

```bash
sbt "project connectors" test
```

The sbt `testOptions` filter is lifted automatically when `ORA_INTEGRATION=true`, so all
tests tagged `OraIntegration` will execute alongside the regular unit test suite.

### Optional: Wallet authentication scenario

When `ORA_WALLET_PATH` is set, scenario 5 (Wallet authentication) also runs.
When it is absent, that test is skipped via `assume()`.

```bash
export ORA_WALLET_PATH=/opt/oracle/wallet
sbt "project connectors" test
```

---

## What the tests cover

| # | Scenario | Key assertion |
|---|----------|---------------|
| 1 | **Full refresh** | `extract()` returns all 5 rows from `PIPELINE_TEST` |
| 2 | **Incremental with watermark** | Pre-seeding `IntegerWatermark(2L)` causes only rows 3, 4, 5 to be returned |
| 3 | **Parallel reads** | Partition options `(id, 1, 5)` produce correct row count across Spark partitions |
| 4 | **Watermark persistence** | Two sequential incremental runs correctly advance the high-water mark from 5 to 7 |
| 5 | **Wallet authentication** | `OracleJdbcConnector` with `walletPath` set succeeds (skipped when `ORA_WALLET_PATH` is absent) |

The test suite creates a `PIPELINE_TEST` table in `beforeAll()`, inserts rows 6 and 7
mid-suite for scenario 4, and drops the table in `afterAll()`.

---

## Oracle version compatibility

| Attribute | Value |
|-----------|-------|
| Minimum version | Oracle Database 19c |
| Recommended version | Oracle Database 21c or 23c |
| JDBC driver | ojdbc11 23.4.0.24.05 |
| Driver cross-version support | ojdbc11 supports Oracle 19c, 21c, and 23c via cross-version compatibility |

The legacy `jdbc:oracle:thin:@host:port:SID` (SID-colon) format is not supported.
`OracleJdbcConnector` always generates the `@//host:port/service` (service-name) format.

---

## Notes on Oracle type handling

Oracle's type system has several important differences from ANSI SQL that affect Spark reads:

- **`DATE` includes time.** Oracle `DATE` stores both date and time; Spark maps it to
  `TimestampType`. Do not assume a `DATE` column is date-only.
- **`TIMESTAMP WITH TIME ZONE`.** Reads as strings unless `oracle.jdbc.J2EE13Compliant=true`
  is set as a JDBC property.
- **`NUMBER` precision.** `NUMBER` without explicit precision maps to `DecimalType(38,10)` in
  Spark. Cast explicitly for columns used as watermarks or partition bounds.

See `docs/connectors/oracle-connector.md` for the full Oracle type-handling reference,
including recommended casts and `extraJdbcOptions` for time-zone preservation.

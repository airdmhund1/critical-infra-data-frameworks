# Oracle JDBC Connector

**Connector class**: `OracleJdbcConnector`  
**Driver**: `oracle.jdbc.OracleDriver` (ojdbc11, operator-supplied)  
**Config key**: `connection.jdbcDriver: "oracle"`

---

## Overview

`OracleJdbcConnector` extracts data from Oracle Database via Spark's JDBC data source. It supports
both full and watermark-based incremental extraction, parallel partition reads across a numeric
column, and Oracle Wallet authentication for mutual TLS environments.

Use this connector when the source system is Oracle Database (11g and later, Thin JDBC compatible).
It is the correct choice for regulated financial-services workloads where the source is an Oracle
trading, risk, or reference-data database.

**OTN licensing note**: The Oracle JDBC driver (ojdbc11) is distributed under the Oracle Technology
Network (OTN) License, which prohibits redistribution. The driver jar is **not bundled** with this
framework. Operators must supply the jar from their own Oracle entitlement before running a pipeline
that uses this connector. See [Prerequisites](#prerequisites) for exact steps.

---

## Prerequisites

### Obtain the JDBC driver

The ojdbc11 jar must come from one of these two sources. Choose the one that fits your
organisation's procurement and build toolchain.

**Option A — Oracle Maven repository** (recommended for repeatable CI/CD builds)

Oracle publishes ojdbc11 to `https://maven.oracle.com`. Access requires a free Oracle account.

1. Create or sign in to an Oracle account at [oracle.com](https://www.oracle.com).
2. Accept the Oracle Technology Network License Agreement for JDBC drivers.
3. Generate a token at `https://maven.oracle.com/` and add credentials to `~/.m2/settings.xml` or
   the equivalent for your build system.
4. Add the dependency and repository to your project's build file. For sbt:

   ```scala
   // build.sbt
   resolvers += "Oracle Maven" at "https://maven.oracle.com/public"
   libraryDependencies += "com.oracle.database.jdbc" % "ojdbc11" % "23.4.0.24.05"
   ```

**Option B — Manual download** (suitable for air-gapped or restricted environments)

1. Download `ojdbc11.jar` from
   [oracle.com/database/technologies/appdev/jdbc-downloads.html](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html).
2. Place the jar in the project's unmanaged dependency directory:

   ```
   lib/ojdbc11.jar
   ```

   sbt picks up all jars in `lib/` automatically. No `build.sbt` change is needed.

### Provide the driver to Spark at runtime

The jar must be on the Spark executor classpath. Choose one approach:

| Approach | Command / action |
|---|---|
| `--jars` flag | `spark-submit --jars /path/to/ojdbc11.jar ...` |
| Pre-installed in Spark home | Copy jar to `$SPARK_HOME/jars/` on all cluster nodes |
| Kubernetes / Docker image | Add `COPY ojdbc11.jar $SPARK_HOME/jars/` to the Dockerfile |

The connector will fail at `OracleJdbcConnector.testConnection` with a `ClassNotFoundException`
if the driver is absent. This is an intentional fast-fail rather than a silent runtime error.

---

## Configuration Reference

The full configuration schema is documented in
[`docs/configuration-schema.md`](../configuration-schema.md). The fields below are the ones
relevant to Oracle.

### `connection` section

| YAML key | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | Must be `"jdbc"`. |
| `credentialsRef` | string | Yes | Vault or KMS path to credentials (see [Authentication](#authentication)). |
| `host` | string | Yes | Oracle server hostname or IP. |
| `port` | integer | Yes | Listener port. Default Oracle port is `1521`. |
| `database` | string | Yes | Oracle **service name** (not SID). Example: `"TRADEDB"`. |
| `jdbcDriver` | string | Yes | Must be `"oracle"`. Selects `OracleJdbcConnector`. |

The JDBC URL constructed from these fields follows the Thin driver service-name format:

```
jdbc:oracle:thin:@//host:port/service
```

SID-format URLs (`@host:port:SID`) are **not supported**. See [Known Limitations](#known-limitations).

### `ingestion` section

| YAML key | Type | Required | Description |
|---|---|---|---|
| `mode` | string | Yes | `"full"` or `"incremental"`. |
| `incrementalColumn` | string | If mode = incremental | Column used to filter new/changed records. Must be monotonically increasing. |
| `watermarkStorage` | string | If mode = incremental | URI where the engine persists the last-seen watermark value between runs. |
| `batchSize` | integer | No | JDBC fetch size (rows per round-trip). Default: `10000`. For Oracle, `25000` is a reasonable starting value for wide rows. |
| `parallelism` | integer | No | Number of concurrent Spark read tasks. Default: `4`. Also sets `numPartitions` when partition options are configured. |
| `schedule` | string | No | Cron expression for automatic triggering. Absent means the pipeline is triggered externally (e.g. via Airflow). |
| `timeout` | integer | No | Maximum wall-clock seconds before the engine aborts. Default: `3600`. |

---

## Authentication

### Username/password (Vault-managed)

Set `credentialsRef` to the Vault path containing the database credentials. The secrets resolver
reads the secret at pipeline startup and injects the username and password into the JDBC connection
without writing them to any log or config file.

```yaml
connection:
  credentialsRef: "vault://secret/prod/oracle-trades/jdbc-credentials"
```

The Vault secret at that path must contain `username` and `password` keys. The resolver constructs
the JDBC connection properties from these values. No credentials appear in the YAML file.

### Oracle Wallet (mutual TLS)

Oracle Wallet authentication replaces embedded credentials with a file-based PKI credential store.
Use this when the DBA has provisioned a wallet instead of (or in addition to) a database password.

**What you need from the DBA:**

The wallet directory must contain these four files:

| File | Purpose |
|---|---|
| `cwallet.sso` | SSO wallet (auto-login, no passphrase needed at runtime) |
| `ewallet.p12` | PKCS#12 wallet (used when passphrase is required) |
| `tnsnames.ora` | TNS alias-to-address mappings |
| `sqlnet.ora` | sqlnet configuration including wallet location |

**Step-by-step setup:**

1. Obtain the wallet directory from the DBA. A typical structure:

   ```
   /opt/oracle/wallet/prod-tradedb/
   ├── cwallet.sso
   ├── ewallet.p12
   ├── tnsnames.ora
   └── sqlnet.ora
   ```

2. Verify the `sqlnet.ora` file already contains the wallet location directive. If not, add it:

   ```
   WALLET_LOCATION =
     (SOURCE =
       (METHOD = FILE)
       (METHOD_DATA =
         (DIRECTORY = /opt/oracle/wallet/prod-tradedb)))
   SSL_CLIENT_AUTHENTICATION = TRUE
   ```

3. The wallet path **must be a resolved on-disk directory path** — it cannot be a Vault reference.
   If the wallet itself is stored in a secrets manager, retrieve and write it to local disk before
   constructing the connector. The `credentialsRef` field still points to a Vault secret (used for
   the connection URL or any additional authentication token); the `walletPath` is a separate
   constructor parameter.

4. Pass the wallet directory path when constructing the connector. The engine injects:

   ```
   oracle.net.wallet_location =
     (SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/opt/oracle/wallet/prod-tradedb)))
   ```

   as an extra JDBC property. Wallet-authenticated URLs use `/@` form with no embedded credentials:

   ```
   jdbc:oracle:thin:/@//host:1521/TRADEDB
   ```

5. Ensure the Spark executor process has read access to the wallet directory on every node. In
   Kubernetes, mount the wallet as a secret volume.

---

## Oracle Type Handling

Oracle's type system has several behaviours that differ from the SQL standard and from what Spark
expects by default. Know these before you configure a watermark column or downstream transformation.

### DATE includes time

Oracle `DATE` stores both date and time components (year, month, day, hour, minute, second). This
is unlike the SQL standard `DATE` which is date-only.

**Effect**: Spark reads Oracle `DATE` columns as `TimestampType`, not `DateType`. A column
declared `DATE` in Oracle schema documentation should be treated as a timestamp throughout the
pipeline.

**Mitigation**: If downstream consumers expect date-only values, apply a Spark `date_trunc` or
`cast(col as DateType)` transformation in the Silver layer — not in the Bronze ingestion config.
The Bronze layer preserves the raw Oracle value including the time component.

**Watermark implication**: `DATE` columns work correctly as watermark columns because the time
component provides sub-day resolution. No special handling is needed.

### TIMESTAMP WITH TIME ZONE

Oracle `TIMESTAMP WITH TIME ZONE` columns are read as `String` by the Thin JDBC driver unless
the JDBC option `oracle.jdbc.J2EE13Compliant=true` is set.

**Effect without the option**: The time zone offset is lost. The column value arrives in Spark as
a plain string like `"2024-03-15 09:30:00.0"`.

**Mitigation**: If time zone preservation is required (e.g. for cross-region trade reporting),
add the following as a JDBC extra property in the connector configuration:

```
oracle.jdbc.J2EE13Compliant = true
```

With this option set, the driver returns the full `TIMESTAMP WITH TIME ZONE` value including
offset, which Spark reads as `TimestampType` with UTC normalisation.

If time zone preservation is not required — for example if all source data is in a single known
time zone — omit the option and cast the column to a timestamp in the Silver layer.

### NUMBER without precision

Oracle `NUMBER` columns declared without explicit precision (e.g. `NUMBER` rather than
`NUMBER(18,0)`) are mapped by Spark JDBC to `DecimalType(38,10)`.

**Effect**: Integer NUMBER columns (e.g. trade sequence numbers, IDs) arrive in Spark with an
unnecessary decimal component. Aggregations and joins on these columns behave correctly but carry
unnecessary precision overhead.

**Watermark implication**: A `NUMBER` column used as a watermark (e.g. an auto-incrementing
`TRADE_SEQ`) should be cast explicitly in the source query to avoid watermark comparison
surprises:

```sql
-- Use a subquery via dbtable option (set in the connector configuration):
SELECT CAST(TRADE_SEQ AS NUMBER(18,0)) AS TRADE_SEQ, ...
FROM TRADE_EXECUTION
```

Do this when `TRADE_SEQ` is configured as the `incrementalColumn` and the watermark store
returns `IntegerWatermark` values. The `WatermarkStore` uses `Long` for integer watermarks; a
`DecimalType(38,10)` column will cause a type mismatch in `computeNewWatermark`.

---

## Parallel Reads

By default, Spark reads the Oracle table with a single JDBC task. For large tables (tens of
millions of rows or more) this creates a throughput bottleneck. Enable parallel reads by providing
a numeric partition column with known bounds.

### How it works

When `partitionCol`, `partitionLower`, and `partitionUpper` are all set, the base class passes
four options to Spark:

| Spark JDBC option | Source |
|---|---|
| `partitionColumn` | `partitionCol` constructor parameter |
| `lowerBound` | `partitionLower` constructor parameter |
| `upperBound` | `partitionUpper` constructor parameter |
| `numPartitions` | `ingestion.parallelism` from config |

Spark splits the `[lowerBound, upperBound]` range into `numPartitions` equal-width intervals and
issues one JDBC query per interval. All queries run concurrently on separate Spark tasks.

The partition column must be a numeric type. `DATE` and `TIMESTAMP` columns are not supported
by Spark JDBC partitioning.

### ROWID partitioning

For tables without a suitable numeric partition column, partitioning by Oracle ROWID is possible
but requires pre-calculated bounds. Oracle ROWIDs are not sequential integers — you must compute
approximate bounds with a query before running the pipeline:

```sql
SELECT
  DBMS_ROWID.ROWID_TO_RESTRICTED(MIN(ROWID), 0) AS row_lower,
  DBMS_ROWID.ROWID_TO_RESTRICTED(MAX(ROWID), 0) AS row_upper
FROM TRADE_EXECUTION;
```

ROWID bounds must be refreshed periodically as rows are inserted or deleted. For high-volume
tables that receive frequent inserts, recalculate bounds before each pipeline run using an
Airflow operator or a pre-task in your Dagster job.

**Limitation**: The current connector implementation passes ROWID bounds through the same
`partitionLower` / `partitionUpper` `Long` parameters used for numeric columns. ROWID
partitioning therefore requires manual operator setup and is not validated by the engine. See
[Known Limitations](#known-limitations).

---

## Incremental Extraction

The connector uses `JdbcConnectorBase`'s watermark mechanism to extract only records added or
modified since the previous run.

### How watermarks work with Oracle

1. On the first run, `WatermarkStore.read` returns `None` and the connector reads the full table.
2. After a successful Bronze write, `persistWatermark` computes `MAX(incrementalColumn)` from the
   extracted data and writes it to `watermarkStorage`.
3. On subsequent runs, `WatermarkStore.read` returns the stored value and the connector issues:

   ```sql
   SELECT * FROM <table> WHERE <incrementalColumn> > <watermark>
   ```

4. The watermark is persisted only after the Bronze write succeeds. A failed write leaves the
   watermark unchanged so the next run re-processes the same window.

### Recommended watermark column types

| Column type | Notes |
|---|---|
| `NUMBER(18,0)` integer ID | Best choice. Maps to `IntegerWatermark(Long)`. Cast explicitly if declared as bare `NUMBER`. |
| Oracle `TIMESTAMP` | Maps to `TimestampWatermark`. Avoid `TIMESTAMP WITH TIME ZONE` unless `J2EE13Compliant=true` is set. |
| Oracle `DATE` | Works. Stored as `TimestampWatermark`. Time component provides sub-day resolution. |

Avoid `VARCHAR2` or `CHAR` columns as watermarks even when they contain date strings — string
comparison ordering is not equivalent to chronological ordering for all date formats.

### Parallel reads with incremental mode

Parallel reads and incremental extraction can be combined. The watermark filter is applied in the
`dbtable` subquery, and the partition options split that filtered result set across Spark tasks:

```sql
-- Effective query with watermark + partitioning:
(SELECT * FROM TRADE_EXECUTION WHERE TRADE_SEQ > 48291700) t
```

Spark then reads this subquery with `numPartitions` parallel tasks using the partition column
bounds.

---

## Known Limitations

- **ojdbc11 not bundled**: The Oracle JDBC driver must be supplied by the operator. The framework
  cannot redistribute it due to the Oracle Technology Network (OTN) License. Missing driver causes
  a fast-fail `ClassNotFoundException` at connector startup.

- **SID-format URLs not supported**: The connector produces service-name format URLs
  (`@//host:port/service`) only. SID format (`@host:port:SID`) is not implemented. If the target
  database is addressable only by SID, contact the DBA to configure a service name alias.

- **ROWID partitioning requires manual bound calculation**: The engine has no built-in mechanism
  to compute ROWID bounds automatically. Operators must query the source table, extract lower and
  upper ROWID values, and supply them as constructor parameters before each pipeline run.

- **TIMESTAMP WITH TIME ZONE loses time zone without extra JDBC option**: The Thin driver returns
  this type as a plain string unless `oracle.jdbc.J2EE13Compliant=true` is set. Time zone
  information is silently dropped otherwise. See [Oracle Type Handling](#oracle-type-handling).

- **Oracle Wallet path must be resolved before connector construction**: The `walletPath`
  constructor parameter accepts a local filesystem path only. It cannot be a Vault reference or
  S3 URI. If the wallet is stored remotely, retrieve it to local disk (e.g. as a Kubernetes secret
  volume mount) before constructing the connector.

---

## Example

A fully annotated production configuration is provided at:

```
examples/configs/financial-services-oracle-trades.yaml
```

That example demonstrates incremental extraction on a trade-execution table with:
- Vault-managed credentials
- Schema enforcement against a JSON Schema registry
- Bronze-layer Delta write partitioned by `trade_date` and `asset_class`
- Dodd-Frank (17 CFR Part 45) compliance annotations
- Seven-year audit retention

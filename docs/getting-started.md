# Getting Started in 15 Minutes

This guide walks you through standing up the Critical Infrastructure Data Frameworks local development environment from scratch. By the end you will have Postgres, MinIO, and Vault running in Docker with seed data loaded, credentials seeded, and the smoke test passing.

No Spark, Java, or sbt installation is required.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Docker Engine 24+ or Docker Desktop | Docker Desktop ships with Compose v2. Use `docker compose` (with a space), not the legacy `docker-compose` command. |
| Git | Any recent version. |
| A terminal | macOS, Linux, or WSL2 on Windows. |

That is all. The ingestion engine runs inside a container; you do not install Spark or the JDK locally.

---

## Step 1 — Clone the repository

```bash
git clone https://github.com/airdmhund1/critical-infra-data-frameworks.git
cd critical-infra-data-frameworks
```

---

## Step 2 — Configure local environment variables

The Docker Compose stack reads all credentials from an `.env` file that you create locally. The file is gitignored and never committed.

```bash
cp deployment/docker/.env.example deployment/docker/.env
```

Open `deployment/docker/.env` in a text editor and replace every `replace-me` placeholder with dev-only values of your choice. The file contains three groups of variables:

**Postgres** (local test source database):

```
POSTGRES_USER=replace-me
POSTGRES_PASSWORD=replace-me
POSTGRES_DB=cidf_dev
```

Set `POSTGRES_USER` and `POSTGRES_PASSWORD` to any local dev credentials. `POSTGRES_DB` defaults to `cidf_dev` and can be left as-is.

**MinIO** (S3-compatible Bronze storage):

```
MINIO_ROOT_USER=replace-me
MINIO_ROOT_PASSWORD=replace-me-min-8-chars
```

Set both to dev-only values. MinIO enforces a minimum password length of 8 characters — the placeholder reminds you of this requirement.

**Vault** (dev-mode secrets manager):

```
VAULT_DEV_ROOT_TOKEN=replace-me-dev-only-token
```

Set this to any string you will remember — you will enter it in the Vault UI in Step 6. This is a development convenience token only; it is never used outside the local compose stack.

> **Important:** Do not put production credentials in this file. All values here are for local development only.

---

## Step 3 — Start all services

```bash
docker compose -f deployment/docker/docker-compose.yml \
  --env-file deployment/docker/.env \
  up -d
```

This command starts three long-running backing services:

- **cidf-postgres** — Postgres 15, listening on `localhost:5432`. Seeded with `meter_readings` test data via an init SQL script on first start.
- **cidf-minio** — MinIO S3-compatible object store. S3 API on `localhost:9000`, web console on `localhost:9001`.
- **cidf-vault** — HashiCorp Vault in dev mode. UI and API on `localhost:8200`.

The **ingestion-engine** service is a one-shot batch container. It is not started here — it is invoked on demand once the backing services are healthy and a runnable JAR is available.

Wait roughly 15–30 seconds for the services to initialise, then check their health:

```bash
docker compose -f deployment/docker/docker-compose.yml ps
```

The `STATUS` column for `cidf-postgres`, `cidf-minio`, and `cidf-vault` should each read `healthy`. If any service shows `starting` or `unhealthy`, wait a few seconds and run the command again. If a service remains unhealthy, check its logs:

```bash
docker compose -f deployment/docker/docker-compose.yml logs <service>
```

Replace `<service>` with `postgres`, `minio`, or `vault`.

---

## Step 4 — Seed Vault and MinIO

Two initialisation scripts must be run once after the stack comes up. Both scripts are idempotent — safe to re-run at any time without producing duplicate state.

### Seed Vault with Postgres credentials

The ingestion engine resolves JDBC credentials at runtime from Vault. This script writes the credential bundle to `secret/dev/pg-meter-db/jdbc-credentials`.

The script reads credentials from your shell environment. Source the `.env` file first so those variables are available:

```bash
set -a; source deployment/docker/.env; set +a
```

Then set the two additional variables the script needs to reach Vault and authenticate:

```bash
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN="${VAULT_DEV_ROOT_TOKEN}"
```

Now run the script:

```bash
bash deployment/docker/vault-init/vault-init.sh
```

The script polls Vault until it is ready, then writes the secret. You will see log lines like:

```
[vault-init] waiting for Vault at http://localhost:8200/v1/sys/health ...
[vault-init] Vault is up.
[vault-init] writing secret/dev/pg-meter-db/jdbc-credentials via vault CLI
[vault-init] secret seeded at secret/dev/pg-meter-db/jdbc-credentials
```

If the `vault` CLI is not installed on your host, the script falls back to writing the secret via the Vault HTTP API using `curl`.

### Create the MinIO bronze bucket

The ingestion engine writes Bronze Delta tables to an S3 bucket named `bronze`. This script creates it.

The script uses the `mc` (MinIO Client) binary. It must either be installed on your host, or the script must be run in a context where `mc` is available. The simplest approach for local development is to install `mc` on your host from the [MinIO documentation](https://min.io/docs/minio/linux/reference/minio-mc.html).

With the environment still sourced from the previous step, set the MinIO endpoint to the host-accessible port:

```bash
export MINIO_ENDPOINT=http://localhost:9000
```

Then run the script:

```bash
bash deployment/docker/minio-init/create-bucket.sh
```

You will see:

```
[minio-init] waiting for MinIO at http://localhost:9000/minio/health/live ...
[minio-init] MinIO is up.
[minio-init] configuring mc alias 'local' -> http://localhost:9000
[minio-init] creating bucket 'bronze' (idempotent)
[minio-init] bucket ready at s3://bronze/
```

---

## Step 5 — Run the smoke test

The smoke test verifies that all infrastructure components are healthy and correctly configured.

Run it from the repository root:

```bash
./deployment/docker/smoke-test.sh --env-file deployment/docker/.env
```

The test runs six checks:

| Step | What it checks |
|------|---------------|
| 1 — Service health | `cidf-postgres`, `cidf-minio`, and `cidf-vault` all report `healthy` via Docker's built-in healthcheck. |
| 2 — Postgres seed | The `meter_readings` table in `cidf_dev` contains at least one row. |
| 3 — Vault credentials | The secret at `secret/dev/pg-meter-db/jdbc-credentials` exists and contains a `username` field. |
| 4 — MinIO bucket | The `bronze` bucket is accessible via `mc ls`. |
| 5 — Engine run | **Skipped.** The ingestion engine has no `Main` class entry point yet. `java -jar /app/engine.jar` would fail with `no main manifest attribute`. This step will become active in the v0.1.0 release. |
| 6 — Bronze storage | The MinIO S3 API is responding and the `bronze` bucket path is reachable. This is an infrastructure-readiness check, not a check for ingestion output. |

A successful run exits with code `0` and prints:

```
========================================
SMOKE TEST RESULT
========================================
  Step 1: Service health     [PASS]
  Step 2: Postgres seed      [PASS]
  Step 3: Vault credentials  [PASS]
  Step 4: MinIO bucket       [PASS]
  Step 5: Engine run         [SKIP — no Main class yet]
  Step 6: Bronze storage     [PASS]
========================================
All infrastructure checks passed.
docker-compose local dev environment is ready.
========================================
```

When all infrastructure checks pass, your local environment is fully ready for development.

---

## Step 6 — Explore the local stack

### MinIO console (Bronze storage)

Open [http://localhost:9001](http://localhost:9001) in a browser.

Log in with the `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` values from your `deployment/docker/.env`. Navigate to the **bronze** bucket to inspect its contents.

### Vault UI (secrets management)

Open [http://localhost:8200](http://localhost:8200) in a browser.

Select the **Token** authentication method and enter the `VAULT_DEV_ROOT_TOKEN` value from your `.env`. Navigate to **secret/dev/pg-meter-db/jdbc-credentials** to confirm the seeded Postgres credentials are present.

> **Note:** Vault dev mode is intentionally insecure — in-memory storage, unsealed at start, predictable root token. This configuration is acceptable for local development only. Production deployments use a properly initialized Vault cluster with AppRole authentication.

### Query the Postgres seed data

```bash
docker compose -f deployment/docker/docker-compose.yml \
  exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT meter_class, COUNT(*) FROM meter_readings GROUP BY meter_class;"
```

This queries the seed data that the smoke test verified in Step 2 — the same data the ingestion engine will extract from Postgres once the v0.1.0 pipeline entry point is available.

---

## Step 7 — Tear down

Stop and remove the containers:

```bash
docker compose -f deployment/docker/docker-compose.yml down
```

This stops the services but preserves the named volumes (`cidf-postgres-data`, `cidf-minio-data`). Postgres seed data and any MinIO bucket contents persist across restarts.

To also remove all volumes (Postgres data, MinIO buckets) for a completely clean state:

```bash
docker compose -f deployment/docker/docker-compose.yml down -v
```

After `down -v`, the next `up -d` will reinitialise Postgres from scratch (seed data re-applied) and you will need to re-run the bucket creation script.

---

## What's next

The infrastructure layer verified by this guide is complete and ready for development. The next milestone is the end-to-end pipeline run: the ingestion engine extracting from Postgres and writing Delta tables to MinIO Bronze storage.

This will be available once the v0.1.0 release adds the `Main` class entry point. When that is in place, the smoke test Step 5 will activate and run:

```bash
docker compose -f deployment/docker/docker-compose.yml run --rm ingestion-engine \
  java -jar /app/engine.jar \
  --config /app/configs/energy-postgres-meter-data.yaml
```

See [ROADMAP.md](../ROADMAP.md) for the full phased release plan.

---

## Troubleshooting

| Problem | Resolution |
|---------|-----------|
| Service not healthy | Run `docker compose -f deployment/docker/docker-compose.yml logs <service>` to view logs. Replace `<service>` with `postgres`, `minio`, or `vault`. |
| `.env` not found | Ensure you copied `.env.example` to `.env`: `cp deployment/docker/.env.example deployment/docker/.env` |
| MinIO password rejected | MinIO requires a minimum of 8 characters for `MINIO_ROOT_PASSWORD`. Check the placeholder comment in `.env.example`. |
| Vault credentials not found (Step 3 fails) | Ensure `vault-init.sh` was run after all three services reached `healthy` status. Re-run it — the script is idempotent. |
| `mc: command not found` | The `create-bucket.sh` script requires `mc` (MinIO Client) on your host PATH. Install it from the [MinIO documentation](https://min.io/docs/minio/linux/reference/minio-mc.html). |
| `replace-me` errors on smoke test startup | You have not edited `deployment/docker/.env`. Open the file and replace all `replace-me` placeholders with dev-only values. |

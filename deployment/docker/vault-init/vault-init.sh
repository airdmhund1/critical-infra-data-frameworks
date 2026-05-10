#!/usr/bin/env bash
# =============================================================================
# Critical Infrastructure Data Frameworks — Vault credential bootstrap (dev)
# =============================================================================
#
# Purpose:
#   Seed the local-dev HashiCorp Vault (service `vault` in
#   deployment/docker/docker-compose.yml, running in `-dev` mode) with the
#   JDBC credential bundle that the smoke-test pipeline declared in:
#
#       examples/configs/energy-postgres-meter-data.yaml
#
#   resolves at runtime via VaultSecretsResolver. The credentials written
#   here MUST stay in lock-step with the values supplied to the postgres
#   container by docker-compose (POSTGRES_USER / POSTGRES_PASSWORD /
#   POSTGRES_DB) — both come from the same .env file, so this script reads
#   them straight out of the environment.
#
# Local-dev conventions (Branch 4 will align the YAML config with these):
#   * Database : cidf_dev          (matches POSTGRES_DB in .env.example)
#   * Table    : meter_readings    (deployment/docker/seed/init.sql)
#   * Vault KV : secret/dev/pg-meter-db/jdbc-credentials   (this script)
#   * Bronze   : s3://bronze/...   (MinIO bucket `bronze`)
#
# Usage:
#   Two invocation modes are supported:
#
#   1. Sidecar / init container (preferred, fully automated):
#        Mount this file into a short-lived container that runs alongside
#        the compose stack and exits when finished. The container needs
#        the `vault` CLI binary and `curl` on PATH.
#
#   2. Manual developer run after `docker compose up -d`:
#        export VAULT_ADDR=http://localhost:8200
#        export VAULT_TOKEN="$VAULT_DEV_ROOT_TOKEN"
#        export POSTGRES_USER=...  POSTGRES_PASSWORD=...  POSTGRES_DB=...
#        ./deployment/docker/vault-init/vault-init.sh
#
# Idempotency:
#   `vault kv put` (and the equivalent KV v2 HTTP API write) overwrite the
#   entire secret at the path on each call. Re-running this script
#   therefore produces the same end state — no errors, no duplicates.
#
# NEVER call this script with production credentials. The dev root token
# auth path is a development convenience; production deployments use
# AppRole. See VaultSecretsResolver for the production auth flow.
# =============================================================================

set -euo pipefail


# -----------------------------------------------------------------------------
# Configuration — every value comes from the environment. Defaults are only
# used when the caller has not been wired up by docker-compose yet (e.g.
# manual invocation from a developer laptop with port-forwarded Vault).
# -----------------------------------------------------------------------------
VAULT_ADDR="${VAULT_ADDR:-http://vault:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-}"

SECRET_PATH="secret/dev/pg-meter-db/jdbc-credentials"

# Postgres connection details — supplied to docker-compose by the same
# .env file that drives the postgres service, so they MUST be present
# in this script's environment when it runs.
POSTGRES_USER="${POSTGRES_USER:-}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
POSTGRES_DB="${POSTGRES_DB:-}"

# `postgres` is the docker-compose service name and resolves to the
# Postgres container on the default compose network. The ingestion-engine
# container reaches Postgres at this host:port pair.
POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"

# sslMode=disable is acceptable HERE — and only here — because:
#   * the Postgres and ingestion-engine containers communicate over the
#     private docker-compose network (not over the public internet), and
#   * the `-dev` Vault and the local Postgres do not hold real production
#     data.
# In any non-local environment the credentialsRef Vault entry must carry
# sslMode=verify-full plus the corresponding sslrootcert payload. See the
# matching note in examples/configs/energy-postgres-meter-data.yaml.
PG_SSL_MODE="disable"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

# Emit a structured log line on stderr so the secret payload (written via
# `vault kv put`, which prints to stdout) is not contaminated.
log() {
    printf '[vault-init] %s\n' "$*" >&2
}

die() {
    log "ERROR: $*"
    exit 1
}

# Poll the Vault health endpoint until it returns 200, or fail after 60s.
# Vault `-dev` mode is unsealed at boot, so /sys/health returns 200 as
# soon as the listener accepts connections.
wait_for_vault() {
    local deadline=$(( $(date +%s) + 60 ))
    log "waiting for Vault at ${VAULT_ADDR}/v1/sys/health ..."
    while true; do
        if curl -fsS -o /dev/null "${VAULT_ADDR}/v1/sys/health"; then
            log "Vault is up."
            return 0
        fi
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            die "Vault did not become ready within 60s at ${VAULT_ADDR}"
        fi
        sleep 1
    done
}

# Returns 0 if the `vault` binary is on PATH, 1 otherwise.
have_vault_cli() {
    command -v vault >/dev/null 2>&1
}

# Write the credential bundle using the `vault` CLI. The CLI handles
# KV v2 path mangling (data/...) automatically — `vault kv put secret/foo`
# is the canonical form.
write_secret_via_cli() {
    log "writing ${SECRET_PATH} via vault CLI"
    VAULT_ADDR="${VAULT_ADDR}" VAULT_TOKEN="${VAULT_TOKEN}" \
        vault kv put "${SECRET_PATH}" \
            username="${POSTGRES_USER}" \
            password="${POSTGRES_PASSWORD}" \
            database="${POSTGRES_DB}" \
            host="${POSTGRES_HOST}" \
            port="${POSTGRES_PORT}" \
            sslMode="${PG_SSL_MODE}" \
        >/dev/null
}

# Fallback path: write the secret via the KV v2 HTTP API using curl.
# Used when the `vault` binary is not available in the runtime image.
# KV v2 expects payloads under a `data` envelope at .../v1/secret/data/<path>.
write_secret_via_http() {
    log "writing ${SECRET_PATH} via Vault HTTP API (curl fallback)"

    # KV v2 HTTP path: KV mount `secret` -> /v1/secret/data/<sub-path>
    # Strip the leading `secret/` since `secret` is the mount, then
    # prefix with `secret/data/` for the v2 data write.
    local sub_path="${SECRET_PATH#secret/}"
    local url="${VAULT_ADDR}/v1/secret/data/${sub_path}"

    # Build the JSON payload with `printf` + manual quoting. Using a
    # heredoc with shell interpolation avoids bringing in jq just for
    # this one call. POSTGRES_PASSWORD is the only field with any
    # realistic chance of containing characters that would need
    # escaping; for local-dev passwords this is acceptable. Production
    # paths use the CLI branch above, which handles escaping safely.
    local payload
    payload=$(cat <<EOF
{
  "data": {
    "username": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}",
    "database": "${POSTGRES_DB}",
    "host": "${POSTGRES_HOST}",
    "port": "${POSTGRES_PORT}",
    "sslMode": "${PG_SSL_MODE}"
  }
}
EOF
)

    curl -fsS \
        -X POST \
        -H "X-Vault-Token: ${VAULT_TOKEN}" \
        -H "Content-Type: application/json" \
        --data "${payload}" \
        "${url}" \
        >/dev/null
}


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
    [ -n "${VAULT_TOKEN}" ]      || die "VAULT_TOKEN is required (dev root token)"
    [ -n "${POSTGRES_USER}" ]    || die "POSTGRES_USER is required"
    [ -n "${POSTGRES_PASSWORD}" ]|| die "POSTGRES_PASSWORD is required"
    [ -n "${POSTGRES_DB}" ]      || die "POSTGRES_DB is required"

    wait_for_vault

    if have_vault_cli; then
        write_secret_via_cli
    else
        log "vault CLI not found on PATH; falling back to HTTP API"
        write_secret_via_http
    fi

    log "secret seeded at ${SECRET_PATH}"
}

main "$@"

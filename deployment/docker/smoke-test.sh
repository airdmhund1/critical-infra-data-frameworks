#!/usr/bin/env bash
# =============================================================================
# Critical Infrastructure Data Frameworks — local dev smoke test
# =============================================================================
#
# Verifies that all four services in the docker-compose stack are healthy and
# that the expected seed data, Vault credentials, and MinIO bucket are in
# place after `docker compose up -d`.
#
# Usage (run from repo root):
#   ./deployment/docker/smoke-test.sh [--env-file <path>]
#
# Default env-file: deployment/docker/.env
#
# Steps tested:
#   1. Service health      — postgres, minio, vault healthy; ingestion-engine present
#   2. Postgres seed       — meter_readings table contains > 0 rows
#   3. Vault credentials   — secret/dev/pg-meter-db/jdbc-credentials contains 'username'
#   4. MinIO bucket        — bronze bucket is accessible via mc ls
#   5. Engine run          — SKIPPED (no Main class entry point yet; planned v0.1.0)
#   6. Bronze storage      — MinIO S3 API is responding and bronze path is reachable
#
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

die() {
  local step="${1}"
  local msg="${2}"
  echo "[ERROR] ${step}: ${msg}" >&2
  exit 1
}

pass() {
  local step="${1}"
  printf '[PASS]  %s\n' "${step}"
}

info() {
  local msg="${1}"
  printf '[INFO]  %s\n' "${msg}"
}

# -----------------------------------------------------------------------------
# Argument parsing — optional --env-file
# -----------------------------------------------------------------------------

ENV_FILE="deployment/docker/.env"

while [[ $# -gt 0 ]]; do
  case "${1}" in
    --env-file)
      shift
      ENV_FILE="${1}"
      ;;
    *)
      echo "Unknown argument: ${1}" >&2
      echo "Usage: $0 [--env-file <path>]" >&2
      exit 1
      ;;
  esac
  shift
done

# -----------------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------------

if [[ -f "${ENV_FILE}" ]]; then
  info "Loading environment from ${ENV_FILE}"
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
else
  info "No env file found at '${ENV_FILE}' — relying on shell environment variables"
fi

# Validate that required variables are set (will trip set -u if missing)
: "${POSTGRES_USER:?POSTGRES_USER must be set in ${ENV_FILE} or the environment}"
: "${POSTGRES_DB:?POSTGRES_DB must be set in ${ENV_FILE} or the environment}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set in ${ENV_FILE} or the environment}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set in ${ENV_FILE} or the environment}"
: "${VAULT_DEV_ROOT_TOKEN:?VAULT_DEV_ROOT_TOKEN must be set in ${ENV_FILE} or the environment}"

COMPOSE_FILE="deployment/docker/docker-compose.yml"

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------

echo ""
echo "========================================"
echo "  CIDF LOCAL DEV SMOKE TEST"
echo "========================================"
echo "  Compose file : ${COMPOSE_FILE}"
echo "  Env file     : ${ENV_FILE}"
echo "  Postgres DB  : ${POSTGRES_DB}"
echo "  Vault token  : (loaded from env)"
echo "========================================"
echo ""

# Track per-step results for the summary block
STEP1_RESULT="[PASS]"
STEP2_RESULT="[PASS]"
STEP3_RESULT="[PASS]"
STEP4_RESULT="[PASS]"
STEP5_RESULT="[SKIP — no Main class yet]"
STEP6_RESULT="[PASS]"

# =============================================================================
# Step 1 — Service health
# =============================================================================

info "Step 1 — Checking service health ..."

# Services that must report 'healthy' from Docker's built-in healthcheck
HEALTHY_SERVICES=("postgres" "minio" "vault")

for svc in "${HEALTHY_SERVICES[@]}"; do
  # Use docker inspect on the container; grep for Health.Status == "healthy"
  container_name="cidf-${svc}"
  set +e
  health=$(docker inspect --format '{{.State.Health.Status}}' "${container_name}" 2>/dev/null)
  inspect_exit=$?
  set -e

  if [[ ${inspect_exit} -ne 0 ]]; then
    STEP1_RESULT="[FAIL]"
    die "Step 1 — Service health" \
      "Container '${container_name}' not found. Is 'docker compose up -d' running?"
  fi

  if [[ "${health}" != "healthy" ]]; then
    STEP1_RESULT="[FAIL]"
    die "Step 1 — Service health" \
      "Service '${svc}' (${container_name}) is '${health}', expected 'healthy'."
  fi

  info "  ${svc} → ${health}"
done

# ingestion-engine has no healthcheck — just verify the container exists
set +e
engine_state=$(docker inspect --format '{{.State.Status}}' cidf-ingestion-engine 2>/dev/null)
engine_inspect_exit=$?
set -e

if [[ ${engine_inspect_exit} -ne 0 ]]; then
  # ingestion-engine may not have been started yet (it is a one-shot batch
  # job, not a daemon). Warn but do not fail — it may be started on demand.
  info "  ingestion-engine → container not found (one-shot service; may not have been started yet)"
else
  info "  ingestion-engine → ${engine_state} (no healthcheck for batch service; existence confirmed)"
fi

pass "Step 1 — Service health"

# =============================================================================
# Step 2 — Postgres: verify seed data exists
# =============================================================================

info "Step 2 — Verifying Postgres seed data (meter_readings) ..."

set +e
count_output=$(docker compose -f "${COMPOSE_FILE}" exec -T postgres \
  psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -c "SELECT COUNT(*) FROM meter_readings;" \
  2>&1)
psql_exit=$?
set -e

if [[ ${psql_exit} -ne 0 ]]; then
  STEP2_RESULT="[FAIL]"
  die "Step 2 — Postgres seed" \
    "psql command failed (exit ${psql_exit}). Output: ${count_output}"
fi

row_count=$(echo "${count_output}" | tr -d '[:space:]')

if [[ -z "${row_count}" ]] || [[ "${row_count}" -le 0 ]] 2>/dev/null; then
  STEP2_RESULT="[FAIL]"
  die "Step 2 — Postgres seed" \
    "meter_readings table is empty (count=${row_count}). Run init.sql seed manually or re-initialise the Postgres volume."
fi

info "  meter_readings row count: ${row_count}"
pass "Step 2 — Postgres seed"

# =============================================================================
# Step 3 — Vault: verify credentials at expected path
# =============================================================================

info "Step 3 — Verifying Vault secret at secret/dev/pg-meter-db/jdbc-credentials ..."

VAULT_SECRET_PATH="secret/dev/pg-meter-db/jdbc-credentials"

set +e
vault_output=$(docker compose -f "${COMPOSE_FILE}" exec -T vault \
  vault kv get \
    -address=http://localhost:8200 \
    -token="${VAULT_DEV_ROOT_TOKEN}" \
    "${VAULT_SECRET_PATH}" \
  2>&1)
vault_exit=$?
set -e

if [[ ${vault_exit} -ne 0 ]]; then
  STEP3_RESULT="[FAIL]"
  die "Step 3 — Vault credentials" \
    "vault kv get failed (exit ${vault_exit}). Has vault-init.sh run? Output: ${vault_output}"
fi

if ! echo "${vault_output}" | grep -q "username"; then
  STEP3_RESULT="[FAIL]"
  die "Step 3 — Vault credentials" \
    "Secret at '${VAULT_SECRET_PATH}' does not contain a 'username' field. Output: ${vault_output}"
fi

info "  Vault path: ${VAULT_SECRET_PATH} → username field found"
pass "Step 3 — Vault credentials"

# =============================================================================
# Step 4 — MinIO: verify bronze bucket exists
# =============================================================================

info "Step 4 — Verifying MinIO bronze bucket ..."

# Configure mc alias inside the minio container, then list the bucket.
set +e
mc_alias_output=$(docker compose -f "${COMPOSE_FILE}" exec -T minio \
  mc alias set local http://localhost:9000 \
    "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --quiet \
  2>&1)
mc_alias_exit=$?
set -e

if [[ ${mc_alias_exit} -ne 0 ]]; then
  STEP4_RESULT="[FAIL]"
  die "Step 4 — MinIO bucket" \
    "mc alias set failed (exit ${mc_alias_exit}). Output: ${mc_alias_output}"
fi

set +e
mc_ls_output=$(docker compose -f "${COMPOSE_FILE}" exec -T minio \
  mc ls local/bronze \
  2>&1)
mc_ls_exit=$?
set -e

if [[ ${mc_ls_exit} -ne 0 ]]; then
  STEP4_RESULT="[FAIL]"
  die "Step 4 — MinIO bucket" \
    "mc ls local/bronze failed (exit ${mc_ls_exit}). Has minio-init/create-bucket.sh run? Output: ${mc_ls_output}"
fi

info "  MinIO bronze bucket: accessible"
pass "Step 4 — MinIO bucket"

# =============================================================================
# Step 5 — Engine end-to-end run (SKIPPED — TODO)
# =============================================================================

info "Step 5 — Engine end-to-end run: SKIPPED"
info "  Reason: no Main class entry point yet (planned for v0.1.0 release issue)."
info "  The IngestionEngine class is a library, not a runnable application."
info "  'java -jar /app/engine.jar' would fail with 'no main manifest attribute'."
info "  When a runnable JAR is available, this step will:"
info "    docker compose -f ${COMPOSE_FILE} run --rm ingestion-engine \\"
info "      java -jar /app/engine.jar \\"
info "        --config /app/configs/energy-postgres-meter-data.yaml"
info "  and verify Bronze Delta output at s3://bronze/energy/pg-meter-readings/ via:"
info "    docker compose -f ${COMPOSE_FILE} exec -T minio mc ls local/bronze/energy/pg-meter-readings/"
info "  TODO: remove this placeholder when CLI entry point is wired (tracked in release issue)."

# Do NOT call die here — informational only.

# =============================================================================
# Step 6 — MinIO: verify bronze storage path is accessible (infrastructure check)
# =============================================================================

info "Step 6 — Verifying Bronze storage target is reachable (infrastructure check, not engine output) ..."
info "  This step confirms the MinIO S3 API is responding and the bronze bucket"
info "  is reachable. It does NOT assert that ingestion output exists there yet"
info "  (that requires Step 5, which is deferred until a runnable JAR exists)."

set +e
mc_ls6_output=$(docker compose -f "${COMPOSE_FILE}" exec -T minio \
  mc ls local/bronze \
  2>&1)
mc_ls6_exit=$?
set -e

if [[ ${mc_ls6_exit} -ne 0 ]]; then
  STEP6_RESULT="[FAIL]"
  die "Step 6 — Bronze storage" \
    "mc ls local/bronze failed (exit ${mc_ls6_exit}). Output: ${mc_ls6_output}"
fi

info "  Bronze storage target (s3://bronze/) is reachable and ready for ingestion output."
pass "Step 6 — Bronze storage"

# =============================================================================
# Exit summary
# =============================================================================

echo ""
echo "========================================"
echo "SMOKE TEST RESULT"
echo "========================================"
printf '  Step 1: Service health     %s\n' "${STEP1_RESULT}"
printf '  Step 2: Postgres seed      %s\n' "${STEP2_RESULT}"
printf '  Step 3: Vault credentials  %s\n' "${STEP3_RESULT}"
printf '  Step 4: MinIO bucket       %s\n' "${STEP4_RESULT}"
printf '  Step 5: Engine run         %s\n' "${STEP5_RESULT}"
printf '  Step 6: Bronze storage     %s\n' "${STEP6_RESULT}"
echo "========================================"
echo "All infrastructure checks passed."
echo "docker-compose local dev environment is ready."
echo "========================================"
echo ""

exit 0

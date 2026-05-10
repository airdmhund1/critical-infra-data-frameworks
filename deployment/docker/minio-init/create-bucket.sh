#!/usr/bin/env bash
# =============================================================================
# Critical Infrastructure Data Frameworks — MinIO Bronze bucket bootstrap
# =============================================================================
#
# Purpose:
#   Create the local-dev Bronze-layer object-storage bucket on the MinIO
#   instance defined in deployment/docker/docker-compose.yml. The
#   ingestion-engine container writes Bronze Delta tables to this bucket
#   via the S3A client (see AWS_S3_ENDPOINT in docker-compose.yml).
#
# Local-dev conventions (Branch 4 will align the YAML config with these):
#   * Database : cidf_dev          (matches POSTGRES_DB in .env.example)
#   * Table    : meter_readings    (deployment/docker/seed/init.sql)
#   * Vault KV : secret/dev/pg-meter-db/jdbc-credentials
#   * Bronze   : s3://bronze/...   (this script — bucket name = "bronze")
#
# Usage:
#   Two invocation modes are supported:
#
#   1. Sidecar / init container (preferred):
#        Use the official `minio/mc` image as a one-shot service that
#        mounts this script and exits when it finishes. The image already
#        provides `mc` and `curl` on PATH.
#
#   2. Manual developer run after `docker compose up -d`:
#        export MINIO_ENDPOINT=http://localhost:9000
#        export MINIO_ROOT_USER=...  MINIO_ROOT_PASSWORD=...
#        ./deployment/docker/minio-init/create-bucket.sh
#
# Idempotency:
#   `mc mb --ignore-existing` is the documented idempotent bucket create —
#   the call succeeds whether or not the bucket already exists. `mc alias
#   set` likewise overwrites the named alias on each call. Re-running this
#   script therefore produces the same end state.
# =============================================================================

set -euo pipefail


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-}"

# `bronze` matches the `s3://bronze/...` paths used by the local-dev
# variant of examples/configs/energy-postgres-meter-data.yaml (wired up
# in Branch 4).
BRONZE_BUCKET="${BRONZE_BUCKET:-bronze}"

# Local mc alias name. Scoped to this script — does not collide with any
# alias the developer may already have configured in their personal mc
# config. `mc alias set` will overwrite it on each run.
MC_ALIAS="local"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
log() {
    printf '[minio-init] %s\n' "$*" >&2
}

die() {
    log "ERROR: $*"
    exit 1
}

# Poll the MinIO liveness endpoint until it returns 200, or fail after 60s.
# /minio/health/live is the documented liveness probe and does NOT require
# authentication, so it is safe to call before the alias is configured.
wait_for_minio() {
    local deadline=$(( $(date +%s) + 60 ))
    log "waiting for MinIO at ${MINIO_ENDPOINT}/minio/health/live ..."
    while true; do
        if curl -fsS -o /dev/null "${MINIO_ENDPOINT}/minio/health/live"; then
            log "MinIO is up."
            return 0
        fi
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            die "MinIO did not become ready within 60s at ${MINIO_ENDPOINT}"
        fi
        sleep 1
    done
}


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
    [ -n "${MINIO_ROOT_USER}" ]     || die "MINIO_ROOT_USER is required"
    [ -n "${MINIO_ROOT_PASSWORD}" ] || die "MINIO_ROOT_PASSWORD is required"

    command -v mc >/dev/null 2>&1 \
        || die "mc (MinIO client) is required on PATH; use the minio/mc image"

    wait_for_minio

    log "configuring mc alias '${MC_ALIAS}' -> ${MINIO_ENDPOINT}"
    mc alias set "${MC_ALIAS}" \
        "${MINIO_ENDPOINT}" \
        "${MINIO_ROOT_USER}" \
        "${MINIO_ROOT_PASSWORD}" \
        >/dev/null

    log "creating bucket '${BRONZE_BUCKET}' (idempotent)"
    mc mb --ignore-existing "${MC_ALIAS}/${BRONZE_BUCKET}"

    # ---------------------------------------------------------------------
    # Optional: bucket-level policies could go here. Two reasonable
    # candidates for a Bronze-layer bucket are:
    #
    #   * Object versioning — `mc version enable ${MC_ALIAS}/${BUCKET}` —
    #     gives a Delta-table-style undo log at the object-store layer.
    #     Probably overkill in local dev; Delta itself already provides
    #     time travel.
    #
    #   * Lifecycle rule — `mc ilm add` to expire stale Bronze objects.
    #     Production concern; adds nothing useful in the smoke test.
    #
    # Both are deliberately omitted to keep the local-dev surface small.
    # ---------------------------------------------------------------------

    log "bucket ready at s3://${BRONZE_BUCKET}/"
}

main "$@"

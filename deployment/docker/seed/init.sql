-- =============================================================================
-- Critical Infrastructure Data Frameworks — Postgres seed (local dev)
-- =============================================================================
--
-- Purpose:
--   Bootstrap the local development PostgreSQL instance (service `postgres`
--   in deployment/docker/docker-compose.yml) with a `meter_readings` table
--   plus a small representative dataset. This table is the source of truth
--   for the smoke-test pipeline declared in:
--
--       examples/configs/energy-postgres-meter-data.yaml
--
--   That config performs WATERMARK-BASED INCREMENTAL extraction driven by
--   the `recorded_at` TIMESTAMP column. The covering index on `recorded_at`
--   below is what keeps `WHERE recorded_at > :lastWatermark` from
--   degenerating into a full-table scan on every 15-minute run.
--
-- Local-dev conventions (Branch 4 will align the YAML config with these):
--   * Database : cidf_dev          (matches POSTGRES_DB in .env.example)
--   * Table    : meter_readings    (this file)
--   * Vault KV : secret/dev/pg-meter-db/jdbc-credentials
--   * Bronze   : s3://bronze/...   (MinIO bucket `bronze`)
--
-- Idempotency:
--   * All DDL uses CREATE ... IF NOT EXISTS.
--   * All seed INSERTs use ON CONFLICT (meter_id, recorded_at) DO NOTHING.
--   Re-running this script is a no-op on the second and later runs.
--
-- Invocation:
--   The official postgres:15-alpine image automatically executes any
--   *.sql / *.sh files mounted into /docker-entrypoint-initdb.d on FIRST
--   container start (i.e. when the data volume is empty). Branch 4 will
--   add the volume mount; this file is a stand-alone artefact until then
--   and may also be applied manually:
--       psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB \
--            -f deployment/docker/seed/init.sql
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Table: meter_readings
-- -----------------------------------------------------------------------------
-- Smart-meter interval readings. One row per (meter_id, recorded_at).
-- Composite primary key reflects the natural uniqueness constraint of
-- interval data (a single meter cannot produce two readings for the same
-- timestamp). PRIMARY KEY also gives us the upsert anchor used by the
-- ON CONFLICT clauses below.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meter_readings (
    meter_id          BIGINT          NOT NULL,
    recorded_at       TIMESTAMP       NOT NULL,
    kwh_delivered     NUMERIC(12, 4)  NOT NULL,
    meter_class       TEXT            NOT NULL,
    region_code       TEXT            NOT NULL,
    reading_quality   TEXT            NOT NULL DEFAULT 'good',
    CONSTRAINT pk_meter_readings PRIMARY KEY (meter_id, recorded_at),
    CONSTRAINT chk_meter_readings_meter_class
        CHECK (meter_class IN ('residential', 'commercial', 'industrial')),
    CONSTRAINT chk_meter_readings_reading_quality
        CHECK (reading_quality IN ('good', 'estimated', 'suspect', 'missing'))
);

-- -----------------------------------------------------------------------------
-- Index: idx_meter_readings_recorded_at
-- -----------------------------------------------------------------------------
-- Covering index on the watermark column. The pipeline runs
--   SELECT ... FROM meter_readings WHERE recorded_at > :lastWatermark
-- every 15 minutes; without this index Postgres would scan the whole
-- table on each run. The PRIMARY KEY index is on (meter_id, recorded_at),
-- which Postgres CANNOT use efficiently for a `WHERE recorded_at > ...`
-- predicate alone — hence this dedicated single-column index.
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_meter_readings_recorded_at
    ON meter_readings (recorded_at);


-- -----------------------------------------------------------------------------
-- Seed data — ~100 rows
-- -----------------------------------------------------------------------------
-- Layout:
--   * 4 distinct meters across all 3 meter_class values
--   * 3 region codes (US-NE-01, US-NE-02, US-MW-01)
--   * 25 fifteen-minute intervals per meter, starting 2026-05-08 06:00:00
--     and ending 2026-05-08 12:00:00 (inclusive of start, ~6 hours)
--   * 100 rows total (4 meters * 25 intervals)
--   * One row deliberately marked reading_quality='estimated' to exercise
--     non-default values on the downstream schema enforcement path.
--
-- Using generate_series + a CTE keeps the script compact and re-runnable.
-- ON CONFLICT (meter_id, recorded_at) DO NOTHING makes re-application a
-- no-op once the rows already exist.
-- -----------------------------------------------------------------------------
INSERT INTO meter_readings
    (meter_id, recorded_at, kwh_delivered, meter_class, region_code, reading_quality)
WITH meters AS (
    SELECT * FROM (VALUES
        (1001::BIGINT, 'residential'::TEXT, 'US-NE-01'::TEXT, 0.4500::NUMERIC(12, 4)),
        (1002::BIGINT, 'residential'::TEXT, 'US-NE-02'::TEXT, 0.6200::NUMERIC(12, 4)),
        (2001::BIGINT, 'commercial'::TEXT,  'US-NE-01'::TEXT, 4.7500::NUMERIC(12, 4)),
        (3001::BIGINT, 'industrial'::TEXT,  'US-MW-01'::TEXT, 38.2500::NUMERIC(12, 4))
    ) AS m(meter_id, meter_class, region_code, base_kwh)
),
intervals AS (
    SELECT
        generate_series(
            TIMESTAMP '2026-05-08 06:00:00',
            TIMESTAMP '2026-05-08 12:00:00',
            INTERVAL '15 minutes'
        ) AS recorded_at
)
SELECT
    m.meter_id,
    i.recorded_at,
    -- Slight per-interval variation so the values look plausible rather
    -- than constant. The expression is deterministic, so the seed remains
    -- stable across re-runs.
    ROUND(
        (m.base_kwh
         + (EXTRACT(EPOCH FROM (i.recorded_at - TIMESTAMP '2026-05-08 06:00:00')) / 3600.0)
           * (m.base_kwh * 0.05))::NUMERIC,
        4
    )                                                         AS kwh_delivered,
    m.meter_class,
    m.region_code,
    -- Mark the 13:th interval of meter 2001 as 'estimated' to provide a
    -- non-default reading_quality value in the seed set.
    CASE
        WHEN m.meter_id = 2001 AND i.recorded_at = TIMESTAMP '2026-05-08 09:00:00'
            THEN 'estimated'
        ELSE 'good'
    END                                                       AS reading_quality
FROM meters m
CROSS JOIN intervals i
ON CONFLICT (meter_id, recorded_at) DO NOTHING;

-- ============================================================================
-- GPS BREADCRUMB TRAIL
-- Real-time technician position tracking for production-grade field maps
-- ============================================================================
--
-- Stores streaming GPS updates (every 10-30 seconds) for each technician.
-- Enables breadcrumb trail visualization, ETA calculation, and geofence
-- detection. Production fleet systems (Samsara, Geotab, Verizon Connect)
-- generate similar data at 1-30 Hz.
--
-- Idempotent: Uses IF NOT EXISTS.
-- ============================================================================

SET client_min_messages = NOTICE;

CREATE TABLE IF NOT EXISTS field_service.gps_breadcrumbs (
    breadcrumb_id   BIGSERIAL PRIMARY KEY,
    technician_id   BIGINT NOT NULL,
    latitude        NUMERIC(10,6) NOT NULL,
    longitude       NUMERIC(10,6) NOT NULL,
    speed_kmh       NUMERIC(5,1) DEFAULT 0,
    heading         NUMERIC(5,1) DEFAULT 0,
    accuracy_m      NUMERIC(6,1) DEFAULT 10,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type      VARCHAR(20) DEFAULT 'position'
                    CHECK (event_type IN ('position', 'stop_start', 'stop_end', 'geofence_enter', 'geofence_exit'))
);

CREATE INDEX IF NOT EXISTS idx_gps_tech_time
    ON field_service.gps_breadcrumbs (technician_id, recorded_at DESC);

-- Note: no partial index on recorded_at (CURRENT_TIMESTAMP is not immutable)
-- Use the (technician_id, recorded_at DESC) index for recent queries instead

COMMENT ON TABLE field_service.gps_breadcrumbs IS
    'Streaming GPS position updates for technicians. Each row is a position '
    'sample (typically every 10-30 seconds). Enables breadcrumb trail '
    'visualization, ETA calculation, and geofence detection.';

-- Cleanup function: delete breadcrumbs older than 7 days
CREATE OR REPLACE FUNCTION field_service.cleanup_old_breadcrumbs()
RETURNS INTEGER AS $$
DECLARE
    deleted INTEGER;
BEGIN
    DELETE FROM field_service.gps_breadcrumbs
    WHERE recorded_at < CURRENT_TIMESTAMP - INTERVAL '7 days';
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;

-- Grants
DO $$
DECLARE
    r TEXT;
BEGIN
    FOREACH r IN ARRAY ARRAY['lakebase_app', 'lakebase_app_perms']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('GRANT SELECT, INSERT, DELETE ON field_service.gps_breadcrumbs TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.gps_breadcrumbs_breadcrumb_id_seq TO %I', r);
            EXECUTE format('GRANT EXECUTE ON FUNCTION field_service.cleanup_old_breadcrumbs() TO %I', r);
        END IF;
    END LOOP;
END $$;

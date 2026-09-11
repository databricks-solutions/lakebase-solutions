-- ============================================================================
-- TIER 3: CREW ASSIGNMENTS + TECHNICIAN MOBILE VIEW SUPPORT
-- ============================================================================
--
-- 1. CREW ASSIGNMENTS — 2-person teams for installs and complex repairs.
--    Dispatch must find compatible crew (same shift, both certified).
--
-- 2. TECHNICIAN SESSIONS — Track clock-in/out for mobile view and
--    availability management.
--
-- Idempotent: Uses IF NOT EXISTS.
-- ============================================================================

SET client_min_messages = NOTICE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. CREW ASSIGNMENTS
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.crew_assignments (
    crew_id          BIGSERIAL PRIMARY KEY,
    work_order_id    BIGINT NOT NULL REFERENCES field_service.work_orders(work_order_id),
    lead_tech_id     BIGINT NOT NULL REFERENCES field_service.technicians(technician_id),
    support_tech_id  BIGINT REFERENCES field_service.technicians(technician_id),
    crew_type        VARCHAR(30) DEFAULT 'standard'
                     CHECK (crew_type IN ('solo', 'standard', 'complex', 'training')),
    status           VARCHAR(20) DEFAULT 'assigned'
                     CHECK (status IN ('assigned', 'en_route', 'on_site', 'completed', 'cancelled')),
    notes            TEXT,
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crew_wo ON field_service.crew_assignments (work_order_id);
CREATE INDEX IF NOT EXISTS idx_crew_lead ON field_service.crew_assignments (lead_tech_id);
CREATE INDEX IF NOT EXISTS idx_crew_support ON field_service.crew_assignments (support_tech_id);

COMMENT ON TABLE field_service.crew_assignments IS
    'Crew assignments for work orders requiring 2+ technicians. '
    'crew_type: solo (1 tech), standard (2 techs), complex (2+ senior), training (senior + junior shadow).';

-- Add crew_required flag to work_orders
ALTER TABLE field_service.work_orders
    ADD COLUMN IF NOT EXISTS crew_required BOOLEAN DEFAULT false;

ALTER TABLE field_service.work_orders
    ADD COLUMN IF NOT EXISTS crew_size INT DEFAULT 1;

-- Backfill: installs require 2-person crews, complex repairs require crews
DO $$
BEGIN
    UPDATE field_service.work_orders
    SET crew_required = true, crew_size = 2
    WHERE category = 'install'
      AND crew_required = false
      AND status NOT IN ('completed', 'cancelled');

    UPDATE field_service.work_orders
    SET crew_required = true, crew_size = 2
    WHERE category = 'repair'
      AND priority IN ('critical', 'high')
      AND subcategory IN ('fiber_cut', 'line_damage')
      AND crew_required = false
      AND status NOT IN ('completed', 'cancelled');
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 2. TECHNICIAN SESSIONS (Clock In/Out)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.technician_sessions (
    session_id       BIGSERIAL PRIMARY KEY,
    technician_id    BIGINT NOT NULL REFERENCES field_service.technicians(technician_id),
    clock_in         TIMESTAMPTZ NOT NULL DEFAULT now(),
    clock_out        TIMESTAMPTZ,
    shift_type       VARCHAR(20) DEFAULT 'day' CHECK (shift_type IN ('day', 'evening', 'on_call', 'overtime')),
    vehicle_id       VARCHAR(50),
    start_lat        NUMERIC(10,6),
    start_lng        NUMERIC(10,6),
    end_lat          NUMERIC(10,6),
    end_lng          NUMERIC(10,6),
    jobs_completed   INT DEFAULT 0,
    miles_driven     NUMERIC(8,1) DEFAULT 0,
    notes            TEXT
);

CREATE INDEX IF NOT EXISTS idx_tsess_tech ON field_service.technician_sessions (technician_id, clock_in DESC);
CREATE INDEX IF NOT EXISTS idx_tsess_active ON field_service.technician_sessions (technician_id)
    WHERE clock_out IS NULL;

COMMENT ON TABLE field_service.technician_sessions IS
    'Technician work sessions. Tracks clock-in/out, shift type, vehicle, '
    'and daily metrics. Used by the technician mobile view and dispatch '
    'availability management.';

-- ═══════════════════════════════════════════════════════════════════════════
-- GRANTS
-- ═══════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
    r TEXT;
BEGIN
    FOREACH r IN ARRAY ARRAY['lakebase_app', 'lakebase_app_perms']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.crew_assignments TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.crew_assignments_crew_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.technician_sessions TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.technician_sessions_session_id_seq TO %I', r);
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- LAKEBASE SHOWCASE FEATURES — Additive Migration
-- ============================================================================
-- This file adds:
--   Feature 4: Event Sourcing / Audit Log
--   Feature 5: Parts Inventory with ACID Transactions (reorder_requests)
--   Feature 6: Real-Time SLA Engine (triggers, materialized views)
--
-- Safe to re-run: uses DROP IF EXISTS + CREATE, OR REPLACE for functions.
-- Must run AFTER field_service_schema.sql (depends on existing tables).
-- ============================================================================

SET client_min_messages = NOTICE;

-- ============================================================================
-- FEATURE 6: SLA ENGINE — Columns, Trigger, Materialized Views
-- ============================================================================

-- Add sla_risk_score and sla_hours_remaining to work_orders
-- (idempotent — skips if already present)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'field_service' AND table_name = 'work_orders'
        AND column_name = 'sla_risk_score'
    ) THEN
        ALTER TABLE field_service.work_orders ADD COLUMN sla_risk_score INTEGER DEFAULT 0;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'field_service' AND table_name = 'work_orders'
        AND column_name = 'sla_hours_remaining'
    ) THEN
        ALTER TABLE field_service.work_orders ADD COLUMN sla_hours_remaining NUMERIC(10,2);
    END IF;
END $$;

-- SLA risk scorer trigger function
CREATE OR REPLACE FUNCTION field_service.calculate_sla_risk()
RETURNS TRIGGER AS $$
BEGIN
    -- Auto-set sla_met on completion
    IF NEW.status IN ('completed', 'cancelled') AND NEW.resolved_at IS NOT NULL AND NEW.sla_due_at IS NOT NULL THEN
        NEW.sla_met := (NEW.resolved_at <= NEW.sla_due_at);
        NEW.sla_risk_score := CASE WHEN NEW.sla_met THEN 0 ELSE 100 END;
        NEW.sla_hours_remaining := 0;
    ELSIF NEW.sla_due_at IS NOT NULL AND NEW.status NOT IN ('completed', 'cancelled') THEN
        -- Calculate hours remaining and risk score for active work orders
        NEW.sla_hours_remaining := EXTRACT(EPOCH FROM (NEW.sla_due_at - CURRENT_TIMESTAMP)) / 3600.0;
        NEW.sla_risk_score := CASE
            WHEN NEW.sla_due_at < CURRENT_TIMESTAMP THEN 100                          -- Already breached
            WHEN NEW.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours' THEN 90      -- Critical
            WHEN NEW.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '4 hours' THEN 70      -- High
            WHEN NEW.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '8 hours' THEN 40      -- Medium
            ELSE 10                                                                    -- Low
        END;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate trigger (idempotent)
DROP TRIGGER IF EXISTS trg_sla_risk ON field_service.work_orders;
CREATE TRIGGER trg_sla_risk
    BEFORE INSERT OR UPDATE ON field_service.work_orders
    FOR EACH ROW EXECUTE FUNCTION field_service.calculate_sla_risk();

-- Disable trigger during backfill to preserve generated sla_met values
ALTER TABLE field_service.work_orders DISABLE TRIGGER trg_sla_risk;

-- Backfill sla_risk_score and sla_hours_remaining for existing work orders
-- (sla_met is already set correctly by field_service_schema.sql data generation)
UPDATE field_service.work_orders
SET sla_risk_score = CASE
        WHEN status IN ('completed', 'cancelled') AND sla_met = TRUE THEN 0
        WHEN status IN ('completed', 'cancelled') AND sla_met = FALSE THEN 100
        WHEN sla_due_at IS NULL THEN 0
        WHEN sla_due_at < CURRENT_TIMESTAMP THEN 100
        WHEN sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours' THEN 90
        WHEN sla_due_at < CURRENT_TIMESTAMP + INTERVAL '4 hours' THEN 70
        WHEN sla_due_at < CURRENT_TIMESTAMP + INTERVAL '8 hours' THEN 40
        ELSE 10
    END,
    sla_hours_remaining = CASE
        WHEN status IN ('completed', 'cancelled') THEN 0
        WHEN sla_due_at IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (sla_due_at - CURRENT_TIMESTAMP)) / 3600.0
    END
WHERE sla_risk_score IS NULL OR sla_risk_score = 0;

-- Re-enable trigger for live operations
ALTER TABLE field_service.work_orders ENABLE TRIGGER trg_sla_risk;

-- Technician leaderboard materialized view
DROP MATERIALIZED VIEW IF EXISTS field_service.mv_technician_leaderboard;
CREATE MATERIALIZED VIEW field_service.mv_technician_leaderboard AS
SELECT
    t.technician_id,
    t.first_name || ' ' || t.last_name AS name,
    t.region_id,
    r.region_name,
    COUNT(*) FILTER (WHERE wo.status = 'completed') AS completed_count,
    COUNT(*) FILTER (WHERE wo.sla_met = TRUE) AS sla_met_count,
    ROUND(AVG(
        CASE WHEN wo.resolved_at IS NOT NULL AND wo.created_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (wo.resolved_at - wo.created_at)) / 3600.0
        END
    )::NUMERIC, 1) AS avg_resolution_hours,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE wo.sla_met = TRUE)
        / NULLIF(COUNT(*) FILTER (WHERE wo.status = 'completed'), 0),
        1
    ) AS sla_pct
FROM field_service.technicians t
LEFT JOIN field_service.work_orders wo ON wo.assigned_technician_id = t.technician_id
LEFT JOIN field_service.service_regions r ON t.region_id = r.region_id
WHERE t.is_active = TRUE
GROUP BY t.technician_id, t.first_name, t.last_name, t.region_id, r.region_name;

-- Regional SLA dashboard materialized view
DROP MATERIALIZED VIEW IF EXISTS field_service.mv_regional_sla;
CREATE MATERIALIZED VIEW field_service.mv_regional_sla AS
SELECT
    r.region_id,
    r.region_name,
    COUNT(*) FILTER (WHERE wo.status NOT IN ('completed', 'cancelled')) AS open_orders,
    COUNT(*) FILTER (WHERE wo.sla_risk_score >= 90) AS critical_risk,
    COUNT(*) FILTER (WHERE wo.sla_risk_score >= 70 AND wo.sla_risk_score < 90) AS high_risk,
    COUNT(*) FILTER (WHERE wo.sla_risk_score >= 40 AND wo.sla_risk_score < 70) AS medium_risk,
    COUNT(*) FILTER (WHERE wo.sla_risk_score < 40 AND wo.sla_risk_score > 0) AS low_risk,
    ROUND(AVG(wo.sla_risk_score)::NUMERIC, 1) AS avg_risk_score,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE wo.sla_met = TRUE)
        / NULLIF(COUNT(*) FILTER (WHERE wo.status = 'completed'), 0),
        1
    ) AS overall_sla_pct
FROM field_service.service_regions r
LEFT JOIN field_service.work_orders wo ON wo.region_id = r.region_id
GROUP BY r.region_id, r.region_name;

-- Indexes for materialized views (for CONCURRENTLY refresh)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_leaderboard_pk ON field_service.mv_technician_leaderboard (technician_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_regional_sla_pk ON field_service.mv_regional_sla (region_id);

-- Function to refresh SLA materialized views (called by app /api/sla/refresh)
CREATE OR REPLACE FUNCTION field_service.refresh_sla_matviews()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY field_service.mv_technician_leaderboard;
    REFRESH MATERIALIZED VIEW CONCURRENTLY field_service.mv_regional_sla;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ============================================================================
-- ASH QUERY LOG — Active query snapshots for historical drill-down
-- ============================================================================

-- ash_query_log is populated by the ASH sampler job, which may have created it
-- under a different role. Wrap creation so an ownership conflict on a mature
-- workspace (deploy role != table owner) does not abort the whole deploy.
DO $$
BEGIN
    CREATE TABLE IF NOT EXISTS field_service.ash_query_log (
        sample_time       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        pid               INTEGER,
        usename           TEXT,
        state             TEXT,
        wait_event_type   TEXT,
        wait_event        TEXT,
        duration          INTERVAL,
        query             TEXT
    );
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ash_query_log table create skipped: %', SQLERRM;
END $$;

DO $$
BEGIN
    CREATE INDEX IF NOT EXISTS idx_ash_query_log_time ON field_service.ash_query_log (sample_time DESC);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ash_query_log index create skipped (likely not table owner): %', SQLERRM;
END $$;


-- ============================================================================
-- FEATURE 4: EVENT SOURCING / AUDIT LOG
-- ============================================================================

DROP TABLE IF EXISTS field_service.events CASCADE;
CREATE TABLE field_service.events (
    event_id      BIGSERIAL PRIMARY KEY,
    event_type    VARCHAR(50) NOT NULL,       -- 'work_order.created', 'work_order.dispatched', etc.
    entity_type   VARCHAR(30) NOT NULL,       -- 'work_order', 'technician', 'customer'
    entity_id     VARCHAR(50) NOT NULL,       -- The ID of the affected entity
    actor         VARCHAR(100),               -- Who/what triggered it ('Simulator', 'Dispatch', user email)
    payload       JSONB,                      -- Full event details (old_status, new_status, metadata)
    created_at    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.events IS 'Immutable event sourcing table. Every state change across the system is recorded here with JSONB payload for full auditability.';

CREATE INDEX IF NOT EXISTS idx_events_entity ON field_service.events (entity_type, entity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_type ON field_service.events (event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_created ON field_service.events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_payload_gin ON field_service.events USING GIN (payload);


-- ============================================================================
-- FEATURE 5: PARTS INVENTORY — Reorder Requests + Auto-Reorder Trigger
-- ============================================================================

DROP TABLE IF EXISTS field_service.reorder_requests CASCADE;
CREATE TABLE field_service.reorder_requests (
    reorder_id        BIGSERIAL PRIMARY KEY,
    equipment_type_id INTEGER REFERENCES field_service.equipment_catalog(equipment_type_id),
    region_id         INTEGER REFERENCES field_service.service_regions(region_id),
    quantity          INTEGER NOT NULL DEFAULT 50,
    status            VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'ordered', 'received', 'cancelled')),
    requested_by      VARCHAR(100) DEFAULT 'system',
    created_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.reorder_requests IS 'Auto-generated reorder requests when regional equipment stock drops below threshold. Demonstrates PG trigger-driven business logic.';

CREATE INDEX IF NOT EXISTS idx_reorder_status ON field_service.reorder_requests (status);
CREATE INDEX IF NOT EXISTS idx_reorder_region ON field_service.reorder_requests (region_id);
CREATE INDEX IF NOT EXISTS idx_reorder_equip ON field_service.reorder_requests (equipment_type_id);

-- Auto-reorder trigger function: fires when equipment_inventory status changes to 'installed'
CREATE OR REPLACE FUNCTION field_service.check_reorder()
RETURNS TRIGGER AS $$
DECLARE
    stock_count INTEGER;
    reorder_threshold INTEGER := 10;
BEGIN
    -- Only fire when status changes to 'installed' or 'assigned'
    IF NEW.status IN ('installed', 'assigned') AND (OLD.status IS DISTINCT FROM NEW.status) THEN
        -- Count remaining in-stock items for this equipment type in this region
        SELECT COUNT(*) INTO stock_count
        FROM field_service.equipment_inventory
        WHERE equipment_type_id = NEW.equipment_type_id
          AND region_id = NEW.region_id
          AND status = 'in_stock';

        -- If below threshold and no pending reorder exists, create one
        IF stock_count < reorder_threshold THEN
            IF NOT EXISTS (
                SELECT 1 FROM field_service.reorder_requests
                WHERE equipment_type_id = NEW.equipment_type_id
                  AND region_id = NEW.region_id
                  AND status IN ('pending', 'approved', 'ordered')
            ) THEN
                INSERT INTO field_service.reorder_requests (
                    equipment_type_id, region_id, quantity, requested_by
                ) VALUES (
                    NEW.equipment_type_id, NEW.region_id, 50, 'auto_reorder_trigger'
                );
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_check_reorder ON field_service.equipment_inventory;
CREATE TRIGGER trg_check_reorder
    AFTER UPDATE ON field_service.equipment_inventory
    FOR EACH ROW EXECUTE FUNCTION field_service.check_reorder();


-- ============================================================================
-- GRANTS — Give app roles access to new objects
-- Supports both patterns:
--   1. Single role: lakebase_app (legacy / simple deployments)
--   2. Dual rotation roles: lakebase_app_perms -> lakebase_app_a / lakebase_app_b
-- ============================================================================
DO $$
DECLARE
    app_role TEXT;
    roles TEXT[] := ARRAY['lakebase_app', 'lakebase_app_perms'];
BEGIN
    FOREACH app_role IN ARRAY roles LOOP
        -- Skip if role doesn't exist
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = app_role) THEN
            RAISE NOTICE 'Role % does not exist — skipping grants', app_role;
            CONTINUE;
        END IF;

        -- Schema access
        EXECUTE format('GRANT USAGE, CREATE ON SCHEMA field_service TO %I', app_role);

        -- All tables in field_service (current + future)
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA field_service TO %I', app_role);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA field_service GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', app_role);

        -- Sequences
        EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA field_service TO %I', app_role);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA field_service GRANT USAGE, SELECT ON SEQUENCES TO %I', app_role);

        -- Functions (refresh_sla_matviews, calculate_sla_risk, etc.)
        EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA field_service TO %I', app_role);

        -- pg_stat_statements (Query Performance tab)
        BEGIN
            EXECUTE format('GRANT SELECT ON pg_stat_statements TO %I', app_role);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not grant pg_stat_statements to %: %', app_role, SQLERRM;
        END;

        -- pg_read_all_stats (session monitoring)
        BEGIN
            EXECUTE format('GRANT pg_read_all_stats TO %I', app_role);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not grant pg_read_all_stats to %: %', app_role, SQLERRM;
        END;

        -- monitoring schema (if exists)
        BEGIN
            EXECUTE format('GRANT USAGE ON SCHEMA monitoring TO %I', app_role);
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO %I', app_role);
        EXCEPTION WHEN OTHERS THEN NULL;
        END;

        RAISE NOTICE 'Granted permissions to %', app_role;
    END LOOP;
END $$;


-- ============================================================================
-- SUMMARY
-- ============================================================================
-- New objects created:
--   Tables:    field_service.events, field_service.reorder_requests
--   Columns:   work_orders.sla_risk_score, work_orders.sla_hours_remaining
--   Triggers:  trg_sla_risk (on work_orders), trg_check_reorder (on equipment_inventory)
--   Functions: field_service.calculate_sla_risk(), field_service.check_reorder()
--   Mat Views: field_service.mv_technician_leaderboard, field_service.mv_regional_sla
-- ============================================================================

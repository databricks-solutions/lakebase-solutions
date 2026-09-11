-- ============================================================================
-- INTELLIGENT DISPATCH OPTIMIZATION
-- Weighted scoring engine for smart work order → technician assignment
-- ============================================================================
--
-- Replaces the greedy rule-based auto-assign with a multi-factor scoring
-- system that considers: skill match, geographic distance, technician
-- capacity, SLA urgency, and performance rating.
--
-- Scoring weights (total 100):
--   Skill match:     30 pts  (expert=30, intermediate=20, basic=10)
--   Distance:        25 pts  (inverse km, capped at 50km)
--   Capacity:        20 pts  (fewer active WOs = higher score)
--   SLA urgency:     15 pts  (tighter deadline = higher priority)
--   Tech rating:     10 pts  (avg_rating * 2)
--
-- Idempotent: Uses DROP IF EXISTS + CREATE for clean re-runs.
-- ============================================================================

SET client_min_messages = NOTICE;

-- ── Max active orders column ────────────────────────────────────────────
ALTER TABLE field_service.technicians
  ADD COLUMN IF NOT EXISTS max_active_orders INTEGER DEFAULT 8;

-- ── Dispatch scores staging table ───────────────────────────────────────
DROP TABLE IF EXISTS field_service.dispatch_scores CASCADE;

CREATE TABLE field_service.dispatch_scores (
    work_order_id   BIGINT NOT NULL,
    technician_id   BIGINT NOT NULL,
    skill_score     NUMERIC(5,2) DEFAULT 0,
    distance_score  NUMERIC(5,2) DEFAULT 0,
    capacity_score  NUMERIC(5,2) DEFAULT 0,
    sla_score       NUMERIC(5,2) DEFAULT 0,
    rating_score    NUMERIC(5,2) DEFAULT 0,
    total_score     NUMERIC(6,2) DEFAULT 0,
    distance_km     NUMERIC(8,2) DEFAULT 0,
    PRIMARY KEY (work_order_id, technician_id)
);

CREATE INDEX idx_ds_wo_score ON field_service.dispatch_scores (work_order_id, total_score DESC);
CREATE INDEX idx_ds_tech ON field_service.dispatch_scores (technician_id);

COMMENT ON TABLE field_service.dispatch_scores IS
    'Staging table for dispatch optimization scores. Truncated and rebuilt each smart-assign run. '
    'Each row scores a (work_order, technician) candidate pair across 5 factors.';

-- ── Grant access ────────────────────────────────────────────────────────
DO $$
DECLARE
    r TEXT;
BEGIN
    FOREACH r IN ARRAY ARRAY['lakebase_app', 'lakebase_app_perms']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON field_service.dispatch_scores TO %I', r);
        END IF;
    END LOOP;
END $$;

-- ── Haversine distance function (km) ────────────────────────────────────
CREATE OR REPLACE FUNCTION field_service.haversine_km(
    lat1 NUMERIC, lng1 NUMERIC,
    lat2 NUMERIC, lng2 NUMERIC
) RETURNS NUMERIC AS $$
DECLARE
    R CONSTANT NUMERIC := 6371.0;  -- Earth radius in km
    dlat NUMERIC;
    dlng NUMERIC;
    a NUMERIC;
BEGIN
    IF lat1 IS NULL OR lng1 IS NULL OR lat2 IS NULL OR lng2 IS NULL THEN
        RETURN NULL;
    END IF;
    dlat := radians(lat2 - lat1);
    dlng := radians(lng2 - lng1);
    a := sin(dlat/2) * sin(dlat/2)
       + cos(radians(lat1)) * cos(radians(lat2))
       * sin(dlng/2) * sin(dlng/2);
    RETURN R * 2 * atan2(sqrt(a), sqrt(1 - a));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ── Main scoring function ───────────────────────────────────────────────
CREATE OR REPLACE FUNCTION field_service.compute_dispatch_scores(
    p_region_id INTEGER DEFAULT NULL,
    p_limit     INTEGER DEFAULT 500
)
RETURNS TABLE (
    scored_count  INTEGER,
    wo_count      INTEGER,
    tech_count    INTEGER,
    avg_score     NUMERIC,
    avg_distance  NUMERIC
) AS $$
DECLARE
    v_scored  INTEGER := 0;
    v_wos     INTEGER := 0;
    v_techs   INTEGER := 0;
    v_avg_s   NUMERIC := 0;
    v_avg_d   NUMERIC := 0;
BEGIN
    -- Clear previous scores
    TRUNCATE field_service.dispatch_scores;

    -- Insert scored pairs: every open/unassigned WO × every eligible tech
    INSERT INTO field_service.dispatch_scores (
        work_order_id, technician_id,
        skill_score, distance_score, capacity_score, sla_score, rating_score,
        total_score, distance_km
    )
    SELECT
        wo.work_order_id,
        t.technician_id,

        -- Skill score (0-30): proficiency match
        CASE ts.proficiency_level
            WHEN 'expert'       THEN 30
            WHEN 'intermediate' THEN 20
            WHEN 'basic'        THEN 10
            ELSE 0
        END AS skill_score,

        -- Distance score (0-25): closer = better, capped at 50km
        CASE WHEN wo.latitude IS NOT NULL AND t.current_latitude IS NOT NULL THEN
            GREATEST(0, 25 * (1.0 - LEAST(
                field_service.haversine_km(t.current_latitude, t.current_longitude,
                                           wo.latitude, wo.longitude)
                / 50.0, 1.0)))
        ELSE 12.5  -- neutral if no GPS
        END AS distance_score,

        -- Capacity score (0-20): fewer active WOs = higher score
        GREATEST(0, 20 * (1.0 - COALESCE(act.cnt, 0)::numeric
                                / COALESCE(t.max_active_orders, 8))) AS capacity_score,

        -- SLA urgency score (0-15): tighter deadline = higher priority
        CASE
            WHEN wo.sla_due_at IS NULL THEN 2
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP THEN 15                          -- already breached
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours' THEN 14
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '4 hours' THEN 12
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '8 hours' THEN 8
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '24 hours' THEN 4
            ELSE 0
        END AS sla_score,

        -- Rating score (0-10): higher rated techs preferred
        COALESCE(t.avg_rating, 3.0) * 2 AS rating_score,

        -- Window fit bonus: WOs with appointment windows get a bonus if
        -- the window is still open. Tighter remaining window = higher bonus.
        -- This ensures appointment-constrained WOs are prioritized.
        -- (Not stored as separate column — folded into total_score)

        -- Total score (sum including window fit bonus 0-10)
        (CASE ts.proficiency_level
            WHEN 'expert' THEN 30 WHEN 'intermediate' THEN 20 WHEN 'basic' THEN 10 ELSE 0
        END)
        + (CASE WHEN wo.latitude IS NOT NULL AND t.current_latitude IS NOT NULL THEN
            GREATEST(0, 25 * (1.0 - LEAST(
                field_service.haversine_km(t.current_latitude, t.current_longitude,
                                           wo.latitude, wo.longitude) / 50.0, 1.0)))
           ELSE 12.5 END)
        + GREATEST(0, 20 * (1.0 - COALESCE(act.cnt, 0)::numeric / COALESCE(t.max_active_orders, 8)))
        + (CASE
            WHEN wo.sla_due_at IS NULL THEN 2
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP THEN 15
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours' THEN 14
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '4 hours' THEN 12
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '8 hours' THEN 8
            WHEN wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '24 hours' THEN 4
            ELSE 0
           END)
        + COALESCE(t.avg_rating, 3.0) * 2
        -- Window fit bonus (0-10): prioritize WOs with appointment windows
        + CASE
            WHEN wo.appointment_window_end IS NULL THEN 0
            WHEN wo.appointment_window_end < CURRENT_TIMESTAMP THEN 10  -- window expired, urgent!
            WHEN wo.appointment_window_end < CURRENT_TIMESTAMP + INTERVAL '1 hour' THEN 8
            WHEN wo.appointment_window_end < CURRENT_TIMESTAMP + INTERVAL '2 hours' THEN 6
            WHEN wo.appointment_window_end < CURRENT_TIMESTAMP + INTERVAL '4 hours' THEN 3
            ELSE 0
          END
        AS total_score,

        -- Raw distance for reporting
        COALESCE(field_service.haversine_km(
            t.current_latitude, t.current_longitude,
            wo.latitude, wo.longitude
        ), 999) AS distance_km

    FROM (
        -- Open unassigned WOs (limit for performance)
        SELECT work_order_id, latitude, longitude, sla_due_at,
               required_skill_id, region_id, priority,
               appointment_window_start, appointment_window_end,
               COALESCE(estimated_duration_min, 60) as est_duration,
               service_order_id
        FROM field_service.work_orders
        WHERE status = 'open'
          AND assigned_technician_id IS NULL
          AND (p_region_id IS NULL OR region_id = p_region_id)
        ORDER BY
            -- Prioritize WOs with appointment windows closing soonest
            COALESCE(appointment_window_end, sla_due_at) ASC NULLS LAST,
            CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1
            WHEN 'medium' THEN 2 ELSE 3 END
        LIMIT p_limit
    ) wo
    CROSS JOIN (
        -- Eligible technicians: active, available or en_route, same region, has capacity
        SELECT t2.technician_id, t2.current_latitude, t2.current_longitude,
               t2.avg_rating, t2.region_id, t2.max_active_orders
        FROM field_service.technicians t2
        WHERE t2.is_active = true
          AND t2.status IN ('available', 'en_route')
          AND (p_region_id IS NULL OR t2.region_id = p_region_id)
    ) t
    -- Same region matching
    LEFT JOIN field_service.technician_skills ts
        ON ts.technician_id = t.technician_id
        AND ts.skill_id = wo.required_skill_id
    -- Active WO count per tech (for capacity scoring)
    LEFT JOIN (
        SELECT assigned_technician_id, COUNT(*) as cnt
        FROM field_service.work_orders
        WHERE status NOT IN ('completed', 'cancelled')
        GROUP BY assigned_technician_id
    ) act ON act.assigned_technician_id = t.technician_id
    WHERE t.region_id = wo.region_id;

    -- Compute summary stats
    SELECT COUNT(*), COUNT(DISTINCT work_order_id), COUNT(DISTINCT technician_id),
           ROUND(AVG(total_score), 1), ROUND(AVG(distance_km), 1)
    INTO v_scored, v_wos, v_techs, v_avg_s, v_avg_d
    FROM field_service.dispatch_scores;

    RETURN QUERY SELECT v_scored, v_wos, v_techs, v_avg_s, v_avg_d;
END;
$$ LANGUAGE plpgsql;

-- ── Grant execute ───────────────────────────────────────────────────────
DO $$
DECLARE
    r TEXT;
BEGIN
    FOREACH r IN ARRAY ARRAY['lakebase_app', 'lakebase_app_perms']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('GRANT EXECUTE ON FUNCTION field_service.compute_dispatch_scores(INTEGER, INTEGER) TO %I', r);
            EXECUTE format('GRANT EXECUTE ON FUNCTION field_service.haversine_km(NUMERIC, NUMERIC, NUMERIC, NUMERIC) TO %I', r);
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- DONE
-- ============================================================================
-- RAISE is PL/pgSQL only — at top level it is a syntax error, which aborted the
-- rest of this file when applied as a single statement.
DO $$
BEGIN
    RAISE NOTICE 'Dispatch optimization schema objects created successfully.';
END $$;

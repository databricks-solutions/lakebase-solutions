-- ============================================================================
-- TIER 4b: FLEET FUEL + EXTERNAL PROVIDER DATA  →  COST & EFFICIENCY SIGNALS
-- ============================================================================
--
-- Consolidates the two fragmented external data sources a real fleet team lives
-- with onto the lakehouse:
--
--   1. fuel_transactions    — fuel-card purchases. THIS is the data that today
--                             flows into Google Sheets via automation and is
--                             viewed through AppSheet. Here it lands in Lakebase
--                             (system of action) and is ingested to the Iceberg
--                             medallion by notebooks/ingest_fuel_external.py
--                             (Volume export -> Auto Loader -> Bronze/Silver/Gold).
--   2. external_maintenance — third-party shop invoices that live in separate
--                             provider systems (not the fleet's own shop).
--
-- Why it matters (operationalized, not a dashboard):
--   * FUEL-EFFICIENCY DECLINE IS A LEADING FAILURE SIGNAL. A van whose km/L is
--     trending down vs. its own baseline is degrading mechanically — fuel data
--     anticipates failures the same way telematics does. v_vehicle_cost_summary
--     exposes a per-vehicle efficiency trend + a fuel_anomaly flag that the app
--     joins to risk/health and feeds the PdM action queue.
--   * TRUE COST-PER-KM / RUNNING-COST TCO (fuel + own-shop + external invoices)
--     feeds the Fleet Planner's retire-vs-repair economics, so "replace this
--     van" is driven by what it actually costs to run, not just age/odometer.
--
-- Each fuel row carries distance_km (km driven since the previous fill) so the
-- efficiency view is a trivial sum(distance_km)/sum(liters) — no window math.
--
-- Idempotent: IF NOT EXISTS + emptiness-guarded seeds. Runs as Tier-4b AFTER
-- data/fleet_management.sql (depends on fleet_vehicles).
-- ============================================================================

SET client_min_messages = NOTICE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. FUEL TRANSACTIONS  (the "off Google Sheets / AppSheet" data set)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.fuel_transactions (
    txn_id            BIGSERIAL PRIMARY KEY,
    vehicle_id        VARCHAR(50) NOT NULL REFERENCES field_service.fleet_vehicles(vehicle_id),
    txn_date          DATE NOT NULL,
    odometer_km       NUMERIC(10,1),
    distance_km       NUMERIC(8,1),         -- km driven since the previous fill (for efficiency)
    liters            NUMERIC(7,2),
    price_per_liter   NUMERIC(5,3),
    total_cost        NUMERIC(8,2),
    merchant          VARCHAR(40),
    fuel_card_last4   VARCHAR(4),
    region_id         INTEGER REFERENCES field_service.service_regions(region_id),
    source            VARCHAR(30) DEFAULT 'fuel_card_appsheet',  -- provenance: where it came from
    created_at        TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE field_service.fuel_transactions IS
    'Fuel-card purchases per vehicle. Mirrors the data a fleet team keeps in '
    'Google Sheets / AppSheet today; here it is centralized in Lakebase and '
    'ingested to the Iceberg medallion. distance_km enables a simple km/L '
    'efficiency trend — a leading mechanical-failure signal.';

CREATE INDEX IF NOT EXISTS idx_fuel_vehicle ON field_service.fuel_transactions (vehicle_id, txn_date DESC);
CREATE INDEX IF NOT EXISTS idx_fuel_date    ON field_service.fuel_transactions (txn_date DESC);

-- Seed ~12 fills over 60 days per active vehicle. Efficiency (km/L) varies by
-- health profile AND DECLINES over time for failing units — recent fills worse
-- than older ones — so the trend itself is the predictive signal.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM field_service.fuel_transactions LIMIT 1) THEN
        INSERT INTO field_service.fuel_transactions (
            vehicle_id, txn_date, odometer_km, distance_km, liters,
            price_per_liter, total_cost, merchant, fuel_card_last4, region_id, source
        )
        SELECT
            v.vehicle_id,
            (CURRENT_DATE - ((11 - g.n) * 5) )::date                          AS txn_date,
            -- odometer climbs toward current value as fills approach today
            GREATEST(0, v.odometer_km - (11 - g.n) * dist.d)::NUMERIC(10,1)    AS odometer_km,
            dist.d::NUMERIC(8,1)                                              AS distance_km,
            ROUND((dist.d / kmpl.v)::numeric, 2)                              AS liters,
            ROUND(price.p::numeric, 3)                                        AS price_per_liter,
            ROUND((dist.d / kmpl.v * price.p)::numeric, 2)                    AS total_cost,
            (ARRAY['Shell Fleet','BP Plus','Pilot Flying J','Costco Fuel','WEX Network'])
                [1 + ((v.assigned_technician_id + g.n) % 5)]                  AS merchant,
            LPAD(((v.assigned_technician_id * 37) % 10000)::text, 4, '0')     AS fuel_card_last4,
            v.region_id,
            'fuel_card_appsheet'
        FROM field_service.fleet_vehicles v
        CROSS JOIN generate_series(0, 11) AS g(n)
        -- distance per fill: ~480-620 km, slightly higher for failing (more road time)
        CROSS JOIN LATERAL (SELECT (480 + (v.assigned_technician_id % 5) * 30
                                    + CASE WHEN v.health_profile = 'failing' THEN 40 ELSE 0 END)::float AS d) dist
        -- km/L base by profile, DECLINING toward recent fills. Failing units drop
        -- steeply (the risk model already flags them). Degrading units (MEDIUM risk)
        -- drop enough to trip the fuel anomaly BEFORE the telematics risk score catches
        -- up — the genuine "fuel economy is a leading failure signal" early warning.
        -- Healthy units stay flat (no false anomalies).
        CROSS JOIN LATERAL (SELECT (CASE v.health_profile
                                      WHEN 'failing'   THEN 7.8 - (g.n::float / 11.0) * 2.6
                                      WHEN 'degrading' THEN 9.3 - (g.n::float / 11.0) * 1.8
                                      ELSE                  10.4 + (RANDOM() * 0.6 - 0.3)
                                    END) AS v) kmpl
        -- fuel price: regional spread + mild upward drift over the window
        CROSS JOIN LATERAL (SELECT (0.94 + (v.region_id % 6) * 0.015
                                    + g.n::float / 11.0 * 0.06 + RANDOM() * 0.02) AS p) price
        WHERE v.status = 'active';

        RAISE NOTICE 'fuel_transactions seeded: % rows',
            (SELECT count(*) FROM field_service.fuel_transactions);
    ELSE
        RAISE NOTICE 'fuel_transactions already populated — skipping seed.';
    END IF;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 2. EXTERNAL MAINTENANCE  (third-party shop invoices from provider systems)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.external_maintenance (
    ext_id              BIGSERIAL PRIMARY KEY,
    vehicle_id          VARCHAR(50) NOT NULL REFERENCES field_service.fleet_vehicles(vehicle_id),
    service_date        DATE NOT NULL,
    vendor              VARCHAR(50),
    service_category    VARCHAR(40),
    odometer_km         NUMERIC(10,1),
    parts_cost          NUMERIC(9,2),
    labor_cost          NUMERIC(9,2),
    total_cost          NUMERIC(9,2),
    invoice_ref         VARCHAR(30),
    source              VARCHAR(30) DEFAULT 'provider_feed',
    created_at          TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE field_service.external_maintenance IS
    'Third-party shop maintenance invoices ingested from external provider '
    'systems. Combined with vehicle_maintenance_history (own shop) to compute '
    'true running cost / TCO per vehicle for the Planner retire-vs-repair call.';

CREATE INDEX IF NOT EXISTS idx_extmaint_vehicle ON field_service.external_maintenance (vehicle_id, service_date DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM field_service.external_maintenance LIMIT 1) THEN
        INSERT INTO field_service.external_maintenance (
            vehicle_id, service_date, vendor, service_category, odometer_km,
            parts_cost, labor_cost, total_cost, invoice_ref, source
        )
        SELECT
            v.vehicle_id,
            (CURRENT_DATE - ((g.n * 70 + (v.assigned_technician_id % 50)) || ' days')::interval)::date,
            (ARRAY['Firestone Fleet','Goodyear Commercial','Midas Pro','TA Truck Service','Local Diesel Co'])
                [1 + ((v.assigned_technician_id + g.n) % 5)],
            (ARRAY['transmission_repair','turbo_replacement','brake_overhaul','ac_compressor',
                   'electrical_diagnostic','exhaust_repair','suspension'])[1 + ((v.assigned_technician_id + g.n) % 7)],
            GREATEST(0, v.odometer_km - (g.n * 11000))::NUMERIC(10,1),
            pc.parts, lc.labor, (pc.parts + lc.labor),
            'INV-' || UPPER(SUBSTRING(MD5(v.vehicle_id || g.n::text) FROM 1 FOR 8)),
            'provider_feed'
        FROM field_service.fleet_vehicles v
        CROSS JOIN LATERAL generate_series(1,
            CASE v.health_profile
                WHEN 'failing'   THEN 3
                WHEN 'degrading' THEN 2
                ELSE CASE WHEN (v.assigned_technician_id % 2) = 0 THEN 1 ELSE 0 END
            END) AS g(n)
        -- failing units rack up larger external invoices
        CROSS JOIN LATERAL (SELECT (CASE v.health_profile
                                      WHEN 'failing'   THEN 600 + (v.assigned_technician_id * 53 + g.n * 211) % 2400
                                      WHEN 'degrading' THEN 300 + (v.assigned_technician_id * 31 + g.n * 97) % 1100
                                      ELSE                  120 + (v.assigned_technician_id * 17 + g.n * 41) % 500
                                    END)::NUMERIC(9,2) AS parts) pc
        CROSS JOIN LATERAL (SELECT (240 + (v.assigned_technician_id * 23 + g.n * 67) % 900)::NUMERIC(9,2) AS labor) lc
        WHERE v.status <> 'retired';

        RAISE NOTICE 'external_maintenance seeded: % rows',
            (SELECT count(*) FROM field_service.external_maintenance);
    ELSE
        RAISE NOTICE 'external_maintenance already populated — skipping seed.';
    END IF;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 3. VEHICLE COST + EFFICIENCY SUMMARY VIEW
-- ═══════════════════════════════════════════════════════════════════════════
-- Per-vehicle fuel economy (km/L), its recent-vs-older trend (the leading
-- failure signal), fuel spend, combined maintenance cost (own shop + external),
-- cost-per-km, and an annualized running-cost estimate for TCO/retire decisions.

CREATE OR REPLACE VIEW field_service.v_vehicle_cost_summary AS
WITH fuel AS (
    SELECT
        ft.vehicle_id,
        count(*)                                              AS fills_60d,
        SUM(ft.liters)                                        AS liters_60d,
        SUM(ft.distance_km)                                   AS distance_60d,
        SUM(ft.total_cost)                                    AS fuel_cost_60d,
        -- overall and split-window efficiency (km per liter)
        CASE WHEN SUM(ft.liters) > 0
             THEN SUM(ft.distance_km) / SUM(ft.liters) END    AS kmpl_overall,
        CASE WHEN SUM(ft.liters) FILTER (WHERE ft.txn_date >= CURRENT_DATE - 30) > 0
             THEN SUM(ft.distance_km) FILTER (WHERE ft.txn_date >= CURRENT_DATE - 30)
                  / SUM(ft.liters)   FILTER (WHERE ft.txn_date >= CURRENT_DATE - 30) END AS kmpl_recent,
        CASE WHEN SUM(ft.liters) FILTER (WHERE ft.txn_date <  CURRENT_DATE - 30) > 0
             THEN SUM(ft.distance_km) FILTER (WHERE ft.txn_date <  CURRENT_DATE - 30)
                  / SUM(ft.liters)   FILTER (WHERE ft.txn_date <  CURRENT_DATE - 30) END AS kmpl_older
    FROM field_service.fuel_transactions ft
    GROUP BY ft.vehicle_id
),
own_maint AS (
    SELECT vehicle_id, SUM(cost) AS own_maint_365d
    FROM field_service.vehicle_maintenance_history
    WHERE service_date >= CURRENT_DATE - 365
    GROUP BY vehicle_id
),
ext_maint AS (
    SELECT vehicle_id, count(*) AS ext_invoices, SUM(total_cost) AS ext_maint_365d
    FROM field_service.external_maintenance
    WHERE service_date >= CURRENT_DATE - 365
    GROUP BY vehicle_id
)
SELECT
    v.vehicle_id,
    v.region_id,
    v.make, v.model, v.model_year, v.risk_category, v.health_score,
    COALESCE(f.fills_60d, 0)                                          AS fills_60d,
    ROUND(COALESCE(f.fuel_cost_60d, 0), 2)                            AS fuel_cost_60d,
    ROUND(COALESCE(f.distance_60d, 0), 1)                             AS distance_60d,
    ROUND(f.kmpl_overall::numeric, 2)                                 AS efficiency_kmpl,
    ROUND(f.kmpl_recent::numeric, 2)                                  AS recent_kmpl,
    ROUND(f.kmpl_older::numeric, 2)                                   AS older_kmpl,
    -- efficiency trend % (negative = getting worse = mechanical degradation)
    CASE WHEN f.kmpl_older > 0
         THEN ROUND(((f.kmpl_recent - f.kmpl_older) / f.kmpl_older * 100)::numeric, 1) END AS eff_trend_pct,
    -- fuel anomaly: recent economy >=8% worse than the earlier window
    (f.kmpl_older > 0 AND f.kmpl_recent < f.kmpl_older * 0.92)        AS fuel_anomaly,
    -- fuel cost per km over the 60-day window
    CASE WHEN COALESCE(f.distance_60d, 0) > 0
         THEN ROUND((f.fuel_cost_60d / f.distance_60d)::numeric, 3) END AS fuel_cost_per_km,
    COALESCE(em.ext_invoices, 0)                                     AS external_invoices,
    ROUND(COALESCE(om.own_maint_365d, 0) + COALESCE(em.ext_maint_365d, 0), 2) AS maint_cost_365d,
    -- annualized running cost = fuel (60d -> x6) + trailing-year maintenance
    ROUND((COALESCE(f.fuel_cost_60d, 0) * 6
           + COALESCE(om.own_maint_365d, 0) + COALESCE(em.ext_maint_365d, 0))::numeric, 0) AS annual_running_cost
FROM field_service.fleet_vehicles v
LEFT JOIN fuel       f  ON f.vehicle_id  = v.vehicle_id
LEFT JOIN own_maint  om ON om.vehicle_id = v.vehicle_id
LEFT JOIN ext_maint  em ON em.vehicle_id = v.vehicle_id;

COMMENT ON VIEW field_service.v_vehicle_cost_summary IS
    'Per-vehicle fuel economy + trend (leading failure signal), fuel spend, '
    'combined maintenance cost (own + external), cost-per-km, and annualized '
    'running cost for the Fleet Planner TCO / retire-vs-repair decision.';

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
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.fuel_transactions TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.fuel_transactions_txn_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.external_maintenance TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.external_maintenance_ext_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT ON field_service.v_vehicle_cost_summary TO %I', r);
        END IF;
    END LOOP;
END $$;

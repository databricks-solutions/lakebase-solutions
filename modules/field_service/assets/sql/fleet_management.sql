-- ============================================================================
-- TIER 4: FLEET MANAGEMENT — VEHICLE PREDICTIVE MAINTENANCE
-- ============================================================================
--
-- Moves the technician vehicle fleet from REACTIVE to PREDICTIVE maintenance.
-- Telematics trends (engine temp, oil life, battery voltage, harsh events) and
-- diagnostic trouble codes (DTCs) feed a maintenance-risk score so failures are
-- anticipated before they cause downtime — then auto-generated as work orders
-- and surfaced to dispatch. Telco field-fleet analog of John Deere
-- (telematics -> predictive health alert -> proactive technician dispatch).
--
--   1. fleet_vehicles            — one vehicle per technician (backfilled from
--                                  technicians.vehicle_id 'VAN-####' labels).
--   2. vehicle_telemetry         — rolling 60-day daily telematics readings with
--                                  health-profile-driven trends (Geotab-style).
--   3. vehicle_dtc_codes         — OBD-II diagnostic trouble codes. AI columns
--                                  (ai_severity / ai_explanation / is_false_positive)
--                                  are populated by notebooks/interpret_dtc_codes.py
--                                  via ai_query() — the "loose fuel cap != engine
--                                  failure" data-trust showcase.
--   4. vehicle_maintenance_history — past service records (predicted vs reactive).
--   5. work_orders.vehicle_id    — links fleet-maintenance WOs to a vehicle.
--
-- Runs as "Tier 4" AFTER the technicians table is loaded, so fleet_vehicles can
-- be backfilled directly from technicians. Idempotent: IF NOT EXISTS + emptiness-
-- guarded seed inserts (safe to re-run from deploy_all).
-- ============================================================================

SET client_min_messages = NOTICE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. FLEET VEHICLES
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.fleet_vehicles (
    vehicle_id               VARCHAR(50) PRIMARY KEY,        -- matches technicians.vehicle_id ('VAN-0001')
    vin                      VARCHAR(17) UNIQUE,
    make                     VARCHAR(40),
    model                    VARCHAR(40),
    model_year               INT,
    vehicle_type             VARCHAR(20) DEFAULT 'cargo_van'
                             CHECK (vehicle_type IN ('cargo_van', 'pickup', 'bucket_truck', 'suv')),
    assigned_technician_id   BIGINT REFERENCES field_service.technicians(technician_id),
    region_id                INTEGER REFERENCES field_service.service_regions(region_id),
    odometer_km              NUMERIC(10,1) DEFAULT 0,
    in_service_date          DATE,
    status                   VARCHAR(20) DEFAULT 'active'
                             CHECK (status IN ('active', 'in_shop', 'retired')),
    health_profile           VARCHAR(20) DEFAULT 'healthy'
                             CHECK (health_profile IN ('healthy', 'degrading', 'failing')),
    -- Current snapshot (seeded here; refreshed by the scoring job in Phase 4)
    health_score             NUMERIC(5,1) DEFAULT 100.0,     -- 0-100, higher = healthier
    risk_category            VARCHAR(10) DEFAULT 'LOW'
                             CHECK (risk_category IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    predicted_failure_date   DATE,                           -- set by predictive scoring job
    current_latitude         NUMERIC(10,6),
    current_longitude        NUMERIC(10,6),
    last_service_date        DATE,
    last_service_odometer_km NUMERIC(10,1),
    next_service_due_km      NUMERIC(10,1),
    purchase_cost            NUMERIC(10,2),
    created_at               TIMESTAMPTZ DEFAULT now(),
    updated_at               TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE field_service.fleet_vehicles IS
    'Technician fleet vehicles (vans/trucks). One row per technician with a '
    'vehicle_id. health_score/risk_category are a current snapshot refreshed by '
    'the predictive maintenance scoring job. Backfilled from technicians.';

CREATE INDEX IF NOT EXISTS idx_fleet_region    ON field_service.fleet_vehicles (region_id);
CREATE INDEX IF NOT EXISTS idx_fleet_tech      ON field_service.fleet_vehicles (assigned_technician_id);
CREATE INDEX IF NOT EXISTS idx_fleet_risk      ON field_service.fleet_vehicles (risk_category);
CREATE INDEX IF NOT EXISTS idx_fleet_status    ON field_service.fleet_vehicles (status);
CREATE INDEX IF NOT EXISTS idx_fleet_health    ON field_service.fleet_vehicles (health_score);

-- Backfill one vehicle per technician (deterministic; correlated to health profile).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM field_service.fleet_vehicles LIMIT 1) THEN
        -- Reliability varies by MODEL and AGE (like a real fleet): a model-specific
        -- failing-rate baseline + an age penalty (older vehicles fail more). This makes
        -- the reliability-by-model analytics meaningful (some models/years are lemons).
        WITH prof AS (
            SELECT
                t.technician_id, t.vehicle_id, t.region_id,
                t.current_latitude, t.current_longitude,
                (t.technician_id % 5)          AS model_idx,
                (2016 + (t.technician_id % 9)) AS model_year,
                -- failing-rate threshold (%): per-model base (Ram worst, Mercedes best)
                -- + age penalty up to +16 for the oldest (2016) units.
                ( (ARRAY[6, 16, 10, 3, 8])[(t.technician_id % 5) + 1]
                  + (8 - (t.technician_id % 9)) * 2 ) AS fail_pct,
                ((t.technician_id * 7) % 100)  AS draw
            FROM field_service.technicians t
            WHERE t.vehicle_id IS NOT NULL AND t.is_active = TRUE
        ),
        scored AS (
            SELECT p.*,
                CASE WHEN p.draw < p.fail_pct        THEN 'failing'
                     WHEN p.draw < p.fail_pct + 22   THEN 'degrading'
                     ELSE 'healthy' END AS health_profile
            FROM prof p
        )
        INSERT INTO field_service.fleet_vehicles (
            vehicle_id, vin, make, model, model_year, vehicle_type,
            assigned_technician_id, region_id, odometer_km, in_service_date, status,
            health_profile, health_score, risk_category,
            current_latitude, current_longitude,
            last_service_date, last_service_odometer_km, next_service_due_km, purchase_cost
        )
        SELECT
            s.vehicle_id,
            UPPER(SUBSTRING(MD5(s.vehicle_id) FROM 1 FOR 17)),
            (ARRAY['Ford','Ram','Chevrolet','Mercedes-Benz','GMC'])[s.model_idx + 1],
            (ARRAY['Transit','ProMaster','Express','Sprinter','Savana'])[s.model_idx + 1],
            s.model_year,
            (ARRAY['cargo_van','cargo_van','cargo_van','pickup','bucket_truck'])[s.model_idx + 1],
            s.technician_id,
            s.region_id,
            -- Mileage: ~22k km/yr of age + per-vehicle spread + a bump for failing units
            ((2025 - s.model_year) * 22000
                + (s.technician_id * 131 % 30000)
                + CASE WHEN s.health_profile = 'failing' THEN 60000 ELSE 0 END)::NUMERIC(10,1),
            (CURRENT_DATE - ((365 * (2025 - s.model_year)) || ' days')::interval)::date,
            'active',
            s.health_profile,
            CASE s.health_profile
                WHEN 'healthy'   THEN 82.0 + (s.technician_id % 15)
                WHEN 'degrading' THEN 55.0 + (s.technician_id % 18)
                ELSE                  18.0 + (s.technician_id % 22)
            END,
            CASE s.health_profile
                WHEN 'healthy'   THEN 'LOW'
                WHEN 'degrading' THEN 'MEDIUM'
                ELSE CASE WHEN s.technician_id % 3 = 0 THEN 'CRITICAL' ELSE 'HIGH' END
            END,
            s.current_latitude,
            s.current_longitude,
            (CURRENT_DATE - ((30 + (s.technician_id * 7 % 150)) || ' days')::interval)::date,
            GREATEST(0, ((2025 - s.model_year) * 22000
                + (s.technician_id * 131 % 30000) - (3000 + (s.technician_id * 53 % 9000))))::NUMERIC(10,1),
            ((2025 - s.model_year) * 22000
                + (s.technician_id * 131 % 30000) - (3000 + (s.technician_id * 53 % 9000)) + 16000)::NUMERIC(10,1),
            (38000 + (s.technician_id * 91 % 22000))::NUMERIC(10,2)
        FROM scored s;

        -- A realistic slice of the worst vehicles are already in the shop.
        UPDATE field_service.fleet_vehicles
        SET status = 'in_shop'
        WHERE risk_category = 'CRITICAL' AND (assigned_technician_id % 3) = 0;

        RAISE NOTICE 'fleet_vehicles seeded: % rows',
            (SELECT count(*) FROM field_service.fleet_vehicles);
    ELSE
        RAISE NOTICE 'fleet_vehicles already populated — skipping seed.';
    END IF;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 2. VEHICLE TELEMETRY (rolling 60-day daily readings, Geotab-style)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.vehicle_telemetry (
    telemetry_id        BIGSERIAL PRIMARY KEY,
    vehicle_id          VARCHAR(50) NOT NULL REFERENCES field_service.fleet_vehicles(vehicle_id),
    recorded_at         TIMESTAMPTZ NOT NULL,
    odometer_km         NUMERIC(10,1),
    speed_avg_kph       NUMERIC(6,1),
    engine_temp_c       NUMERIC(5,1),       -- coolant temperature
    oil_life_pct        NUMERIC(5,1),       -- remaining oil life
    battery_voltage     NUMERIC(4,2),
    fuel_level_pct      NUMERIC(5,1),
    tire_pressure_psi   NUMERIC(5,1),
    engine_rpm_avg      INTEGER,
    harsh_brake_count   INTEGER DEFAULT 0,
    harsh_accel_count   INTEGER DEFAULT 0,
    idle_minutes        NUMERIC(6,1) DEFAULT 0,
    dtc_active_count    INTEGER DEFAULT 0,
    health_score        NUMERIC(5,1),       -- 0-100 derived from this reading
    latitude            NUMERIC(10,6),
    longitude           NUMERIC(10,6)
);

COMMENT ON TABLE field_service.vehicle_telemetry IS
    'Daily vehicle telematics readings (Geotab-style). Trends vary by the '
    'vehicle health_profile: failing units show rising engine temp, falling oil '
    'life and battery voltage, and more harsh-driving events over time.';

CREATE INDEX IF NOT EXISTS idx_vtel_vehicle ON field_service.vehicle_telemetry (vehicle_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vtel_time    ON field_service.vehicle_telemetry (recorded_at DESC);

-- Seed 60 daily readings per vehicle with profile-driven degradation trends.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM field_service.vehicle_telemetry LIMIT 1) THEN
        INSERT INTO field_service.vehicle_telemetry (
            vehicle_id, recorded_at, odometer_km, speed_avg_kph, engine_temp_c,
            oil_life_pct, battery_voltage, fuel_level_pct, tire_pressure_psi,
            engine_rpm_avg, harsh_brake_count, harsh_accel_count, idle_minutes,
            dtc_active_count, health_score, latitude, longitude
        )
        SELECT
            v.vehicle_id,
            (now() - ((59 - d) || ' days')::interval)                                   AS recorded_at,
            -- Odometer climbs toward the current value as readings approach today
            GREATEST(0, v.odometer_km - (59 - d) * (70 + (v.assigned_technician_id % 60)))::NUMERIC(10,1) AS odometer_km,
            (38 + RANDOM() * 18)::NUMERIC(6,1)                                           AS speed_avg_kph,
            -- Engine (coolant) temperature: failing units trend toward overheat
            (CASE v.health_profile
                WHEN 'failing'   THEN 92 + (d::float/59) * 30 + RANDOM() * 5
                WHEN 'degrading' THEN 88 + (d::float/59) * 9  + RANDOM() * 4
                ELSE                  86 + RANDOM() * 6
             END)::NUMERIC(5,1)                                                          AS engine_temp_c,
            -- Oil life: declines fastest on failing units
            GREATEST(2, CASE v.health_profile
                WHEN 'failing'   THEN 42 - (d::float/59) * 36 + RANDOM() * 5
                WHEN 'degrading' THEN 58 - (d::float/59) * 28 + RANDOM() * 8
                ELSE                  62 + RANDOM() * 33
             END)::NUMERIC(5,1)                                                          AS oil_life_pct,
            -- Battery voltage: sags on failing units
            (CASE v.health_profile
                WHEN 'failing'   THEN 12.5 - (d::float/59) * 1.5 + RANDOM() * 0.2
                WHEN 'degrading' THEN 12.4 - (d::float/59) * 0.5 + RANDOM() * 0.2
                ELSE                  12.5 + RANDOM() * 0.5
             END)::NUMERIC(4,2)                                                          AS battery_voltage,
            (RANDOM() * 100)::NUMERIC(5,1)                                               AS fuel_level_pct,
            (CASE v.health_profile
                WHEN 'failing'   THEN 28 + RANDOM() * 6
                ELSE                  33 + RANDOM() * 4
             END)::NUMERIC(5,1)                                                          AS tire_pressure_psi,
            (1700 + (RANDOM() * 600)::int
                + CASE v.health_profile WHEN 'failing' THEN 300 ELSE 0 END)             AS engine_rpm_avg,
            (CASE v.health_profile
                WHEN 'failing'   THEN (RANDOM() * 6)::int + 2
                WHEN 'degrading' THEN (RANDOM() * 4)::int
                ELSE                  (RANDOM() * 2)::int
             END)                                                                        AS harsh_brake_count,
            (CASE v.health_profile
                WHEN 'failing'   THEN (RANDOM() * 6)::int + 2
                WHEN 'degrading' THEN (RANDOM() * 4)::int
                ELSE                  (RANDOM() * 2)::int
             END)                                                                        AS harsh_accel_count,
            (RANDOM() * 90)::NUMERIC(6,1)                                                AS idle_minutes,
            (CASE v.health_profile
                WHEN 'failing'   THEN (RANDOM() * 3)::int + 1
                WHEN 'degrading' THEN (RANDOM() * 2)::int
                ELSE                  0
             END)                                                                        AS dtc_active_count,
            -- Per-reading health score: penalize hot engine, low oil, low battery
            GREATEST(0, LEAST(100,
                100
                - CASE WHEN v.health_profile = 'failing'   THEN 5 + (d::float/59) * 45
                       WHEN v.health_profile = 'degrading' THEN 3 + (d::float/59) * 20
                       ELSE RANDOM() * 12 END
            ))::NUMERIC(5,1)                                                             AS health_score,
            v.current_latitude  + (RANDOM() * 0.04 - 0.02)                               AS latitude,
            v.current_longitude + (RANDOM() * 0.04 - 0.02)                               AS longitude
        FROM field_service.fleet_vehicles v
        CROSS JOIN generate_series(0, 59) AS d;

        RAISE NOTICE 'vehicle_telemetry seeded: % rows',
            (SELECT count(*) FROM field_service.vehicle_telemetry);
    ELSE
        RAISE NOTICE 'vehicle_telemetry already populated — skipping seed.';
    END IF;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 3. VEHICLE DTC CODES (OBD-II diagnostic trouble codes)
-- ═══════════════════════════════════════════════════════════════════════════
--
-- ai_severity / ai_explanation / is_false_positive are intentionally NULL here.
-- notebooks/interpret_dtc_codes.py reads these rows, calls ai_query() with the
-- code + recent telemetry context, and writes back the interpretation — the
-- "loose fuel cap (P0455) is NOT an engine failure" data-trust demo moment.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.vehicle_dtc_codes (
    dtc_id              BIGSERIAL PRIMARY KEY,
    vehicle_id          VARCHAR(50) NOT NULL REFERENCES field_service.fleet_vehicles(vehicle_id),
    code                VARCHAR(10) NOT NULL,        -- e.g. P0455
    raw_description     TEXT,                        -- manufacturer/OBD description
    dtc_system          VARCHAR(20) DEFAULT 'powertrain'
                        CHECK (dtc_system IN ('powertrain', 'body', 'chassis', 'network')),
    reported_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    odometer_at_report  NUMERIC(10,1),
    raw_severity        VARCHAR(10) DEFAULT 'medium'
                        CHECK (raw_severity IN ('low', 'medium', 'high', 'critical')),
    status              VARCHAR(10) DEFAULT 'active'
                        CHECK (status IN ('active', 'cleared')),
    -- AI-interpreted columns (populated by interpret_dtc_codes.py via ai_query)
    ai_severity         VARCHAR(10),
    ai_explanation      TEXT,
    is_false_positive   BOOLEAN,
    ai_interpreted_at   TIMESTAMPTZ
);

COMMENT ON TABLE field_service.vehicle_dtc_codes IS
    'OBD-II diagnostic trouble codes per vehicle. ai_* columns are filled by '
    'notebooks/interpret_dtc_codes.py using ai_query() to denoise false positives '
    '(e.g. P0455 loose fuel cap mis-flagged as engine failure).';

CREATE INDEX IF NOT EXISTS idx_vdtc_vehicle ON field_service.vehicle_dtc_codes (vehicle_id, reported_at DESC);
CREATE INDEX IF NOT EXISTS idx_vdtc_active  ON field_service.vehicle_dtc_codes (status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_vdtc_code    ON field_service.vehicle_dtc_codes (code);

-- Seed DTCs weighted by health profile. Every profile gets a chance at the
-- benign P0455/P0457 EVAP codes (the classic loose-fuel-cap false positive),
-- while failing units accumulate genuinely serious powertrain codes.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM field_service.vehicle_dtc_codes LIMIT 1) THEN
        -- Healthy/degrading: 0-1 mostly benign codes. Failing: 1-3 serious codes.
        WITH dtc_ref(code, descr, sys, sev) AS (
            VALUES
                ('P0455', 'EVAP system large leak detected',                'powertrain', 'high'),
                ('P0457', 'EVAP leak detected (fuel cap loose/off)',        'powertrain', 'medium'),
                ('P0128', 'Coolant thermostat below regulating temperature','powertrain', 'medium'),
                ('P0300', 'Random/multiple cylinder misfire detected',      'powertrain', 'critical'),
                ('P0217', 'Engine coolant over temperature condition',      'powertrain', 'critical'),
                ('P0562', 'System voltage low',                             'powertrain', 'high'),
                ('P0521', 'Engine oil pressure sensor circuit range',       'powertrain', 'high'),
                ('P0420', 'Catalyst system efficiency below threshold',     'powertrain', 'medium'),
                ('P0606', 'ECM/PCM processor fault',                        'powertrain', 'high'),
                ('C0035', 'Left front wheel speed sensor circuit',          'chassis',    'medium'),
                ('B1318', 'Battery voltage low',                            'body',       'low')
        )
        INSERT INTO field_service.vehicle_dtc_codes (
            vehicle_id, code, raw_description, dtc_system, reported_at,
            odometer_at_report, raw_severity, status
        )
        SELECT
            v.vehicle_id, r.code, r.descr, r.sys,
            now() - ((g.n * 4 + (v.assigned_technician_id % 7)) || ' days')::interval,
            v.odometer_km - (g.n * 800)::NUMERIC(10,1),
            r.sev,
            'active'
        FROM field_service.fleet_vehicles v
        CROSS JOIN LATERAL generate_series(1,
            CASE v.health_profile
                WHEN 'failing'   THEN 3
                WHEN 'degrading' THEN 1
                ELSE CASE WHEN (v.assigned_technician_id % 3) = 0 THEN 1 ELSE 0 END
            END) AS g(n)
        -- Pick a code: failing units pull serious codes; others pull benign EVAP/sensor codes
        CROSS JOIN LATERAL (
            SELECT code, descr, sys, sev FROM dtc_ref
            WHERE CASE
                WHEN v.health_profile = 'failing'
                    THEN code IN ('P0300','P0217','P0562','P0521','P0606','P0455')
                ELSE code IN ('P0455','P0457','P0128','P0420','C0035','B1318')
            END
            -- Deterministic-but-varied pick: hash the code with the vehicle + draw index
            ORDER BY md5(code || v.vehicle_id || g.n::text)
            LIMIT 1
        ) AS r;

        RAISE NOTICE 'vehicle_dtc_codes seeded: % rows',
            (SELECT count(*) FROM field_service.vehicle_dtc_codes);
    ELSE
        RAISE NOTICE 'vehicle_dtc_codes already populated — skipping seed.';
    END IF;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 4. VEHICLE MAINTENANCE HISTORY
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.vehicle_maintenance_history (
    record_id            BIGSERIAL PRIMARY KEY,
    vehicle_id           VARCHAR(50) NOT NULL REFERENCES field_service.fleet_vehicles(vehicle_id),
    service_date         DATE NOT NULL,
    service_type         VARCHAR(40) NOT NULL
                         CHECK (service_type IN ('oil_change', 'brake_service', 'tire_rotation',
                                'battery_replacement', 'coolant_flush', 'transmission_service',
                                'inspection', 'unplanned_repair')),
    odometer_at_service  NUMERIC(10,1),
    cost                 NUMERIC(10,2),
    was_predicted        BOOLEAN DEFAULT FALSE,   -- TRUE if our model flagged it before failure
    notes                TEXT,
    created_at           TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE field_service.vehicle_maintenance_history IS
    'Past vehicle service records. was_predicted distinguishes proactive '
    '(model-predicted) maintenance from reactive breakdowns — the core PdM metric.';

CREATE INDEX IF NOT EXISTS idx_vmaint_vehicle ON field_service.vehicle_maintenance_history (vehicle_id, service_date DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM field_service.vehicle_maintenance_history LIMIT 1) THEN
        INSERT INTO field_service.vehicle_maintenance_history (
            vehicle_id, service_date, service_type, odometer_at_service, cost, was_predicted, notes
        )
        SELECT
            v.vehicle_id,
            (CURRENT_DATE - ((g.n * 95 + (v.assigned_technician_id % 60)) || ' days')::interval)::date,
            (ARRAY['oil_change','tire_rotation','inspection','brake_service',
                   'battery_replacement','coolant_flush','unplanned_repair'])[1 + ((v.assigned_technician_id + g.n) % 7)],
            GREATEST(0, v.odometer_km - (g.n * 14000))::NUMERIC(10,1),
            (60 + (v.assigned_technician_id * 7 + g.n * 53) % 1400)::NUMERIC(10,2),
            -- Failing units historically had more reactive (unplanned) repairs:
            -- fewer of their services were predicted (only every 3rd) vs every other for healthy.
            CASE WHEN v.health_profile = 'failing' THEN (g.n % 3 = 0) ELSE (g.n % 2 = 0) END,
            'Service record auto-generated for demo history.'
        FROM field_service.fleet_vehicles v
        CROSS JOIN LATERAL generate_series(1,
            CASE v.health_profile WHEN 'failing' THEN 6 WHEN 'degrading' THEN 4 ELSE 3 END) AS g(n);

        RAISE NOTICE 'vehicle_maintenance_history seeded: % rows',
            (SELECT count(*) FROM field_service.vehicle_maintenance_history);
    ELSE
        RAISE NOTICE 'vehicle_maintenance_history already populated — skipping seed.';
    END IF;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 5. LINK WORK ORDERS TO VEHICLES
-- ═══════════════════════════════════════════════════════════════════════════
-- Fleet-maintenance work orders (created by score_and_create_work_orders.py)
-- reference the vehicle via this column. category='maintenance',
-- subcategory='fleet_maintenance' — no category CHECK change needed.

ALTER TABLE field_service.work_orders
    ADD COLUMN IF NOT EXISTS vehicle_id VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_wo_vehicle ON field_service.work_orders (vehicle_id)
    WHERE vehicle_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- 6. FLEET HEALTH SUMMARY VIEW (convenience for the app / Genie)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW field_service.v_fleet_health_summary AS
SELECT
    r.region_id,
    r.region_code,
    r.region_name,
    count(*)                                              AS total_vehicles,
    count(*) FILTER (WHERE v.status = 'in_shop')          AS in_shop,
    count(*) FILTER (WHERE v.risk_category = 'CRITICAL')  AS critical,
    count(*) FILTER (WHERE v.risk_category = 'HIGH')      AS high_risk,
    count(*) FILTER (WHERE v.risk_category = 'MEDIUM')    AS medium_risk,
    count(*) FILTER (WHERE v.risk_category = 'LOW')       AS low_risk,
    round(avg(v.health_score), 1)                         AS avg_health_score,
    round(avg(v.odometer_km), 0)                          AS avg_odometer_km
FROM field_service.fleet_vehicles v
JOIN field_service.service_regions r ON r.region_id = v.region_id
GROUP BY r.region_id, r.region_code, r.region_name;

COMMENT ON VIEW field_service.v_fleet_health_summary IS
    'Per-region fleet health rollup for the Fleet dashboard and Genie.';

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
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.fleet_vehicles TO %I', r);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.vehicle_telemetry TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.vehicle_telemetry_telemetry_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.vehicle_dtc_codes TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.vehicle_dtc_codes_dtc_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.vehicle_maintenance_history TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.vehicle_maintenance_history_record_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT ON field_service.v_fleet_health_summary TO %I', r);
        END IF;
    END LOOP;
END $$;

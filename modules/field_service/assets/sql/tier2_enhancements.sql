-- ============================================================================
-- TIER 2: PRODUCTION TEMPLATE ENHANCEMENTS
-- Customer comms, alarm correlation, truck roll avoidance
-- ============================================================================
--
-- 1. CUSTOMER COMMUNICATIONS — SMS/email log for tech ETA, confirmations,
--    rescheduling. Required by every real telco dispatch system.
--
-- 2. ALARM CORRELATION — Groups raw network alerts into root cause incidents.
--    Reduces noise (1000 alerts → 10 incidents) for NOC operators.
--
-- 3. TRUCK ROLL PREDICTION — Tracks whether WOs were resolved remotely or
--    required a field visit. Enables ML to predict truck roll avoidance.
--
-- Idempotent: Uses IF NOT EXISTS.
-- ============================================================================

SET client_min_messages = NOTICE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. CUSTOMER COMMUNICATION LOG
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.customer_communications (
    comm_id          BIGSERIAL PRIMARY KEY,
    work_order_id    BIGINT REFERENCES field_service.work_orders(work_order_id),
    customer_id      BIGINT REFERENCES field_service.customers(customer_id),
    channel          VARCHAR(20) NOT NULL CHECK (channel IN ('sms', 'email', 'phone', 'app_push')),
    direction        VARCHAR(10) NOT NULL CHECK (direction IN ('outbound', 'inbound')),
    comm_type        VARCHAR(50) NOT NULL,  -- 'eta_notification', 'appointment_confirmation', 'reschedule_request', 'completion_notice', 'survey'
    subject          VARCHAR(200),
    body             TEXT,
    status           VARCHAR(20) DEFAULT 'sent' CHECK (status IN ('pending', 'sent', 'delivered', 'failed', 'read')),
    sent_at          TIMESTAMPTZ DEFAULT now(),
    delivered_at     TIMESTAMPTZ,
    read_at          TIMESTAMPTZ,
    metadata         JSONB DEFAULT '{}'  -- template_id, carrier_response, etc.
);

CREATE INDEX IF NOT EXISTS idx_cc_wo ON field_service.customer_communications (work_order_id);
CREATE INDEX IF NOT EXISTS idx_cc_customer ON field_service.customer_communications (customer_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_cc_type ON field_service.customer_communications (comm_type, sent_at DESC);

COMMENT ON TABLE field_service.customer_communications IS
    'Customer notification history. Tracks SMS ETA alerts, appointment confirmations, '
    'reschedule requests, completion notices, and satisfaction surveys. '
    'Required for TCPA compliance and customer experience tracking.';

-- Backfill: Generate sample communications for recent completed WOs
DO $$
DECLARE
    inserted INT := 0;
BEGIN
    IF (SELECT COUNT(*) FROM field_service.customer_communications) > 0 THEN
        RAISE NOTICE 'Customer communications already populated — skipping backfill.';
        RETURN;
    END IF;

    -- Generate ETA notifications for recently assigned WOs
    INSERT INTO field_service.customer_communications (
        work_order_id, customer_id, channel, direction, comm_type,
        subject, body, status, sent_at, delivered_at
    )
    SELECT
        wo.work_order_id,
        wo.customer_id,
        CASE (wo.work_order_id % 3)
            WHEN 0 THEN 'sms' WHEN 1 THEN 'email' ELSE 'app_push'
        END,
        'outbound',
        'eta_notification',
        'Technician ETA Update',
        'Your technician is on the way and will arrive in approximately 30-45 minutes. WO#' || wo.work_order_number,
        'delivered',
        wo.updated_at - INTERVAL '1 hour',
        wo.updated_at - INTERVAL '55 minutes'
    FROM field_service.work_orders wo
    WHERE wo.status IN ('assigned', 'en_route', 'in_progress', 'completed')
      AND wo.assigned_technician_id IS NOT NULL
    ORDER BY wo.updated_at DESC
    LIMIT 10000;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    RAISE NOTICE 'Created % customer communications', inserted;

    -- Add completion surveys for completed WOs
    INSERT INTO field_service.customer_communications (
        work_order_id, customer_id, channel, direction, comm_type,
        subject, body, status, sent_at
    )
    SELECT
        wo.work_order_id,
        wo.customer_id,
        'email',
        'outbound',
        'survey',
        'How was your service experience?',
        'Please rate your recent service visit. Your feedback helps us improve.',
        CASE WHEN RANDOM() > 0.3 THEN 'read' ELSE 'delivered' END,
        wo.resolved_at + INTERVAL '2 hours'
    FROM field_service.work_orders wo
    WHERE wo.status = 'completed'
      AND wo.resolved_at IS NOT NULL
    ORDER BY wo.resolved_at DESC
    LIMIT 5000;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 2. ALARM CORRELATION (Network Incidents)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS field_service.network_incidents (
    incident_id      BIGSERIAL PRIMARY KEY,
    incident_number  VARCHAR(20) UNIQUE NOT NULL,
    severity         VARCHAR(20) NOT NULL CHECK (severity IN ('critical', 'major', 'minor', 'warning')),
    classification   VARCHAR(50) NOT NULL,  -- 'fiber_cut', 'power_outage', 'equipment_failure', 'capacity_exceeded', 'software_fault'
    status           VARCHAR(20) DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'mitigating', 'resolved', 'closed')),
    region           VARCHAR(100),
    root_cause       TEXT,
    affected_nodes   INTEGER DEFAULT 0,
    affected_customers INTEGER DEFAULT 0,
    raw_alarm_count  INTEGER DEFAULT 0,    -- number of correlated raw alarms
    first_alarm_at   TIMESTAMPTZ,
    detected_at      TIMESTAMPTZ DEFAULT now(),
    resolved_at      TIMESTAMPTZ,
    mttr_minutes     INTEGER,              -- mean time to repair
    created_by       VARCHAR(100) DEFAULT 'correlation_engine',
    notes            TEXT,
    metadata         JSONB DEFAULT '{}'    -- linked_alarms[], affected_services[], etc.
);

CREATE INDEX IF NOT EXISTS idx_ni_status ON field_service.network_incidents (status, severity);
CREATE INDEX IF NOT EXISTS idx_ni_region ON field_service.network_incidents (region, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_ni_detected ON field_service.network_incidents (detected_at DESC);

COMMENT ON TABLE field_service.network_incidents IS
    'Correlated network incidents. Raw alarms (1000s) are grouped into '
    'root cause incidents (10-20) by the alarm correlation engine. '
    'Classification: fiber_cut, power_outage, equipment_failure, capacity_exceeded, software_fault.';

-- Backfill sample incidents
DO $$
DECLARE
    inc_count INT := 0;
BEGIN
    IF (SELECT COUNT(*) FROM field_service.network_incidents) > 0 THEN
        RAISE NOTICE 'Network incidents already populated — skipping backfill.';
        RETURN;
    END IF;

    INSERT INTO field_service.network_incidents (
        incident_number, severity, classification, status, region,
        root_cause, affected_nodes, affected_customers, raw_alarm_count,
        first_alarm_at, detected_at, resolved_at, mttr_minutes
    )
    SELECT
        'INC-' || LPAD(g::text, 6, '0'),
        CASE (g % 4)
            WHEN 0 THEN 'critical' WHEN 1 THEN 'major'
            WHEN 2 THEN 'minor' ELSE 'warning'
        END,
        CASE (g % 5)
            WHEN 0 THEN 'fiber_cut' WHEN 1 THEN 'power_outage'
            WHEN 2 THEN 'equipment_failure' WHEN 3 THEN 'capacity_exceeded'
            ELSE 'software_fault'
        END,
        CASE WHEN g > 180 THEN 'open'
             WHEN g > 170 THEN 'investigating'
             ELSE 'resolved'
        END,
        (ARRAY['Pacific Northwest','Southwest','South Central','Southeast','Midwest','Northeast'])[1 + (g % 6)],
        CASE (g % 5)
            WHEN 0 THEN 'Fiber cut at splice point — backhoe damage on 3rd-party dig'
            WHEN 1 THEN 'Power failure at cell site — UPS battery exhausted after 4-hour grid outage'
            WHEN 2 THEN 'OLT line card failure — hardware EOL, replacement needed'
            WHEN 3 THEN 'Bandwidth saturation during peak hours — capacity upgrade required'
            ELSE 'Software bug in router firmware v12.3.1 — rollback recommended'
        END,
        2 + (g % 8),
        50 + (g * 37) % 500,
        5 + (g * 13) % 50,
        now() - (g || ' hours')::interval - INTERVAL '10 minutes',
        now() - (g || ' hours')::interval,
        CASE WHEN g <= 170 THEN now() - (g || ' hours')::interval + ((30 + g % 180) || ' minutes')::interval ELSE NULL END,
        CASE WHEN g <= 170 THEN 30 + g % 180 ELSE NULL END
    FROM generate_series(1, 200) g;

    GET DIAGNOSTICS inc_count = ROW_COUNT;
    RAISE NOTICE 'Created % network incidents', inc_count;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- 3. TRUCK ROLL TRACKING (for ML prediction)
-- ═══════════════════════════════════════════════════════════════════════════

-- Add resolution_method to work_orders for truck roll avoidance ML
ALTER TABLE field_service.work_orders
    ADD COLUMN IF NOT EXISTS resolution_method VARCHAR(30)
    CHECK (resolution_method IN ('on_site', 'remote', 'customer_self_fix', 'no_fault_found', NULL));

-- Backfill: tag completed WOs with resolution method
DO $$
BEGIN
    UPDATE field_service.work_orders
    SET resolution_method = CASE
        -- Maintenance jobs are more often remote-resolvable
        WHEN category = 'maintenance' AND subcategory IN ('firmware_update', 'line_test') THEN
            CASE WHEN (work_order_id * 17) % 100 < 60 THEN 'remote' ELSE 'on_site' END
        -- Disconnect is usually remote
        WHEN category = 'disconnect' THEN
            CASE WHEN (work_order_id * 23) % 100 < 80 THEN 'remote' ELSE 'on_site' END
        -- Most repairs require on_site
        WHEN category = 'repair' THEN
            CASE WHEN (work_order_id * 31) % 100 < 15 THEN 'remote'
                 WHEN (work_order_id * 31) % 100 < 20 THEN 'customer_self_fix'
                 WHEN (work_order_id * 31) % 100 < 25 THEN 'no_fault_found'
                 ELSE 'on_site' END
        -- Install always on_site
        WHEN category = 'install' THEN 'on_site'
        -- Upgrade is mixed
        WHEN category = 'upgrade' THEN
            CASE WHEN (work_order_id * 41) % 100 < 30 THEN 'remote' ELSE 'on_site' END
        ELSE 'on_site'
    END
    WHERE status = 'completed'
      AND resolution_method IS NULL;
END $$;

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
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.customer_communications TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.customer_communications_comm_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.network_incidents TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.network_incidents_incident_id_seq TO %I', r);
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- DONE
-- ============================================================================

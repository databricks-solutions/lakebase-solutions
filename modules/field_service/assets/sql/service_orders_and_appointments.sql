-- ============================================================================
-- SERVICE ORDERS + APPOINTMENT WINDOWS
-- Real-world telco data model enhancements
-- ============================================================================
--
-- Adds two key entities that every production telco FSM system has:
--
-- 1. SERVICE ORDERS — parent entity for work orders. A customer request
--    (e.g., "install fiber") creates one service order with 1-3 child WOs
--    (drop placement, ONT install, provisioning). Completion requires all
--    child WOs to be done.
--
-- 2. APPOINTMENT WINDOWS — customer-agreed time slots. Dispatch MUST assign
--    techs who can arrive within the window. This is the #1 constraint in
--    real telco dispatch and was previously missing.
--
-- Idempotent: Uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
-- ============================================================================

SET client_min_messages = NOTICE;

-- ── Service Orders ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS field_service.service_orders (
    service_order_id     BIGSERIAL PRIMARY KEY,
    service_order_number VARCHAR(20) UNIQUE NOT NULL,
    customer_id          BIGINT REFERENCES field_service.customers(customer_id),
    order_type           VARCHAR(50) NOT NULL,
    status               VARCHAR(20) DEFAULT 'open'
                         CHECK (status IN ('open', 'in_progress', 'completed', 'cancelled')),
    priority             VARCHAR(20) DEFAULT 'medium',
    requested_date       DATE,
    completion_date      DATE,
    total_work_orders    INT DEFAULT 0,
    completed_work_orders INT DEFAULT 0,
    created_at           TIMESTAMPTZ DEFAULT now(),
    updated_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_so_customer ON field_service.service_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_so_status ON field_service.service_orders (status);
CREATE INDEX IF NOT EXISTS idx_so_created ON field_service.service_orders (created_at DESC);

COMMENT ON TABLE field_service.service_orders IS
    'Parent entity for work orders. Maps to TMF 641 Service Order Management. '
    'A customer request (install, repair, upgrade) creates one SO with 1-3 child WOs.';

-- ── Link work orders to service orders ──────────────────────────────────
ALTER TABLE field_service.work_orders
    ADD COLUMN IF NOT EXISTS service_order_id BIGINT
    REFERENCES field_service.service_orders(service_order_id);

CREATE INDEX IF NOT EXISTS idx_wo_service_order
    ON field_service.work_orders (service_order_id);

-- ── Appointment windows on work orders ──────────────────────────────────
ALTER TABLE field_service.work_orders
    ADD COLUMN IF NOT EXISTS appointment_window_start TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS appointment_window_end TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS estimated_duration_min INT DEFAULT 60;

-- ── Auto-update service order status trigger ────────────────────────────
CREATE OR REPLACE FUNCTION field_service.trg_update_service_order()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.service_order_id IS NOT NULL THEN
        UPDATE field_service.service_orders
        SET completed_work_orders = (
                SELECT COUNT(*) FROM field_service.work_orders
                WHERE service_order_id = NEW.service_order_id
                  AND status = 'completed'
            ),
            total_work_orders = (
                SELECT COUNT(*) FROM field_service.work_orders
                WHERE service_order_id = NEW.service_order_id
            ),
            status = CASE
                WHEN (SELECT COUNT(*) FROM field_service.work_orders
                      WHERE service_order_id = NEW.service_order_id
                        AND status NOT IN ('completed', 'cancelled')) = 0
                THEN 'completed'
                WHEN (SELECT COUNT(*) FROM field_service.work_orders
                      WHERE service_order_id = NEW.service_order_id
                        AND status IN ('assigned', 'en_route', 'in_progress')) > 0
                THEN 'in_progress'
                ELSE 'open'
            END,
            updated_at = now()
        WHERE service_order_id = NEW.service_order_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_wo_update_so ON field_service.work_orders;
CREATE TRIGGER trg_wo_update_so
    AFTER INSERT OR UPDATE OF status ON field_service.work_orders
    FOR EACH ROW
    WHEN (NEW.service_order_id IS NOT NULL)
    EXECUTE FUNCTION field_service.trg_update_service_order();

-- ── Backfill: Generate service orders from existing work orders ─────────
-- Group WOs by customer + category into service orders (1 SO per group)
DO $$
DECLARE
    so_count INT := 0;
BEGIN
    -- Only backfill if service_orders is empty
    IF (SELECT COUNT(*) FROM field_service.service_orders) > 0 THEN
        RAISE NOTICE 'Service orders already populated — skipping backfill.';
        RETURN;
    END IF;

    -- Create service orders from distinct (customer, category) groups
    -- Use only active + recently completed WOs for realistic grouping
    INSERT INTO field_service.service_orders (
        service_order_number, customer_id, order_type, status, priority,
        requested_date, created_at
    )
    SELECT
        'SO-' || LPAD(ROW_NUMBER() OVER (ORDER BY MIN(wo.created_at))::text, 8, '0'),
        wo.customer_id,
        wo.category,
        CASE
            WHEN COUNT(*) FILTER (WHERE wo.status NOT IN ('completed','cancelled')) = 0 THEN 'completed'
            WHEN COUNT(*) FILTER (WHERE wo.status IN ('assigned','en_route','in_progress')) > 0 THEN 'in_progress'
            ELSE 'open'
        END,
        (array_agg(wo.priority ORDER BY CASE wo.priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END))[1],
        MIN(wo.created_at)::date,
        MIN(wo.created_at)
    FROM field_service.work_orders wo
    WHERE wo.status NOT IN ('cancelled')
    GROUP BY wo.customer_id, wo.category
    LIMIT 50000;

    GET DIAGNOSTICS so_count = ROW_COUNT;
    RAISE NOTICE 'Created % service orders', so_count;

    -- Link work orders to their service orders
    UPDATE field_service.work_orders wo
    SET service_order_id = so.service_order_id
    FROM field_service.service_orders so
    WHERE so.customer_id = wo.customer_id
      AND so.order_type = wo.category
      AND wo.service_order_id IS NULL;

    RAISE NOTICE 'Linked work orders to service orders';

    -- Update counts
    UPDATE field_service.service_orders so
    SET total_work_orders = (
            SELECT COUNT(*) FROM field_service.work_orders WHERE service_order_id = so.service_order_id
        ),
        completed_work_orders = (
            SELECT COUNT(*) FROM field_service.work_orders
            WHERE service_order_id = so.service_order_id AND status = 'completed'
        );
END $$;

-- ── Backfill: Appointment windows on existing work orders ───────────────
DO $$
BEGIN
    -- Only backfill WOs that don't have windows yet
    UPDATE field_service.work_orders
    SET appointment_window_start = created_at + INTERVAL '4 hours',
        appointment_window_end = created_at + INTERVAL '8 hours',
        estimated_duration_min = CASE category
            WHEN 'install' THEN 120
            WHEN 'repair' THEN 90
            WHEN 'maintenance' THEN 60
            WHEN 'upgrade' THEN 90
            ELSE 60
        END
    WHERE appointment_window_start IS NULL
      AND status NOT IN ('completed', 'cancelled');

    RAISE NOTICE 'Backfilled appointment windows on active work orders';
END $$;

-- ── Grants ──────────────────────────────────────────────────────────────
DO $$
DECLARE
    r TEXT;
BEGIN
    FOREACH r IN ARRAY ARRAY['lakebase_app', 'lakebase_app_perms']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON field_service.service_orders TO %I', r);
            EXECUTE format('GRANT USAGE, SELECT ON field_service.service_orders_service_order_id_seq TO %I', r);
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- DONE
-- ============================================================================

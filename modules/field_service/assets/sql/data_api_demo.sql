-- ============================================================================
-- Lakebase Data API demo objects
-- ============================================================================
-- Backs the "Data API" page (app/routes/data_api.py + templates/data_api.html),
-- which demonstrates the Lakebase Data API (PostgREST-compatible REST front end)
-- answering a customer's questions about complex query support.
--
-- Objects created here (all in the `public` schema, which the Data API exposes by
-- default so they are reachable as REST resources / RPCs):
--   * public.data_api_demo                    — sandbox table for bulk write demos
--                                                (POST/PATCH/DELETE never touch
--                                                 field_service production data)
--   * public.sla_compliance_by_region()       — aggregation RPC (GROUP BY region)
--   * public.top_technicians_by_completions() — nested subquery + JOIN + window RPC
--
-- MANUAL / OWNER PREREQUISITES (cannot be scripted — no management API; see CLAUDE.md):
--   1. Enable the Data API on the dba-lakebase-1 project (project → Data API tab).
--   2. In Advanced settings, also expose the `field_service` schema (for the real
--      multi-table join / embedding demos) and set a max-rows cap (e.g. 1000).
--   3. Create a PG role for the app service principal and grant access, e.g. run as
--      the database OWNER:
--        SELECT databricks_create_role('<app_sp_client_id>', 'SERVICE_PRINCIPAL');
--        GRANT USAGE ON SCHEMA public, field_service TO "<app_sp_client_id>";
--        GRANT SELECT ON field_service.work_orders, field_service.customers,
--              field_service.appointments, field_service.technicians,
--              field_service.service_regions TO "<app_sp_client_id>";
--        GRANT SELECT, INSERT, UPDATE, DELETE ON public.data_api_demo TO "<app_sp_client_id>";
--        GRANT USAGE, SELECT ON SEQUENCE public.data_api_demo_id_seq TO "<app_sp_client_id>";
--        GRANT EXECUTE ON FUNCTION public.sla_compliance_by_region(),
--              public.top_technicians_by_completions(int) TO "<app_sp_client_id>";
--      (CMEG app SP client_id: 72f84bc0-b93b-4e74-b817-4fd175016ab4)
-- This file itself is safe to (re-)apply anytime; it does not depend on the role.
-- ============================================================================

-- ── Sandbox table for bulk write demos ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.data_api_demo (
    id          BIGSERIAL PRIMARY KEY,
    label       TEXT NOT NULL,
    value       INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.data_api_demo IS
    'Sandbox table for Lakebase Data API bulk write demos (POST/PATCH/DELETE). '
    'Safe to mutate — never holds production data.';

-- Seed a few rows only if empty (idempotent).
INSERT INTO public.data_api_demo (label, value)
SELECT 'seed-' || g, g * 10
FROM generate_series(1, 5) AS g
WHERE NOT EXISTS (SELECT 1 FROM public.data_api_demo);

-- ── Materialized summaries for the aggregation RPCs ─────────────────────────
-- The Data API imposes a ~8s statement timeout. A cold, minimum-CU endpoint
-- (e.g. after an idle weekend) cannot aggregate ~5M rows within that budget, so
-- the RPCs returned `57014 statement timeout`. Fix: precompute the aggregates
-- into small summary tables (refreshed off the demo path by
-- refresh_data_api_summaries), and have the RPCs read those tiny tables — instant
-- even on a stone-cold endpoint. See docs/LAKEBASE_DATA_API.md §Monday cold-start.
CREATE TABLE IF NOT EXISTS public.data_api_sla_by_region (
    region_name       TEXT PRIMARY KEY,
    completed_orders  BIGINT,
    sla_met_orders    BIGINT,
    compliance_pct    NUMERIC,
    refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.data_api_tech_rankings (
    technician_id  BIGINT,
    tech_name      TEXT,
    region_name    TEXT,
    completions    BIGINT,
    region_rank    BIGINT,
    refreshed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_data_api_tech_rankings_rank
    ON public.data_api_tech_rankings (region_rank);

-- Recompute both summaries from field_service. Runs OFF the demo path (called by
-- the owner / a scheduled job over a direct PG connection — no Data API gateway
-- timeout), so the heavy aggregation can take as long as it needs.
CREATE OR REPLACE FUNCTION public.refresh_data_api_summaries()
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE ts TIMESTAMPTZ := now();
BEGIN
    TRUNCATE public.data_api_sla_by_region;
    INSERT INTO public.data_api_sla_by_region
        (region_name, completed_orders, sla_met_orders, compliance_pct, refreshed_at)
    WITH by_region AS (
        SELECT region_id,
               COUNT(*)                        AS completed_orders,
               COUNT(*) FILTER (WHERE sla_met)  AS sla_met_orders
        FROM field_service.work_orders
        WHERE status = 'completed'
        GROUP BY region_id
    )
    SELECT sr.region_name::TEXT, br.completed_orders, br.sla_met_orders,
           ROUND(100.0 * br.sla_met_orders / NULLIF(br.completed_orders, 0), 1), ts
    FROM by_region br
    JOIN field_service.service_regions sr ON sr.region_id = br.region_id;

    TRUNCATE public.data_api_tech_rankings;
    INSERT INTO public.data_api_tech_rankings
        (technician_id, tech_name, region_name, completions, region_rank, refreshed_at)
    WITH tech_completions AS (
        SELECT assigned_technician_id AS technician_id, COUNT(*) AS completions
        FROM field_service.work_orders
        WHERE status = 'completed' AND assigned_technician_id IS NOT NULL
        GROUP BY assigned_technician_id
    )
    SELECT t.technician_id,
           (t.first_name || ' ' || t.last_name)::TEXT,
           sr.region_name::TEXT,
           COALESCE(tc.completions, 0),
           ROW_NUMBER() OVER (PARTITION BY t.region_id
                              ORDER BY COALESCE(tc.completions, 0) DESC),
           ts
    FROM field_service.technicians t
    JOIN field_service.service_regions sr ON sr.region_id = t.region_id
    LEFT JOIN tech_completions tc ON tc.technician_id = t.technician_id;

    RETURN ts;
END;
$$;

COMMENT ON FUNCTION public.refresh_data_api_summaries() IS
    'Recompute the Data API summary tables (SLA-by-region, tech rankings) off the demo path.';

-- ── Aggregation RPC: SLA compliance by region ───────────────────────────────
-- Reads the precomputed summary (recommended Data API pattern for analytics — the
-- PostgREST `select` grammar does not aggregate; a SETOF function/view does).
CREATE OR REPLACE FUNCTION public.sla_compliance_by_region()
RETURNS TABLE (
    region_name       TEXT,
    completed_orders  BIGINT,
    sla_met_orders    BIGINT,
    compliance_pct    NUMERIC
)
LANGUAGE sql
STABLE
AS $$
    SELECT region_name, completed_orders, sla_met_orders, compliance_pct
    FROM public.data_api_sla_by_region
    ORDER BY compliance_pct DESC NULLS LAST;
$$;

COMMENT ON FUNCTION public.sla_compliance_by_region() IS
    'Data API aggregation demo: SLA compliance % by region (reads precomputed summary).';

-- ── Nested subquery + JOIN + window RPC: top technicians per region ──────────
-- Reads the precomputed ranking summary (the CTE + JOIN + window ran in the
-- refresh); the RPC just filters top-N per region from ~2,500 rows — instant.
CREATE OR REPLACE FUNCTION public.top_technicians_by_completions(n INTEGER DEFAULT 5)
RETURNS TABLE (
    technician_id  BIGINT,
    tech_name      TEXT,
    region_name    TEXT,
    completions    BIGINT,
    region_rank    BIGINT
)
LANGUAGE sql
STABLE
AS $$
    SELECT technician_id, tech_name, region_name, completions, region_rank
    FROM public.data_api_tech_rankings
    WHERE region_rank <= GREATEST(n, 1)
    ORDER BY region_name, region_rank;
$$;

COMMENT ON FUNCTION public.top_technicians_by_completions(INTEGER) IS
    'Data API nested-query demo: top-N technicians per region (reads precomputed ranking summary).';

-- Row-count estimate for the large-result card. An exact count(*) over 5M rows is
-- a full scan that exceeds the ~8s Data API timeout when cold; the planner's
-- reltuples statistic (maintained by autovacuum/ANALYZE) is instant at any scale —
-- the correct way to size a huge table. Exposed as an RPC the card calls.
CREATE OR REPLACE FUNCTION public.work_orders_estimate()
RETURNS BIGINT
LANGUAGE sql
STABLE
AS $$
    SELECT reltuples::BIGINT FROM pg_class WHERE oid = 'field_service.work_orders'::regclass;
$$;

COMMENT ON FUNCTION public.work_orders_estimate() IS
    'Data API large-result demo: planner row estimate for work_orders (instant, no scan).';

-- Populate the summaries now (safe at apply time — direct PG connection, no Data
-- API gateway timeout). The scheduled refresh job keeps them current thereafter.
SELECT public.refresh_data_api_summaries();

-- ── Index for the filter/paginate card ──────────────────────────────────────
-- The card filters priority='critical', orders by created_at DESC, and requests
-- Prefer: count=exact. Without this index the exact count of ~522K critical rows
-- over 5M seq-scans and exceeds the ~8s Data API timeout when cold. This composite
-- index serves both the ordered page and the exact count as index range scans.
CREATE INDEX IF NOT EXISTS idx_wo_priority_created
    ON field_service.work_orders (priority, created_at DESC);

-- Refresh planner statistics so it reliably chooses the new index for the Filter
-- card's count/page instead of a sequential scan.
ANALYZE field_service.work_orders;

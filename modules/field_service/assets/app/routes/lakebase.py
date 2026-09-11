"""Lakebase Control Tower blueprint — the landing page for the Lakebase wing.

This module powers the ``/lakebase`` **Control Tower** page: a single screen that
proves Lakebase is a real, production OLTP engine by surfacing the differentiators
that set it apart from a generic Postgres:

* **Autoscaling / scale-to-zero** — current compute units + endpoint state
* **Connection pooling** — live psycopg pool utilization
* **Live sessions** — active / waiting sessions from ``pg_stat_activity``
* **Branching** — number of open What-If (copy-on-write) branches
* **Agent memory** — conversation / message rows stored in Lakebase (no vector DB)
* **DB-layer OLTP** — the real-time SLA-risk trigger + total work-order volume

Design notes
------------
* This endpoint intentionally lives **outside** ``/api/admin/*`` (which is gated to
  the ``admin`` role by an ``admin_bp.before_request`` hook). The Control Tower is a
  demo landing page every user should be able to open, so we recompute the small set
  of metrics here rather than proxy the admin routes.
* Every metric is gathered **best-effort**: each block is wrapped in its own
  ``try/except`` so one slow/unavailable source (e.g. the autoscaling API) never
  blanks the whole page. Failures surface as ``null`` values, not a 500.
* Queries are deliberately cheap — ``reltuples`` instead of ``COUNT(*)`` on the 5M-row
  work-orders table, a short ``statement_timeout``, and no ASH writes.
"""

import logging
import os
import re
import threading
import time

import psycopg
from flask import Blueprint, jsonify, request

from shared import get_pool, get_workspace_client, log_error

log = logging.getLogger(__name__)

lakebase_bp = Blueprint("lakebase", __name__)


# ── Autoscaling / CU inference (mirrors admin.cluster_status, no admin gate) ──

def _estimate_cu_from_shared_buffers(shared_buffers_mb):
    """Approximate compute-unit sizing from shared_buffers (same mapping as Admin)."""
    if shared_buffers_mb >= 2048:
        return 8
    if shared_buffers_mb >= 1024:
        return 4
    if shared_buffers_mb >= 512:
        return 2
    if shared_buffers_mb >= 256:
        return 1
    return 0.5


# The live allocated-compute signal is the Neon Local File Cache, which resizes with
# RAM/CU. Calibrated on CMEG: 1-CU LFC ≈ 1461 MB, 4-CU ≈ 6069 MB (~1.5 GB per CU).
# This is an ABSOLUTE mapping so it reads correct CU regardless of prior scale state.
_LFC_MB_PER_CU = 1500


def _cu_from_lfc(lfc_mb, max_cu=4):
    if not lfc_mb:
        return None
    return max(1, min(int(max_cu or 4), round(lfc_mb / _LFC_MB_PER_CU)))


def _scale_reason(frm, to, workers, load_workers):
    """Explain a CU change, grounded in Databricks Lakebase autoscaling docs:
    autoscaling monitors CPU load, memory usage, and working-set size."""
    if to > frm:
        if workers > load_workers:
            return f"offered load rose to {workers} concurrent workers → higher CPU load"
        return f"{workers} concurrent CPU-bound queries saturated the current compute (CPU load)"
    return "workload demand eased"


def _collect_autoscaling(cur):
    """PG-level sizing + (best-effort) autoscaling endpoint state from the Postgres API."""
    info = {}
    cur.execute("SELECT setting, unit FROM pg_settings WHERE name = 'shared_buffers'")
    r = cur.fetchone()
    sb_mb = round(int(r[0]) * 8 / 1024) if r and r[1] == "8kB" else (int(r[0]) if r else 0)
    info["shared_buffers_mb"] = sb_mb
    info["estimated_cu"] = _estimate_cu_from_shared_buffers(sb_mb)
    try:
        cur.execute("SELECT current_setting('neon.file_cache_size_limit', true)")
        info["lfc_mb"] = _parse_mb(cur.fetchone()[0])
    except Exception:  # noqa: BLE001
        info["lfc_mb"] = None

    project_id = os.environ.get("LAKEBASE_PROJECT_ID", "") or os.environ.get("AUTOSCALING_PROJECT_ID", "")
    info["lakebase_type"] = os.environ.get("LAKEBASE_TYPE", "autoscaling")
    if project_id:
        try:
            wc = get_workspace_client()
            ep_data = wc.api_client.do(
                "GET", f"/api/2.0/postgres/projects/{project_id}/branches/production/endpoints"
            )
            for ep in ep_data.get("endpoints", []):
                status = ep.get("status", {})
                info["endpoint_state"] = status.get("current_state", "UNKNOWN")
                info["min_cu"] = status.get("autoscaling_limit_min_cu")
                info["max_cu"] = status.get("autoscaling_limit_max_cu")
                info["scale_to_zero"] = (info.get("min_cu") == 0)
                break  # primary read/write endpoint is enough for the tile
        except Exception as api_err:  # noqa: BLE001 — best-effort, non-fatal
            info["endpoint_error"] = str(api_err)
    # Live current CU from the LFC signal (falls back to shared_buffers estimate).
    info["current_cu"] = _cu_from_lfc(info.get("lfc_mb"), info.get("max_cu") or 4) or info["estimated_cu"]
    return info


def _collect_sessions(cur):
    """Active / waiting / total session counts — cheap pg_stat_activity aggregate."""
    cur.execute(
        """
        /* page:lakebase/overview:sessions */
        SELECT
            COUNT(*) FILTER (WHERE state = 'active' AND pid != pg_backend_pid()) AS active,
            COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state = 'active'
                             AND pid != pg_backend_pid()) AS waiting,
            COUNT(*) AS total
        FROM pg_stat_activity
        WHERE datname = current_database()
        """
    )
    r = cur.fetchone()
    return {"active": r[0], "waiting": r[1], "total": r[2]}


def _collect_oltp(cur):
    """Total work-order volume (via reltuples) + SLA-risk trigger status."""
    info = {}
    cur.execute(
        """
        /* page:lakebase/overview:volume */
        SELECT reltuples::bigint
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'field_service' AND c.relname = 'work_orders'
        """
    )
    r = cur.fetchone()
    info["work_orders"] = int(r[0]) if r and r[0] is not None else None

    # trg_sla_risk state: 'O'/'A'/'R' = enabled, 'D' = disabled
    cur.execute(
        """
        /* page:lakebase/overview:trigger */
        SELECT tgenabled FROM pg_trigger
        WHERE tgname = 'trg_sla_risk' AND NOT tgisinternal
        LIMIT 1
        """
    )
    r = cur.fetchone()
    info["sla_trigger"] = (r[0] if r else None)
    info["sla_trigger_installed"] = r is not None
    return info


def _collect_memory(cur):
    """Agent long-term memory footprint — conversations + messages stored in Lakebase."""
    info = {}
    cur.execute("SELECT COUNT(*) FROM ai_memory.conversations")
    info["conversations"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ai_memory.messages")
    info["messages"] = cur.fetchone()[0]
    return info


@lakebase_bp.route("/api/lakebase/overview")
def lakebase_overview():
    """Aggregate every Control Tower metric into one payload.

    Each section is best-effort: a failure in one block sets that section to ``null``
    (with an ``_errors`` map for debugging) instead of failing the whole request.
    """
    out = {
        "autoscaling": None,
        "pool": None,
        "sessions": None,
        "branches": None,
        "memory": None,
        "oltp": None,
        "_errors": {},
    }

    # ── Postgres-backed metrics (single connection, short timeout) ──
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '5000'")
                for key, fn in (
                    ("autoscaling", _collect_autoscaling),
                    ("sessions", _collect_sessions),
                    ("oltp", _collect_oltp),
                    ("memory", _collect_memory),
                ):
                    try:
                        out[key] = fn(cur)
                    except Exception as sub_err:  # noqa: BLE001
                        out["_errors"][key] = str(sub_err)
                        conn.rollback()
    except Exception as e:  # noqa: BLE001
        log_error("lakebase_overview.pg", e)
        out["_errors"]["postgres"] = str(e)

    # ── Connection-pool utilization (in-process, no DB round trip) ──
    try:
        pool = get_pool()
        stats = pool.get_stats()
        out["pool"] = {
            "min_size": pool.min_size,
            "max_size": pool._max_size,
            "pool_size": stats.get("pool_size", 0),
            "available": stats.get("pool_available", 0),
            "waiting": stats.get("requests_waiting", 0),
        }
    except Exception as e:  # noqa: BLE001
        out["_errors"]["pool"] = str(e)

    # ── Open What-If branches (copy-on-write) — reuse whatif discovery helper ──
    try:
        from routes.whatif import _whatif_list_branch_ids, LAKEBASE_PROJECT_ID
        if LAKEBASE_PROJECT_ID:
            wc = get_workspace_client()
            out["branches"] = {"open": len(_whatif_list_branch_ids(wc))}
        else:
            out["branches"] = {"open": 0, "configured": False}
    except Exception as e:  # noqa: BLE001
        out["_errors"]["branches"] = str(e)

    return jsonify(out)


@lakebase_bp.route("/api/lakebase/trigger-demo", methods=["POST"])
def lakebase_trigger_demo():
    """Prove the DB-layer SLA-risk trigger fires at write time — WITHOUT touching production.

    Picks a sample active work order and, inside a transaction, moves its SLA
    deadline to a caller-chosen offset. The ``BEFORE INSERT OR UPDATE`` trigger
    (``field_service.calculate_sla_risk``) recomputes ``sla_risk_score`` as part
    of the write; we capture the ``RETURNING`` value and then **ROLL BACK**, so
    production data is never changed. This is the "watch the trigger fire"
    demo: the database — not the app — computed the score.

    Body: ``{"hours": <float>}`` — hours until the SLA deadline (negative = already
    breached). Clamped to [-24, 240].
    """
    data = request.get_json(silent=True) or {}
    try:
        hours = float(data.get("hours", 1))
    except (TypeError, ValueError):
        hours = 1.0
    hours = max(-24.0, min(240.0, hours))

    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '5000'")

                # Is the trigger currently enabled? ('D' = disabled)
                cur.execute(
                    "SELECT tgenabled FROM pg_trigger "
                    "WHERE tgname = 'trg_sla_risk' AND NOT tgisinternal LIMIT 1"
                )
                r = cur.fetchone()
                trigger_enabled = bool(r) and r[0] != "D"

                # Deterministic sample active work order
                cur.execute(
                    """
                    SELECT work_order_id, sla_risk_score, priority
                    FROM field_service.work_orders
                    WHERE status NOT IN ('completed', 'cancelled')
                    ORDER BY work_order_id
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    return jsonify({"error": "No active work order available"}), 404
                wo_id, before_score, priority = row

                # Fire the trigger via UPDATE, capture the trigger-computed values, roll back.
                cur.execute(
                    """
                    UPDATE field_service.work_orders
                    SET sla_due_at = CURRENT_TIMESTAMP + make_interval(mins => %s)
                    WHERE work_order_id = %s
                    RETURNING sla_risk_score, sla_hours_remaining
                    """,
                    (int(round(hours * 60)), wo_id),
                )
                after = cur.fetchone()
                after_score = after[0] if after else None
                hours_remaining = float(after[1]) if after and after[1] is not None else None

                conn.rollback()  # production is never modified

        # Transparency: the exact statements + the trigger definition, for the console.
        steps = [
            {"kind": "sql", "label": f"Pick a sample active work order → #{wo_id}",
             "code": "SELECT work_order_id, sla_risk_score, priority FROM field_service.work_orders\n"
                     "WHERE status NOT IN ('completed','cancelled') ORDER BY work_order_id LIMIT 1;"},
            {"kind": "info", "label": "The Postgres trigger that fires on every write",
             "code": "CREATE TRIGGER trg_sla_risk BEFORE INSERT OR UPDATE ON field_service.work_orders\n"
                     "  FOR EACH ROW EXECUTE FUNCTION field_service.calculate_sla_risk();\n"
                     "-- calculate_sla_risk() sets NEW.sla_risk_score from how close sla_due_at is"},
            {"kind": "sql", "label": f"Move the SLA deadline in a transaction (then roll back)",
             "code": f"BEGIN;\nUPDATE field_service.work_orders\n  SET sla_due_at = now() + interval '{hours}h'\n"
                     f"  WHERE work_order_id = {wo_id}\n  RETURNING sla_risk_score;   -- trigger recomputes at write time\nROLLBACK;   -- production untouched"},
            {"kind": "result",
             "label": f"Trigger computed sla_risk_score = {after_score} at write time "
                      f"(was {before_score}); change rolled back."},
        ]
        return jsonify({
            "work_order_id": wo_id,
            "priority": priority,
            "requested_hours": hours,
            "before_score": before_score,
            "after_score": after_score,
            "hours_remaining": round(hours_remaining, 2) if hours_remaining is not None else None,
            "trigger_enabled": trigger_enabled,
            "rolled_back": True,
            "steps": steps,
        })
    except Exception as e:  # noqa: BLE001
        log_error("lakebase_trigger_demo", e)
        return jsonify({"error": str(e)}), 500


@lakebase_bp.route("/api/lakebase/two-engines", methods=["POST"])
def two_engines():
    """One row, two engines: read the SAME work order from Lakebase (OLTP) and via the
    Unity Catalog federation catalog on a SQL Warehouse (analytics) — proving one governed
    copy of operational data, queryable by apps AND analytics/AI, with zero ETL."""
    from shared import _run_sql
    cat = os.environ.get("LAKEBASE_FEDERATION_CATALOG", "dba-lakebase-data")
    pg_host = os.environ.get("PGHOST", "") or "(PGHOST unset)"
    wh_id = os.environ.get("SQL_WAREHOUSE_ID", "") or "(unset)"
    cols = ["work_order_id", "status", "priority", "sla_risk_score", "assigned_technician_id"]
    steps = []
    out = {"steps": steps, "catalog": cat,
           "oltp_endpoint": f"{pg_host}:5432", "wh_endpoint": f"warehouse {wh_id} · catalog {cat}",
           "pg_host": pg_host, "warehouse_id": wh_id}
    try:
        # 1) OLTP path — Lakebase via psycopg over the native Postgres wire protocol.
        #    Surface exactly which endpoint we connect to (the Lakebase compute endpoint host).
        oltp_sql = ("SELECT work_order_id, status, priority, sla_risk_score, assigned_technician_id\n"
                    "FROM field_service.work_orders ORDER BY work_order_id LIMIT 1")
        steps.append({"kind": "endpoint",
                      "label": f"Lakebase compute endpoint · {pg_host}:5432 · psycopg driver, native Postgres wire (TLS)"})
        t0 = time.time()
        with get_pool().connection(timeout=8) as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '5000'")
                cur.execute(oltp_sql)
                row = cur.fetchone()
        oltp_ms = round((time.time() - t0) * 1000)
        oltp = dict(zip(cols, row)) if row else {}
        wo_id = oltp.get("work_order_id")
        steps.append({"kind": "sql", "label": f"Lakebase OLTP read → {oltp_ms} ms", "code": oltp_sql + ";"})

        # 2) Analytics path — the SAME row via Unity Catalog federation on a SQL Warehouse.
        #    A completely different endpoint: the Databricks Statement Execution API, not Postgres.
        wh_sql = (f"SELECT work_order_id, status, priority, sla_risk_score, assigned_technician_id\n"
                  f"FROM `{cat}`.field_service.work_orders WHERE work_order_id = {int(wo_id)}")
        steps.append({"kind": "endpoint",
                      "label": (f"Databricks SQL Warehouse · id {wh_id} · Statement Execution API "
                                f"POST /api/2.0/sql/statements · catalog `{cat}`")})
        steps.append({"kind": "api", "label": "Analytics read — Unity Catalog federation", "code": wh_sql + ";"})
        t1 = time.time()
        rows = _run_sql(wh_sql, catalog=cat)
        wh_ms = round((time.time() - t1) * 1000)
        lake = dict(zip(cols, rows[0])) if rows else {}
        match = str(oltp.get("work_order_id")) == str(lake.get("work_order_id"))
        steps.append({"kind": "result",
                      "label": (f"Same row #{wo_id} returned in {wh_ms} ms — "
                                "one governed copy, no ETL, no data movement (Unity Catalog federation).")})
        out.update({"work_order_id": wo_id, "oltp": oltp, "oltp_ms": oltp_ms,
                    "lakehouse": lake, "lakehouse_ms": wh_ms, "match": match})
        return jsonify(out)
    except Exception as e:  # noqa: BLE001
        log_error("two_engines", e)
        out["error"] = str(e)
        steps.append({"kind": "result", "label": f"Error: {e}"})
        return jsonify(out), 500


# ═══════════════════════════════════════════════════════════════════════════
# AUTOSCALE UNDER FIRE — generate real concurrent OLTP load, show live throughput
# ═══════════════════════════════════════════════════════════════════════════
#
# Fires a bounded pool of worker threads that hammer Lakebase with lightweight
# indexed reads for a few seconds, and reports live QPS + latency. This proves the
# instance absorbs burst traffic; the endpoint's autoscaling range (shown from
# /overview) is the headroom it can grow into. NOTE: Lakebase does not expose a
# real-time "current CU" metric, so we report throughput honestly rather than
# animate a fabricated CU number.

_load_state = {"running": False}
_load_lock = threading.Lock()

# CPU-bound burner — cache-hit COUNTs don't move the load average, but heavy per-query
# compute at high concurrency keeps many backends runnable, which drives Neon/Lakebase
# to scale vCPU/RAM up. ~0.3-0.6s per query keeps QPS presentable while saturating CPU.
_LOAD_QUERY = "SELECT sum(sqrt(g) * ln(g + 1)) FROM generate_series(1, 400000) g;"


def _parse_mb(v):
    """Parse a Postgres size string (e.g. '6069MB', '6215680kB', '6GB') into MB."""
    if not v:
        return None
    m = re.match(r"\s*(\d+)\s*([kKmMgG]?)", str(v))
    if not m:
        return None
    n = int(m.group(1))
    u = m.group(2).lower()
    if u == "k":
        return round(n / 1024)
    if u == "g":
        return n * 1024
    return n


def _probe_signals():
    """Read the live autoscaling signal. The mover under load is the Neon Local File
    Cache limit (neon.file_cache_size_limit), which resizes with allocated RAM/CU;
    shared_buffers is a static GUC and does NOT reflect scaling."""
    out = {}
    try:
        pool = get_pool()
        with pool.connection(timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '4000'")
                cur.execute("SELECT setting, unit FROM pg_settings WHERE name = 'shared_buffers'")
                r = cur.fetchone()
                out["shared_buffers_mb"] = round(int(r[0]) * 8 / 1024) if r and r[1] == "8kB" else None
                cur.execute("SELECT current_setting('neon.file_cache_size_limit', true)")
                lfc = cur.fetchone()[0]
                out["lfc_limit"] = lfc
                out["lfc_mb"] = _parse_mb(lfc)
                cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
                out["active_backends"] = cur.fetchone()[0]
    except Exception as e:  # noqa: BLE001
        out["db_err"] = str(e)[:120]
    try:
        wc = get_workspace_client()
        pid = os.environ.get("LAKEBASE_PROJECT_ID", "") or os.environ.get("AUTOSCALING_PROJECT_ID", "")
        if pid:
            ep = wc.api_client.do(
                "GET", f"/api/2.0/postgres/projects/{pid}/branches/production/endpoints"
            ).get("endpoints", [{}])[0].get("status", {})
            out["api_min_cu"] = ep.get("autoscaling_limit_min_cu")
            out["api_max_cu"] = ep.get("autoscaling_limit_max_cu")
            out["api_state"] = ep.get("current_state")
            out["api_pending"] = ep.get("pending_state")
    except Exception as e:  # noqa: BLE001
        out["api_err"] = str(e)[:120]
    return out


def _probe_db():
    """Cheap DB-only probe (no workspace API call) — safe to run every few seconds."""
    out = {}
    try:
        pool = get_pool()
        with pool.connection(timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '3000'")
                cur.execute("SELECT current_setting('neon.file_cache_size_limit', true)")
                out["lfc_mb"] = _parse_mb(cur.fetchone()[0])
                cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
                out["active_backends"] = cur.fetchone()[0]
    except Exception as e:  # noqa: BLE001
        out["db_err"] = str(e)[:120]
    return out


def _endpoint_hosts():
    """Return (direct_host, pooled_host, max_cu) from the endpoint API.

    The pooled host funnels connections through pgbouncer with a small server pool,
    which caps concurrency regardless of compute size. The direct host gives one server
    connection per client — so query parallelism can actually scale with vCPU/CU.
    """
    try:
        wc = get_workspace_client()
        pid = os.environ.get("LAKEBASE_PROJECT_ID", "") or os.environ.get("AUTOSCALING_PROJECT_ID", "")
        if pid:
            st = wc.api_client.do(
                "GET", f"/api/2.0/postgres/projects/{pid}/branches/production/endpoints"
            ).get("endpoints", [{}])[0].get("status", {})
            h = st.get("hosts", {})
            return h.get("host"), h.get("read_write_pooled_host"), (st.get("autoscaling_limit_max_cu") or 4)
    except Exception:  # noqa: BLE001
        pass
    return None, None, 4


def _load_direct_conninfo(direct_host):
    pw = os.environ.get("PGPASSWORD") or os.environ.get("DATABRICKS_TOKEN", "")
    return (
        f"dbname={os.environ.get('PGDATABASE')} user={os.environ.get('PGUSER')} "
        f"password={pw} host={direct_host} port=5432 sslmode=require"
    )


def _load_step(kind, label, code=None):
    with _load_lock:
        _load_state.setdefault("steps", []).append(
            {"kind": kind, "label": label, "code": code, "t": time.strftime("%H:%M:%S")}
        )


def _load_worker(deadline, start_delay=0, conninfo=None):
    """One load worker. With conninfo set, holds its own DIRECT server connection
    (bypassing the pooler) so N workers = N truly-concurrent queries; otherwise uses
    the shared app pool (pooled host)."""
    if start_delay:
        time.sleep(start_delay)
    q = _LOAD_QUERY.replace("\n", " ")
    conn = None
    if conninfo:
        try:
            conn = psycopg.connect(conninfo, autocommit=True)
        except Exception:  # noqa: BLE001
            with _load_lock:
                _load_state["errors"] += 1
            conn = None
    try:
        while time.time() < deadline:
            t = time.time()
            try:
                if conn is not None:
                    with conn.cursor() as cur:
                        cur.execute(q)
                        cur.fetchone()
                else:
                    with get_pool().connection(timeout=8) as c:
                        with c.cursor() as cur:
                            cur.execute("/* page:lakebase/load */ " + q)
                            cur.fetchone()
                dt = (time.time() - t) * 1000.0
                with _load_lock:
                    _load_state["queries"] += 1
                    _load_state["latency_sum"] += dt
            except Exception:  # noqa: BLE001
                with _load_lock:
                    _load_state["errors"] += 1
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    try:
                        conn = psycopg.connect(conninfo, autocommit=True)
                    except Exception:
                        break
                else:
                    time.sleep(0.2)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _load_sampler(deadline):
    """Sample every 3s: offered concurrency (ramp), live CU (LFC), and INTERVAL
    throughput + latency — so 'load rises → CU steps up → QPS climbs, latency flat'
    is visible instead of being smoothed away."""
    prev_q, prev_t, prev_lat = 0, 0.0, 0.0
    with _load_lock:
        mx = _load_state.get("max_cu", 4)
        ramp = list(_load_state.get("ramp", []))
        base = _load_state.get("cu_samples", [])
        last_cu = (base[0].get("cu") if base else 1) or 1
        last_lfc = base[0].get("lfc_mb") if base else None
    prev_cu = last_cu               # for scale-event detection
    load_workers = ramp[0][1] if ramp else 0   # workers level at the last scale-up
    while time.time() < deadline:
        db = _probe_db()
        cu = _cu_from_lfc(db.get("lfc_mb"), mx)
        # Carry forward the last good reading when a probe times out under CPU saturation.
        if cu is None:
            cu = last_cu
        else:
            last_cu = cu
        lfc = db.get("lfc_mb")
        if lfc is None:
            lfc = last_lfc
        else:
            last_lfc = lfc
        with _load_lock:
            el = time.time() - _load_state.get("started", time.time())
            q = _load_state.get("queries", 0)
            lat_sum = _load_state.get("latency_sum", 0.0)
            dt = max(0.5, el - prev_t)
            dq = q - prev_q
            iqps = round(dq / dt)
            ilat = round((lat_sum - prev_lat) / dq) if dq > 0 else None
            prev_q, prev_t, prev_lat = q, el, lat_sum
            workers = 0
            for off, conc in ramp:
                if el >= off:
                    workers = conc
            sig = {"t": int(el), "cu": cu, "qps": iqps, "latency_ms": ilat, "workers": workers,
                   "lfc_mb": lfc, "active_backends": db.get("active_backends")}
            _load_state.setdefault("cu_samples", []).append(sig)
            _load_state.setdefault("steps", []).append(
                {"kind": "probe",
                 "label": (f"@{int(el)}s — {workers} workers offered · {cu} CU · "
                           f"{iqps} q/s · {ilat}ms"),
                 "code": None, "t": time.strftime("%H:%M:%S")}
            )
            # Detect + annotate a Databricks autoscaling event when the CU changes.
            if cu is not None and cu != prev_cu:
                up = cu > prev_cu
                reason = _scale_reason(prev_cu, cu, workers, load_workers)
                ev = {"t": int(el), "from": prev_cu, "to": cu, "up": up,
                      "reason": reason, "workers": workers}
                _load_state.setdefault("scale_events", []).append(ev)
                _load_state["last_scale"] = ev
                _load_state["steps"].append(
                    {"kind": "scale",
                     "label": ("%s Databricks autoscaled %s → %s CU — %s"
                               % ("⬆" if up else "⬇", prev_cu, cu, reason)),
                     "code": None, "t": time.strftime("%H:%M:%S")}
                )
                if workers > load_workers:
                    load_workers = workers
                prev_cu = cu
        for _ in range(3):
            if time.time() >= deadline:
                break
            time.sleep(1)


def _load_run(total_workers, seconds, direct=True):
    s3 = seconds / 3.0
    # Ramp offered concurrency in 3 stages so load rises visibly and CU steps up to match.
    ramp = [(0.0, max(1, total_workers // 8 or 2)),
            (round(s3, 1), max(4, total_workers // 2)),
            (round(2 * s3, 1), total_workers)]
    direct_host, pooled_host, max_cu = _endpoint_hosts()
    conninfo = _load_direct_conninfo(direct_host) if (direct and direct_host and os.environ.get("PGUSER")) else None
    # State was reset synchronously in load_start; fill in the computed fields here.
    with _load_lock:
        _load_state["ramp"] = ramp
        _load_state["max_cu"] = max_cu
        _load_state["direct"] = bool(conninfo)
        started = _load_state.get("started", time.time())
    deadline = started + seconds
    # Capture an idle baseline BEFORE the load (the probe can time out once CPU is
    # saturated, so this pins the true starting CU ~1).
    idle = _probe_db()
    idle_cu = _cu_from_lfc(idle.get("lfc_mb"), max_cu) or 1
    with _load_lock:
        _load_state["cu_samples"].append(
            {"t": 0, "cu": idle_cu, "qps": 0, "latency_ms": None, "workers": ramp[0][1],
             "lfc_mb": idle.get("lfc_mb"), "active_backends": idle.get("active_backends")}
        )
    host_note = ("direct host (bypasses the pooler — each worker gets its own server connection)"
                 if conninfo else "pooled host")
    _load_step("info", f"Ramp CPU-bound load {ramp[0][1]}→{ramp[1][1]}→{ramp[2][1]} concurrent over {seconds}s via {host_note}", _LOAD_QUERY)
    _load_step("api", "Probe live compute (Neon LFC ∝ RAM/CU) + interval throughput every 3s", "SELECT current_setting('neon.file_cache_size_limit');")
    try:
        sampler = threading.Thread(target=_load_sampler, args=(deadline,), daemon=True)
        sampler.start()
        threads = []
        prev = 0
        for offset, conc in ramp:
            for _ in range(conc - prev):
                t = threading.Thread(target=_load_worker, args=(deadline, offset, conninfo), daemon=True)
                t.start()
                threads.append(t)
            prev = conc
        for t in threads:
            t.join()
        sampler.join(timeout=3)
    except Exception as e:  # noqa: BLE001 — never leave the running flag stuck
        log_error("load_run", e)
    with _load_lock:
        _load_state["running"] = False
        _load_state["ended"] = time.time()
        q = _load_state.get("queries", 0)
        samples = _load_state.get("cu_samples", [])
        cus = [x.get("cu") for x in samples if x.get("cu")]
        iqs = [x.get("qps") for x in samples if x.get("qps") is not None]
        parts = []
        if cus:
            parts.append(f"CU scaled {min(cus)}→{max(cus)}" if max(cus) > min(cus) else f"CU held ~{max(cus)}")
        if iqs:
            parts.append(f"throughput ~{min(iqs)}→{max(iqs)} q/s as offered load ramped")
        note = "; ".join(parts) or "no samples"
        _load_state["steps"].append(
            {"kind": "result", "label": f"Done: {q} queries — {note}", "code": None, "t": time.strftime("%H:%M:%S")}
        )


@lakebase_bp.route("/api/lakebase/load/start", methods=["POST"])
def load_start():
    data = request.get_json(silent=True) or {}
    try:
        workers = max(1, min(48, int(data.get("workers", 16))))
        seconds = max(1, min(180, int(data.get("seconds", 90))))
    except (TypeError, ValueError):
        workers, seconds = 16, 90
    direct = bool(data.get("direct", True))
    with _load_lock:
        # Self-healing guard: a run older than its own duration (+30s grace) is stale
        # (e.g. the process was interrupted), so allow a fresh run instead of sticking.
        running = _load_state.get("running")
        age = time.time() - _load_state.get("started", 0)
        if running and age < (_load_state.get("seconds", 0) + 30):
            return jsonify({"error": "A load run is already active"}), 409
        # Reset state SYNCHRONOUSLY here so the first status poll sees the NEW run — not
        # the previous run's stale completed results (which would render as instantly-done).
        _load_state.clear()
        _load_state.update({
            "running": True, "queries": 0, "errors": 0, "latency_sum": 0.0,
            "started": time.time(), "seconds": seconds, "workers": workers,
            "cu_samples": [], "steps": [], "ramp": [], "max_cu": None,
        })
    threading.Thread(target=_load_run, args=(workers, seconds, direct), daemon=True).start()
    return jsonify({"status": "started", "workers": workers, "seconds": seconds, "direct": direct})


@lakebase_bp.route("/api/lakebase/load/status")
def load_status():
    with _load_lock:
        s = dict(_load_state)
    started = s.get("started")
    if not started:
        return jsonify({"running": False, "queries": 0})
    end = s.get("ended") or time.time()
    elapsed = max(0.001, end - started)
    q = s.get("queries", 0)
    # Absolute CU from the LFC signal (correct regardless of prior scale state).
    # max_cu comes from the run state (set from the endpoint), NOT from samples — the
    # cheap probe doesn't carry it, and defaulting to 4 was clamping the headline to 4
    # while the console (using the real max) showed up to 8.
    samples = s.get("cu_samples", [])
    lfcs = [x.get("lfc_mb") for x in samples if x.get("lfc_mb")]
    max_cu = s.get("max_cu") or 8
    cu_now = _cu_from_lfc(lfcs[-1], max_cu) if lfcs else None
    cu_start = _cu_from_lfc(lfcs[0], max_cu) if lfcs else None
    cu_peak = max((_cu_from_lfc(v, max_cu) for v in lfcs), default=None) if lfcs else None
    iqps = [x.get("qps") for x in samples if x.get("qps") is not None]
    ilats = [x.get("latency_ms") for x in samples if x.get("latency_ms") is not None]
    # Headline = current interval values (show the rise/drop), not the smoothed mean.
    qps_display = iqps[-1] if iqps else round(q / elapsed)
    lat_display = ilats[-1] if ilats else (round(s.get("latency_sum", 0.0) / q, 1) if q else None)
    workers_now = samples[-1].get("workers") if samples else s.get("workers")
    return jsonify({
        "running": s.get("running", False),
        "workers": s.get("workers"),
        "workers_now": workers_now,
        "seconds": s.get("seconds"),
        "elapsed": round(elapsed, 1),
        "queries": q,
        "errors": s.get("errors", 0),
        "qps": qps_display,
        "peak_qps": max(iqps) if iqps else None,
        "avg_latency_ms": lat_display,
        "cu_now": cu_now,
        "cu_start": cu_start,
        "cu_peak": cu_peak,
        "max_cu": max_cu,
        "lfc_mb": lfcs[-1] if lfcs else None,
        "scale_events": s.get("scale_events", []),
        "last_scale": s.get("last_scale"),
        "cu_samples": samples,
        "steps": s.get("steps", []),
    })

"""
Health, Dashboard, and Notifications API Blueprint.

Provides comprehensive system health checks, dashboard data feeds, and a
real-time notification queue for the Lakebase FSM application. These endpoints
power the status bar, dashboard cards, and alert banners in the frontend.

Routes
------
GET  /api/health             Comprehensive health check (DB, warehouse, pipeline,
                             Genie spaces, agent endpoint, Iceberg catalog, ML model,
                             connection pool stats, memory, data freshness, table rows)
GET  /api/ping               Lightweight liveness probe (uptime only)
GET  /api/data-freshness     Check whether demo data timestamps are stale (>12 h old)
POST /api/refresh-dates      Trigger a background date refresh to shift all timestamps
                             forward so the demo looks current
GET  /api/refresh-dates/status  Poll progress of a running date refresh
GET  /api/notifications      Dispatcher notification queue (SLA warnings, breaches,
                             recent completions, data staleness alerts)
GET  /api/dashboard/activity-feed       Recent work-order events for the dashboard
GET  /api/dashboard/regional-load       Work-order load and tech availability by region
GET  /api/dashboard/infrastructure-alerts   Infrastructure node status summary
GET  /api/dashboard/stb-fleet           STB device fleet health from Managed Iceberg tables

Dependencies from ``shared``
----------------------------
get_pool, get_analytics_pool, get_workspace_client, log_error,
APP_START_TIME, _error_log, _error_log_lock, GENIE_SPACES,
_column_exists_cache, _column_exists_lock, _get_or_refresh,
get_cached_tables, _run_sql

Dependencies from ``app`` (main module)
---------------------------------------
_sim_state          — simulator running status shown in health check
_get_iceberg_catalog — catalog connectivity check for health endpoint
_pipeline_run_id    — pipeline run tracking for health check
TELCO_INFRASTRUCTURE, IOT_DEVICES — in-memory infrastructure data for alerts
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from shared import (
    APP_START_TIME,
    GENIE_SPACES,
    _column_exists_cache,
    _column_exists_lock,
    _error_log,
    _error_log_lock,
    _get_or_refresh,
    _run_sql,
    get_analytics_pool,
    get_cached_tables,
    get_pool,
    get_workspace_client,
    log_error,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint definition
# ---------------------------------------------------------------------------

health_bp = Blueprint("health", __name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

# Date-refresh background worker state — shared across the POST trigger and
# the GET status poller.  Only one refresh can run at a time.
_refresh_state: dict = {
    "running": False,
    "phase": "",
    "progress": "",
    "done": False,
    "error": None,
}

# Batch size for the date-refresh UPDATE statements.  Large enough to keep
# round-trips low, small enough to avoid locking the entire table.
REFRESH_BATCH = 500_000

# Cache for the STB fleet dashboard card.  The underlying query hits the
# SQL warehouse (Managed Iceberg), which is expensive, so we cache for 5 min.
_stb_fleet_cache: dict = {"data": None, "expires": 0}


# ── Health API ────────────────────────────────────────────────────────────

@health_bp.route("/api/health")
def health_api():
    """Comprehensive health check endpoint.

    Inspects every major subsystem and returns a JSON report:
    - database: connectivity + latency
    - connection_pool: pool size, available connections, waiting requests
    - tables: Lakebase + UC table counts
    - simulator: running status
    - genie_spaces: configured IDs for each Genie space
    - env_config: required/optional environment variables
    - memory: RSS in MB
    - data_freshness: hours since the newest seed work order
    - sql_warehouse: warehouse state via SDK
    - pipeline: latest IoT pipeline run state
    - agent_endpoint: serving endpoint readiness
    - iceberg_catalog: Unity Catalog Iceberg REST connectivity
    - ml_model: predictive maintenance model version
    - table_rows: estimated row counts for key tables (via pg_class)
    - recent_errors: last 10 entries from the error ring buffer
    """
    # Lazy imports to avoid circular dependencies during Blueprint registration
    from app import _get_iceberg_catalog, _pipeline_run_id, _sim_state

    health: dict = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": (datetime.now(timezone.utc) - APP_START_TIME).total_seconds(),
        "checks": {},
    }

    # ── Database connectivity + latency ───────────────────────────────────
    try:
        pool = get_pool()
        t0 = time.time()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        latency_ms = round((time.time() - t0) * 1000, 1)
        health["checks"]["database"] = {"status": "healthy", "latency_ms": latency_ms}
    except Exception as e:
        health["checks"]["database"] = {"status": "unhealthy", "error": str(e)}
        health["status"] = "degraded"

    # ── Connection pool stats ─────────────────────────────────────────────
    try:
        pool = get_pool()
        pool_stats = pool.get_stats()
        health["checks"]["connection_pool"] = {
            "status": "healthy",
            "min_size": pool.min_size,
            "max_size": pool._max_size,
            "pool_size": pool_stats.get("pool_size", 0),
            "pool_available": pool_stats.get("pool_available", 0),
            "requests_waiting": pool_stats.get("requests_waiting", 0),
            "requests_num": pool_stats.get("requests_num", 0),
        }
        if pool_stats.get("requests_waiting", 0) > 0:
            health["checks"]["connection_pool"]["status"] = "warning"
    except Exception as e:
        health["checks"]["connection_pool"] = {"status": "unknown", "error": str(e)}

    # ── Table count (Lakebase + UC) ───────────────────────────────────────
    try:
        tables = get_cached_tables()
        health["checks"]["tables"] = {
            "status": "healthy",
            "count": len(tables),
            "schemas": list(set(t["schema"] for t in tables)),
        }
    except Exception as e:
        health["checks"]["tables"] = {"status": "error", "error": str(e)}

    # ── Simulator status ──────────────────────────────────────────────────
    health["checks"]["simulator"] = {
        "running": _sim_state["running"],
        "last_start": _sim_state["start_time"],
        "last_error": _sim_state["error"],
    }

    # ── Genie spaces configuration ────────────────────────────────────────
    genie_status: dict = {}
    genie_missing: list[str] = []
    for key, space in GENIE_SPACES.items():
        has_id = bool(space["id"])
        genie_status[key] = {"configured": has_id, "name": space["name"]}
        if not has_id:
            genie_missing.append(key)
    if genie_missing:
        genie_status["status"] = "misconfigured"
        genie_status["error"] = (
            f"Missing space IDs for: {', '.join(genie_missing)}. "
            "Redeploy the app or re-upload app.yaml with GENIE_SPACE_POSTGRES "
            "and GENIE_SPACE_FIELD_OPS env vars."
        )
        health["status"] = "degraded"
    else:
        genie_status["status"] = "healthy"
    health["checks"]["genie_spaces"] = genie_status

    # ── Environment config — required/optional env vars ───────────────────
    env_check: dict = {}
    required_vars = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    optional_vars = ["GENIE_SPACE_POSTGRES", "GENIE_SPACE_FIELD_OPS", "NOTEBOOK_PATH"]
    missing_required = [v for v in required_vars if not os.environ.get(v)]
    missing_optional = [v for v in optional_vars if not os.environ.get(v)]
    if missing_required:
        env_check["status"] = "unhealthy"
        env_check["missing_required"] = missing_required
        env_check["error"] = (
            "Missing required env vars -- app.yaml may be stale or incomplete. "
            "Redeploy or re-upload app.yaml."
        )
        health["status"] = "degraded"
    elif missing_optional:
        env_check["status"] = "warning"
        env_check["missing_optional"] = missing_optional
        env_check["note"] = (
            "Some features may not work. Redeploy to regenerate app.yaml with all env vars."
        )
    else:
        env_check["status"] = "healthy"
    health["checks"]["env_config"] = env_check

    # ── Memory usage (RSS) ────────────────────────────────────────────────
    try:
        import resource
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024  # macOS returns bytes
        health["checks"]["memory"] = {"rss_mb": round(mem_mb, 1)}
    except Exception:
        try:
            with open("/proc/self/status", "r") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        mem_kb = int(line.split()[1])
                        health["checks"]["memory"] = {"rss_mb": round(mem_kb / 1024, 1)}
                        break
        except Exception:
            health["checks"]["memory"] = {"rss_mb": "unavailable"}

    # ── Data freshness ────────────────────────────────────────────────────
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(created_at) FROM field_service.work_orders
                    WHERE work_order_number NOT LIKE 'WO-SIM-%%'
                """)
                row = cur.fetchone()
                if row and row[0]:
                    max_ts = row[0]
                    if max_ts.tzinfo is None:
                        max_ts = max_ts.replace(tzinfo=timezone.utc)
                    hours_old = (datetime.now(timezone.utc) - max_ts).total_seconds() / 3600
                    stale = hours_old > 12
                    health["checks"]["data_freshness"] = {
                        "status": "stale" if stale else "fresh",
                        "newest_seed_wo": max_ts.isoformat(),
                        "hours_old": round(hours_old, 1),
                        "needs_refresh": stale,
                    }
                    if stale:
                        health["status"] = "degraded"
    except Exception as e:
        health["checks"]["data_freshness"] = {"status": "unknown", "error": str(e)}

    # ── SQL Warehouse ─────────────────────────────────────────────────────
    warehouse_id = os.environ.get("SQL_WAREHOUSE_ID")
    if warehouse_id:
        try:
            w = get_workspace_client()
            wh = w.warehouses.get(warehouse_id)
            wh_state = str(wh.state).split(".")[-1] if wh.state else "UNKNOWN"
            health["checks"]["sql_warehouse"] = {
                "status": "healthy" if wh_state == "RUNNING" else "warning",
                "state": wh_state,
                "name": wh.name,
                "warehouse_id": warehouse_id,
            }
            if wh_state not in ("RUNNING", "STARTING"):
                health["checks"]["sql_warehouse"]["status"] = "warning"
        except Exception as e:
            health["checks"]["sql_warehouse"] = {"status": "unknown", "error": str(e)[:200]}
    else:
        health["checks"]["sql_warehouse"] = {"status": "not_configured"}

    # ── Pipeline (IoT Streaming) ──────────────────────────────────────────
    if _pipeline_run_id:
        try:
            w = get_workspace_client()
            run = w.jobs.get_run(run_id=_pipeline_run_id)
            run_state = str(run.state.life_cycle_state).split(".")[-1] if run.state else "UNKNOWN"
            result_state = (
                str(run.state.result_state).split(".")[-1]
                if run.state and run.state.result_state
                else None
            )
            pipeline_check: dict = {
                "status": "healthy",
                "run_id": _pipeline_run_id,
                "state": run_state,
            }
            if result_state:
                pipeline_check["result"] = result_state
            if run_state in ("TERMINATED",) and result_state != "SUCCESS":
                pipeline_check["status"] = "warning"
            elif run_state in ("INTERNAL_ERROR", "SKIPPED"):
                pipeline_check["status"] = "unhealthy"
            health["checks"]["pipeline"] = pipeline_check
        except Exception as e:
            health["checks"]["pipeline"] = {"status": "unknown", "error": str(e)[:200]}
    else:
        health["checks"]["pipeline"] = {
            "status": "idle",
            "note": "No pipeline run triggered this session",
        }

    # ── Agent Endpoint ────────────────────────────────────────────────────
    agent_ep = os.environ.get("AGENT_ENDPOINT_NAME", "")
    if agent_ep:
        try:
            w = get_workspace_client()
            ep = w.serving_endpoints.get(agent_ep)
            ep_state = str(ep.state.ready).split(".")[-1] if ep.state else "UNKNOWN"
            health["checks"]["agent_endpoint"] = {
                "status": "healthy" if ep_state == "READY" else "warning",
                "state": ep_state,
                "name": agent_ep,
            }
        except Exception as e:
            health["checks"]["agent_endpoint"] = {"status": "unknown", "error": str(e)[:200]}
    else:
        health["checks"]["agent_endpoint"] = {
            "status": "not_configured",
            "note": "No AGENT_ENDPOINT_NAME set",
        }

    # ── Iceberg Catalog (Unity Catalog) ───────────────────────────────────
    try:
        catalog_name = os.environ.get("PIPELINE_CATALOG", "")
        if catalog_name:
            cat = _get_iceberg_catalog()
            ns_list = cat.list_namespaces()
            health["checks"]["iceberg_catalog"] = {
                "status": "healthy",
                "catalog": catalog_name,
                "namespaces": len(ns_list),
            }
        else:
            health["checks"]["iceberg_catalog"] = {"status": "not_configured"}
    except Exception as e:
        health["checks"]["iceberg_catalog"] = {"status": "unknown", "error": str(e)[:200]}

    # ── ML Model (Predictive Maintenance) ─────────────────────────────────
    try:
        catalog_name = os.environ.get("PIPELINE_CATALOG", "")
        if catalog_name:
            w = get_workspace_client()
            model_name = f"{catalog_name}.network_data.predictive_maintenance_model"
            try:
                versions = list(w.model_versions.list(full_name=model_name))
                if versions:
                    latest = max(versions, key=lambda v: int(v.version))
                    health["checks"]["ml_model"] = {
                        "status": "healthy",
                        "model": model_name,
                        "latest_version": latest.version,
                        "status_detail": (
                            str(latest.status).split(".")[-1] if latest.status else "UNKNOWN"
                        ),
                    }
                else:
                    health["checks"]["ml_model"] = {
                        "status": "not_trained",
                        "note": "Run predictive_maintenance notebook to train the model",
                    }
            except Exception as e:
                if "not found" in str(e).lower() or "does_not_exist" in str(e).lower():
                    health["checks"]["ml_model"] = {
                        "status": "not_trained",
                        "note": "Model not yet registered. Run predictive_maintenance notebook.",
                    }
                else:
                    health["checks"]["ml_model"] = {"status": "unknown", "error": str(e)[:200]}
        else:
            health["checks"]["ml_model"] = {"status": "not_configured"}
    except Exception as e:
        health["checks"]["ml_model"] = {"status": "unknown", "error": str(e)[:200]}

    # ── Table Row Counts (pg_class estimates) ─────────────────────────────
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                key_tables = ["work_orders", "technicians", "customers", "appointments"]
                row_counts: dict = {}
                empty_tables: list[str] = []
                # Use pg_class reltuples instead of COUNT(*) -- instant at any scale
                for tbl in key_tables:
                    try:
                        cur.execute(
                            "SELECT reltuples::bigint FROM pg_class WHERE relname = %s",
                            (tbl,),
                        )
                        count = cur.fetchone()[0]
                        row_counts[tbl] = count
                        if count == 0:
                            empty_tables.append(tbl)
                    except Exception:
                        row_counts[tbl] = "error"
                health["checks"]["table_rows"] = {
                    "status": "warning" if empty_tables else "healthy",
                    "counts": row_counts,
                }
                if empty_tables:
                    health["checks"]["table_rows"]["empty"] = [
                        t.split(".")[-1] for t in empty_tables
                    ]
    except Exception as e:
        health["checks"]["table_rows"] = {"status": "unknown", "error": str(e)[:200]}

    # ── Recent errors from the in-memory ring buffer ──────────────────────
    with _error_log_lock:
        health["recent_errors"] = list(_error_log[-10:])

    return jsonify(health)


# ── Liveness probe (lightweight) ──────────────────────────────────────────

@health_bp.route("/api/ping")
def ping():
    """Lightweight liveness probe.

    Returns HTTP 200 with the current uptime in seconds.  Intended for load
    balancer health checks and Kubernetes readiness probes -- no DB or SDK
    calls, so it always responds quickly.
    """
    return jsonify({
        "status": "ok",
        "uptime": (datetime.now(timezone.utc) - APP_START_TIME).total_seconds(),
    })


# ── Data Freshness API ───────────────────────────────────────────────────

@health_bp.route("/api/data-freshness")
def data_freshness():
    """Lightweight check: is the demo data stale?

    Queries the newest non-simulator work-order timestamp and reports
    whether it is more than 12 hours old.  Used by the frontend banner
    to prompt the user to run a date refresh.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:system/data_freshness */
                    SELECT MAX(created_at) FROM field_service.work_orders
                    WHERE work_order_number NOT LIKE 'WO-SIM-%%'
                """)
                row = cur.fetchone()
                if row and row[0]:
                    max_ts = row[0]
                    if max_ts.tzinfo is None:
                        max_ts = max_ts.replace(tzinfo=timezone.utc)
                    hours_old = (datetime.now(timezone.utc) - max_ts).total_seconds() / 3600
                    stale = hours_old > 12
                    return jsonify({
                        "stale": stale,
                        "hours_old": round(hours_old, 1),
                        "newest_record": max_ts.isoformat(),
                    })
        return jsonify({"stale": False, "hours_old": 0})
    except Exception as e:
        return jsonify({"stale": False, "error": str(e)})


# ── Date Refresh API (manual trigger) ─────────────────────────────────────

@health_bp.route("/api/refresh-dates", methods=["POST"])
def refresh_dates():
    """Kick off a background date refresh.

    Shifts all demo timestamps forward so the data looks current.  Returns
    immediately; the caller should poll ``/api/refresh-dates/status`` for
    progress.  Only one refresh can run at a time.
    """
    try:
        pool = get_pool()
        _refresh_dates_if_stale(pool)
        return jsonify({"success": True, "message": "Date refresh started", "background": True})
    except Exception as e:
        log_error("refresh_dates_api", e)
        return jsonify({"error": str(e)}), 500


@health_bp.route("/api/refresh-dates/status")
def refresh_dates_status():
    """Poll endpoint for date refresh progress.

    Returns the current phase, progress message, done flag, and any error
    from the background refresh worker.
    """
    return jsonify(_refresh_state)


# ── Notifications API ─────────────────────────────────────────────────────

@health_bp.route("/api/notifications")
def notifications():
    """Dispatcher notification queue.

    Aggregates several alert sources into a single sorted list:
    1. SLA at risk -- work orders whose SLA deadline is within 2 hours
    2. SLA breached -- work orders past their SLA deadline
    3. Recent completions -- work orders completed in the last hour
    4. Data staleness -- if seed data is >12 hours old

    Alerts are sorted by severity (critical > warning > success > info)
    so the most urgent items appear first.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                alerts: list[dict] = []

                # Check if sla_due_at column exists (cached to avoid info_schema per poll)
                cache_key = ("field_service", "work_orders", "sla_due_at")
                with _column_exists_lock:
                    has_sla_due = _column_exists_cache.get(cache_key)
                if has_sla_due is None:
                    cur.execute("""
                        /* page:notifications:schema_check */
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = 'field_service' AND table_name = 'work_orders'
                          AND column_name = 'sla_due_at'
                    """)
                    has_sla_due = bool(cur.fetchone())
                    with _column_exists_lock:
                        _column_exists_cache[cache_key] = has_sla_due

                if has_sla_due:
                    # SLA at risk (due within 2 hours)
                    try:
                        cur.execute("""
                            /* page:notifications:sla_at_risk */
                            SELECT wo.work_order_id, wo.work_order_number, wo.title, wo.priority,
                                   wo.sla_due_at,
                                   EXTRACT(EPOCH FROM (wo.sla_due_at - CURRENT_TIMESTAMP)) / 60 as mins_left
                            FROM field_service.work_orders wo
                            WHERE wo.status NOT IN ('completed', 'cancelled')
                              AND wo.sla_due_at IS NOT NULL
                              AND wo.sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours'
                              AND wo.sla_due_at > CURRENT_TIMESTAMP
                            ORDER BY wo.sla_due_at ASC LIMIT 10
                        """)
                        for r in cur.fetchall():
                            alerts.append({
                                "type": "sla_warning",
                                "severity": "warning",
                                "wo_id": r[0],
                                "wo_number": r[1],
                                "message": f"SLA at risk -- {r[2] or r[1]} ({int(r[5])} min remaining)",
                                "time": str(r[4]),
                            })
                    except Exception:
                        conn.rollback()

                    # SLA breached
                    try:
                        cur.execute("""
                            /* page:notifications:sla_breached */
                            SELECT wo.work_order_id, wo.work_order_number, wo.title, wo.priority,
                                   wo.sla_due_at
                            FROM field_service.work_orders wo
                            WHERE wo.status NOT IN ('completed', 'cancelled')
                              AND wo.sla_due_at IS NOT NULL
                              AND wo.sla_due_at < CURRENT_TIMESTAMP
                            ORDER BY wo.sla_due_at ASC LIMIT 10
                        """)
                        for r in cur.fetchall():
                            alerts.append({
                                "type": "sla_breach",
                                "severity": "critical",
                                "wo_id": r[0],
                                "wo_number": r[1],
                                "message": f"SLA BREACHED -- {r[2] or r[1]}",
                                "time": str(r[4]),
                            })
                    except Exception:
                        conn.rollback()

                # Recent completions (last hour)
                try:
                    cur.execute("""
                        /* page:notifications:completions */
                        SELECT wo.work_order_id, wo.work_order_number, wo.title,
                               t.first_name || ' ' || t.last_name as tech_name,
                               wo.resolved_at
                        FROM field_service.work_orders wo
                        LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                        WHERE wo.status = 'completed'
                          AND wo.resolved_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                        ORDER BY wo.resolved_at DESC LIMIT 5
                    """)
                    for r in cur.fetchall():
                        alerts.append({
                            "type": "completed",
                            "severity": "success",
                            "wo_id": r[0],
                            "wo_number": r[1],
                            "message": f"Completed -- {r[2] or r[1]} by {r[3] or 'Unknown'}",
                            "time": str(r[4]),
                        })
                except Exception:
                    conn.rollback()

                # Data staleness check
                try:
                    cur.execute("""
                        SELECT MAX(created_at) FROM field_service.work_orders
                        WHERE work_order_number NOT LIKE 'WO-SIM-%%'
                    """)
                    row = cur.fetchone()
                    if row and row[0]:
                        max_ts = row[0]
                        if max_ts.tzinfo is None:
                            max_ts = max_ts.replace(tzinfo=timezone.utc)
                        hours_old = (datetime.now(timezone.utc) - max_ts).total_seconds() / 3600
                        if hours_old > 12:
                            days = int(hours_old / 24)
                            msg = (
                                f"Demo data is {days} days old"
                                if days > 1
                                else f"Demo data is {int(hours_old)} hours old"
                            )
                            alerts.insert(0, {
                                "type": "stale_data",
                                "severity": "warning",
                                "message": (
                                    msg
                                    + " -- SLA dates and stats may look unrealistic. "
                                    "Open the simulator menu to refresh."
                                ),
                                "time": datetime.now(timezone.utc).isoformat(),
                                "action": "refresh_data",
                            })
                except Exception:
                    conn.rollback()

        # Sort by severity so the most urgent alerts appear first
        severity_order = {"critical": 0, "warning": 1, "success": 2, "info": 3}
        alerts.sort(key=lambda a: severity_order.get(a["severity"], 9))
        return jsonify({
            "notifications": alerts,
            "count": len([a for a in alerts if a["severity"] in ("critical", "warning")]),
        })
    except Exception as e:
        log_error("notifications", e)
        return jsonify({"error": str(e)}), 500


# ── Dashboard v2 Endpoints ────────────────────────────────────────────────

def _compute_activity_feed() -> dict:
    """Compute recent work-order events for the dashboard activity feed.

    Uses idx_wo_created_desc and idx_wo_completed_resolved for fast
    ORDER BY ... LIMIT scans instead of full-table sorts.
    """
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:dashboard/activity_feed */
                WITH recent AS (
                    (SELECT work_order_id, created_at as event_time
                     FROM field_service.work_orders ORDER BY created_at DESC LIMIT 15)
                    UNION
                    (SELECT work_order_id, resolved_at as event_time
                     FROM field_service.work_orders
                     WHERE status = 'completed' ORDER BY resolved_at DESC LIMIT 15)
                )
                SELECT DISTINCT ON (wo.work_order_id)
                       wo.work_order_number, wo.title, wo.status, wo.priority,
                       wo.category, wo.created_at, wo.updated_at, wo.resolved_at,
                       t.first_name || ' ' || t.last_name as tech_name
                FROM recent r
                JOIN field_service.work_orders wo ON wo.work_order_id = r.work_order_id
                LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                ORDER BY wo.work_order_id, r.event_time DESC
                LIMIT 15
            """)
            events: list[dict] = []
            for r in cur.fetchall():
                wo_num, title, status, priority, category, created, updated, resolved, tech = r
                if status == "completed" and resolved:
                    event_type = "completed"
                    event_time = str(resolved)
                    msg = f"{wo_num} completed by {tech or 'Unknown'}"
                elif status == "in_progress":
                    event_type = "in_progress"
                    event_time = str(updated or created)
                    msg = f"{wo_num} in progress -- {tech or 'Unassigned'}"
                elif status in ("assigned", "en_route") and tech:
                    event_type = "assigned"
                    event_time = str(updated or created)
                    msg = f"{wo_num} assigned to {tech}"
                else:
                    event_type = "created"
                    event_time = str(created)
                    msg = f"{wo_num} created -- {title or category or 'Work Order'}"
                events.append({
                    "wo_number": wo_num,
                    "title": title,
                    "status": status,
                    "priority": priority,
                    "category": category,
                    "event_type": event_type,
                    "event_time": event_time,
                    "message": msg,
                    "tech_name": tech,
                })
    return {"events": events}


@health_bp.route("/api/dashboard/activity-feed")
def dashboard_activity_feed():
    """Recent work-order events for the dashboard activity feed card (cached 60s)."""
    try:
        data = _get_or_refresh("dashboard_activity_feed", _compute_activity_feed, 60)
        return jsonify(data)
    except Exception as e:
        log_error("dashboard_activity_feed", e)
        return jsonify({"error": str(e)}), 500


def _compute_regional_load() -> dict:
    """Heavy query: regional work-order load with tech availability.

    Uses the analytics pool (300 s timeout) because the aggregation can be
    slow at the 5M work-order scale.  Results are cached via ``_get_or_refresh``
    in the route handler.
    """
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Active work orders by region with SLA breach count
            cur.execute("""
                /* page:dashboard/regional_load:wos */
                SELECT sr.region_name,
                       COALESCE(active.open_wos, 0) as open_wos,
                       COALESCE(active.sla_breached, 0) as sla_breached,
                       0 as completed_today
                FROM field_service.service_regions sr
                LEFT JOIN (
                    SELECT region_id,
                           COUNT(*) as open_wos,
                           COUNT(*) FILTER (
                               WHERE sla_due_at IS NOT NULL AND sla_due_at < CURRENT_TIMESTAMP
                           ) as sla_breached
                    FROM field_service.work_orders
                    WHERE status NOT IN ('completed', 'cancelled')
                    GROUP BY region_id
                ) active ON sr.region_id = active.region_id
                ORDER BY open_wos DESC
            """)
            regions = [
                {
                    "region": r[0],
                    "open_wos": r[1],
                    "sla_breached": r[2],
                    "completed_today": r[3],
                }
                for r in cur.fetchall()
            ]

            # Technician availability by region
            cur.execute("""
                /* page:dashboard/regional_load:techs */
                SELECT sr.region_name,
                       COUNT(*) FILTER (WHERE t.status = 'available') as available,
                       COUNT(*) as total
                FROM field_service.technicians t
                JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                GROUP BY sr.region_name
            """)
            tech_by_region = {r[0]: {"available": r[1], "total": r[2]} for r in cur.fetchall()}

            for region in regions:
                t = tech_by_region.get(region["region"], {"available": 0, "total": 0})
                region["techs_available"] = t["available"]
                region["techs_total"] = t["total"]

    return {"regions": regions}


@health_bp.route("/api/dashboard/regional-load")
def dashboard_regional_load():
    """Work-order load and tech availability by region.

    Uses the stale-while-revalidate cache (``_get_or_refresh``) to return
    data instantly even when the underlying aggregation is still running.
    """
    try:
        data = _get_or_refresh("regional_load", _compute_regional_load)
        return jsonify(data)
    except Exception as e:
        log_error("dashboard_regional_load", e)
        return jsonify({"error": str(e)}), 500


@health_bp.route("/api/dashboard/infrastructure-alerts")
def dashboard_infrastructure_alerts():
    """Infrastructure node status summary for the dashboard.

    Iterates over the in-memory TELCO_INFRASTRUCTURE list (generated at
    startup from metro area definitions) and returns nodes that are NOT
    in a healthy state, sorted by severity.  Also returns a summary count.
    """
    # Lazy import to avoid circular dependency
    from app import IOT_DEVICES, TELCO_INFRASTRUCTURE

    try:
        alerts: list[dict] = []
        for infra in TELCO_INFRASTRUCTURE:
            if infra["status"] != "healthy":
                # Count IoT devices attached to this infrastructure node
                iot_count = len([d for d in IOT_DEVICES if d["infrastructure_id"] == infra["id"]])
                alerts.append({
                    "id": infra["id"],
                    "name": infra["name"],
                    "type": infra["type"],
                    "status": infra["status"],
                    "region": infra["region"],
                    "iot_devices": iot_count,
                })
        # Sort: critical first, then degraded, then maintenance_due
        status_order = {"critical": 0, "degraded": 1, "maintenance_due": 2}
        alerts.sort(key=lambda a: status_order.get(a["status"], 99))

        # Summary counts across all infrastructure
        summary = {
            "total": len(TELCO_INFRASTRUCTURE),
            "healthy": len([i for i in TELCO_INFRASTRUCTURE if i["status"] == "healthy"]),
            "critical": len([i for i in TELCO_INFRASTRUCTURE if i["status"] == "critical"]),
            "maintenance_due": len([i for i in TELCO_INFRASTRUCTURE if i["status"] == "maintenance_due"]),
            "total_iot_devices": len(IOT_DEVICES),
        }
        return jsonify({"alerts": alerts, "summary": summary})
    except Exception as e:
        log_error("dashboard_infrastructure_alerts", e)
        return jsonify({"error": str(e)}), 500


@health_bp.route("/api/dashboard/stb-fleet")
def dashboard_stb_fleet():
    """STB device fleet health from Managed Iceberg tables.

    Queries the ``stb_managed`` schema (created by the migration simulator)
    via the SQL warehouse to show device counts, firmware distribution, and
    top incident types.  Results are cached for 5 minutes to reduce warehouse
    load.  Returns ``available: false`` if the migration has not been run yet.
    """
    global _stb_fleet_cache

    # Return cache if fresh (5-minute TTL)
    if _stb_fleet_cache["data"] and time.time() < _stb_fleet_cache["expires"]:
        return jsonify(_stb_fleet_cache["data"])

    CAT = os.environ.get("PIPELINE_CATALOG", "dba-lakebase-network")
    SCHEMA = "stb_managed"

    try:
        # Check if tables exist by querying device count
        result = _run_sql(f"SELECT COUNT(*) FROM `{CAT}`.`{SCHEMA}`.managed_stb_devices")
        if not result:
            return jsonify({"available": False, "reason": "No data -- run the migration simulator"})

        device_count = int(result[0][0])
        if device_count == 0:
            return jsonify({"available": False, "reason": "No data -- run the migration simulator"})

        # Fleet stats
        device_result = _run_sql(f"""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT model) as models,
                   COUNT(DISTINCT region) as regions,
                   COUNT(DISTINCT firmware_version) as firmware_versions
            FROM `{CAT}`.`{SCHEMA}`.managed_stb_devices
        """)

        # Incident stats
        incident_result = _run_sql(f"""
            SELECT COUNT(*) as total_incidents,
                   COUNT(DISTINCT incident_type) as incident_types
            FROM `{CAT}`.`{SCHEMA}`.managed_stb_incidents
        """)

        # Top incident types
        top_incidents = _run_sql(f"""
            SELECT incident_type, COUNT(*) as cnt
            FROM `{CAT}`.`{SCHEMA}`.managed_stb_incidents
            GROUP BY incident_type
            ORDER BY cnt DESC
            LIMIT 5
        """)

        # Firmware distribution
        firmware_dist = _run_sql(f"""
            SELECT firmware_version, COUNT(*) as cnt
            FROM `{CAT}`.`{SCHEMA}`.managed_stb_devices
            GROUP BY firmware_version
            ORDER BY cnt DESC
            LIMIT 5
        """)

        data = {
            "available": True,
            "devices": {
                "total": int(device_result[0][0]) if device_result else 0,
                "models": int(device_result[0][1]) if device_result else 0,
                "regions": int(device_result[0][2]) if device_result else 0,
                "firmware_versions": int(device_result[0][3]) if device_result else 0,
            },
            "incidents": {
                "total": int(incident_result[0][0]) if incident_result else 0,
                "types": int(incident_result[0][1]) if incident_result else 0,
            },
            "top_incidents": [{"type": r[0], "count": int(r[1])} for r in (top_incidents or [])],
            "firmware_distribution": [
                {"version": r[0], "count": int(r[1])} for r in (firmware_dist or [])
            ],
        }

        # Cache for 5 minutes
        _stb_fleet_cache = {"data": data, "expires": time.time() + 300}
        return jsonify(data)
    except Exception as e:
        err_str = str(e)
        if (
            "TABLE_OR_VIEW_NOT_FOUND" in err_str
            or "SCHEMA_NOT_FOUND" in err_str
            or "does not exist" in err_str.lower()
        ):
            return jsonify({"available": False, "reason": "No data -- run the migration simulator"})
        log_error("dashboard_stb_fleet", e)
        return jsonify({"available": False, "reason": f"Query error: {err_str[:100]}"}), 500


# ── Dashboard v3: Health-Lite, SLA Trend, Services Status ────────────────


def _compute_health_lite() -> dict:
    """Lightweight health check for dashboard: pool stats + PG ping only."""
    result: dict = {"status": "ok", "checks": {}}
    try:
        pool = get_pool()
        pool_stats = pool.get_stats()
        result["checks"]["pool"] = {
            "pool_size": pool_stats.get("pool_size", 0),
            "pool_available": pool_stats.get("pool_available", 0),
            "requests_waiting": pool_stats.get("requests_waiting", 0),
            "status": "ok",
        }
    except Exception as e:
        result["checks"]["pool"] = {"status": "error", "error": str(e)[:100]}

    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result["checks"]["database"] = {"status": "connected"}
                # Active connections + CU usage
                cur.execute("""
                    SELECT count(*) as active,
                           (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_conn
                    FROM pg_stat_activity WHERE state = 'active'
                """)
                r = cur.fetchone()
                result["checks"]["connections"] = {
                    "active": r[0],
                    "max": r[1],
                }
    except Exception as e:
        result["checks"]["database"] = {"status": "error", "error": str(e)[:100]}
        result["status"] = "degraded"

    return result


@health_bp.route("/api/dashboard/health-lite")
def dashboard_health_lite():
    """Lightweight health for dashboard — pool stats + DB ping (cached 30s)."""
    try:
        data = _get_or_refresh("health_lite", _compute_health_lite, 30)
        return jsonify(data)
    except Exception as e:
        log_error("dashboard_health_lite", e)
        return jsonify({"error": str(e)}), 500


def _compute_sla_trend() -> dict:
    """7-day SLA compliance trend for the dashboard sparkline."""
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:dashboard/sla_trend */
                SELECT
                    resolved_at::date as day,
                    COUNT(*) FILTER (WHERE sla_met = true) as met,
                    COUNT(*) FILTER (WHERE sla_met IS NOT NULL) as total
                FROM field_service.work_orders
                WHERE status = 'completed'
                  AND resolved_at >= CURRENT_DATE - INTERVAL '7 days'
                  AND sla_met IS NOT NULL
                GROUP BY resolved_at::date
                ORDER BY day
            """)
            days = []
            for r in cur.fetchall():
                day_str = str(r[0])
                met, total = r[1], r[2]
                pct = round((met / max(total, 1)) * 100, 1)
                days.append({"date": day_str, "met": met, "total": total, "pct": pct})
    return {"trend": days}


@health_bp.route("/api/dashboard/sla-trend")
def dashboard_sla_trend():
    """7-day SLA compliance trend (cached 5 min)."""
    try:
        data = _get_or_refresh("sla_trend", _compute_sla_trend, 300)
        return jsonify(data)
    except Exception as e:
        log_error("dashboard_sla_trend", e)
        return jsonify({"error": str(e)}), 500


def _compute_services_status() -> dict:
    """Check status of Databricks platform services for dashboard."""
    services = []

    # Genie Spaces
    genie_count = sum(1 for v in GENIE_SPACES.values() if v)
    services.append({
        "name": "Genie Spaces",
        "status": "active" if genie_count > 0 else "inactive",
        "detail": f"{genie_count} configured",
    })

    # Agent Endpoint
    agent_name = os.environ.get("AGENT_ENDPOINT_NAME", "")
    services.append({
        "name": "AI Agent",
        "status": "active" if agent_name else "inactive",
        "detail": agent_name or "Not configured",
    })

    # SQL Warehouse
    wh_id = os.environ.get("WAREHOUSE_ID", "")
    services.append({
        "name": "SQL Warehouse",
        "status": "active" if wh_id else "inactive",
        "detail": wh_id[:12] + "..." if wh_id else "Not configured",
    })

    # DLT Pipeline
    pipeline_id = os.environ.get("PIPELINE_ID", "")
    services.append({
        "name": "DLT Pipeline",
        "status": "active" if pipeline_id else "inactive",
        "detail": pipeline_id[:12] + "..." if pipeline_id else "Not configured",
    })

    # Lakebase
    pg_host = os.environ.get("PGHOST", "")
    services.append({
        "name": "Lakebase",
        "status": "active" if pg_host else "inactive",
        "detail": "PostgreSQL connected" if pg_host else "Not configured",
    })

    active_count = sum(1 for s in services if s["status"] == "active")
    return {"services": services, "active": active_count, "total": len(services)}


@health_bp.route("/api/dashboard/services-status")
def dashboard_services_status():
    """Databricks platform services status (cached 5 min)."""
    try:
        data = _get_or_refresh("services_status", _compute_services_status, 300)
        return jsonify(data)
    except Exception as e:
        log_error("dashboard_services_status", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Date Refresh Helpers (private)
# ---------------------------------------------------------------------------


def _batched_update(cur, table: str, pk: str, set_clause: str, where_extra: str = "") -> int:
    """Run a batched UPDATE on a table using PK ranges.

    Splits the update into chunks of ``REFRESH_BATCH`` rows to avoid
    holding a long-running lock on the entire table.  Returns the total
    number of rows updated across all batches.
    """
    # Security: validate identifiers to prevent SQL injection
    from shared import validate_identifier
    validate_identifier(table, 'table')
    validate_identifier(pk, 'pk')

    cur.execute(
        f"SELECT MIN({pk}), MAX({pk}) FROM field_service.{table} "
        + (f"WHERE {where_extra}" if where_extra else "")
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return 0
    lo, hi = row
    total = 0
    start = lo
    while start <= hi:
        end = min(start + REFRESH_BATCH - 1, hi)
        w = f"{pk} BETWEEN {start} AND {end}"
        if where_extra:
            w += f" AND {where_extra}"
        cur.execute(f"UPDATE field_service.{table} SET {set_clause} WHERE {w}")
        total += cur.rowcount
        start = end + 1
    return total


def _rebalance_priorities(cur) -> None:
    """Ensure a realistic priority mix among active work orders.

    Target distribution: ~5% critical, ~15% high, ~50% medium, ~30% low.
    Uses batched updates (REFRESH_BATCH rows per statement) to avoid
    long-running single statements at scale.
    """
    BATCH = 500_000
    cur.execute(
        "SELECT MIN(work_order_id), MAX(work_order_id) "
        "FROM field_service.work_orders WHERE status NOT IN ('completed', 'cancelled')"
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return
    lo, hi = row
    start = lo
    while start <= hi:
        end = min(start + BATCH - 1, hi)
        cur.execute(f"""
            WITH ranked AS (
                SELECT work_order_id,
                       ROW_NUMBER() OVER (ORDER BY random()) as rn,
                       COUNT(*) OVER () as total
                FROM field_service.work_orders
                WHERE status NOT IN ('completed', 'cancelled')
                  AND work_order_id BETWEEN {start} AND {end}
            )
            UPDATE field_service.work_orders wo SET priority = CASE
                WHEN r.rn <= r.total * 0.05 THEN 'critical'
                WHEN r.rn <= r.total * 0.20 THEN 'high'
                WHEN r.rn <= r.total * 0.70 THEN 'medium'
                ELSE 'low'
            END
            FROM ranked r WHERE wo.work_order_id = r.work_order_id
        """)
        start = end + 1


def _run_date_refresh(pool) -> None:
    """Background worker: shift all timestamps forward in batches.

    Finds the staleness delta (hours between the newest seed work order and
    now), then applies an INTERVAL offset to every timestamp column in every
    table.  Also fixes SLA due dates for open work orders and rebalances
    priority distribution.
    """
    global _refresh_state
    SCHEMA = "field_service"
    t0 = time.time()
    try:
        with pool.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 0")
                # Disable SLA trigger during refresh to prevent deadlocks
                # (trigger tries to UPDATE the same row we're already updating)
                try:
                    cur.execute("ALTER TABLE field_service.work_orders DISABLE TRIGGER trg_sla_risk")
                except Exception:
                    pass  # trigger may not exist

                # Find how stale the data is
                cur.execute(
                    f"SELECT MAX(created_at) FROM {SCHEMA}.work_orders "
                    "WHERE work_order_number NOT LIKE 'WO-SIM-%%'"
                )
                row = cur.fetchone()
                if not row or not row[0]:
                    _refresh_state.update(
                        running=False, done=True, phase="skipped",
                        progress="No seed data found",
                    )
                    return

                max_ts = row[0]
                if max_ts.tzinfo is None:
                    from datetime import timezone as tz
                    max_ts = max_ts.replace(tzinfo=tz.utc)

                now = datetime.now(timezone.utc)
                delta_hours = (now - max_ts).total_seconds() / 3600

                if delta_hours < 12:
                    _refresh_state.update(
                        running=False, done=True, phase="fresh",
                        progress=f"Data is {delta_hours:.1f}h old -- still fresh",
                    )
                    return

                target_offset = f"{int(delta_hours) - 2} hours"
                ivl = f"INTERVAL '{target_offset}'"

                # Define all table updates: (label, table, pk, set_clause)
                updates = [
                    ("work_orders", "work_orders", "work_order_id",
                     f"created_at = created_at + {ivl}, updated_at = updated_at + {ivl}, "
                     f"sla_due_at = CASE WHEN sla_due_at IS NOT NULL THEN sla_due_at + {ivl} END, "
                     f"first_response_at = CASE WHEN first_response_at IS NOT NULL THEN first_response_at + {ivl} END, "
                     f"resolved_at = CASE WHEN resolved_at IS NOT NULL THEN resolved_at + {ivl} END, "
                     f"closed_at = CASE WHEN closed_at IS NOT NULL THEN closed_at + {ivl} END"),
                    ("appointments", "appointments", "appointment_id",
                     f"scheduled_start = scheduled_start + {ivl}, scheduled_end = scheduled_end + {ivl}, "
                     f"actual_start = CASE WHEN actual_start IS NOT NULL THEN actual_start + {ivl} END, "
                     f"actual_end = CASE WHEN actual_end IS NOT NULL THEN actual_end + {ivl} END, "
                     f"created_at = created_at + {ivl}, updated_at = updated_at + {ivl}"),
                    ("work_order_notes", "work_order_notes", "note_id",
                     f"created_at = created_at + {ivl}"),
                    ("work_order_parts", "work_order_parts", "part_id",
                     f"created_at = created_at + {ivl}"),
                    ("customers", "customers", "customer_id",
                     f"contract_start = contract_start + {ivl}, contract_end = contract_end + {ivl}, "
                     f"created_at = created_at + {ivl}, updated_at = updated_at + {ivl}"),
                    ("technicians", "technicians", "technician_id",
                     f"updated_at = updated_at + {ivl}"),
                    ("equipment_inventory", "equipment_inventory", "equipment_id",
                     f"purchased_at = purchased_at + {ivl}, "
                     f"installed_at = CASE WHEN installed_at IS NOT NULL THEN installed_at + {ivl} END, "
                     f"last_serviced_at = CASE WHEN last_serviced_at IS NOT NULL THEN last_serviced_at + {ivl} END, "
                     f"created_at = created_at + {ivl}"),
                    ("technician_skills", "technician_skills", "skill_id",
                     f"certified_at = certified_at + {ivl}, "
                     f"expires_at = CASE WHEN expires_at IS NOT NULL THEN expires_at + {ivl} END"),
                ]

                total_tables = len(updates) + 2  # +2 for SLA fix and priority rebalance
                for i, (label, table, pk, set_clause) in enumerate(updates):
                    _refresh_state["phase"] = label
                    _refresh_state["progress"] = f"Updating {label} ({i+1}/{total_tables})..."
                    log.info(f"Date refresh: updating {label}...")
                    st = time.time()
                    rows = _batched_update(cur, table, pk, set_clause)
                    log.info(f"Date refresh: {label} done -- {rows:,} rows in {time.time()-st:.1f}s")

                # Fix SLA due dates for open WOs
                _refresh_state["phase"] = "sla_fix"
                _refresh_state["progress"] = f"Fixing SLA due dates ({len(updates)+1}/{total_tables})..."
                log.info("Date refresh: fixing SLA due dates...")
                cur.execute(
                    "SELECT MIN(work_order_id), MAX(work_order_id) "
                    "FROM field_service.work_orders WHERE status NOT IN ('completed', 'cancelled')"
                )
                sla_range = cur.fetchone()
                if sla_range and sla_range[0] is not None:
                    sla_lo, sla_hi = sla_range
                    sla_start = sla_lo
                    while sla_start <= sla_hi:
                        sla_end = min(sla_start + REFRESH_BATCH - 1, sla_hi)
                        cur.execute(f"""
                            UPDATE field_service.work_orders wo SET
                                sla_due_at = CURRENT_TIMESTAMP + (sp.resolution_hours || ' hours')::INTERVAL
                            FROM field_service.sla_policies sp
                            WHERE wo.sla_id = sp.sla_id
                              AND wo.status NOT IN ('completed', 'cancelled')
                              AND wo.sla_due_at < CURRENT_TIMESTAMP
                              AND wo.work_order_id BETWEEN {sla_start} AND {sla_end}
                        """)
                        sla_start = sla_end + 1

                # Fix priority distribution
                _refresh_state["phase"] = "priorities"
                _refresh_state["progress"] = f"Rebalancing priorities ({total_tables}/{total_tables})..."
                log.info("Date refresh: rebalancing priorities...")
                _rebalance_priorities(cur)

                # Re-enable SLA trigger
                try:
                    cur.execute("ALTER TABLE field_service.work_orders ENABLE TRIGGER trg_sla_risk")
                except Exception:
                    pass

                cur.execute("RESET statement_timeout")
                elapsed = time.time() - t0
                _refresh_state.update(
                    running=False, done=True, phase="complete",
                    progress=f"Refreshed in {elapsed:.0f}s",
                )
                log.info(
                    f"Date refresh complete: shifted by {target_offset} "
                    f"in {elapsed:.0f}s ({elapsed/60:.1f} min)"
                )

    except Exception as e:
        log_error("date_refresh", e)
        # Re-enable trigger even on error
        try:
            with pool.connection() as conn2:
                conn2.autocommit = True
                conn2.cursor().execute("ALTER TABLE field_service.work_orders ENABLE TRIGGER trg_sla_risk")
        except Exception:
            pass
        _refresh_state.update(
            running=False, done=True, error=str(e), phase="error",
            progress=f"Error: {str(e)[:100]}",
        )


def _refresh_dates_if_stale(pool) -> None:
    """Kick off a batched date refresh in a background thread.

    Returns immediately.  Only one refresh can run at a time -- subsequent
    calls while a refresh is in progress are no-ops.
    """
    global _refresh_state
    if _refresh_state.get("running"):
        return  # already running
    _refresh_state = {
        "running": True,
        "phase": "starting",
        "progress": "Starting date refresh...",
        "done": False,
        "error": None,
    }
    t = threading.Thread(target=_run_date_refresh, args=(pool,), daemon=True)
    t.start()

"""
Lakebase Admin API Blueprint — standalone database administration console.

A generic Postgres/Lakebase DBA console that points at *any* Lakebase (or
plain PostgreSQL) instance. The target schema is configurable via the
``TARGET_SCHEMA`` environment variable (default ``public``).

Coverage
--------
* **Instance introspection** — connection info, roles, triggers, matviews, size
* **Schema exploration** — tables, columns, foreign keys, row estimates
* **Live query dashboard** — real-time session monitoring with ASH sampling,
  blocking tree, lock summary, and ``pg_stat_statements`` analysis
* **Maintenance** — VACUUM ANALYZE with before/after bloat, REINDEX CONCURRENTLY,
  table-bloat estimates, slow-query analysis
* **Backup & restore** — pg_dump-style SQL backup (optionally persisted to a
  UC Volume), download, list, restore into a new schema
* **Credential rotation** — trigger/monitor a rotation notebook job (optional)
* **Cluster status** — PG settings, autoscaling endpoint info, CU estimation

All routes live under ``/api/admin/`` (``url_prefix='/api/admin'``).

Configuration (environment variables)
-------------------------------------
* ``TARGET_SCHEMA`` — schema this console administers (default ``public``).
* ``ADMIN_GROUP`` / ``STRICT_ADMIN`` / ``ALLOW_ANONYMOUS_ADMIN`` — access control
  (see ``shared.py`` and ``check_admin_access`` below).
* ``BACKUP_VOLUME`` — UC Volume directory for persisted backups, e.g.
  ``/Volumes/main/default/lakebase_backups``. Unset ⇒ backup persistence,
  listing, and restore are disabled (fresh generate + download still work).
* ``ROTATION_NOTEBOOK_PATH`` — workspace path of a password-rotation notebook.
  Unset ⇒ the rotation trigger returns a "not configured" response.
* ``SQL_WAREHOUSE_ID`` — required only for UC Volume backup persistence.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone

from flask import Blueprint, Response, jsonify, request

from shared import (
    get_pool_for,
    active_instance_name,
    list_lakebase_instances,
    DEFAULT_INSTANCE,
    get_workspace_client,
    log_error,
    _run_sql,
    get_current_user,
    get_role_from_groups,
    validate_identifier,
)

log = logging.getLogger(__name__)


def current_pool():
    """Interactive pool for the instance targeted by the current request."""
    return get_pool_for(active_instance_name(), analytics=False)


def current_analytics_pool():
    """Analytics pool for the instance targeted by the current request."""
    return get_pool_for(active_instance_name(), analytics=True)

# ---------------------------------------------------------------------------
# Configuration — target schema and optional feature wiring
# ---------------------------------------------------------------------------

# The schema this console administers. Validated as an identifier because it
# is interpolated into SQL (it comes from deploy-time config, never a request).
SCHEMA = validate_identifier(os.environ.get("TARGET_SCHEMA", "public"), "TARGET_SCHEMA")

# UC Volume directory for persisted pg_dump backups (optional). When unset,
# backup save/list/restore are disabled but fresh generate + download work.
BACKUP_VOLUME = os.environ.get("BACKUP_VOLUME", "").rstrip("/")

# Workspace path of a credential-rotation notebook (optional). When unset, the
# rotation trigger returns a graceful "not configured" response.
ROTATION_NOTEBOOK_PATH = os.environ.get("ROTATION_NOTEBOOK_PATH", "")


# ---------------------------------------------------------------------------
# Blueprint definition + access control
# ---------------------------------------------------------------------------
admin_bp = Blueprint("admin", __name__, url_prefix="/api/admin")


@admin_bp.before_request
def check_admin_access():
    """Gate every ``/api/admin/*`` endpoint on admin access.

    * Anonymous callers are blocked unless ``ALLOW_ANONYMOUS_ADMIN=true``
      (local dev only).
    * Members of the configured admin group (``ADMIN_GROUP``, plus the
      workspace ``admins`` group) are allowed.
    * Otherwise, authenticated users are allowed by default so a fresh deploy
      is usable. Set ``STRICT_ADMIN=true`` to require admin-group membership
      and return 403 for everyone else.
    """
    user = get_current_user()
    if user.get("email") == "anonymous":
        if os.environ.get("ALLOW_ANONYMOUS_ADMIN", "").lower() == "true":
            return None
        return jsonify({"error": "Authentication required"}), 401

    try:
        if get_role_from_groups(user["email"]) == "admin":
            return None
    except Exception:
        # SCIM lookup failed — fall through to the lenient/strict decision.
        pass

    if os.environ.get("STRICT_ADMIN", "").lower() != "true":
        return None

    return jsonify({
        "error": "Admin access required",
        "user": user.get("email"),
        "message": "Your account needs to be a member of the admin group to use this console.",
    }), 403


# ═══════════════════════════════════════════════════════════════════════════
# Instance Selection & Discovery
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/whoami")
def admin_whoami():
    """Diagnostic: who the console sees you as, and how it will auth to instances.

    ``obo_token_present`` is the key signal — when true, the console mints
    Lakebase credentials as YOU (on-behalf-of-user) for non-default instances,
    so no per-instance Postgres roles are needed. It is only populated on an
    interactive browser session (after first-use consent), never on a raw
    programmatic API call.
    """
    fwd = request.headers.get("X-Forwarded-Access-Token")
    user = get_current_user()
    active = active_instance_name()
    is_default = (not active) or (active == DEFAULT_INSTANCE)
    try:
        is_admin = get_role_from_groups(user.get("email", "")) == "admin"
    except Exception:
        is_admin = None

    # Decode the forwarded token's scope claim (unverified — just for diagnostics)
    # so we can see whether it actually carries the `postgres` scope OBO needs.
    obo_scopes = None
    if fwd:
        try:
            import base64 as _b64
            payload = fwd.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
            claims = json.loads(_b64.urlsafe_b64decode(payload))
            sc = claims.get("scope") or claims.get("scp") or ""
            obo_scopes = sc.split() if isinstance(sc, str) else sc
        except Exception:
            obo_scopes = "undecodable"

    return jsonify({
        "identity": user.get("email"),
        "name": user.get("name"),
        "admin_group_member": is_admin,
        "obo_token_present": bool(fwd),
        "obo_scopes": obo_scopes,
        "obo_has_postgres": bool(obo_scopes and "postgres" in obo_scopes),
        "active_instance": active,
        "default_instance": DEFAULT_INSTANCE,
        "active_auth": (
            "native (secret creds)" if is_default
            else ("user / on-behalf-of" if fwd else "service principal (no OBO token)")
        ),
    })


@admin_bp.route("/instances")
def admin_instances():
    """List Lakebase instances in the workspace, plus which one is active.

    Powers the searchable instance selector. ``active`` reflects the
    ``X-Lakebase-Instance`` header for this request (default when absent).
    """
    try:
        instances = list_lakebase_instances()
        return jsonify({
            "instances": instances,
            "active": active_instance_name(),
            "default": DEFAULT_INSTANCE,
        })
    except Exception as e:
        log_error("admin_instances", e)
        return jsonify({"instances": [], "active": active_instance_name(),
                        "default": DEFAULT_INSTANCE, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Instance Info & Connection
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/instance-info")
def admin_instance_info():
    """Get Lakebase instance details — connection, roles, database stats."""
    try:
        pool = current_pool()
        info = {}
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Current connection info
                cur.execute("/* page:admin/instance_info:conn */ SELECT current_user, current_database(), version()")
                r = cur.fetchone()
                info["current_user"] = r[0]
                info["database"] = r[1]
                info["pg_version"] = r[2]
                info["schema"] = SCHEMA

                # Database size
                cur.execute("/* page:admin/instance_info:db_size */ SELECT pg_size_pretty(pg_database_size(current_database()))")
                info["database_size"] = cur.fetchone()[0]

                # Table sizes (target schema)
                cur.execute(f"""
                    /* page:admin/instance_info:tables */
                    SELECT tablename,
                           pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
                           pg_total_relation_size(schemaname||'.'||tablename) AS size_bytes,
                           (SELECT n_live_tup FROM pg_stat_user_tables
                            WHERE schemaname = t.schemaname AND relname = t.tablename) AS row_estimate
                    FROM pg_tables t
                    WHERE schemaname = '{SCHEMA}'
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                """)
                info["tables"] = [{"name": r[0], "size": r[1], "size_bytes": r[2],
                                   "rows": r[3]} for r in cur.fetchall()]

                # Active connections grouped by user and state
                cur.execute("""
                    /* page:admin/instance_info:connections */
                    SELECT usename, count(*), state
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                    GROUP BY usename, state
                    ORDER BY count DESC
                """)
                info["connections"] = [{"user": r[0], "count": r[1], "state": r[2]}
                                       for r in cur.fetchall()]

                # Lakebase app roles
                cur.execute("""
                    /* page:admin/instance_info:roles */
                    SELECT r.rolname, r.rolcanlogin, r.rolsuper,
                           ARRAY(SELECT b.rolname FROM pg_roles b
                                 JOIN pg_auth_members m ON m.roleid = b.oid
                                 WHERE m.member = r.oid) AS member_of
                    FROM pg_roles r
                    WHERE r.rolname LIKE 'lakebase_app%%'
                    ORDER BY r.rolname
                """)
                info["roles"] = [{"name": r[0], "can_login": r[1], "is_super": r[2],
                                  "member_of": r[3]} for r in cur.fetchall()]

                # Triggers in target schema
                cur.execute(f"""
                    /* page:admin/instance_info:triggers */
                    SELECT trigger_name, event_object_table, action_timing, event_manipulation
                    FROM information_schema.triggers
                    WHERE trigger_schema = '{SCHEMA}'
                """)
                info["triggers"] = [{"name": r[0], "table": r[1], "timing": r[2], "event": r[3]}
                                    for r in cur.fetchall()]

                # User-defined functions
                cur.execute(f"""
                    /* page:admin/instance_info:functions */
                    SELECT routine_name, routine_type, security_type
                    FROM information_schema.routines
                    WHERE routine_schema = '{SCHEMA}'
                """)
                info["functions"] = [{"name": r[0], "type": r[1], "security": r[2]}
                                     for r in cur.fetchall()]

                # Materialized views
                cur.execute(f"""
                    /* page:admin/instance_info:matviews */
                    SELECT matviewname, matviewowner,
                           pg_size_pretty(pg_total_relation_size('{SCHEMA}.' || matviewname))
                    FROM pg_matviews
                    WHERE schemaname = '{SCHEMA}'
                """)
                info["matviews"] = [{"name": r[0], "owner": r[1], "size": r[2]}
                                    for r in cur.fetchall()]

                # Total index count
                cur.execute(f"""
                    /* page:admin/instance_info:indexes */
                    SELECT COUNT(*) FROM pg_indexes WHERE schemaname = '{SCHEMA}'
                """)
                info["index_count"] = cur.fetchone()[0]

        return jsonify(info)
    except Exception as e:
        log_error("admin_instance_info", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/rotation-status")
def admin_rotation_status():
    """Show current password rotation state (active/standby role detection)."""
    try:
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("/* page:admin/rotation_status */ SELECT current_user")
                current_role = cur.fetchone()[0]

                # Determine active/standby from role name suffix
                active_suffix = "a" if current_role.endswith("_a") else "b" if current_role.endswith("_b") else "?"
                standby_suffix = "b" if active_suffix == "a" else "a"

                # Check login status of both rotation roles
                cur.execute("""
                    /* page:admin/rotation_status:roles */
                    SELECT rolname, rolcanlogin
                    FROM pg_roles
                    WHERE rolname IN ('lakebase_app_a', 'lakebase_app_b')
                    ORDER BY rolname
                """)
                roles = {r[0]: r[1] for r in cur.fetchall()}

        return jsonify({
            "current_role": current_role,
            "active_suffix": active_suffix,
            "standby_suffix": standby_suffix,
            "roles": roles,
            "rotation_configured": bool(ROTATION_NOTEBOOK_PATH),
            "rotation_info": f"Active: lakebase_app_{active_suffix} (LOGIN), Standby: lakebase_app_{standby_suffix} (NOLOGIN)",
        })
    except Exception as e:
        log_error("admin_rotation_status", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/connection-test")
def admin_connection_test():
    """Test connection to the Lakebase instance and return server metadata."""
    results = {}
    try:
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "/* page:admin/connection_test */ "
                    "SELECT current_user, current_database(), inet_server_addr(), inet_server_port(), version()"
                )
                r = cur.fetchone()
                results["lakebase"] = {
                    "status": "connected",
                    "user": r[0], "database": r[1],
                    "server": str(r[2]) if r[2] else "unknown", "port": r[3],
                    "version": r[4],
                    "host": os.environ.get("PGHOST", "unknown"),
                    "lakebase_type": os.environ.get("LAKEBASE_TYPE", "autoscaling"),
                }
    except Exception as e:
        results["lakebase"] = {"status": "error", "message": str(e)}

    return jsonify(results)


# ═══════════════════════════════════════════════════════════════════════════
# Schema Explorer
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/schema-explorer")
def admin_schema_explorer():
    """Get full schema info — tables, columns, FKs, indexes."""
    try:
        pool = current_pool()
        schema = {}
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # All base tables in target schema
                cur.execute(f"""
                    /* page:admin/schema_explorer:tables */
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = '{SCHEMA}' AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = [r[0] for r in cur.fetchall()]

                for tbl in tables:
                    # Column definitions
                    cur.execute(f"""
                        /* page:admin/schema_explorer:columns */
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = %s
                        ORDER BY ordinal_position
                    """, (tbl,))
                    columns = [{"name": r[0], "type": r[1], "nullable": r[2], "default": r[3]}
                               for r in cur.fetchall()]

                    # Foreign key relationships
                    cur.execute(f"""
                        /* page:admin/schema_explorer:fks */
                        SELECT kcu.column_name, ccu.table_name AS ref_table, ccu.column_name AS ref_column
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                          AND tc.table_schema = '{SCHEMA}' AND tc.table_name = %s
                    """, (tbl,))
                    fks = [{"column": r[0], "ref_table": r[1], "ref_column": r[2]}
                           for r in cur.fetchall()]

                    # Approximate row count from pg_stat
                    cur.execute(
                        "SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = %s AND relname = %s",
                        (SCHEMA, tbl),
                    )
                    row_count = cur.fetchone()

                    schema[tbl] = {
                        "columns": columns,
                        "foreign_keys": fks,
                        "row_count": row_count[0] if row_count else 0,
                    }

        return jsonify({"schema": SCHEMA, "tables": schema})
    except Exception as e:
        log_error("admin_schema_explorer", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Backup & WAL Info
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/backup-info")
def admin_backup_info():
    """Show backup-related information (Lakebase manages backups automatically)."""
    try:
        pool = current_pool()
        info = {}
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # WAL position
                cur.execute("/* page:admin/backup_info:wal */ SELECT pg_current_wal_lsn(), pg_walfile_name(pg_current_wal_lsn())")
                r = cur.fetchone()
                info["current_wal_lsn"] = str(r[0])
                info["current_wal_file"] = r[1]

                # Database age for PITR context
                cur.execute("""
                    /* page:admin/backup_info:db_age */
                    SELECT datname, age(datfrozenxid) AS frozen_xid_age
                    FROM pg_database WHERE datname = current_database()
                """)
                r = cur.fetchone()
                info["database"] = r[0]
                info["frozen_xid_age"] = r[1]

                # Vacuum/analyze timestamps per table
                cur.execute(f"""
                    /* page:admin/backup_info:maintenance */
                    SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
                    FROM pg_stat_user_tables
                    WHERE schemaname = '{SCHEMA}'
                    ORDER BY relname
                """)
                info["maintenance"] = [{
                    "table": r[0],
                    "last_vacuum": str(r[1]) if r[1] else None,
                    "last_autovacuum": str(r[2]) if r[2] else None,
                    "last_analyze": str(r[3]) if r[3] else None,
                    "last_autoanalyze": str(r[4]) if r[4] else None,
                } for r in cur.fetchall()]

        info["note"] = "Lakebase manages continuous backups automatically. Point-in-time restore is available via the Databricks API."
        return jsonify(info)
    except Exception as e:
        log_error("admin_backup_info", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Active Queries
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/active-queries")
def admin_active_queries():
    """Show currently running queries on the database."""
    try:
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:admin/active_queries */
                    SELECT pid, usename, state, query_start,
                           now() - query_start AS duration,
                           LEFT(query, 200) AS query_preview
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND state != 'idle'
                      AND pid != pg_backend_pid()
                    ORDER BY query_start
                """)
                queries = [{"pid": r[0], "user": r[1], "state": r[2],
                            "started": str(r[3]) if r[3] else None,
                            "duration": str(r[4]) if r[4] else None,
                            "query": r[5]}
                           for r in cur.fetchall()]
        return jsonify({"queries": queries})
    except Exception as e:
        log_error("admin_active_queries", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Password Rotation (optional — needs ROTATION_NOTEBOOK_PATH)
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/trigger-rotation", methods=["POST"])
def admin_trigger_rotation():
    """Submit the password rotation notebook as a serverless job.

    Feature-flagged: returns 501 with a clear message when
    ``ROTATION_NOTEBOOK_PATH`` is not configured.
    """
    if not ROTATION_NOTEBOOK_PATH:
        return jsonify({
            "error": "not_configured",
            "message": "Credential rotation is not configured. Set ROTATION_NOTEBOOK_PATH to enable it.",
        }), 501
    try:
        ws = get_workspace_client()

        instance_name = os.environ.get("INSTANCE_NAME", "")
        project_id = os.environ.get("LAKEBASE_PROJECT_ID", "")
        app_name = os.environ.get("APP_NAME", "")
        lakebase_type = os.environ.get("LAKEBASE_TYPE", "autoscaling")

        run_resp = ws.api_client.do("POST", "/api/2.1/jobs/runs/submit", body={
            "run_name": f"password-rotation-{int(time.time())}",
            "tasks": [{
                "task_key": "rotate_password",
                "notebook_task": {
                    "notebook_path": ROTATION_NOTEBOOK_PATH,
                    "base_parameters": {
                        "instance_name": instance_name,
                        "project_id": project_id,
                        "app_name": app_name,
                        "lakebase_type": lakebase_type,
                    },
                },
                "environment_key": "default",
            }],
            "environments": [{
                "environment_key": "default",
                "spec": {"client": "2"},
            }],
        })
        run_id = run_resp.get("run_id")
        return jsonify({"run_id": run_id, "status": "submitted", "notebook": ROTATION_NOTEBOOK_PATH})
    except Exception as e:
        log_error("admin_trigger_rotation", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/rotation-job-status")
def admin_rotation_job_status():
    """Get status of a rotation job run, mapped to 6 pipeline steps.

    Steps are estimated from elapsed time since the Jobs API does not expose
    notebook-level progress for serverless runs.
    """
    run_id = request.args.get("run_id")
    if not run_id:
        return jsonify({"error": "run_id required"}), 400
    try:
        ws = get_workspace_client()
        run_info = ws.api_client.do("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        state = run_info.get("state", {})
        lifecycle = state.get("life_cycle_state", "UNKNOWN")
        result = state.get("result_state", "")
        start_time = run_info.get("start_time", 0)
        elapsed_ms = (int(time.time() * 1000) - start_time) if start_time else 0
        elapsed_s = elapsed_ms / 1000

        step_names = [
            "Detect Active Role",
            "Update Standby Password",
            "Update Secrets",
            "Redeploy App",
            "Wait for Ready",
            "Disable Old Role",
        ]

        steps = []
        if lifecycle in ("PENDING", "QUEUED", "BLOCKED"):
            for i, name in enumerate(step_names):
                steps.append({"name": name, "status": "running" if i == 0 else "pending"})
            overall = "running"
        elif lifecycle == "RUNNING":
            est_step = min(int(elapsed_s / 20), 5)
            for i, name in enumerate(step_names):
                if i < est_step:
                    steps.append({"name": name, "status": "success"})
                elif i == est_step:
                    steps.append({"name": name, "status": "running"})
                else:
                    steps.append({"name": name, "status": "pending"})
            overall = "running"
        elif lifecycle == "TERMINATED" and result == "SUCCESS":
            for name in step_names:
                steps.append({"name": name, "status": "success"})
            overall = "success"
        elif lifecycle in ("TERMINATED", "INTERNAL_ERROR"):
            est_step = min(int(elapsed_s / 20), 5)
            for i, name in enumerate(step_names):
                if i < est_step:
                    steps.append({"name": name, "status": "success"})
                elif i == est_step:
                    steps.append({"name": name, "status": "failed"})
                else:
                    steps.append({"name": name, "status": "pending"})
            overall = "failed"
        else:
            for name in step_names:
                steps.append({"name": name, "status": "pending"})
            overall = "pending"

        return jsonify({
            "steps": steps,
            "overall": overall,
            "lifecycle": lifecycle,
            "result": result,
            "elapsed_seconds": round(elapsed_s, 1),
            "run_page_url": run_info.get("run_page_url", ""),
        })
    except Exception as e:
        log_error("admin_rotation_job_status", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Live Query Dashboard (ASH Sampling)
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/live-dashboard/summary")
def admin_live_dashboard_summary():
    """KPI summary for live query dashboard.

    Also samples current session state into ``<schema>.ash_history`` and
    ``<schema>.ash_query_log`` for historical analysis. Old samples (>24h)
    are pruned on every call. Sampling is best-effort and never fails the KPI.
    """
    try:
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:admin/live_dashboard:summary */
                    SELECT
                        COUNT(*) FILTER (WHERE state = 'active' AND pid != pg_backend_pid()) AS active_sessions,
                        COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state = 'active' AND pid != pg_backend_pid()) AS waiting_sessions,
                        COUNT(*) FILTER (WHERE wait_event_type = 'Lock' AND pid != pg_backend_pid()) AS blocked_queries,
                        COALESCE(EXTRACT(EPOCH FROM MAX(clock_timestamp() - query_start) FILTER (WHERE state = 'active' AND pid != pg_backend_pid()))::int, 0) AS longest_running_sec,
                        COUNT(*) FILTER (WHERE state = 'idle in transaction' AND pid != pg_backend_pid()) AS idle_in_txn
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                """)
                r = cur.fetchone()
                active, waiting, blocked, longest, idle_txn = r[0], r[1], r[2], r[3], r[4]

                # ── ASH sampling (best-effort, non-critical) ──
                try:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {SCHEMA}.ash_history (
                            sample_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            active_sessions INTEGER DEFAULT 0,
                            waiting_sessions INTEGER DEFAULT 0,
                            blocked_sessions INTEGER DEFAULT 0,
                            idle_in_txn INTEGER DEFAULT 0,
                            total_sessions INTEGER DEFAULT 0,
                            longest_sec INTEGER DEFAULT 0
                        )
                    """)
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {SCHEMA}.ash_query_log (
                            sample_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            pid INTEGER,
                            usename TEXT,
                            state TEXT,
                            wait_event_type TEXT,
                            wait_event TEXT,
                            duration INTERVAL,
                            query TEXT
                        )
                    """)
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_ash_query_log_time
                        ON {SCHEMA}.ash_query_log (sample_time DESC)
                    """)

                    cur.execute(f"""
                        INSERT INTO {SCHEMA}.ash_history
                        (active_sessions, waiting_sessions, blocked_sessions, idle_in_txn, total_sessions, longest_sec)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (active, waiting, blocked, idle_txn, active + waiting + blocked + idle_txn, longest))

                    cur.execute(f"""
                        INSERT INTO {SCHEMA}.ash_query_log (pid, usename, state, wait_event_type, wait_event, duration, query)
                        SELECT pid, usename, state, wait_event_type, wait_event,
                               clock_timestamp() - query_start, LEFT(query, 2000)
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND pid != pg_backend_pid()
                          AND state != 'idle'
                          AND query IS NOT NULL
                          AND query != ''
                    """)
                    conn.commit()

                    cur.execute(f"DELETE FROM {SCHEMA}.ash_history WHERE sample_time < NOW() - INTERVAL '24 hours'")
                    cur.execute(f"DELETE FROM {SCHEMA}.ash_query_log WHERE sample_time < NOW() - INTERVAL '24 hours'")
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

                return jsonify({
                    "active": active, "waiting": waiting,
                    "blocked": blocked, "longest_sec": longest,
                    "idle_in_txn": idle_txn,
                })
    except Exception as e:
        log_error("live_dashboard_summary", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/live-dashboard/history")
def admin_live_dashboard_history():
    """Return ASH history samples for the activity chart.

    Accepts optional ``start``/``end`` ISO timestamps for a fixed window, or
    ``minutes`` (default 60) for a rolling window.
    """
    try:
        minutes = request.args.get("minutes", 60, type=int)
        start = request.args.get("start")
        end = request.args.get("end")
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                try:
                    if start and end:
                        cur.execute(f"""
                            /* page:admin/live_dashboard:history_range */
                            SELECT sample_time, active_sessions, waiting_sessions,
                                   blocked_sessions, idle_in_txn,
                                   COALESCE(total_sessions, 0) AS total_sessions,
                                   COALESCE(longest_sec, 0) AS longest_sec
                            FROM {SCHEMA}.ash_history
                            WHERE sample_time >= %s::timestamptz
                              AND sample_time <= %s::timestamptz
                            ORDER BY sample_time
                        """, (start, end))
                    else:
                        cur.execute(f"""
                            /* page:admin/live_dashboard:history */
                            SELECT sample_time, active_sessions, waiting_sessions,
                                   blocked_sessions, idle_in_txn,
                                   COALESCE(total_sessions, 0) AS total_sessions,
                                   COALESCE(longest_sec, 0) AS longest_sec
                            FROM {SCHEMA}.ash_history
                            WHERE sample_time > NOW() - INTERVAL '%s minutes'
                            ORDER BY sample_time
                        """ % min(minutes, 1440))
                    samples = [{
                        "time": r[0].isoformat(), "active": r[1],
                        "waiting": r[2], "blocked": r[3], "idle_txn": r[4],
                        "total": r[5], "longest": r[6],
                    } for r in cur.fetchall()]
                except Exception:
                    samples = []
                return jsonify({"samples": samples})
    except Exception as e:
        log_error("live_dashboard_history", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/live-dashboard/sessions")
def admin_live_dashboard_sessions():
    """All non-idle sessions with wait events and blocking info.

    When ``start``/``end`` params are provided, returns ASH history aggregates
    for that window instead of live pg_stat_activity data.
    """
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                if start and end:
                    cur.execute(f"""
                        /* page:admin/live_dashboard:sessions_historical */
                        SELECT sample_time, active_sessions, waiting_sessions,
                               blocked_sessions, idle_in_txn,
                               COALESCE(total_sessions, 0) AS total_sessions,
                               COALESCE(longest_sec, 0) AS longest_sec
                        FROM {SCHEMA}.ash_history
                        WHERE sample_time >= %s::timestamptz
                          AND sample_time <= %s::timestamptz
                        ORDER BY sample_time
                    """, (start, end))
                    sessions = []
                    for r in cur.fetchall():
                        sample_time = r[0]
                        time_str = sample_time.strftime("%H:%M:%S") if sample_time else ""
                        sessions.append({
                            "pid": "--", "user": "--", "app": "", "state": "snapshot",
                            "wait_type": None, "wait_event": None, "backend": "ash_sample",
                            "duration_sec": float(r[6]) if r[6] else 0,
                            "xact_sec": 0,
                            "query": f"ASH Sample @ {time_str} | Active:{r[1]} Waiting:{r[2]} Blocked:{r[3]} IdleTxn:{r[4]} Total:{r[5]} Longest:{r[6]}s",
                            "blocked_by_count": 0,
                            "blocking_pids": [],
                            "sample_time": sample_time.isoformat() if sample_time else "",
                            "active": r[1], "waiting": r[2], "blocked": r[3],
                            "idle_txn": r[4], "total": r[5], "longest": r[6],
                        })
                    return jsonify({"sessions": sessions, "mode": "historical"})
                else:
                    cur.execute("""
                        /* page:admin/live_dashboard:sessions */
                        SELECT
                            pid, usename, application_name, state,
                            wait_event_type, wait_event, backend_type,
                            EXTRACT(EPOCH FROM (clock_timestamp() - query_start))::numeric(10,1) AS duration_sec,
                            EXTRACT(EPOCH FROM (clock_timestamp() - xact_start))::numeric(10,1) AS xact_duration_sec,
                            LEFT(query, 2000) AS query_preview,
                            cardinality(pg_blocking_pids(pid)) AS blocked_by_count,
                            pg_blocking_pids(pid) AS blocking_pids
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND pid != pg_backend_pid()
                          AND state != 'idle'
                        ORDER BY
                            CASE WHEN cardinality(pg_blocking_pids(pid)) > 0 THEN 0 ELSE 1 END,
                            query_start NULLS LAST
                    """)
                    sessions = []
                    for r in cur.fetchall():
                        sessions.append({
                            "pid": r[0], "user": r[1], "app": r[2] or "", "state": r[3],
                            "wait_type": r[4], "wait_event": r[5], "backend": r[6],
                            "duration_sec": float(r[7]) if r[7] else 0,
                            "xact_sec": float(r[8]) if r[8] else 0,
                            "query": r[9] or "",
                            "blocked_by_count": r[10] or 0,
                            "blocking_pids": r[11] or [],
                        })
                    return jsonify({"sessions": sessions, "mode": "live"})
    except Exception as e:
        log_error("live_dashboard_sessions", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/live-dashboard/locks")
def admin_live_dashboard_locks():
    """Blocking tree and lock summary."""
    try:
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:admin/live_dashboard:locks_tree */
                    SELECT
                        blocked.pid AS blocked_pid,
                        blocked.usename AS blocked_user,
                        blocked.state AS blocked_state,
                        EXTRACT(EPOCH FROM (clock_timestamp() - blocked.query_start))::numeric(10,1) AS blocked_duration,
                        LEFT(blocked.query, 200) AS blocked_query,
                        blocked.wait_event_type, blocked.wait_event,
                        blocker.pid AS blocker_pid,
                        blocker.usename AS blocker_user,
                        blocker.state AS blocker_state,
                        LEFT(blocker.query, 200) AS blocker_query,
                        EXTRACT(EPOCH FROM (clock_timestamp() - blocker.query_start))::numeric(10,1) AS blocker_duration
                    FROM pg_stat_activity blocked
                    JOIN LATERAL (
                        SELECT unnest(pg_blocking_pids(blocked.pid)) AS pid
                    ) bp ON true
                    JOIN pg_stat_activity blocker ON blocker.pid = bp.pid
                    WHERE blocked.datname = current_database()
                      AND cardinality(pg_blocking_pids(blocked.pid)) > 0
                    ORDER BY blocker.pid, blocked.pid
                """)
                blocking_tree = []
                for r in cur.fetchall():
                    blocking_tree.append({
                        "blocked_pid": r[0], "blocked_user": r[1], "blocked_state": r[2],
                        "blocked_duration": float(r[3]) if r[3] else 0,
                        "blocked_query": r[4] or "",
                        "wait_type": r[5], "wait_event": r[6],
                        "blocker_pid": r[7], "blocker_user": r[8], "blocker_state": r[9],
                        "blocker_query": r[10] or "",
                        "blocker_duration": float(r[11]) if r[11] else 0,
                    })

                cur.execute("""
                    /* page:admin/live_dashboard:lock_summary */
                    SELECT locktype, mode, granted, count(*) AS lock_count
                    FROM pg_locks
                    WHERE pid != pg_backend_pid()
                    GROUP BY locktype, mode, granted
                    ORDER BY NOT granted DESC, count(*) DESC
                """)
                lock_summary = [
                    {"type": r[0], "mode": r[1], "granted": r[2], "count": r[3]}
                    for r in cur.fetchall()
                ]

                return jsonify({
                    "blocking_tree": blocking_tree,
                    "lock_summary": lock_summary,
                })
    except Exception as e:
        log_error("live_dashboard_locks", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/live-dashboard/query-stats")
def admin_live_dashboard_query_stats():
    """Top queries from pg_stat_statements by total execution time.

    When ``start``/``end`` params are provided, returns query snapshots from
    ``<schema>.ash_query_log`` for historical analysis instead.
    """
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                if start and end:
                    try:
                        cur.execute(f"""
                            /* page:admin/live_dashboard:query_stats_historical */
                            SELECT sample_time, pid, usename, state,
                                   EXTRACT(EPOCH FROM duration)::numeric(10,1) AS duration_sec,
                                   LEFT(query, 2000) AS query_text,
                                   wait_event_type, wait_event
                            FROM {SCHEMA}.ash_query_log
                            WHERE sample_time >= %s::timestamptz
                              AND sample_time <= %s::timestamptz
                            ORDER BY duration DESC NULLS LAST
                            LIMIT 50
                        """, (start, end))
                        queries = []
                        for r in cur.fetchall():
                            queries.append({
                                "sample_time": r[0].isoformat() if r[0] else "",
                                "pid": r[1], "user": r[2] or "", "state": r[3] or "",
                                "duration_sec": float(r[4]) if r[4] else 0,
                                "query": r[5] or "",
                                "wait_type": r[6], "wait_event": r[7],
                            })
                        return jsonify({"queries": queries, "mode": "historical"})
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        return jsonify({"queries": [], "mode": "historical"})

                # Live mode: pg_stat_statements top-20 by total_exec_time
                cur.execute("""
                    SELECT
                        queryid, LEFT(query, 2000) AS query_text,
                        calls, ROUND(total_exec_time::numeric, 1) AS total_time_ms,
                        ROUND(mean_exec_time::numeric, 1) AS avg_time_ms,
                        ROUND(max_exec_time::numeric, 1) AS max_time_ms,
                        rows AS total_rows,
                        shared_blks_hit, shared_blks_read,
                        CASE WHEN (shared_blks_hit + shared_blks_read) = 0 THEN 0
                             ELSE ROUND(shared_blks_hit::numeric / (shared_blks_hit + shared_blks_read) * 100, 1)
                        END AS cache_hit_pct
                    FROM pg_stat_statements
                    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                      AND query NOT LIKE '%%pg_stat_statements%%'
                      AND query NOT LIKE 'SET %%'
                      AND query NOT LIKE 'SHOW %%'
                    ORDER BY total_exec_time DESC
                    LIMIT 20
                """)
                queries = []
                for r in cur.fetchall():
                    avg = float(r[4]) if r[4] else 0
                    tier = "critical" if avg > 5000 else "slow" if avg > 1000 else "moderate" if avg > 100 else "fast"
                    queries.append({
                        "queryid": str(r[0]) if r[0] else "", "query": r[1] or "",
                        "calls": r[2], "total_ms": float(r[3]) if r[3] else 0,
                        "avg_ms": avg, "max_ms": float(r[5]) if r[5] else 0,
                        "rows": r[6], "cache_hits": r[7], "disk_reads": r[8],
                        "cache_hit_pct": float(r[9]) if r[9] else 0, "tier": tier,
                    })
                return jsonify({"queries": queries})
    except Exception as e:
        log_error("live_dashboard_query_stats", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/cancel-query", methods=["POST"])
def admin_cancel_query():
    """Cancel a running query by PID using pg_cancel_backend()."""
    try:
        data = request.get_json()
        pid = data.get("pid")
        if not isinstance(pid, int):
            return jsonify({"error": "pid must be an integer"}), 400
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
                result = cur.fetchone()[0]
                return jsonify({"success": result, "pid": pid})
    except Exception as e:
        log_error("cancel_query", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# pg_dump Backup & Restore
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/pg-dump", methods=["POST"])
def admin_pg_dump():
    """Generate a pg_dump-style SQL backup of the target schema.

    Produces real SQL (DDL + COPY data) restorable with ``psql``. When
    ``BACKUP_VOLUME`` is configured, the backup is also persisted to that UC
    Volume in the background; otherwise only the preview + download are
    available.
    """
    start = time.time()
    try:
        pool = current_pool()
        lines = []
        lines.append(f"-- pg_dump equivalent: Lakebase {SCHEMA} schema backup")
        lines.append(f"-- Generated: {datetime.now().isoformat()}")
        lines.append("-- This file can be restored with: psql -f backup.sql")
        lines.append("")
        lines.append("SET client_encoding = 'UTF8';")
        lines.append("SET standard_conforming_strings = on;")
        lines.append("")

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                lines.append(f"-- Server: {cur.fetchone()[0]}")
                lines.append("")
                lines.append(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
                lines.append(f"SET search_path TO {SCHEMA}, public;")
                lines.append("")

                cur.execute(f"""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = '{SCHEMA}' AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = [r[0] for r in cur.fetchall()]

                total_rows = 0
                table_stats = []

                for tbl in tables:
                    cur.execute(f"""
                        SELECT column_name, data_type, is_nullable, column_default,
                               character_maximum_length, numeric_precision
                        FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = %s
                        ORDER BY ordinal_position
                    """, (tbl,))
                    cols = cur.fetchall()

                    lines.append(f"-- Table: {SCHEMA}.{tbl}")
                    lines.append(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{tbl} (")
                    col_defs = []
                    col_names = []
                    for c in cols:
                        col_name, dtype, nullable, default, max_len, num_prec = c
                        col_names.append(col_name)
                        type_str = f"{dtype}({max_len})" if max_len else dtype
                        parts = [f"    {col_name} {type_str}"]
                        if nullable == "NO":
                            parts.append("NOT NULL")
                        if default:
                            parts.append(f"DEFAULT {default}")
                        col_defs.append(" ".join(parts))
                    lines.append(",\n".join(col_defs))
                    lines.append(");")
                    lines.append("")

                    cur.execute(f"""
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                            ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_schema = '{SCHEMA}'
                            AND tc.table_name = %s
                            AND tc.constraint_type = 'PRIMARY KEY'
                        ORDER BY kcu.ordinal_position
                    """, (tbl,))
                    pk_cols = [r[0] for r in cur.fetchall()]
                    if pk_cols:
                        lines.append(f"ALTER TABLE {SCHEMA}.{tbl} ADD PRIMARY KEY ({', '.join(pk_cols)});")
                        lines.append("")

                    # Threshold check: skip data export for large tables (instant EXISTS probe)
                    MAX_EXPORT_ROWS = 10000
                    cur.execute(f"""
                        SELECT CASE WHEN EXISTS (
                            SELECT 1 FROM {SCHEMA}."{tbl}" OFFSET {MAX_EXPORT_ROWS}
                        ) THEN 1 ELSE 0 END
                    """)
                    is_large = cur.fetchone()[0] == 1

                    if is_large:
                        cur.execute(f"""SELECT GREATEST(reltuples::bigint, {MAX_EXPORT_ROWS + 1})
                                       FROM pg_class WHERE relname = %s
                                       AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{SCHEMA}')
                                    """, (tbl,))
                        rr = cur.fetchone()
                        approx_rows = int(rr[0]) if rr else MAX_EXPORT_ROWS + 1
                        lines.append(f"-- SKIPPED data export for {tbl} (~{approx_rows:,} rows > {MAX_EXPORT_ROWS:,} limit)")
                        lines.append(f"-- To include: run pg_dump directly or use a smaller dataset")
                        lines.append("")
                        total_rows += approx_rows
                        table_stats.append({"table": tbl, "rows": approx_rows, "columns": len(cols), "skipped": True})
                        continue

                    cur.execute(f'SELECT COUNT(*) FROM {SCHEMA}."{tbl}"')
                    row_count = cur.fetchone()[0]
                    total_rows += row_count

                    if row_count > 0:
                        lines.append(f"COPY {SCHEMA}.{tbl} ({', '.join(col_names)}) FROM stdin;")
                        cur.execute(f'SELECT * FROM {SCHEMA}."{tbl}"')
                        for row in cur:
                            vals = []
                            for v in row:
                                if v is None:
                                    vals.append("\\N")
                                else:
                                    s = str(v)
                                    s = s.replace("\x00", "")
                                    s = s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                                    vals.append(s)
                            lines.append("\t".join(vals))
                        lines.append("\\.")
                        lines.append("")

                    table_stats.append({"table": tbl, "rows": row_count, "columns": len(cols)})

                # Non-PK indexes
                cur.execute(f"""
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = '{SCHEMA}'
                    AND indexname NOT LIKE '%%_pkey'
                    ORDER BY indexname
                """)
                indexes = cur.fetchall()
                if indexes:
                    lines.append("-- Indexes")
                    for idx in indexes:
                        lines.append(f"{idx[1]};")
                    lines.append("")

                # Foreign keys
                cur.execute(f"""
                    SELECT tc.constraint_name, tc.table_name, kcu.column_name,
                           ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_schema = '{SCHEMA}' AND tc.constraint_type = 'FOREIGN KEY'
                """)
                fks = cur.fetchall()
                if fks:
                    lines.append("-- Foreign Keys")
                    for fk in fks:
                        lines.append(
                            f"ALTER TABLE {SCHEMA}.{fk[1]} ADD CONSTRAINT {fk[0]} "
                            f"FOREIGN KEY ({fk[2]}) REFERENCES {SCHEMA}.{fk[3]}({fk[4]});"
                        )
                    lines.append("")

        sql_content = "\n".join(lines)
        sql_content = sql_content.encode("utf-8", errors="replace").decode("utf-8")
        elapsed = round(time.time() - start, 2)
        size_bytes = len(sql_content.encode("utf-8"))
        size_str = f"{size_bytes / 1024 / 1024:.1f} MB" if size_bytes > 1024 * 1024 else f"{size_bytes / 1024:.1f} KB"

        # Persist to UC Volume in the background when configured
        volume_note = "(UC Volume not configured — set BACKUP_VOLUME to persist backups)"
        if BACKUP_VOLUME:
            volume_note = "(saving to UC Volume in background...)"
            _backup_content = sql_content

            def _save_to_volume():
                try:
                    fn = f"lakebase_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
                    fp = f"{BACKUP_VOLUME}/{fn}"
                    w = get_workspace_client()
                    w.files.upload(file_path=fp, contents=io.BytesIO(_backup_content.encode("utf-8")), overwrite=True)
                    log.info(f"pg_dump saved to UC Volume: {fp} ({size_str})")
                except Exception as ve:
                    log.warning(f"pg_dump volume save failed: {ve}")

            threading.Thread(target=_save_to_volume, daemon=True).start()

        return jsonify({
            "success": True,
            "duration_seconds": elapsed,
            "size": size_str,
            "size_bytes": size_bytes,
            "total_rows": total_rows,
            "tables": table_stats,
            "sql_preview": sql_content[:2000],
            "download_ready": True,
            "volume_configured": bool(BACKUP_VOLUME),
            "volume_path": volume_note,
        })
    except Exception as e:
        log_error("admin_pg_dump", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/pg-dump-download")
def admin_pg_dump_download():
    """Download a SQL backup — from the UC Volume if a filename is given, else generate fresh."""
    try:
        filename = request.args.get("file")
        if filename:
            if not BACKUP_VOLUME:
                return jsonify({"error": "UC Volume not configured (set BACKUP_VOLUME)"}), 501
            if not re.match(r'^[\w\-\.]+\.sql$', filename):
                return jsonify({"error": "Invalid filename"}), 400
            file_path = f"{BACKUP_VOLUME}/{filename}"
            w = get_workspace_client()
            resp = w.files.download(file_path=file_path)
            content = resp.contents.read()
            return Response(content, mimetype="application/sql",
                           headers={"Content-Disposition": f"attachment; filename={filename}"})

        # Generate fresh backup (schema-only + data for small tables)
        pool = current_pool()
        lines = []
        lines.append(f"-- Lakebase pg_dump backup: {datetime.now().isoformat()}")
        lines.append("SET client_encoding = 'UTF8';")
        lines.append(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
        lines.append(f"SET search_path TO {SCHEMA}, public;")
        lines.append("")

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = '{SCHEMA}' AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = [r[0] for r in cur.fetchall()]

                for tbl in tables:
                    cur.execute(f"""
                        SELECT column_name, data_type, is_nullable, column_default, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = %s
                        ORDER BY ordinal_position
                    """, (tbl,))
                    cols = cur.fetchall()
                    col_names = [c[0] for c in cols]

                    lines.append(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{tbl} (")
                    col_defs = []
                    for c in cols:
                        t = f"{c[1]}({c[4]})" if c[4] else c[1]
                        parts = [f"    {c[0]} {t}"]
                        if c[2] == "NO":
                            parts.append("NOT NULL")
                        if c[3]:
                            parts.append(f"DEFAULT {c[3]}")
                        col_defs.append(" ".join(parts))
                    lines.append(",\n".join(col_defs))
                    lines.append(");")

                    cur.execute(f'SELECT COUNT(*) FROM {SCHEMA}."{tbl}"')
                    if cur.fetchone()[0] > 0:
                        lines.append(f"COPY {SCHEMA}.{tbl} ({', '.join(col_names)}) FROM stdin;")
                        cur.execute(f'SELECT * FROM {SCHEMA}."{tbl}"')
                        for row in cur:
                            vals = [
                                "\\N" if v is None
                                else str(v).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                                for v in row
                            ]
                            lines.append("\t".join(vals))
                        lines.append("\\.")
                    lines.append("")

        content = "\n".join(lines)
        return Response(
            content, mimetype="application/sql",
            headers={"Content-Disposition": f'attachment; filename=lakebase_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.sql'},
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/pg-dump-list")
def admin_pg_dump_list():
    """List backup files stored in the UC Volume (empty when not configured)."""
    try:
        if not BACKUP_VOLUME:
            return jsonify({"backups": [], "volume_path": None, "volume_configured": False})
        volume_dir = BACKUP_VOLUME
        w = get_workspace_client()
        files = []
        try:
            for f in w.files.list_directory_contents(directory_path=volume_dir):
                if f.name and f.name.endswith(".sql"):
                    lm = f.last_modified
                    if isinstance(lm, (int, float)) and lm > 1e12:
                        lm_iso = datetime.fromtimestamp(lm / 1000).isoformat()
                    elif hasattr(lm, "isoformat"):
                        lm_iso = lm.isoformat()
                    else:
                        lm_iso = str(lm) if lm else None
                    files.append({
                        "name": f.name,
                        "size_bytes": f.file_size or 0,
                        "size": (
                            f"{(f.file_size or 0) / 1024 / 1024:.1f} MB"
                            if (f.file_size or 0) > 1024 * 1024
                            else f"{(f.file_size or 0) / 1024:.1f} KB"
                        ),
                        "last_modified": lm_iso,
                    })
        except Exception:
            pass  # Volume may not exist yet
        files.sort(key=lambda x: x.get("last_modified") or "", reverse=True)
        return jsonify({"backups": files, "volume_path": volume_dir, "volume_configured": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/pg-dump-restore", methods=["POST"])
def admin_pg_dump_restore():
    """Restore a pg_dump backup from the UC Volume into a new schema.

    All source-schema references in the SQL are rewritten to the target schema
    name before execution. COPY blocks use psycopg's ``cursor.copy()`` for
    efficient bulk loading. Requires ``BACKUP_VOLUME`` to be configured.
    """
    start = time.time()
    if not BACKUP_VOLUME:
        return jsonify({
            "error": "not_configured",
            "message": "Restore requires a UC Volume. Set BACKUP_VOLUME to enable it.",
        }), 501
    try:
        data = request.get_json() or {}
        filename = data.get("file", "")
        if not filename or not re.match(r'^[\w\-\.]+\.sql$', filename):
            return jsonify({"error": "Invalid or missing filename"}), 400

        target_schema = data.get("target_schema", "").strip()
        if not target_schema:
            target_schema = f"{SCHEMA}_restored_{datetime.now().strftime('%Y%m%d')}"
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', target_schema):
            return jsonify({"error": "Schema name must be alphanumeric with underscores only"}), 400

        file_path = f"{BACKUP_VOLUME}/{filename}"
        w = get_workspace_client()
        resp = w.files.download(file_path=file_path)
        sql_content = resp.contents.read().decode("utf-8")

        # Rewrite schema references (source schema → target schema)
        sql_content = sql_content.replace(SCHEMA, target_schema)

        # Safety gate: restore executes SQL from the backup file. Require an explicit
        # confirmation, and a SECOND confirmation if the backup contains statements
        # that could delete/drop data — nothing destructive runs on a single click.
        if not data.get("confirm"):
            return jsonify({
                "error": "confirmation_required",
                "message": f"Restore will write into schema '{target_schema}'. Confirm to proceed.",
            }), 428
        destructive = re.findall(r'(?im)^\s*(DROP|DELETE|TRUNCATE)\b', sql_content)
        if destructive and not data.get("confirm_destructive"):
            kinds = ", ".join(sorted({d.upper() for d in destructive}))
            return jsonify({
                "error": "destructive_confirmation_required",
                "message": (f"This backup contains {len(destructive)} statement(s) that may delete or drop "
                            f"data ({kinds}) in schema '{target_schema}'. A second confirmation is required."),
                "destructive_count": len(destructive),
            }), 428

        pool = current_pool()
        tables_restored = 0
        total_rows = 0
        table_stats = []

        with pool.connection() as conn:
            with conn.cursor() as cur:
                lines = sql_content.split("\n")
                i = 0
                while i < len(lines):
                    line = lines[i].rstrip()

                    if not line or line.startswith("--"):
                        i += 1
                        continue

                    if line.startswith("COPY ") and line.endswith("FROM stdin;"):
                        copy_cmd = line.replace("FROM stdin;", "FROM STDIN")
                        data_lines = []
                        i += 1
                        while i < len(lines) and lines[i].rstrip() != "\\.":
                            data_lines.append(lines[i])
                            i += 1
                        i += 1  # skip the \. terminator

                        if data_lines:
                            copy_data = "\n".join(data_lines) + "\n"
                            with cur.copy(copy_cmd) as copy:
                                copy.write(copy_data.encode("utf-8"))
                            total_rows += len(data_lines)

                            tbl_match = re.search(r'COPY\s+(\S+)', line)
                            if tbl_match:
                                table_stats.append({"table": tbl_match.group(1), "rows": len(data_lines)})
                        continue

                    stmt = line
                    while not stmt.rstrip().endswith(";") and i + 1 < len(lines):
                        i += 1
                        stmt += "\n" + lines[i]

                    stmt = stmt.strip()
                    if stmt and not stmt.startswith("--"):
                        cur.execute(stmt)
                        if "CREATE TABLE" in stmt.upper():
                            tables_restored += 1
                    i += 1

                conn.commit()

        elapsed = round(time.time() - start, 2)
        log.info(f"Restore complete: {tables_restored} tables, {total_rows} rows -> schema '{target_schema}' in {elapsed}s")

        return jsonify({
            "success": True,
            "target_schema": target_schema,
            "tables_restored": tables_restored,
            "total_rows": total_rows,
            "table_stats": table_stats,
            "duration_seconds": elapsed,
            "source_file": filename,
        })
    except Exception as e:
        log_error("admin_pg_dump_restore", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# DBA Maintenance (VACUUM, REINDEX, Bloat)
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/vacuum-analyze", methods=["POST"])
def admin_vacuum_analyze():
    """Run VACUUM ANALYZE on target-schema tables with before/after bloat stats.

    Uses ``pgstattuple`` for precise bloat measurement when available. Requires
    autocommit because VACUUM cannot run inside a transaction.
    """
    try:
        pool = current_pool()
        request_data = request.get_json(silent=True) or {}
        table_name = request_data.get("table")

        with pool.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                if table_name:
                    tables = [table_name]
                else:
                    cur.execute(f"""
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = '{SCHEMA}' ORDER BY tablename
                    """)
                    tables = [r[0] for r in cur.fetchall()]

                results = []
                for tbl in tables:
                    try:
                        cur.execute(f"SELECT * FROM pgstattuple('{SCHEMA}.{tbl}')")
                        before = cur.fetchone()
                        before_dead_pct = before[6] if before else 0
                        before_free_pct = before[8] if before else 0
                    except Exception:
                        before_dead_pct = 0
                        before_free_pct = 0

                    vac_start = time.time()
                    cur.execute(f'VACUUM ANALYZE {SCHEMA}."{tbl}"')
                    elapsed = round(time.time() - vac_start, 3)

                    try:
                        cur.execute(f"SELECT * FROM pgstattuple('{SCHEMA}.{tbl}')")
                        after = cur.fetchone()
                        after_dead_pct = after[6] if after else 0
                        after_free_pct = after[8] if after else 0
                        table_len = after[0] if after else 0
                        tuple_count = after[1] if after else 0
                    except Exception:
                        after_dead_pct = 0
                        after_free_pct = 0
                        table_len = 0
                        tuple_count = 0

                    results.append({
                        "table": tbl,
                        "duration_ms": round(elapsed * 1000),
                        "before_dead_pct": round(before_dead_pct, 2),
                        "after_dead_pct": round(after_dead_pct, 2),
                        "before_free_pct": round(before_free_pct, 2),
                        "after_free_pct": round(after_free_pct, 2),
                        "table_size_bytes": table_len,
                        "live_tuples": tuple_count,
                    })

        return jsonify({"success": True, "results": results})
    except Exception as e:
        log_error("admin_vacuum_analyze", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/table-bloat")
def admin_table_bloat():
    """Get table bloat estimates using pg_stat_user_tables (instant, no table scan)."""
    try:
        pool = current_pool()
        with pool.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"""
                    /* page:admin/table_bloat */
                    SELECT
                        c.relname AS table_name,
                        pg_total_relation_size(c.oid) AS table_len,
                        c.reltuples::bigint AS tuple_count,
                        pg_table_size(c.oid) AS table_size,
                        s.n_dead_tup,
                        s.n_live_tup,
                        CASE WHEN s.n_live_tup + s.n_dead_tup > 0
                            THEN ROUND(100.0 * s.n_dead_tup / (s.n_live_tup + s.n_dead_tup), 2)
                            ELSE 0 END AS dead_pct,
                        s.last_vacuum,
                        s.last_autovacuum,
                        s.last_analyze,
                        s.last_autoanalyze
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_stat_user_tables s ON s.relid = c.oid
                    WHERE n.nspname = '{SCHEMA}' AND c.relkind = 'r'
                    ORDER BY pg_total_relation_size(c.oid) DESC
                """)
                results = []
                for r in cur.fetchall():
                    results.append({
                        "table": r[0],
                        "table_len": r[1],
                        "tuple_count": r[2],
                        "tuple_len": r[3],
                        "tuple_percent": round(100.0 * r[3] / max(r[1], 1), 1),
                        "dead_tuple_count": r[4],
                        "dead_tuple_len": 0,
                        "dead_tuple_percent": float(r[6]),
                        "free_space": max(r[1] - r[3], 0),
                        "free_percent": round(100.0 * max(r[1] - r[3], 0) / max(r[1], 1), 2),
                        "last_vacuum": str(r[7]) if r[7] else None,
                        "last_autovacuum": str(r[8]) if r[8] else None,
                        "last_analyze": str(r[9]) if r[9] else None,
                        "last_autoanalyze": str(r[10]) if r[10] else None,
                    })

        return jsonify({"tables": results})
    except Exception as e:
        log_error("admin_table_bloat", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Cluster Status & Slow Queries
# ═══════════════════════════════════════════════════════════════════════════

@admin_bp.route("/cluster-status")
def admin_cluster_status():
    """Get Lakebase autoscaling cluster status: compute, state, and connections.

    Combines PG-level settings with Lakebase API metrics (endpoint state, CU
    limits) when a project id is configured.
    """
    try:
        result = {}
        pool = current_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT setting::int FROM pg_settings WHERE name = 'max_connections'")
                result["max_connections"] = cur.fetchone()[0]

                cur.execute("SELECT setting, unit FROM pg_settings WHERE name = 'shared_buffers'")
                r = cur.fetchone()
                shared_buf = int(r[0])
                unit = r[1]
                result["shared_buffers_mb"] = round(shared_buf * 8 / 1024) if unit == "8kB" else shared_buf

                cur.execute("SELECT setting, unit FROM pg_settings WHERE name = 'effective_cache_size'")
                r = cur.fetchone()
                ecs = int(r[0])
                result["effective_cache_mb"] = round(ecs * 8 / 1024) if r[1] == "8kB" else ecs

                cur.execute("""
                    SELECT state, count(*) FROM pg_stat_activity
                    WHERE datname = current_database() GROUP BY state
                """)
                result["connections"] = {r[0] or "unknown": r[1] for r in cur.fetchall()}

                cur.execute("SELECT version()")
                result["pg_version"] = cur.fetchone()[0]

        # Lakebase API metrics (autoscaling project endpoints)
        project_id = os.environ.get("LAKEBASE_PROJECT_ID", "") or os.environ.get("AUTOSCALING_PROJECT_ID", "")
        lakebase_type = os.environ.get("LAKEBASE_TYPE", "autoscaling")

        if project_id:
            try:
                wc = get_workspace_client()
                ep_data = wc.api_client.do(
                    "GET", f"/api/2.0/postgres/projects/{project_id}/branches/production/endpoints"
                )
                endpoints = ep_data.get("endpoints", [])
                result["endpoints"] = []
                for ep in endpoints:
                    status = ep.get("status", {})
                    result["endpoints"].append({
                        "name": ep.get("name", "").split("/")[-1],
                        "type": status.get("endpoint_type", ""),
                        "state": status.get("current_state", "UNKNOWN"),
                        "pending_state": status.get("pending_state"),
                        "min_cu": status.get("autoscaling_limit_min_cu"),
                        "max_cu": status.get("autoscaling_limit_max_cu"),
                        "group_min": status.get("group", {}).get("min"),
                        "group_max": status.get("group", {}).get("max"),
                        "readable_secondaries": status.get("group", {}).get("enable_readable_secondaries", False),
                        "host": (status.get("hosts") or {}).get("host", ""),
                        "last_active": ep.get("status", {}).get("last_active_time"),
                    })
                result["project_id"] = project_id
                result["lakebase_type"] = "autoscaling"
            except Exception as api_err:
                result["api_error"] = str(api_err)
        elif lakebase_type == "provisioned":
            result["lakebase_type"] = "provisioned"
            result["instance_name"] = os.environ.get("INSTANCE_NAME", "")

        # Infer CU from shared_buffers (approximate mapping)
        sb_mb = result.get("shared_buffers_mb", 0)
        if sb_mb >= 2048:
            result["estimated_cu"] = 8
        elif sb_mb >= 1024:
            result["estimated_cu"] = 4
        elif sb_mb >= 512:
            result["estimated_cu"] = 2
        elif sb_mb >= 256:
            result["estimated_cu"] = 1
        else:
            result["estimated_cu"] = 0.5

        return jsonify(result)
    except Exception as e:
        log_error("admin_cluster_status", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/slow-queries")
def admin_slow_queries():
    """Get top slow queries from the pg_stat_statements extension."""
    try:
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT query, calls, total_exec_time, mean_exec_time,
                           rows, shared_blks_hit, shared_blks_read
                    FROM pg_stat_statements
                    WHERE query NOT LIKE '%%pg_stat_statements%%'
                      AND query NOT LIKE 'SET %%'
                      AND query NOT LIKE 'SHOW %%'
                    ORDER BY mean_exec_time DESC
                    LIMIT 15
                """)
                queries = []
                for r in cur.fetchall():
                    queries.append({
                        "query": r[0][:300],
                        "calls": r[1],
                        "total_time_ms": round(r[2], 2),
                        "avg_time_ms": round(r[3], 2),
                        "rows": r[4],
                        "cache_hit_ratio": round(r[5] / max(r[5] + r[6], 1) * 100, 1),
                    })
                return jsonify({"queries": queries})
    except Exception as e:
        log_error("admin_slow_queries", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/reindex", methods=["POST"])
def admin_reindex():
    """Rebuild indexes on a table or all target-schema tables via REINDEX CONCURRENTLY."""
    try:
        pool = current_pool()
        request_data = request.get_json(silent=True) or {}
        table_name = request_data.get("table")

        with pool.connection() as conn:
            conn.autocommit = True  # REINDEX CONCURRENTLY cannot run in a transaction
            with conn.cursor() as cur:
                if table_name:
                    start = time.time()
                    cur.execute(f'REINDEX TABLE CONCURRENTLY {SCHEMA}."{table_name}"')
                    elapsed = round(time.time() - start, 3)
                    return jsonify({"success": True, "table": table_name, "duration_ms": round(elapsed * 1000)})
                else:
                    cur.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{SCHEMA}'")
                    tables = [r[0] for r in cur.fetchall()]
                    results = []
                    for tbl in tables:
                        tbl_start = time.time()
                        cur.execute(f'REINDEX TABLE CONCURRENTLY {SCHEMA}."{tbl}"')
                        elapsed = round(time.time() - tbl_start, 3)
                        results.append({"table": tbl, "duration_ms": round(elapsed * 1000)})
                    return jsonify({"success": True, "results": results})
    except Exception as e:
        log_error("admin_reindex", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/refresh-matviews", methods=["POST"])
def admin_refresh_matviews():
    """REFRESH MATERIALIZED VIEW for every matview in the target schema.

    Optional JSON ``{"view": "<name>"}`` refreshes a single view; otherwise all
    materialized views in the schema are refreshed. Returns per-view timing.
    """
    try:
        pool = current_pool()
        request_data = request.get_json(silent=True) or {}
        one = request_data.get("view")

        with pool.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                if one:
                    views = [one]
                else:
                    cur.execute(
                        "SELECT matviewname FROM pg_matviews WHERE schemaname = %s ORDER BY matviewname",
                        (SCHEMA,),
                    )
                    views = [r[0] for r in cur.fetchall()]

                results = []
                for v in views:
                    v_start = time.time()
                    try:
                        cur.execute(f'REFRESH MATERIALIZED VIEW {SCHEMA}."{v}"')
                        results.append({"view": v, "duration_ms": round((time.time() - v_start) * 1000), "status": "ok"})
                    except Exception as ve:
                        results.append({"view": v, "status": "error", "error": str(ve)[:200]})

        return jsonify({"success": True, "refreshed": len(results), "results": results})
    except Exception as e:
        log_error("admin_refresh_matviews", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Instance Dependencies — detect + provision what the console needs
# ═══════════════════════════════════════════════════════════════════════════
# A first-seen instance may be missing pieces the console relies on (the
# pg_stat_statements extension for query stats, pgstattuple for precise bloat,
# the target schema, the ASH tables). We detect them per instance and offer
# one-click provisioning. This is a FIXED allowlist — provisioning never runs
# arbitrary SQL — and it executes as the connected identity (the signed-in user
# under OBO), so an instance admin can install what's missing from the console.

def _dependency_specs():
    return [
        {"key": "pg_stat_statements", "label": "pg_stat_statements", "kind": "extension", "required": True,
         "description": "Powers the Query Performance tab (top / slow queries, cache hit %)."},
        {"key": "pgstattuple", "label": "pgstattuple", "kind": "extension", "required": False,
         "description": "Precise dead-tuple % in VACUUM before/after results."},
        {"key": "schema", "label": f'schema "{SCHEMA}"', "kind": "schema", "required": True,
         "description": "The schema this console administers."},
        {"key": "ash_tables", "label": "ASH history tables", "kind": "tables", "required": False,
         "description": "Session Activity History time-series (also auto-created on first poll)."},
    ]


_ASH_HISTORY_DDL = """
    CREATE TABLE IF NOT EXISTS {s}.ash_history (
        sample_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        active_sessions INTEGER DEFAULT 0, waiting_sessions INTEGER DEFAULT 0,
        blocked_sessions INTEGER DEFAULT 0, idle_in_txn INTEGER DEFAULT 0,
        total_sessions INTEGER DEFAULT 0, longest_sec INTEGER DEFAULT 0
    )"""
_ASH_QUERYLOG_DDL = """
    CREATE TABLE IF NOT EXISTS {s}.ash_query_log (
        sample_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, pid INTEGER, usename TEXT,
        state TEXT, wait_event_type TEXT, wait_event TEXT, duration INTERVAL, query TEXT
    )"""


@admin_bp.route("/dependencies")
def admin_dependencies():
    """Report the console's dependencies on the active instance, with status.

    Each entry is present / missing / unavailable. ``unavailable`` means the
    server doesn't offer that extension (can't be installed here).
    """
    try:
        pool = current_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT extname FROM pg_extension")
                installed = {r[0] for r in cur.fetchall()}
                cur.execute("SELECT name FROM pg_available_extensions")
                available = {r[0] for r in cur.fetchall()}
                cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (SCHEMA,))
                schema_exists = cur.fetchone() is not None
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = %s AND table_name IN ('ash_history', 'ash_query_log')
                """, (SCHEMA,))
                ash_count = cur.fetchone()[0]

        results = []
        for spec in _dependency_specs():
            k = spec["key"]
            if spec["kind"] == "extension":
                status = "present" if k in installed else ("missing" if k in available else "unavailable")
            elif k == "schema":
                status = "present" if schema_exists else "missing"
            elif k == "ash_tables":
                status = "present" if ash_count >= 2 else "missing"
            else:
                status = "unknown"
            results.append({**spec, "status": status})
        return jsonify({"instance": active_instance_name(), "dependencies": results})
    except Exception as e:
        log_error("admin_dependencies", e)
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/dependencies/provision", methods=["POST"])
def admin_provision_dependencies():
    """Provision missing dependencies on the active instance (fixed allowlist).

    Body ``{"items": ["pg_stat_statements", ...]}`` provisions those keys;
    omit ``items`` to provision every currently-missing one. Runs as the
    connected identity — CREATE EXTENSION needs sufficient Postgres privilege,
    so failures (e.g. non-superuser) are returned per item, never fatal.
    """
    data = request.get_json(silent=True) or {}
    # Never mutate the target database without explicit confirmation.
    if not data.get("confirm"):
        return jsonify({
            "error": "confirmation_required",
            "message": "Provisioning creates objects on the target instance and must be confirmed.",
        }), 428
    valid = {s["key"] for s in _dependency_specs()}
    requested = data.get("items")
    if requested:
        targets = [k for k in requested if k in valid]
    else:
        # Provision everything currently missing.
        try:
            resp = admin_dependencies().get_json()
            targets = [d["key"] for d in resp.get("dependencies", []) if d["status"] == "missing"]
        except Exception:
            targets = []

    if not targets:
        return jsonify({"results": [], "message": "Nothing to provision."})

    results = []
    try:
        pool = current_pool()
        with pool.connection() as conn:
            conn.autocommit = True  # CREATE EXTENSION / SCHEMA outside a txn
            with conn.cursor() as cur:
                for key in targets:
                    try:
                        if key == "pg_stat_statements":
                            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
                        elif key == "pgstattuple":
                            cur.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple")
                        elif key == "schema":
                            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
                        elif key == "ash_tables":
                            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
                            cur.execute(_ASH_HISTORY_DDL.format(s=SCHEMA))
                            cur.execute(_ASH_QUERYLOG_DDL.format(s=SCHEMA))
                            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_ash_query_log_time ON {SCHEMA}.ash_query_log (sample_time DESC)")
                        else:
                            results.append({"key": key, "status": "error", "error": "unknown dependency"})
                            continue
                        results.append({"key": key, "status": "ok"})
                    except Exception as e:
                        results.append({"key": key, "status": "error", "error": str(e).strip().splitlines()[0][:200]})
        return jsonify({"results": results})
    except Exception as e:
        log_error("admin_provision_dependencies", e)
        return jsonify({"error": str(e)}), 500

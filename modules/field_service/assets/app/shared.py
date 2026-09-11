"""
Shared utilities for the Lakebase FSM Flask application.

This module extracts cross-cutting concerns from app.py into a single
importable location so that Flask blueprints (dispatch, genie, admin, etc.)
can access connection pools, caches, configuration, and helper functions
without circular imports back into the main app module.

What it provides
================
* **Logging** — ``log_error()`` ring buffer and ``APP_START_TIME`` for uptime.
* **Connection pools** — Two lazy-initialized psycopg pools: one for
  interactive queries (30 s statement timeout) and a separate analytics pool
  for heavy aggregations (300 s timeout).  Two pools prevent a single slow
  dashboard query from starving interactive API calls.
* **Workspace client** — Lazy ``WorkspaceClient`` singleton.
* **SQL helpers** — ``validate_identifier()`` to prevent injection,
  ``_run_sql()`` for Statement Execution API calls via the workspace client.
* **Query caching** — Thread-safe TTL cache with stale-while-revalidate
  background refresh.  Keeps the dashboard snappy while expensive PG
  aggregations run asynchronously.
* **Table / column caches** — Longer-lived caches for metadata that rarely
  changes at runtime (table lists, column existence checks).
* **Genie config** — Space IDs and the agent serving-endpoint name, all
  sourced from environment variables set in ``app.yaml``.
* **Config loader** — Best-effort loader for ``config.yaml`` with a blocklist
  that prevents secrets from leaking into the architecture panel.

Why it exists
=============
``app.py`` is a 9 000+ line monolith.  Extracting shared state is the first
step toward breaking it into focused blueprint modules.  This file is
deliberately kept *self-contained* (its own imports, no dependency on the
Flask ``app`` object) so blueprints can ``from shared import get_pool``
without importing the entire application.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from psycopg_pool import ConnectionPool

# ---------------------------------------------------------------------------
# Public API — explicitly listed so ``from shared import *`` is predictable.
# ---------------------------------------------------------------------------

__all__ = [
    # Logging
    "APP_START_TIME",
    "log_error",
    "_error_log",
    "_error_log_lock",
    "MAX_ERROR_LOG",
    # Connection pools
    "connection_pool",
    "get_pool",
    "_analytics_pool",
    "get_analytics_pool",
    # Pool configuration constants
    "POOL_MIN_SIZE",
    "POOL_MAX_SIZE",
    "POOL_TIMEOUT",
    "POOL_STMT_TIMEOUT",
    "ANALYTICS_POOL_MIN_SIZE",
    "ANALYTICS_POOL_MAX_SIZE",
    "ANALYTICS_STMT_TIMEOUT",
    # Workspace client
    "workspace_client",
    "get_workspace_client",
    # SQL helpers
    "IDENTIFIER_RE",
    "validate_identifier",
    "_run_sql",
    # Query cache
    "_query_cache",
    "_query_cache_lock",
    "QUERY_CACHE_TTL",
    "_bg_refresh_in_progress",
    "_run_cached_query",
    "_bg_refresh_cache",
    "_get_or_refresh",
    # Shared cached queries
    "get_active_wo_counts_by_tech",
    # Table cache
    "_table_cache",
    "_table_cache_lock",
    "TABLE_CACHE_TTL",
    "get_cached_tables",
    # Column cache
    "_column_exists_cache",
    "_column_exists_lock",
    # Genie / Agent config
    "GENIE_SPACES",
    "AGENT_ENDPOINT_NAME",
    # User identity + RBAC
    "get_current_user",
    "get_user_role_and_regions",
    # PII masking
    "mask_phone",
    "mask_email",
    # Config loader
    "_CONFIG_BLOCKLIST",
    "_config_yaml_cache",
    "_load_config_yaml",
]

# ---------------------------------------------------------------------------
# Structured logging (mirrors app.py setup)
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)

# ── App start time — used by /api/health for uptime calculation ───────────
APP_START_TIME = datetime.now(timezone.utc)

# ── Recent-errors ring buffer ─────────────────────────────────────────────
# Keeps the last MAX_ERROR_LOG errors in memory so the admin panel can show
# recent failures without hitting an external store.
_error_log: list[dict] = []
_error_log_lock = threading.Lock()
MAX_ERROR_LOG = 50


def log_error(source: str, error: object) -> None:
    """Log an error to both stderr and the in-memory ring buffer.

    The ring buffer is capped at ``MAX_ERROR_LOG`` entries.  Each entry
    stores an ISO-8601 timestamp, the source label, and the first 500 chars
    of the error string.
    """
    log.error(f"{source}: {error}")
    with _error_log_lock:
        _error_log.append({
            "time": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "error": str(error)[:500],
        })
        if len(_error_log) > MAX_ERROR_LOG:
            _error_log.pop(0)


# ---------------------------------------------------------------------------
# Genie Space IDs — read from env vars set in app.yaml
# ---------------------------------------------------------------------------

GENIE_SPACES: dict[str, dict] = {
    "postgres": {
        "id": os.environ.get("GENIE_SPACE_POSTGRES", ""),
        "name": "PostgresAdmin",
        "description": "PostgreSQL monitoring and performance data",
    },
    "field_ops": {
        "id": os.environ.get("GENIE_SPACE_FIELD_OPS", ""),
        "name": "Field Service Operations",
        "description": "Work orders, technicians, dispatch, and SLA data",
    },
    "network_health": {
        "id": os.environ.get("GENIE_SPACE_NETWORK_HEALTH", ""),
        "name": "Network Health & Telemetry",
        "description": "Network node health, outages, IoT telemetry, and maintenance risk",
    },
    "sla_workforce": {
        "id": os.environ.get("GENIE_SPACE_SLA_WORKFORCE", ""),
        "name": "SLA & Workforce Analytics",
        "description": "SLA compliance, technician performance, and regional analytics",
    },
}

# Agent serving endpoint (deployed by 08_create_agent.py)
AGENT_ENDPOINT_NAME: str = os.environ.get("AGENT_ENDPOINT_NAME", "")


# ---------------------------------------------------------------------------
# User identity (extracted from Databricks OAuth token)
# ---------------------------------------------------------------------------

_user_cache: dict = {}  # per-request cache (cleared by Flask lifecycle)


def get_current_user() -> dict:
    """Extract the logged-in user from the Databricks OAuth token. Cached per-request.

    Databricks Apps proxies requests with the user's workspace OAuth token
    in the Authorization header. We decode the JWT payload (without
    verification — the proxy already validated it) to get the user email.

    Returns ``{"email": "user@example.com", "name": "User Name"}`` or
    ``{"email": "anonymous", "name": "Anonymous"}`` if no token is present.
    """
    try:
        from flask import request as flask_request
        import base64 as _b64
        import json as _json

        # Check per-request cache (avoid decoding JWT multiple times per request)
        cache_key = id(flask_request._get_current_object())
        if cache_key in _user_cache:
            return _user_cache[cache_key]

        # Method 1: Databricks Apps proxy headers (most reliable)
        for header in ("X-Forwarded-Email", "X-Forwarded-User", "X-Real-User"):
            val = flask_request.headers.get(header, "")
            if val and "@" in val:
                result = {"email": val, "name": val.split("@")[0]}
                _user_cache[cache_key] = result
                return result

        # Method 2: Decode OAuth JWT from Authorization header
        auth = flask_request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
            parts = token.split(".")
            if len(parts) >= 2:
                payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
                data = _json.loads(_b64.urlsafe_b64decode(payload))
                email = data.get("sub", data.get("email", data.get("username", "")))
                name = data.get("name", email.split("@")[0] if email else "")
                if email and "@" in email:
                    result = {"email": email, "name": name}
                    _user_cache[cache_key] = result
                    return result
                # Some tokens use 'sub' as a UUID, not email — check for that
                if email and len(email) > 5:
                    result = {"email": email, "name": name or email[:20]}
                    _user_cache[cache_key] = result
                    return result
    except Exception:
        pass
    return {"email": "anonymous", "name": "Anonymous"}


# Databricks group → app role mapping (configurable via env vars)
# Default: workspace 'admins' group = app admin role
DATABRICKS_GROUP_ROLE_MAP = {
    os.environ.get("RBAC_ADMIN_GROUP", "fsm-admins"): "admin",
    # Workspace admins are app admins. Without this the only admin route is the
    # fsm-admins group, which does not exist on a fresh workspace, so nobody could
    # reach /admin until a row was hand-seeded into ai_memory.app_users.
    "admins": "admin",
    os.environ.get("RBAC_DISPATCHER_GROUP", "fsm-dispatchers"): "dispatcher",
    os.environ.get("RBAC_MANAGER_GROUP", "fsm-managers"): "manager",
}

_groups_cache: dict = {}  # email → (groups, timestamp)
_GROUPS_CACHE_TTL = 300   # 5 minutes


def get_user_databricks_groups(email: str) -> list[str]:
    """Get the user's Databricks workspace group memberships via SCIM API.

    Uses the workspace client's service principal token to query SCIM.
    Results cached for 5 minutes.
    """
    now = time.time()
    cached = _groups_cache.get(email)
    if cached and now - cached[1] < _GROUPS_CACHE_TTL:
        return cached[0]

    groups = []
    try:
        w = get_workspace_client()
        # SCIM filter to find user by email
        from databricks.sdk.service.iam import ListAccountGroupsRequest
        user_list = w.users.list(filter=f'userName eq "{email}"')
        for u in user_list:
            if u.groups:
                groups = [g.display for g in u.groups if g.display]
            break
    except Exception:
        # Fallback: if SCIM fails, return empty (will fall through to DB role)
        pass

    _groups_cache[email] = (groups, now)
    return groups


def get_role_from_groups(email: str) -> str | None:
    """Map Databricks workspace groups to app role.

    Returns the highest-privilege role found, or None if no group matches.
    Priority: admin > manager > dispatcher.
    """
    groups = get_user_databricks_groups(email)
    group_names = set(g.lower() for g in groups)

    for group_name, role in DATABRICKS_GROUP_ROLE_MAP.items():
        if group_name.lower() in group_names:
            if role == "admin":
                return "admin"  # highest, return immediately
    # Check non-admin roles
    for group_name, role in DATABRICKS_GROUP_ROLE_MAP.items():
        if group_name.lower() in group_names and role != "admin":
            return role
    return None


def get_user_role_and_regions() -> tuple[str, list[int] | None]:
    """Get the current user's role and allowed region IDs.

    Checks Databricks workspace groups FIRST (governance-driven),
    falls back to app_users.role in Lakebase if no group match.

    Returns (role, region_ids) where:
      - role: 'admin' | 'manager' | 'dispatcher' | 'user'
      - region_ids: list of allowed region IDs, or None if unrestricted

    Admin and users with no region mapping get None (all regions).
    """
    user = get_current_user()
    email = user.get("email", "anonymous")
    if email == "anonymous":
        return ("user", None)

    # Method 1: Check Databricks workspace groups (governance-driven)
    group_role = get_role_from_groups(email)
    if group_role == "admin":
        return ("admin", None)

    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Method 2: Fall back to app_users.role in Lakebase
                cur.execute("SELECT role FROM ai_memory.app_users WHERE user_id = %s", (email,))
                row = cur.fetchone()
                role = row[0] if row else "user"

                if role == "admin":
                    return ("admin", None)

                # Get region mapping
                cur.execute(
                    "SELECT region_id FROM ai_memory.user_region_mapping WHERE user_id = %s",
                    (email,)
                )
                regions = [r[0] for r in cur.fetchall()]
                return (role, regions if regions else None)
    except Exception:
        return ("user", None)


# ---------------------------------------------------------------------------
# Database connection pools (lazy initialization)
# ---------------------------------------------------------------------------
# Two separate pools exist to isolate workloads:
#
# 1. **Interactive pool** (``get_pool``): min 5 / max 20 connections, 30 s
#    statement timeout.  Used by API routes that return data to the browser.
#    Keeps latency predictable for UI interactions.
#
# 2. **Analytics pool** (``get_analytics_pool``): min 1 / max 4 connections,
#    300 s (5 min) statement timeout.  Used for heavy aggregations that feed
#    the background cache refresh.  A separate pool prevents one slow
#    dashboard query from exhausting connections needed for interactive use.

# -- Interactive pool constants --
POOL_MIN_SIZE = 10
POOL_MAX_SIZE = 40
POOL_TIMEOUT = 30          # seconds to wait for a connection from the pool
POOL_STMT_TIMEOUT = 30000  # milliseconds — PG statement_timeout for interactive queries

# -- Analytics pool constants --
ANALYTICS_POOL_MIN_SIZE = 2
ANALYTICS_POOL_MAX_SIZE = 8
ANALYTICS_STMT_TIMEOUT = 300000  # 5 minutes — heavy aggregations at scale

connection_pool: ConnectionPool | None = None
_analytics_pool: ConnectionPool | None = None


def _build_conninfo() -> str:
    """Build the libpq connection string from environment variables.

    Auth hierarchy: ``PGPASSWORD`` (native PG auth via Databricks Secrets)
    takes precedence, falling back to ``DATABRICKS_TOKEN`` for backwards
    compatibility during development.
    """
    pg_password = os.environ.get("PGPASSWORD") or os.environ.get("DATABRICKS_TOKEN", "")
    return (
        f"dbname={os.environ.get('PGDATABASE')} "
        f"user={os.environ.get('PGUSER')} "
        f"password={pg_password} "
        f"host={os.environ.get('PGHOST')} "
        f"port=5432 sslmode=require"
    )


def get_pool() -> ConnectionPool:
    """Return the interactive connection pool, creating it lazily on first call."""
    global connection_pool
    if connection_pool is None:
        connection_pool = ConnectionPool(
            conninfo=_build_conninfo(),
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
            open=False,
            timeout=POOL_TIMEOUT,
            kwargs={"options": f"-c statement_timeout={POOL_STMT_TIMEOUT}"},
        )
        connection_pool.open(wait=False)
        log.info(
            "Database connection pool opened "
            f"(min_size={POOL_MIN_SIZE}, max_size={POOL_MAX_SIZE}, growing in background)"
        )
    return connection_pool


def get_analytics_pool() -> ConnectionPool:
    """Return the analytics connection pool, creating it lazily on first call.

    This pool has a much longer statement timeout (5 min) to accommodate heavy
    aggregation queries that feed the background cache refresh.  It is
    intentionally small (max 4 connections) to limit resource consumption.
    """
    global _analytics_pool
    if _analytics_pool is None:
        _analytics_pool = ConnectionPool(
            conninfo=_build_conninfo(),
            min_size=ANALYTICS_POOL_MIN_SIZE,
            max_size=ANALYTICS_POOL_MAX_SIZE,
            open=False,
            timeout=POOL_TIMEOUT,
            kwargs={"options": f"-c statement_timeout={ANALYTICS_STMT_TIMEOUT}"},
        )
        _analytics_pool.open(wait=False)
        log.info(
            f"Analytics pool opened (statement_timeout={ANALYTICS_STMT_TIMEOUT // 1000}s)"
        )
    return _analytics_pool


# ---------------------------------------------------------------------------
# Workspace client (lazy singleton)
# ---------------------------------------------------------------------------

workspace_client: WorkspaceClient | None = None


def get_workspace_client() -> WorkspaceClient:
    """Return the shared ``WorkspaceClient``, creating it on first call."""
    global workspace_client
    if workspace_client is None:
        workspace_client = WorkspaceClient()
        log.info("WorkspaceClient initialized")
    return workspace_client


# ---------------------------------------------------------------------------
# SQL identifier validation
# ---------------------------------------------------------------------------

# Allows alphanumeric, underscore, and hyphen — must start with a letter or
# underscore.  Used to sanitize user-supplied schema/table names before they
# are interpolated into SQL strings.
# ---------------------------------------------------------------------------
# PII masking helpers
# ---------------------------------------------------------------------------

def mask_phone(phone: str | None) -> str | None:
    """Mask phone number: show only last 4 digits. '555-123-4567' → '***-***-4567'."""
    if not phone:
        return phone
    digits = ''.join(c for c in phone if c.isdigit())
    if len(digits) >= 4:
        return '***-***-' + digits[-4:]
    return '***'


def mask_email(email: str | None) -> str | None:
    """Mask email: show first char + domain. 'john.doe@company.com' → 'j***@company.com'."""
    if not email or '@' not in email:
        return email
    local, domain = email.split('@', 1)
    return local[0] + '***@' + domain


# ---------------------------------------------------------------------------
# SQL identifier validation
# ---------------------------------------------------------------------------

# Hyphens allowed because Databricks catalog/schema names often contain them
# (e.g., dba-lakebase-network). Always use backtick quoting when interpolating.
IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate a SQL identifier to prevent injection.

    Raises ``ValueError`` if *name* does not match ``IDENTIFIER_RE``.
    Rejects semicolons, quotes, comments, and other SQL meta-characters.
    """
    if not name or len(name) > 128:
        raise ValueError(f"Invalid {label}: empty or too long")
    if not IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {label}: {name[:50]}")
    return name


# ---------------------------------------------------------------------------
# SQL Statement Execution API helper
# ---------------------------------------------------------------------------


def _run_sql(sql: str, catalog: str | None = None) -> list[list]:
    """Execute SQL via the Databricks Statement Execution API.

    Uses ``wait_timeout=50s`` (the maximum the API allows) and then polls
    with exponential backoff (100 ms -> 2 s cap, up to ~10 min total) if
    the statement is still PENDING/RUNNING after the initial wait.

    Returns the ``data_array`` from the response (list of rows, each row a
    list of string values).
    """
    wh_id = os.environ.get("SQL_WAREHOUSE_ID", "")
    if not wh_id:
        raise ValueError("SQL_WAREHOUSE_ID not configured")
    w = get_workspace_client()
    body: dict = {
        "warehouse_id": wh_id,
        "statement": sql,
        "wait_timeout": "50s",
    }
    if catalog:
        body["catalog"] = catalog
    resp = w.api_client.do("POST", "/api/2.0/sql/statements", body=body)
    stmt_id = resp.get("statement_id", "")
    state = resp.get("status", {}).get("state", "")

    # Poll with exponential backoff (100 ms -> 2 s cap, max ~10 min total)
    wait = 0.1
    elapsed = 0.0
    while state in ("PENDING", "RUNNING") and elapsed < 600:
        time.sleep(wait)
        elapsed += wait
        wait = min(wait * 1.5, 2.0)
        resp = w.api_client.do("GET", f"/api/2.0/sql/statements/{stmt_id}")
        state = resp.get("status", {}).get("state", "")

    if state == "FAILED":
        err = resp.get("status", {}).get("error", {}).get("message", "SQL execution failed")
        raise RuntimeError(err)
    if state not in ("SUCCEEDED",):
        raise RuntimeError(f"SQL statement ended in unexpected state: {state}")
    return resp.get("result", {}).get("data_array", [])


# ---------------------------------------------------------------------------
# Column-existence cache
# ---------------------------------------------------------------------------
# Avoids repeated information_schema queries to check whether a column
# exists (e.g., to decide whether to include ``sla_risk_level`` in a query).
# The cache is never explicitly expired because schema changes require a
# redeploy, which restarts the process.

_column_exists_cache: dict[str, bool] = {}
_column_exists_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Query result cache (stale-while-revalidate)
# ---------------------------------------------------------------------------
# Caches expensive aggregation queries (dispatch summary, SLA breakdown,
# technician roster) so the dashboard does not block on every page load.
# Background refresh keeps data fresh within the TTL window.
#
# TTL reasoning: 45 seconds balances freshness (the data generator produces
# events every ~10 s) against PG load (heavy aggregations can take 5-15 s
# at the 5M work-order scale).

_query_cache: dict[str, dict] = {}
_query_cache_lock = threading.Lock()
QUERY_CACHE_TTL = 45  # seconds — dashboard data refreshes every 45 s
_bg_refresh_in_progress: set[str] = set()


def _run_cached_query(cache_key: str, query_fn, ttl: float = QUERY_CACHE_TTL):
    """Return cached result if fresh; otherwise run *query_fn* synchronously.

    On error, falls back to stale cached data if available.  This prevents
    a transient PG hiccup from breaking every dashboard panel at once.
    """
    now = time.time()
    with _query_cache_lock:
        entry = _query_cache.get(cache_key)
        if entry and now < entry["expires"]:
            return entry["data"]
        stale_data = entry["data"] if entry else None

    # Compute fresh data
    try:
        data = query_fn()
        with _query_cache_lock:
            _query_cache[cache_key] = {"data": data, "expires": now + ttl}
        return data
    except Exception as e:
        log.warning(f"Query cache miss for {cache_key}: {e}")
        if stale_data is not None:
            return stale_data
        raise


def _bg_refresh_cache(cache_key: str, query_fn, ttl: float = QUERY_CACHE_TTL):
    """Trigger a background thread to refresh a cache entry.

    No-ops if a refresh for *cache_key* is already in progress.  The caller
    should return stale data immediately while the refresh runs.
    """
    if cache_key in _bg_refresh_in_progress:
        return
    _bg_refresh_in_progress.add(cache_key)

    def _refresh():
        try:
            data = query_fn()
            with _query_cache_lock:
                _query_cache[cache_key] = {"data": data, "expires": time.time() + ttl}
        except Exception as e:
            log.warning(f"Background refresh failed for {cache_key}: {e}")
        finally:
            _bg_refresh_in_progress.discard(cache_key)

    threading.Thread(target=_refresh, daemon=True).start()


def _get_or_refresh(cache_key: str, query_fn, ttl: float = QUERY_CACHE_TTL):
    """Best-effort cached query: return cache hit, refresh in background if stale.

    * **Fresh cache** -> return immediately.
    * **Stale cache** -> return stale data, kick off background refresh.
    * **No cache at all** -> block and compute via ``_run_cached_query``.
    """
    now = time.time()
    with _query_cache_lock:
        entry = _query_cache.get(cache_key)
        if entry and now < entry["expires"]:
            return entry["data"]
        if entry:
            # Stale — return stale data but trigger background refresh
            _bg_refresh_cache(cache_key, query_fn, ttl)
            return entry["data"]
    # No cache at all — must block and compute
    return _run_cached_query(cache_key, query_fn, ttl)


# ---------------------------------------------------------------------------
# Shared cached queries — used by multiple blueprints
# ---------------------------------------------------------------------------

def _compute_active_wo_counts_by_tech() -> dict[int, int]:
    """Active work-order count per technician.

    Uses ``idx_wo_tech_active`` partial index for a fast scan of only
    non-completed/cancelled rows.  Result is a dict mapping
    ``technician_id -> active_wo_count``.
    """
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* shared:active_wo_by_tech */
                SELECT assigned_technician_id, COUNT(*) as cnt
                FROM field_service.work_orders
                WHERE status NOT IN ('completed', 'cancelled')
                  AND assigned_technician_id IS NOT NULL
                GROUP BY assigned_technician_id
            """)
            return {row[0]: row[1] for row in cur.fetchall()}


def get_active_wo_counts_by_tech() -> dict[int, int]:
    """Cached active WO counts per technician (45 s TTL).

    Shared by dispatch/technicians, dispatch/technician-roster, and map/data.
    Deduplicates a query that previously ran 3 times independently.
    """
    return _get_or_refresh(
        "active_wo_by_tech", _compute_active_wo_counts_by_tech
    )


# ---------------------------------------------------------------------------
# Table list cache
# ---------------------------------------------------------------------------
# Tables rarely change at runtime (new tables require a pipeline run or DDL),
# so a 5-minute TTL is generous.  The cache stores both Lakebase (PG) tables
# and Unity Catalog tables from the network_data schema.

_table_cache: dict[str, object] = {"data": None, "expires": 0}
_table_cache_lock = threading.Lock()
TABLE_CACHE_TTL = 300  # 5 minutes — tables rarely change at runtime


def get_cached_tables() -> list[dict]:
    """Return a cached list of tables from both Lakebase and Unity Catalog.

    Lakebase tables come from ``information_schema.tables``; UC tables come
    from ``SHOW TABLES`` via the Statement Execution API.  Results are merged
    into a single list with a ``source`` field (``'lakebase'`` or ``'uc'``).
    """
    now = time.time()
    with _table_cache_lock:
        if _table_cache["data"] is not None and now < _table_cache["expires"]:
            return _table_cache["data"]

    # ── Lakebase tables ──
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """)
            tables = [
                {"schema": row[0], "table": row[1], "source": "lakebase"}
                for row in cur.fetchall()
            ]

    # ── Unity Catalog tables (network_data) ──
    # Fetched in a background thread to avoid blocking the response.
    # The SQL Warehouse can take 30-120s to cold-start, which would
    # make the Data Explorer page appear broken if we waited.
    _table_cache["uc_loading"] = True

    def _fetch_uc_tables_bg():
        try:
            catalog = os.environ.get("PIPELINE_CATALOG", "dba-lakebase-network")
            uc_schema = "network_data"
            rows = _run_sql(
                f"SHOW TABLES IN `{catalog}`.`{uc_schema}`",
                catalog=catalog,
            )
            uc_tables = []
            for row in rows:
                tbl_name = row[1] if len(row) > 1 else row[0]
                uc_tables.append({
                    "schema": uc_schema,
                    "table": tbl_name,
                    "source": "uc",
                    "catalog": catalog,
                })
            # Merge into cache
            with _table_cache_lock:
                if _table_cache["data"] is not None:
                    _table_cache["data"] = [
                        t for t in _table_cache["data"] if t.get("source") != "uc"
                    ] + uc_tables
                _table_cache["uc_loading"] = False
            log.info(f"UC tables loaded in background: {len(uc_tables)} tables")
        except Exception as e:
            log.warning(f"Could not fetch UC tables (background): {e}")
            _table_cache["uc_loading"] = False

    import threading
    threading.Thread(target=_fetch_uc_tables_bg, daemon=True).start()

    with _table_cache_lock:
        _table_cache["data"] = tables
        _table_cache["expires"] = time.time() + TABLE_CACHE_TTL

    return tables


# ---------------------------------------------------------------------------
# Config loader (config.yaml)
# ---------------------------------------------------------------------------
# config.yaml lives in the deployment/ directory and contains non-secret
# configuration (IDs, URLs, feature flags).  It is loaded once and cached
# for the lifetime of the process.  A blocklist prevents accidental exposure
# of any keys whose names suggest they hold secrets.

_CONFIG_BLOCKLIST: set[str] = {
    "app_password", "token", "password", "secret", "pgpassword",
}

_config_yaml_cache: dict[str, object] = {"data": None, "loaded": False}


def _load_config_yaml() -> dict:
    """Load ``config.yaml`` from the deployment directory (best-effort, cached).

    Search order:
    1. ``../deployment/config.yaml`` relative to this file (works in both
       local dev and the Databricks Apps container).
    2. ``/Workspace/deployment/config.yaml`` (legacy fallback).
    3. The path in the ``DEPLOYMENT_CONFIG_PATH`` environment variable.

    Returns an empty dict if no config file is found.
    """
    if _config_yaml_cache["loaded"]:
        return _config_yaml_cache["data"]
    try:
        import yaml as _yaml

        for candidate in [
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..",
                "deployment",
                "config.yaml",
            ),
            "/Workspace/deployment/config.yaml",
        ]:
            try:
                with open(candidate) as f:
                    _config_yaml_cache["data"] = _yaml.safe_load(f) or {}
                    _config_yaml_cache["loaded"] = True
                    return _config_yaml_cache["data"]
            except Exception:
                continue
        # Fallback: env-var-based deployment path
        deploy_dir = os.environ.get("DEPLOYMENT_CONFIG_PATH", "")
        if deploy_dir:
            with open(deploy_dir) as f:
                _config_yaml_cache["data"] = _yaml.safe_load(f) or {}
                _config_yaml_cache["loaded"] = True
                return _config_yaml_cache["data"]
    except Exception:
        pass
    _config_yaml_cache["loaded"] = True
    _config_yaml_cache["data"] = {}
    return {}

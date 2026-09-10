"""
Shared utilities for the standalone Lakebase Admin Console.

This module holds the cross-cutting concerns the admin blueprint needs —
connection pools, the Databricks workspace client, identifier validation,
the Statement Execution API helper, user identity, and group-based access
control — with no dependency on the Flask ``app`` object so the blueprint
can ``from shared import get_pool`` without a circular import.

Everything here works against *any* Lakebase (or plain PostgreSQL) instance and
is deliberately generic — no application-specific data models or business logic.

Environment variables
======================
Connection (native PG auth via Databricks Secrets ``valueFrom``):
  * ``PGHOST`` / ``PGUSER`` / ``PGPASSWORD`` / ``PGDATABASE``
Access control:
  * ``ADMIN_GROUP`` — Databricks workspace group whose members are admins
    (default ``admins``). Workspace ``admins`` are always treated as admins.
  * ``ALLOW_ANONYMOUS_ADMIN`` — ``true`` to allow unauthenticated access
    (local dev only; never set in a deployed app).
Optional:
  * ``SQL_WAREHOUSE_ID`` — enables the Statement Execution API helper used
    for UC Volume backup save/restore.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timezone

import psycopg
from databricks.sdk import WorkspaceClient
from psycopg_pool import ConnectionPool

__all__ = [
    "APP_START_TIME",
    "log_error",
    "_error_log",
    "_error_log_lock",
    "MAX_ERROR_LOG",
    "get_pool",
    "get_analytics_pool",
    "POOL_MIN_SIZE",
    "POOL_MAX_SIZE",
    "POOL_TIMEOUT",
    "POOL_STMT_TIMEOUT",
    "ANALYTICS_POOL_MIN_SIZE",
    "ANALYTICS_POOL_MAX_SIZE",
    "ANALYTICS_STMT_TIMEOUT",
    "get_workspace_client",
    "IDENTIFIER_RE",
    "validate_identifier",
    "_run_sql",
    "get_current_user",
    "get_role_from_groups",
    "ADMIN_GROUP",
    # Multi-instance
    "DEFAULT_INSTANCE",
    "active_instance_name",
    "list_lakebase_instances",
    "get_pool_for",
]

log = logging.getLogger(__name__)

# ── App start time — used by /api/health for uptime calculation ───────────
APP_START_TIME = datetime.now(timezone.utc)

# ── Recent-errors ring buffer ─────────────────────────────────────────────
_error_log: list[dict] = []
_error_log_lock = threading.Lock()
MAX_ERROR_LOG = 50


def log_error(source: str, error: object) -> None:
    """Log an error to both stderr and the in-memory ring buffer.

    The ring buffer is capped at ``MAX_ERROR_LOG`` entries so the admin UI
    can surface recent failures without an external store.
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
# User identity (extracted from the Databricks Apps OAuth token)
# ---------------------------------------------------------------------------

_user_cache: dict = {}  # per-request cache keyed by request object id


def get_current_user() -> dict:
    """Extract the logged-in user from the Databricks OAuth token.

    Databricks Apps proxies requests with the user's workspace OAuth token in
    the Authorization header (and, more reliably, ``X-Forwarded-*`` headers).
    We read those to identify the caller.

    Returns ``{"email": ..., "name": ...}`` or the anonymous sentinel when no
    identity is present.
    """
    try:
        from flask import request as flask_request
        import base64 as _b64
        import json as _json

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

        # Method 2: Decode the OAuth JWT from the Authorization header
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
                if email and len(email) > 5:
                    result = {"email": email, "name": name or email[:20]}
                    _user_cache[cache_key] = result
                    return result
    except Exception:
        pass
    return {"email": "anonymous", "name": "Anonymous"}


# ---------------------------------------------------------------------------
# Group-based access control
# ---------------------------------------------------------------------------
# The admin console is gated on Databricks workspace group membership. The
# admin group is configurable; the built-in workspace ``admins`` group always
# counts as admin so a fresh deploy is never locked out for a workspace admin.

ADMIN_GROUP: str = os.environ.get("ADMIN_GROUP", "admins")

_groups_cache: dict = {}  # email → (groups, timestamp)
_GROUPS_CACHE_TTL = 300   # 5 minutes


def get_user_databricks_groups(email: str) -> list[str]:
    """Return the user's Databricks workspace group display names (SCIM).

    Uses the app service principal's token to query SCIM. Cached for 5 minutes.
    Returns an empty list if SCIM is unavailable (the caller decides how to
    treat that).
    """
    now = time.time()
    cached = _groups_cache.get(email)
    if cached and now - cached[1] < _GROUPS_CACHE_TTL:
        return cached[0]

    groups: list[str] = []
    try:
        w = get_workspace_client()
        user_list = w.users.list(filter=f'userName eq "{email}"')
        for u in user_list:
            if u.groups:
                groups = [g.display for g in u.groups if g.display]
            break
    except Exception:
        pass

    _groups_cache[email] = (groups, now)
    return groups


def get_role_from_groups(email: str) -> str | None:
    """Return ``"admin"`` if the user belongs to the admin group, else ``None``.

    Both the configurable ``ADMIN_GROUP`` and the built-in workspace
    ``admins`` group grant admin.
    """
    group_names = {g.lower() for g in get_user_databricks_groups(email)}
    admin_groups = {ADMIN_GROUP.lower(), "admins"}
    if group_names & admin_groups:
        return "admin"
    return None


# ---------------------------------------------------------------------------
# Database connection pools (lazy initialization)
# ---------------------------------------------------------------------------
# Two pools isolate workloads: an interactive pool (30 s statement timeout)
# for API routes, and a smaller analytics pool (5 min timeout) so a heavy
# aggregation cannot starve interactive calls of connections.

POOL_MIN_SIZE = int(os.environ.get("POOL_MIN_SIZE", "4"))
POOL_MAX_SIZE = int(os.environ.get("POOL_MAX_SIZE", "20"))
POOL_TIMEOUT = 30           # seconds to wait for a connection from the pool
POOL_STMT_TIMEOUT = 30000   # ms — PG statement_timeout for interactive queries

ANALYTICS_POOL_MIN_SIZE = int(os.environ.get("ANALYTICS_POOL_MIN_SIZE", "1"))
ANALYTICS_POOL_MAX_SIZE = int(os.environ.get("ANALYTICS_POOL_MAX_SIZE", "4"))
ANALYTICS_STMT_TIMEOUT = 300000  # 5 minutes — heavy aggregations

connection_pool: ConnectionPool | None = None
_analytics_pool: ConnectionPool | None = None


def _build_conninfo() -> str:
    """Build the libpq connection string from environment variables.

    Native PG auth: ``PGPASSWORD`` (from a Databricks Secret) is preferred,
    falling back to ``DATABRICKS_TOKEN`` only for local development.
    """
    pg_password = os.environ.get("PGPASSWORD") or os.environ.get("DATABRICKS_TOKEN", "")
    return (
        f"dbname={os.environ.get('PGDATABASE')} "
        f"user={os.environ.get('PGUSER')} "
        f"password={pg_password} "
        f"host={os.environ.get('PGHOST')} "
        f"port={os.environ.get('PGPORT', '5432')} sslmode=require"
    )


def get_pool() -> ConnectionPool:
    """Return the interactive connection pool, creating it lazily."""
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
    """Return the analytics connection pool, creating it lazily.

    Longer statement timeout (5 min) for heavy work; intentionally small to
    limit resource consumption.
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
# Multi-instance support
# ---------------------------------------------------------------------------
# The console can target any Lakebase instance in the workspace. The DEFAULT
# instance uses native PG auth (PGUSER/PGPASSWORD from Databricks Secrets) — the
# guaranteed-working path. Every other instance is reached with a short-lived
# OAuth **database credential** (POST /api/2.0/postgres/credentials), minted with
# the logged-in user's forwarded token when present (so you connect as yourself,
# with your own Postgres permissions), otherwise with the app service principal.
#
# Pools are created lazily per (instance, identity, kind) and rebuilt before the
# ~1 h credential expires.

# Short id of the default instance (e.g. "dba-lakebase-1"). Its connection uses
# the native PGHOST/PGUSER/PGPASSWORD env vars. Empty ⇒ single-instance mode.
DEFAULT_INSTANCE: str = os.environ.get("DEFAULT_INSTANCE", "").strip()

# On-demand instance pools are intentionally small.
INSTANCE_POOL_MIN = int(os.environ.get("INSTANCE_POOL_MIN", "1"))
INSTANCE_POOL_MAX = int(os.environ.get("INSTANCE_POOL_MAX", "6"))
_CRED_TTL_SAFETY = 120        # rebuild the pool this many seconds before token expiry
_INSTANCE_LIST_TTL = 60       # seconds to cache the discovered instance list

_pools: dict[str, dict] = {}          # key -> {"pool": ConnectionPool, "exp": float}
_pools_lock = threading.Lock()
_instance_list_cache: dict = {"data": None, "exp": 0.0}
_host_cache: dict[str, tuple] = {}    # instance id -> (host, endpoint_full_name)
_own_identity_cache: dict = {"v": None}


def _strip_projects(name: str) -> str:
    """Normalize 'projects/foo' | 'foo' -> 'foo'."""
    return name.split("/", 1)[1] if name.startswith("projects/") else name


def _own_identity() -> str:
    """The app's own identity (service principal userName) — used as PGUSER
    when minting a credential as the app rather than on behalf of a user."""
    if _own_identity_cache["v"] is None:
        try:
            _own_identity_cache["v"] = get_workspace_client().current_user.me().user_name
        except Exception:
            _own_identity_cache["v"] = os.environ.get("DATABRICKS_CLIENT_ID", "app")
    return _own_identity_cache["v"]


def active_instance_name() -> str:
    """Resolve the target instance for the current request.

    Reads the ``X-Lakebase-Instance`` header (set by the frontend from the
    selector); falls back to ``DEFAULT_INSTANCE``.
    """
    try:
        from flask import request as _rq
        v = (_rq.headers.get("X-Lakebase-Instance") or "").strip()
    except Exception:
        v = ""
    return _strip_projects(v) if v else DEFAULT_INSTANCE


def list_lakebase_instances() -> list[dict]:
    """Discover Lakebase projects in the workspace (cached ~60 s).

    Returns ``[{"name", "display", "state", "is_default"}]``. Host resolution is
    deferred to connect time to keep this call to a single API request.
    """
    now = time.time()
    if _instance_list_cache["data"] is not None and now < _instance_list_cache["exp"]:
        return _instance_list_cache["data"]

    items: list[dict] = []
    try:
        w = get_workspace_client()
        resp = w.api_client.do("GET", "/api/2.0/postgres/projects")
        projects = resp.get("projects", resp) if isinstance(resp, dict) else resp
        for p in (projects or []):
            raw = p.get("name", "") if isinstance(p, dict) else str(p)
            pid = _strip_projects(raw)
            if not pid:
                continue
            spec = p.get("spec", {}) if isinstance(p, dict) else {}
            status = p.get("status", {}) if isinstance(p, dict) else {}
            items.append({
                "name": pid,
                "display": spec.get("display_name") or pid,
                "state": status.get("current_state") or status.get("state") or "",
                "is_default": pid == DEFAULT_INSTANCE,
            })
    except Exception as e:
        log_error("list_lakebase_instances", e)

    # Ensure the default instance is always present, even if discovery fails.
    if DEFAULT_INSTANCE and not any(i["name"] == DEFAULT_INSTANCE for i in items):
        items.insert(0, {"name": DEFAULT_INSTANCE, "display": DEFAULT_INSTANCE,
                         "state": "", "is_default": True})

    items.sort(key=lambda i: (not i["is_default"], i["display"].lower()))
    _instance_list_cache["data"] = items
    _instance_list_cache["exp"] = now + _INSTANCE_LIST_TTL
    return items


def _resolve_host(instance_id: str) -> tuple[str, str]:
    """Return ``(host, endpoint_full_name)`` for an instance's primary RW endpoint."""
    if instance_id in _host_cache:
        return _host_cache[instance_id]
    w = get_workspace_client()
    resp = w.api_client.do(
        "GET", f"/api/2.0/postgres/projects/{instance_id}/branches/production/endpoints"
    )
    endpoints = resp.get("endpoints", []) if isinstance(resp, dict) else (resp or [])
    chosen = None
    for ep in endpoints:
        status = ep.get("status", {})
        if status.get("endpoint_type") == "ENDPOINT_TYPE_READ_WRITE":
            chosen = ep
            break
    chosen = chosen or (endpoints[0] if endpoints else None)
    if not chosen:
        raise RuntimeError(f"No endpoint found for instance {instance_id}")
    host = chosen.get("status", {}).get("hosts", {}).get("host", "")
    ep_name = chosen.get("name") or f"projects/{instance_id}/branches/production/endpoints/primary"
    _host_cache[instance_id] = (host, ep_name)
    return host, ep_name


def _parse_expiry(resp: dict) -> float:
    """Parse the credential ``expire_time`` (ISO-8601); default ~55 min out."""
    exp_raw = resp.get("expire_time")
    if exp_raw:
        try:
            return datetime.fromisoformat(exp_raw.replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
    return time.time() + 3300


def _mint_credential(endpoint_full_name: str) -> tuple[str, float, str, str]:
    """Mint an OAuth database credential for an endpoint.

    Prefers the logged-in user's forwarded token (on-behalf-of) so the console
    uses the user's own Postgres permissions; falls back to the app service
    principal. Returns ``(token, expiry_epoch, pguser, auth_mode)`` where
    ``auth_mode`` is ``"user"`` or ``"sp"``.

    The user path mints with a *direct* HTTPS call (Bearer = the forwarded
    token) rather than a second ``WorkspaceClient``: constructing one with an
    explicit token inside the app collides with the SP's ambient
    ``DATABRICKS_CLIENT_ID/SECRET`` env auth and raises, which would otherwise
    silently drop us back to the service principal.
    """
    base = get_workspace_client()

    fwd = None
    try:
        from flask import request as _rq
        fwd = _rq.headers.get("X-Forwarded-Access-Token")
    except Exception:
        fwd = None

    if fwd:
        import json as _json
        import urllib.error as _urlerr
        import urllib.request as _urlreq
        host = base.config.host or os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = "https://" + host
        pguser = get_current_user().get("email") or _own_identity()
        req = _urlreq.Request(
            f"{host.rstrip('/')}/api/2.0/postgres/credentials",
            data=_json.dumps({"endpoint": endpoint_full_name}).encode(),
            headers={"Authorization": f"Bearer {fwd}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with _urlreq.urlopen(req, timeout=15) as r:
                resp = _json.loads(r.read().decode())
            return resp["token"], _parse_expiry(resp), pguser, "user"
        except _urlerr.HTTPError as he:
            body = ""
            try:
                body = he.read().decode()[:300]
            except Exception:
                pass
            # A forwarded token was present — surface the real reason rather than
            # masking it behind a service-principal fallback that would also fail.
            raise RuntimeError(
                f"on-behalf-of credential mint for '{pguser}' failed: HTTP {he.code} {body}"
            )
        except Exception as e:
            raise RuntimeError(f"on-behalf-of credential mint for '{pguser}' failed: {e}")

    # Service-principal path (no forwarded token at all).
    resp = base.api_client.do("POST", "/api/2.0/postgres/credentials",
                              body={"endpoint": endpoint_full_name})
    return resp["token"], _parse_expiry(resp), _own_identity(), "sp"


def _conninfo(host: str, user: str, password: str) -> str:
    return (
        f"dbname={os.environ.get('PGDATABASE', 'databricks_postgres')} "
        f"user={user} password={password} host={host} "
        f"port={os.environ.get('PGPORT', '5432')} sslmode=require"
    )


# Per-(instance,kind) build locks so concurrent first-requests don't each build a
# pool and close each other's mid-use (the "pool is already closed" race).
_build_locks: dict[str, threading.Lock] = {}
_build_locks_guard = threading.Lock()


def _prefix_lock(prefix: str) -> threading.Lock:
    with _build_locks_guard:
        lk = _build_locks.get(prefix)
        if lk is None:
            lk = threading.Lock()
            _build_locks[prefix] = lk
        return lk


def _schedule_pool_close(pool: ConnectionPool, delay: float = 45.0) -> None:
    """Close a replaced pool after a grace period so in-flight requests finish
    first (closing it synchronously is what caused 'pool is already closed')."""
    def _close():
        try:
            pool.close()
        except Exception:
            pass
    t = threading.Timer(delay, _close)
    t.daemon = True
    t.start()


def get_pool_for(instance_id: str | None, analytics: bool = False) -> ConnectionPool:
    """Return a connection pool for *instance_id* (native for the default, OAuth
    otherwise), creating or refreshing it as needed.

    Falls back to the native default pool when *instance_id* is empty or matches
    ``DEFAULT_INSTANCE``. Builds are serialized per (instance, kind) so concurrent
    requests share one pool, and a replaced pool is closed only after a grace delay.
    """
    instance_id = _strip_projects(instance_id) if instance_id else ""
    is_default = (not instance_id) or (instance_id == DEFAULT_INSTANCE)

    # Default instance → the existing native-auth pools.
    if is_default and os.environ.get("PGHOST"):
        return get_analytics_pool() if analytics else get_pool()

    kind = "a" if analytics else "i"
    prefix = f"{instance_id}::{kind}::"

    def _live_pool():
        now = time.time()
        with _pools_lock:
            for key, e in _pools.items():
                if key.startswith(prefix) and now < e["exp"]:
                    return e["pool"]
        return None

    # Fast path: a live, non-expired pool.
    p = _live_pool()
    if p is not None:
        return p

    # Serialize the build for this (instance, kind). Concurrent callers wait here
    # and reuse the pool the first builder creates instead of racing to build.
    with _prefix_lock(prefix):
        p = _live_pool()  # re-check: another thread may have just built it
        if p is not None:
            return p

        now = time.time()
        host, endpoint_full_name = _resolve_host(instance_id)
        token, tok_exp, pguser, auth_mode = _mint_credential(endpoint_full_name)
        conninfo = _conninfo(host, pguser, token)

        # Fail fast with a clear error rather than letting the pool retry for 30 s.
        try:
            _probe = psycopg.connect(conninfo + " connect_timeout=6")
            _probe.close()
        except Exception as ce:
            msg = str(ce).strip().splitlines()[0][:220] if str(ce).strip() else "connection failed"
            raise RuntimeError(
                f"Cannot connect to instance '{instance_id}' as '{pguser}' (auth={auth_mode}): {msg}. "
                f"The {'user' if auth_mode == 'user' else 'app service principal'} needs a Postgres "
                f"login/role on this instance."
            )

        stmt_timeout = ANALYTICS_STMT_TIMEOUT if analytics else POOL_STMT_TIMEOUT
        max_size = ANALYTICS_POOL_MAX_SIZE if analytics else INSTANCE_POOL_MAX
        pool = ConnectionPool(
            conninfo=conninfo,
            min_size=INSTANCE_POOL_MIN,
            max_size=max_size,
            open=False,
            timeout=10,  # don't hang the request thread; connections are pre-validated
            max_lifetime=3000,  # recycle connections well within the ~1 h token life
            kwargs={"options": f"-c statement_timeout={stmt_timeout}", "connect_timeout": 8},
        )
        pool.open(wait=False)

        key = f"{prefix}{pguser}"
        exp = min(now + 3300, tok_exp - _CRED_TTL_SAFETY)
        with _pools_lock:
            old = _pools.get(key)
            _pools[key] = {"pool": pool, "exp": exp}
        if old and old["pool"] is not pool:
            _schedule_pool_close(old["pool"])  # grace-close; never kill an in-use pool
        log.info(f"Opened OAuth pool for instance '{instance_id}' as '{pguser}' (kind={kind})")
        return pool


# ---------------------------------------------------------------------------
# Workspace client (lazy singleton)
# ---------------------------------------------------------------------------

workspace_client: WorkspaceClient | None = None


def get_workspace_client() -> WorkspaceClient:
    """Return the shared ``WorkspaceClient``, creating it on first call.

    Inside a Databricks App this authenticates automatically via the app's
    service-principal OAuth credentials.
    """
    global workspace_client
    if workspace_client is None:
        workspace_client = WorkspaceClient()
        log.info("WorkspaceClient initialized")
    return workspace_client


# ---------------------------------------------------------------------------
# SQL identifier validation
# ---------------------------------------------------------------------------
# Allows alphanumerics, underscore, and hyphen; must start with a letter or
# underscore. Used to sanitize schema/table names before interpolation.

IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate a SQL identifier to prevent injection.

    Raises ``ValueError`` if *name* is empty, too long, or contains anything
    other than the allowed characters (rejects semicolons, quotes, comments).
    """
    if not name or len(name) > 128:
        raise ValueError(f"Invalid {label}: empty or too long")
    if not IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {label}: {name[:50]}")
    return name


# ---------------------------------------------------------------------------
# SQL Statement Execution API helper (optional — needs SQL_WAREHOUSE_ID)
# ---------------------------------------------------------------------------


def _run_sql(sql: str, catalog: str | None = None) -> list[list]:
    """Execute SQL via the Databricks Statement Execution API.

    Waits up to 50 s (the API maximum) then polls with exponential backoff.
    Used only by the optional UC Volume backup save/restore path. Raises
    ``ValueError`` if ``SQL_WAREHOUSE_ID`` is not configured.
    """
    wh_id = os.environ.get("SQL_WAREHOUSE_ID", "")
    if not wh_id:
        raise ValueError("SQL_WAREHOUSE_ID not configured")
    w = get_workspace_client()
    body: dict = {"warehouse_id": wh_id, "statement": sql, "wait_timeout": "50s"}
    if catalog:
        body["catalog"] = catalog
    resp = w.api_client.do("POST", "/api/2.0/sql/statements", body=body)
    stmt_id = resp.get("statement_id", "")
    state = resp.get("status", {}).get("state", "")

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

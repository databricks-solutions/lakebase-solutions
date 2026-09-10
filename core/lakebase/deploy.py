"""core/lakebase deploy step.

This step now **provisions** the whole Lakebase surface via the Databricks
Python SDK (``WorkspaceClient``). ``databricks bundle deploy`` cannot run on
notebook/job compute, so the in-workspace notebook drives everything through the
SDK instead of the CLI. In order, the step:

0. ensures the deployment's standalone secret **scope** exists
   (``w.secrets.create_scope`` -- idempotent; ``RESOURCE_ALREADY_EXISTS`` swallowed),
1. creates the autoscaling ``postgres`` **project** idempotently via the Postgres
   REST API: ``GET /api/2.0/postgres/projects/<id>`` and, on 404,
   ``POST /api/2.0/postgres/projects`` (``ALREADY_EXISTS`` swallowed). Creating the
   project auto-provisions the ``production`` branch + ``primary`` read-write endpoint,
2. sets the autoscaling CU range on the ``primary`` endpoint
   (``PATCH .../endpoints/primary`` with ``update_mask`` + min/max from params) and
   polls ``GET .../projects/<id>`` / ``GET .../endpoints`` until it is available,
3. resolves the ``primary`` endpoint host + an admin connection credential
   (workspace email + OAuth token from ``POST /api/2.0/postgres/credentials``),
4. connects as admin and runs **idempotent** SQL to create the workshop
   **database** then the workshop **schema** inside it,
5. writes the connection info (host/db/schema/user/password) to the scope.

Every Postgres call goes through ``w.api_client.do(...)`` because the
notebook-runtime ``databricks-sdk`` has no typed autoscaling-postgres service
(``WorkspaceClient has no attribute 'postgres'``); ``api_client`` is present on
every SDK version and raises on HTTP error with the API message.

Autoscaling gotcha: the endpoint's default ``postgres`` database has a
restricted ``public`` schema, so the workshop DB must be created explicitly.
``CREATE DATABASE`` has no ``IF NOT EXISTS`` and cannot run inside a transaction
block, so it runs on an autocommit connection to the maintenance db and a
duplicate-database error is swallowed (idempotent/repeatable deploy).

When no live clients are injected (e.g. the orchestrator smoke tests), it logs
intent and returns a ``stub`` result -- the live run happens in-workspace.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from bootstrap.adapters import (
    POSTGRES_API_BASE,
    endpoint_cu,
    endpoint_resource_name,
    resolve_endpoint_host,
    resolve_primary_endpoint,
)

# Connection-info secret keys this step writes (and teardown removes).
CONN_SECRET_KEYS: List[str] = ["pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"]

# Maintenance/default db on an autoscaling endpoint; `CREATE DATABASE` runs here.
MAINTENANCE_DB = "postgres"

# Endpoint-availability poll budget (verify timing at live run).
_ENDPOINT_POLL_ATTEMPTS = 60
_ENDPOINT_POLL_DELAY_SECONDS = 5.0

# Autoscaling CU PATCH: apply, then read back and retry if the endpoint has not
# adopted the requested range yet (a freshly provisioned endpoint reports its
# default range until the update is reconciled).
_CU_PATCH_ATTEMPTS = 5
_CU_PATCH_DELAY_SECONDS = 4.0


def _is_already_exists(exc: Exception) -> bool:
    """True if ``exc`` looks like an ALREADY_EXISTS / RESOURCE_ALREADY_EXISTS error.

    Checked offline-safe (no ``databricks.sdk.errors`` import): inspect an
    ``error_code`` attribute if present, else fall back to the error text.
    """

    code = str(getattr(exc, "error_code", "") or "").upper()
    if "ALREADY_EXISTS" in code:
        return True
    text = str(exc).lower()
    return "already exists" in text or "already_exists" in text


def _is_not_found(exc: Exception) -> bool:
    """True if ``exc`` looks like a 404 / NOT_FOUND from ``w.api_client.do``.

    Offline-safe (no ``databricks.sdk.errors`` import): inspect ``error_code`` /
    ``status_code`` if present, else fall back to the error text and class name.
    """

    code = str(getattr(exc, "error_code", "") or "").upper()
    if "NOT_FOUND" in code or "DOES_NOT_EXIST" in code:
        return True
    if getattr(exc, "status_code", None) == 404:
        return True
    text = str(exc).lower()
    if "not found" in text or "does not exist" in text or "404" in text:
        return True
    return exc.__class__.__name__.lower().endswith("notfound")


def _wait_for_primary_endpoint(w: Any, project: str, logger: Any) -> Any:
    """Poll until the project's ``primary`` endpoint exists; return its object.

    Creating a project auto-provisions the ``primary`` endpoint, but not
    instantly -- so this must complete BEFORE the autoscaling-CU PATCH, or the
    PATCH races a not-yet-present endpoint and silently no-ops. Availability is
    signalled by a resolvable ``status.hosts.host`` on the endpoint. Returns the
    full endpoint object (host + CU status live on it); ``None`` if it never
    appears within the budget.
    """

    for _ in range(_ENDPOINT_POLL_ATTEMPTS):
        try:  # GET project surfaces provisioning state; best-effort.
            w.api_client.do("GET", f"{POSTGRES_API_BASE}/projects/{project}")
        except Exception as exc:  # pragma: no cover - live-only shape variance
            logger.info("core/lakebase.deploy: GET project %r not ready yet: %s", project, exc)
        # Project creation is ASYNC: right after POST the project (and its
        # branch/endpoints) are briefly not queryable and GET endpoints 404s with
        # "project not found". That is a not-ready signal, NOT a fatal error -- so
        # swallow it and keep polling rather than aborting the whole deploy.
        try:
            endpoint = resolve_primary_endpoint(w, project)
        except Exception as exc:  # pragma: no cover - live-only provisioning lag
            logger.info("core/lakebase.deploy: endpoints for %r not ready yet: %s", project, exc)
            endpoint = None
        if endpoint is not None and _host_from_endpoint_obj(endpoint):
            return endpoint
        time.sleep(_ENDPOINT_POLL_DELAY_SECONDS)  # pragma: no cover - live-only wait
    try:  # pragma: no cover - final best-effort after the budget
        return resolve_primary_endpoint(w, project)
    except Exception:
        return None


def _host_from_endpoint_obj(endpoint: Any) -> str:
    """Best-effort ``status.hosts.host`` from an endpoint object (dict or typed)."""

    if isinstance(endpoint, dict):
        status = endpoint.get("status") or {}
        hosts = status.get("hosts") if isinstance(status, dict) else None
    else:  # pragma: no cover - defensive
        status = getattr(endpoint, "status", None)
        hosts = getattr(status, "hosts", None)
    if isinstance(hosts, (list, tuple)):
        hosts = hosts[0] if hosts else None
    if isinstance(hosts, dict):
        return hosts.get("host") or ""
    return getattr(hosts, "host", "") or ""


def _cu_matches(actual: Any, requested: float) -> bool:
    """True if a read-back CU value equals the requested one (float-tolerant)."""

    try:
        return actual is not None and abs(float(actual) - float(requested)) < 1e-9
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return False


def _apply_autoscaling_cu(
    w: Any, endpoint_name: str, project: str, min_cu: float, max_cu: float, logger: Any
) -> Dict[str, Any]:
    """PATCH the primary endpoint's autoscaling CU range, then verify it took.

    The CLI issues ``PATCH .../endpoints/primary?update_mask=spec.autoscaling_limit_min_cu,
    spec.autoscaling_limit_max_cu`` with body ``{"spec": {min, max}}`` (confirmed
    via ``--debug``). The write can lag on a freshly provisioned endpoint, so this
    PATCHes then reads ``status.autoscaling_limit_*`` back, retrying until the
    range matches (or the budget is exhausted). Returns the applied/observed CU
    values plus whether verification succeeded.
    """

    last_min: Any = None
    last_max: Any = None
    for attempt in range(_CU_PATCH_ATTEMPTS):
        try:
            w.api_client.do(
                "PATCH",
                f"{POSTGRES_API_BASE}/{endpoint_name}",
                query={
                    "update_mask": "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu"
                },
                body={
                    "spec": {"autoscaling_limit_min_cu": min_cu, "autoscaling_limit_max_cu": max_cu}
                },
            )
            endpoint = resolve_primary_endpoint(w, project)
            last_min, last_max = endpoint_cu(endpoint) if endpoint is not None else (None, None)
        except Exception as exc:  # pragma: no cover - live-only: transient/endpoint lag
            logger.info(
                "core/lakebase.deploy: CU PATCH attempt %d not ready (%s); retrying.",
                attempt + 1,
                exc,
            )
            time.sleep(_CU_PATCH_DELAY_SECONDS)
            continue
        if _cu_matches(last_min, min_cu) and _cu_matches(last_max, max_cu):
            logger.info(
                "core/lakebase.deploy: autoscaling CU set to %s-%s on %r (verified).",
                min_cu,
                max_cu,
                endpoint_name,
            )
            return {"min_cu": min_cu, "max_cu": max_cu, "verified": True}
        time.sleep(_CU_PATCH_DELAY_SECONDS)  # pragma: no cover - live-only wait

    logger.warning(
        "core/lakebase.deploy: autoscaling CU PATCH not confirmed after %d attempt(s); "
        "requested %s-%s, endpoint reports %s-%s.",
        _CU_PATCH_ATTEMPTS,
        min_cu,
        max_cu,
        last_min,
        last_max,
    )
    return {
        "min_cu": min_cu,
        "max_cu": max_cu,
        "verified": False,
        "observed_min_cu": last_min,
        "observed_max_cu": last_max,
    }


def create_database_sql(database: str) -> str:
    """Non-idempotent ``CREATE DATABASE`` (guarded by a duplicate-error catch)."""

    return f'CREATE DATABASE "{database}"'


def workshop_schema_sql(schema: str) -> List[str]:
    """Idempotent DDL that bootstraps the workshop schema.

    ``CREATE SCHEMA IF NOT EXISTS`` is Postgres-native idempotency, so the step
    is safe to re-run (immutable/repeatable deploy, SPEC section 4).
    """

    return [f'CREATE SCHEMA IF NOT EXISTS "{schema}"']


def _is_duplicate_database(exc: Exception) -> bool:
    """True if ``exc`` is a Postgres duplicate_database (SQLSTATE 42P04) error.

    Checked without importing psycopg (offline-safe): inspect ``sqlstate`` /
    ``pgcode`` if present, else fall back to the error text.
    """

    code = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if code == "42P04":
        return True
    return "already exists" in str(exc).lower()


def deploy(ctx: Any) -> Dict[str, Any]:
    project = ctx.resolved_names.get("lakebase_project", ctx.deployment_id)
    database = ctx.params.get("database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/lakebase.deploy: no live clients injected; would create "
            "database %r + schema %r on project %r (primary endpoint) and write "
            "connection secrets to %r. P1 live run happens in-workspace.",
            database,
            schema,
            project,
            scope,
        )
        return {"project": project, "database": database, "schema": schema, "status": "stub"}

    w = ctx.workspace_client()
    # Autoscaling CU limits are numeric on the REST API (min can be fractional).
    min_cu = float(ctx.params.get("autoscaling_min_cu") or 0.5)
    max_cu = float(ctx.params.get("autoscaling_max_cu") or 2)
    endpoint_name = endpoint_resource_name(project)
    provisioned: Dict[str, Any] = {}

    # (0) Ensure the standalone secret scope exists (idempotent).
    try:
        w.secrets.create_scope(scope)
        provisioned["scope_created"] = True
    except Exception as exc:
        if _is_already_exists(exc):
            ctx.logger.info("core/lakebase.deploy: secret scope %r already exists.", scope)
            provisioned["scope_created"] = False
        else:
            raise

    # (1) Provision the autoscaling `postgres` PROJECT (idempotent) via REST:
    #     GET the project; on 404, POST to create it (auto-provisions the
    #     `production` branch + `primary` endpoint). ALREADY_EXISTS is swallowed.
    try:
        w.api_client.do("GET", f"{POSTGRES_API_BASE}/projects/{project}")
        provisioned["project_created"] = False
        ctx.logger.info("core/lakebase.deploy: postgres project %r already exists.", project)
    except Exception as exc:
        if not _is_not_found(exc):
            raise
        try:
            # project_id is a QUERY parameter (verified via CLI debug); the body
            # carries only the spec.
            w.api_client.do(
                "POST",
                f"{POSTGRES_API_BASE}/projects",
                query={"project_id": project},
                body={"spec": {"display_name": project}},
            )
            provisioned["project_created"] = True
        except Exception as create_exc:
            if _is_already_exists(create_exc):
                ctx.logger.info(
                    "core/lakebase.deploy: postgres project %r already exists.", project
                )
                provisioned["project_created"] = False
            else:
                raise

    # (2) Wait for the auto-provisioned primary endpoint to exist, THEN set its
    #     autoscaling CU range and verify the write took. Ordering matters: a PATCH
    #     issued before the endpoint is provisioned silently no-ops, which left the
    #     endpoint on Lakebase defaults in earlier runs.
    endpoint = _wait_for_primary_endpoint(w, project, ctx.logger)
    host = _host_from_endpoint_obj(endpoint) if endpoint is not None else None
    cu_result = _apply_autoscaling_cu(w, endpoint_name, project, min_cu, max_cu, ctx.logger)
    provisioned["autoscaling_min_cu"] = cu_result["min_cu"]
    provisioned["autoscaling_max_cu"] = cu_result["max_cu"]
    provisioned["autoscaling_cu_verified"] = cu_result["verified"]

    # (3) Resolve an admin connection credential for the primary endpoint.
    #     PG user is the workspace email; password is the OAuth token.
    if not host:  # pragma: no cover - live-only: endpoint never surfaced a host
        host = resolve_endpoint_host(w, project)
    cred = w.api_client.do(
        "POST",
        f"{POSTGRES_API_BASE}/credentials",
        body={"endpoint": endpoint_name},
    )
    token = cred.get("token") if isinstance(cred, dict) else getattr(cred, "token", None)
    pg_user = w.current_user.me().user_name

    executed: List[str] = []

    # (2a) Create the workshop DATABASE on the maintenance db (autocommit; no
    #      IF NOT EXISTS, so swallow duplicate_database). Do NOT commit here --
    #      autocommit handles it, keeping the single commit for the schema step.
    admin_conn = ctx.pg_connection(role="admin", database=MAINTENANCE_DB)
    try:  # `CREATE DATABASE` cannot run in a transaction block.
        admin_conn.autocommit = True
    except Exception:  # pragma: no cover - fake/driver without the attribute
        pass
    admin_cur = admin_conn.cursor()
    create_db = create_database_sql(database)
    try:
        admin_cur.execute(create_db)
        executed.append(create_db)
    except Exception as exc:
        if _is_duplicate_database(exc):
            ctx.logger.info("core/lakebase.deploy: database %r already exists.", database)
        else:
            raise

    # (2b) Connect to the workshop db and create the workshop schema (idempotent).
    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    for stmt in workshop_schema_sql(schema):
        cur.execute(stmt)
        executed.append(stmt)
    conn.commit()

    # (3) Persist connection info to the standalone secret scope.
    secrets_written: Dict[str, str] = {
        "pghost": host or "",
        "pgdatabase": database,
        "pgschema": schema,
        "pguser": pg_user or "",
        "pgpassword": token or "",
    }
    for key, value in secrets_written.items():
        w.secrets.put_secret(scope=scope, key=key, string_value=value)

    ctx.logger.info(
        "core/lakebase.deploy: ensured database %r + schema %r on project %r; "
        "wrote %d connection secret(s) to %r.",
        database,
        schema,
        project,
        len(secrets_written),
        scope,
    )
    return {
        "project": project,
        "database": database,
        "schema": schema,
        "host": host,
        "secret_scope": scope,
        "sql": executed,
        "secrets_written": sorted(secrets_written),
        "provisioned": provisioned,
        "status": "deployed",
    }

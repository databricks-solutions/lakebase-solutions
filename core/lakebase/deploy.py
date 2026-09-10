"""core/lakebase deploy step.

This step now **provisions** the whole Lakebase surface via the Databricks
Python SDK (``WorkspaceClient``). ``databricks bundle deploy`` cannot run on
notebook/job compute, so the in-workspace notebook drives everything through the
SDK instead of the CLI. In order, the step:

0. ensures the deployment's standalone secret **scope** exists
   (``w.secrets.create_scope`` -- idempotent; ``RESOURCE_ALREADY_EXISTS`` swallowed),
1. creates the autoscaling ``postgres`` **project** (``w.postgres.create_project``
   -- idempotent; ``ALREADY_EXISTS`` swallowed). Creating the project
   auto-provisions the ``production`` branch + ``primary`` read-write endpoint,
2. sets the autoscaling CU range on the ``primary`` endpoint
   (``w.postgres.update_endpoint`` with min/max from params) and polls
   ``get_project`` / ``list_endpoints`` until the primary endpoint is available,
3. resolves the ``primary`` endpoint host + an admin connection credential
   (workspace email + OAuth token from ``generate-database-credential``),
4. connects as admin and runs **idempotent** SQL to create the workshop
   **database** then the workshop **schema** inside it,
5. writes the connection info (host/db/schema/user/password) to the scope.

Autoscaling gotcha: the endpoint's default ``postgres`` database has a
restricted ``public`` schema, so the workshop DB must be created explicitly.
``CREATE DATABASE`` has no ``IF NOT EXISTS`` and cannot run inside a transaction
block, so it runs on an autocommit connection to the maintenance db and a
duplicate-database error is swallowed (idempotent/repeatable deploy).

The exact SDK request/response shapes (``create_project`` / ``update_endpoint``
args, endpoint availability state) are best-effort equivalents of the
``databricks postgres`` CLI and are marked "verify at live run".

When no live clients are injected (e.g. the orchestrator smoke tests), it logs
intent and returns a ``stub`` result -- the live run happens in-workspace.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from bootstrap.adapters import endpoint_resource_name, resolve_endpoint_host

# Connection-info secret keys this step writes (and teardown removes).
CONN_SECRET_KEYS: List[str] = ["pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"]

# Maintenance/default db on an autoscaling endpoint; `CREATE DATABASE` runs here.
MAINTENANCE_DB = "postgres"

# Endpoint-availability poll budget (verify timing at live run).
_ENDPOINT_POLL_ATTEMPTS = 60
_ENDPOINT_POLL_DELAY_SECONDS = 5.0


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


def _wait_for_primary_endpoint(w: Any, project: str, logger: Any) -> Any:
    """Poll until the project's ``primary`` endpoint is available; return its host.

    Calls ``get_project`` (for its state side-effect) and ``list_endpoints`` (via
    ``resolve_endpoint_host``) until a host is resolvable, then returns it. The
    availability signal (a resolvable ``status.hosts.host``) is a best-effort
    stand-in for an endpoint state field -- verify at live run.
    """

    for _ in range(_ENDPOINT_POLL_ATTEMPTS):
        try:  # get_project surfaces provisioning state; best-effort.
            w.postgres.get_project(project)
        except Exception as exc:  # pragma: no cover - live-only shape variance
            logger.info("core/lakebase.deploy: get_project(%r) not ready yet: %s", project, exc)
        host = resolve_endpoint_host(w, project)
        if host:
            return host
        time.sleep(_ENDPOINT_POLL_DELAY_SECONDS)  # pragma: no cover - live-only wait
    return resolve_endpoint_host(w, project)  # pragma: no cover - final best-effort


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
    min_cu = ctx.params.get("autoscaling_min_cu") or "0.5"
    max_cu = ctx.params.get("autoscaling_max_cu") or "2"
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

    # (1) Provision the autoscaling `postgres` PROJECT (idempotent). Creating the
    #     project auto-provisions the `production` branch + `primary` endpoint.
    #     verify request/response shape at live run.
    try:
        w.postgres.create_project(project)
        provisioned["project_created"] = True
    except Exception as exc:
        if _is_already_exists(exc):
            ctx.logger.info("core/lakebase.deploy: postgres project %r already exists.", project)
            provisioned["project_created"] = False
        else:
            raise

    # (2) Set the autoscaling CU range on the primary endpoint, then poll until it
    #     is available. verify update_endpoint arg shape at live run.
    w.postgres.update_endpoint(
        endpoint_name,
        autoscaling_limit_min_cu=min_cu,
        autoscaling_limit_max_cu=max_cu,
    )
    provisioned["autoscaling_min_cu"] = min_cu
    provisioned["autoscaling_max_cu"] = max_cu

    # (3) Resolve the primary endpoint host + an admin connection credential.
    #     PG user is the workspace email; password is the OAuth token.
    host = _wait_for_primary_endpoint(w, project, ctx.logger)
    cred = w.postgres.generate_database_credential(endpoint_name)
    token = getattr(cred, "token", None)
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

"""core/lakebase deploy step (P1).

The Lakebase autoscaling ``postgres`` **project** (+ its default ``production``
branch and ``primary`` read-write endpoint) is provisioned by the bundle
(``databricks.yml`` ``postgres_project`` / ``postgres_endpoint``; SDK
``w.postgres.create_project`` is the documented fallback). The secret *scope* is
the GA ``secret_scope`` resource. This step owns the part the bundle cannot
express:

1. resolve the ``primary`` endpoint host + an admin connection credential
   (workspace email + OAuth token from ``generate-database-credential``),
2. connect as admin and run **idempotent** SQL to create the workshop
   **database** then the workshop **schema** inside it,
3. write the connection info (host/db/schema/user/password) to the deployment's
   standalone secret scope.

Autoscaling gotcha: the endpoint's default ``postgres`` database has a
restricted ``public`` schema, so the workshop DB must be created explicitly.
``CREATE DATABASE`` has no ``IF NOT EXISTS`` and cannot run inside a transaction
block, so it runs on an autocommit connection to the maintenance db and a
duplicate-database error is swallowed (idempotent/repeatable deploy).

When no live clients are injected (e.g. the orchestrator smoke tests), it logs
intent and returns a ``stub`` result -- the live run happens in-workspace.
"""

from __future__ import annotations

from typing import Any, Dict, List

from bootstrap.adapters import endpoint_resource_name, resolve_endpoint_host

# Connection-info secret keys this step writes (and teardown removes).
CONN_SECRET_KEYS: List[str] = ["pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"]

# Maintenance/default db on an autoscaling endpoint; `CREATE DATABASE` runs here.
MAINTENANCE_DB = "postgres"


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

    # (1) Resolve the primary endpoint host + an admin connection credential.
    #     PG user is the workspace email; password is the OAuth token.
    host = resolve_endpoint_host(w, project)
    cred = w.postgres.generate_database_credential(endpoint_resource_name(project))
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
        "status": "deployed",
    }

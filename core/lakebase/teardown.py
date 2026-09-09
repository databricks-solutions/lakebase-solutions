"""core/lakebase teardown step (P1).

Drops the workshop schema this component created (SQL) and removes the
connection-info secrets it wrote. Runs LAST in teardown order because every
other component depends on it.

The Lakebase **instance** and the **secret scope** themselves are DABs-managed
(GA ``database_instance`` / ``secret_scope`` resources) -- they are destroyed by
``databricks bundle destroy``, NOT here.

When no live clients are injected it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict, List

# Mirror of ``deploy.CONN_SECRET_KEYS`` -- kept local because the orchestrator
# loads each step file flat (no package context), so relative imports between
# sibling step files are not available at load time.
CONN_SECRET_KEYS: List[str] = ["pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"]


def workshop_schema_drop_sql(schema: str) -> List[str]:
    """Idempotent DDL that removes the workshop schema and its contents."""

    return [f'DROP SCHEMA IF EXISTS "{schema}" CASCADE']


def teardown(ctx: Any) -> Dict[str, Any]:
    instance = ctx.resolved_names.get("lakebase_instance", ctx.name("lakebase"))
    database = ctx.params.get("database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/lakebase.teardown: no live clients injected; would drop "
            "schema %r from %s.%s and delete connection secrets from %r "
            "(instance + scope are DABs-managed -- `bundle destroy`).",
            schema,
            instance,
            database,
            scope,
        )
        return {"instance": instance, "schema": schema, "status": "stub"}

    conn = ctx.pg_connection(role="admin", database=database)
    executed: List[str] = []
    cur = conn.cursor()
    for stmt in workshop_schema_drop_sql(schema):
        cur.execute(stmt)
        executed.append(stmt)
    conn.commit()

    w = ctx.workspace_client()
    deleted: List[str] = []
    for key in CONN_SECRET_KEYS:
        try:
            w.secrets.delete_secret(scope=scope, key=key)
            deleted.append(key)
        except Exception as exc:  # secret may already be gone -- best effort
            ctx.logger.warning("core/lakebase.teardown: delete secret %r failed: %s", key, exc)

    ctx.logger.info(
        "core/lakebase.teardown: dropped schema %r from %s.%s; deleted %d secret(s) "
        "from %r. Instance + scope are DABs-managed (`bundle destroy`).",
        schema,
        instance,
        database,
        len(deleted),
        scope,
    )
    return {
        "instance": instance,
        "database": database,
        "schema": schema,
        "secret_scope": scope,
        "sql": executed,
        "secrets_deleted": deleted,
        "status": "torn_down",
    }

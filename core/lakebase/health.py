"""core/lakebase health check (P1).

Connects as admin to the workshop database, runs ``SELECT 1`` to confirm the
endpoint is reachable, and verifies the workshop schema exists
(``information_schema.schemata``).

Needs only a PG connection; when none is injected it returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict

_SELECT_ONE = "SELECT 1"
_SCHEMA_EXISTS = "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s"


def health_check(ctx: Any) -> Dict[str, Any]:
    project = ctx.resolved_names.get("lakebase_project", ctx.deployment_id)
    database = ctx.params.get("database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")

    if not ctx.has_pg_connection():
        ctx.logger.info(
            "[stub] core/lakebase.health: no PG connection injected; would "
            "SELECT 1 and confirm schema %r exists in %s.",
            schema,
            database,
        )
        return {"project": project, "schema": schema, "healthy": None, "status": "stub"}

    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    cur.execute(_SELECT_ONE)
    reachable = cur.fetchone() is not None
    cur.execute(_SCHEMA_EXISTS, (schema,))
    schema_exists = cur.fetchone() is not None
    healthy = reachable and schema_exists

    ctx.logger.info(
        "core/lakebase.health: project %r reachable=%s schema %r exists=%s.",
        project,
        reachable,
        schema,
        schema_exists,
    )
    return {
        "project": project,
        "database": database,
        "schema": schema,
        "reachable": reachable,
        "schema_exists": schema_exists,
        "healthy": healthy,
        "status": "ok" if healthy else "unhealthy",
    }

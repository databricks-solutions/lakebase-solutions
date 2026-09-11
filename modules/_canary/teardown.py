"""modules/_canary teardown -- removes everything deploy created.

Teardown runs in REVERSE dependency order (the orchestrator tears modules down
before the core they depend on), and must be best-effort + idempotent: a
resource that is already gone must not raise. Mirror your deploy: whatever you
create in deploy.py, remove here.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    schema = ctx.params.get("canary_schema", "canary")

    # Same stub guard as deploy: off-Databricks, log intent and return a stub.
    if not ctx.has_workspace_client() or not ctx.has_pg_connection():
        ctx.logger.info("[stub] _canary.teardown: would DROP SCHEMA %s CASCADE.", schema)
        return {"schema": schema, "status": "stub"}

    conn = ctx.pg_connection(
        role="admin", database=ctx.params.get("database") or "databricks_postgres"
    )
    try:
        conn.autocommit = True
    except Exception:  # pragma: no cover
        pass
    conn.cursor().execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    ctx.logger.info("_canary.teardown: dropped schema %s (CASCADE).", schema)
    return {"schema": schema, "status": "torn_down"}

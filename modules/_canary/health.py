"""modules/_canary health check -- proves the whole contract actually worked.

A health check answers one question: did the thing this module deployed end up
working? For the canary, a successful ``SELECT count(*)`` on the heartbeat table
proves discovery -> dependency-ordering-behind-core -> deploy -> connectivity
all succeeded. Return ``healthy: True/False`` (or ``None`` when stubbed).
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    schema = ctx.params.get("canary_schema", "canary")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] _canary.health: would SELECT count(*) FROM %s.heartbeat and assert >= 1.",
            schema,
        )
        return {"table": f"{schema}.heartbeat", "healthy": None, "status": "stub"}

    conn = ctx.pg_connection(
        role="admin", database=ctx.params.get("database") or "databricks_postgres"
    )
    cur = conn.cursor()
    cur.execute(f'SELECT count(*) FROM "{schema}".heartbeat')
    row = cur.fetchone()
    rows = row[0] if row else 0
    healthy = rows is not None and rows >= 1
    return {
        "table": f"{schema}.heartbeat",
        "rows": rows,
        "healthy": healthy,
        "status": "ok" if healthy else "unhealthy",
    }

"""modules/_canary health check (P0 stub).

What the real step will do (P1+): confirm the canary table exists and is
readable, e.g.

    SELECT count(*) FROM canary.heartbeat;

A successful count proves the full contract worked: discovery, dependency
ordering behind core, deploy, and connectivity.

P0: logs intent only -- no live SQL is executed.
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    schema = ctx.params.get("canary_schema", "canary")
    ctx.logger.info("[stub] would SELECT count(*) FROM %s.heartbeat and assert >= 1 row.", schema)
    # TODO(P3): connect via psycopg; SELECT count(*); assert the row exists.
    return {"table": f"{schema}.heartbeat", "healthy": None, "status": "stub"}

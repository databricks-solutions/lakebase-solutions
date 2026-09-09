"""core/lakebase health check (P0 stub).

Responsibility: confirm the instance is reachable and the base database exists
(e.g. `SELECT 1` over psycopg).

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    instance = ctx.resolved_names.get("lakebase_instance", ctx.name("lakebase"))
    ctx.logger.info("[stub] would health-check Lakebase instance %r (SELECT 1).", instance)
    # TODO(P1): connect via psycopg and run `SELECT 1`; assert instance AVAILABLE.
    return {"instance": instance, "healthy": None, "status": "stub"}

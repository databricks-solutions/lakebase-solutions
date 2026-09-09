"""modules/_canary teardown step (P0 stub).

What the real step will do (P1+): drop everything the canary created, e.g.

    DROP SCHEMA IF EXISTS canary CASCADE;

Teardown runs in reverse dependency order, so the canary is torn down BEFORE its
core dependencies (lakebase, security).

P0: logs intent only -- no live SQL is executed.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    schema = ctx.params.get("canary_schema", "canary")
    ctx.logger.info("[stub] would DROP SCHEMA %s CASCADE (removes %s.heartbeat).", schema, schema)
    # TODO(P3): connect via psycopg; `DROP SCHEMA IF EXISTS <schema> CASCADE`.
    return {"schema": schema, "status": "stub"}

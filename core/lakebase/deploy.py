"""core/lakebase deploy step (P0 stub).

Responsibility: provision the Lakebase Postgres instance via the GA
`database_instance` DABs resource (autoscaling-only). This step also owns the
base database/schema bootstrap that later core components build on.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def deploy(ctx: Any) -> Dict[str, Any]:
    instance = ctx.resolved_names.get("lakebase_instance", ctx.name("lakebase"))
    capacity = ctx.params.get("capacity", "CU_1")
    node_count = ctx.params.get("node_count", "1")
    ctx.logger.info(
        "[stub] would deploy Lakebase instance %r (capacity=%s, node_count=%s) "
        "via the GA database_instance DABs resource (autoscaling-only).",
        instance,
        capacity,
        node_count,
    )
    # TODO(P1): set DABs var `prefix`/`capacity`/`node_count`, run
    #   `databricks bundle deploy` for the database_instance, then create the
    #   base database via `CREATE ROLE`/`CREATE SCHEMA` SQL over psycopg.
    return {"instance": instance, "status": "stub"}

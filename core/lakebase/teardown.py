"""core/lakebase teardown step (P0 stub).

Responsibility: destroy the Lakebase instance and its bundle-managed resources
(`bundle destroy` for the DABs half). Runs LAST in teardown order because every
other component depends on it.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    instance = ctx.resolved_names.get("lakebase_instance", ctx.name("lakebase"))
    ctx.logger.info(
        "[stub] would destroy Lakebase instance %r (via `databricks bundle destroy`).",
        instance,
    )
    # TODO(P1): `databricks bundle destroy` for the database_instance resource.
    return {"instance": instance, "status": "stub"}

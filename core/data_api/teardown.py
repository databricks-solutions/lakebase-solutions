"""core/data_api teardown step (P0 stub).

Responsibility: remove the dedicated Data API SP, drop the `databricks_auth`
role, and remove RLS policies. The manual UI *disable* (mirror of the phase-1
enable) is called out for the operator.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    ctx.logger.info(
        "[stub] would drop `databricks_auth` role + RLS policies and delete Data API SP %r. "
        "Reminder: disabling the Data API in the UI is a manual step.",
        ctx.name("data-api-sp"),
    )
    # TODO(P1): `DROP ROLE databricks_auth`; drop RLS policies; delete SP.
    return {"sp": ctx.name("data-api-sp"), "status": "stub"}

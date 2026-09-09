"""core/user_management teardown step (P0 stub).

Responsibility: drop participant PG roles and (optionally) the workshop group
created for this deployment. Admin group removal is opt-in to avoid clobbering a
shared admin group.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    workshop_group = ctx.params.get("workshop_group") or ctx.resolved_names.get("workshop_group")
    ctx.logger.info(
        "[stub] would drop participant PG roles and remove workshop group %r.",
        workshop_group,
    )
    # TODO(P1): `DROP ROLE` participant roles; optionally delete workshop group.
    return {"workshop_group": workshop_group, "status": "stub"}

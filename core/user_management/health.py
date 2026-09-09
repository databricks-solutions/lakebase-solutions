"""core/user_management health check (P0 stub).

Responsibility: confirm the admin/workshop groups exist and participant PG roles
are present.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")
    workshop_group = ctx.params.get("workshop_group") or ctx.resolved_names.get("workshop_group")
    ctx.logger.info(
        "[stub] would verify groups %r and %r exist and participant PG roles are present.",
        admin_group,
        workshop_group,
    )
    # TODO(P1): check group membership via SDK; verify PG roles exist.
    return {"admin_group": admin_group, "workshop_group": workshop_group, "healthy": None, "status": "stub"}

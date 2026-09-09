"""core/user_management deploy step (P0 stub).

Responsibility: workshop identity plumbing --
- ensure the admin and workshop Databricks groups exist,
- map participants to Postgres roles (via `CREATE ROLE` SQL) so each attendee
  has appropriately scoped DB access.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def deploy(ctx: Any) -> Dict[str, Any]:
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")
    workshop_group = ctx.params.get("workshop_group") or ctx.resolved_names.get("workshop_group")
    ctx.logger.info(
        "[stub] would ensure Databricks groups %r (admins) and %r (participants) "
        "and map participant PG roles via `CREATE ROLE` SQL.",
        admin_group,
        workshop_group,
    )
    # TODO(P1): create/verify groups via SDK; create participant PG roles + grants.
    return {"admin_group": admin_group, "workshop_group": workshop_group, "status": "stub"}

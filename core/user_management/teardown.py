"""core/user_management teardown step (P1 -- real).

Removes what ``deploy`` created:

* drops the participant PG role, and
* deletes the admin/workshop workspace groups **only when they are the
  deployment-owned defaults** (``${prefix}-admins`` / ``${prefix}-participants``).
  If the SA passed a custom, pre-existing group name via params, we leave it
  alone to avoid clobbering a shared group.

Best-effort throughout: a failure defers rather than aborting the run.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scim  # noqa: E402


def teardown(ctx: Any) -> Dict[str, Any]:
    workshop_group = ctx.params.get("workshop_group") or ctx.resolved_names.get("workshop_group")
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")
    database = ctx.params.get("database") or ctx.resolved_names.get("workshop_database") or "databricks_postgres"
    participant_role = ctx.resolved_names.get("pg_participant_role", f"{ctx.deployment_id}_participant")

    # Only delete groups we ourselves created (blank param => derived default).
    owns_admin = not ctx.params.get("admin_group")
    owns_workshop = not ctx.params.get("workshop_group")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/user_management.teardown: would drop PG role %r and delete "
            "owned groups (admin=%s, workshop=%s).",
            participant_role,
            owns_admin,
            owns_workshop,
        )
        return {"workshop_group": workshop_group, "pg_roles": [participant_role], "status": "stub"}

    result: Dict[str, Any] = {"workshop_group": workshop_group, "pg_roles": [participant_role]}
    deleted: List[str] = []

    # (1) Drop the participant PG role.
    try:
        conn = ctx.pg_connection(role="admin", database=database)
        cur = conn.cursor()
        cur.execute(f'DROP ROLE IF EXISTS "{participant_role}"')
        conn.commit()
        result["dropped_role"] = participant_role
    except Exception as exc:
        ctx.logger.error("[user_management] drop role deferred: %s", exc)
        result["pg_role_error"] = str(exc)

    # (2) Delete deployment-owned workspace groups.
    try:
        w = ctx.workspace_client()
        for name, owned in ((admin_group, owns_admin), (workshop_group, owns_workshop)):
            if not owned:
                continue
            grp = scim.find_group(w, name)
            if grp and grp.get("id"):
                scim.delete_group(w, grp["id"])
                deleted.append(name)
        result["deleted_groups"] = deleted
    except Exception as exc:
        ctx.logger.error("[user_management] group delete deferred: %s", exc)
        result["groups_error"] = str(exc)

    result["status"] = (
        "deferred" if ("pg_role_error" in result or "groups_error" in result) else "torn_down"
    )
    return result

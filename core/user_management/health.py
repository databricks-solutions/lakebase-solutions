"""core/user_management health check (P1 -- real).

Confirms the identity plumbing is in place:

* the admin workspace group exists and the deploying identity is a member
  (this is exactly what the fail-closed admin console gates on), and
* the participant PG role exists.

Returns ``healthy: True`` only when both hold. Off-Databricks (no live
clients) it returns a ``stub`` result with ``healthy: None``.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scim  # noqa: E402


def health_check(ctx: Any) -> Dict[str, Any]:
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")
    workshop_group = ctx.params.get("workshop_group") or ctx.resolved_names.get("workshop_group")
    database = ctx.params.get("database") or ctx.resolved_names.get("workshop_database") or "databricks_postgres"
    participant_role = ctx.resolved_names.get("pg_participant_role", f"{ctx.deployment_id}_participant")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/user_management.health: would verify group %r exists with "
            "the deployer as a member and PG role %r exists.",
            admin_group,
            participant_role,
        )
        return {
            "admin_group": admin_group,
            "workshop_group": workshop_group,
            "healthy": None,
            "status": "stub",
        }

    result: Dict[str, Any] = {"admin_group": admin_group, "workshop_group": workshop_group}
    admin_ok = False
    member_ok = False
    role_ok = False

    try:
        w = ctx.workspace_client()
        deployer = w.current_user.me().user_name
        grp = scim.find_group(w, admin_group)
        admin_ok = bool(grp)
        if grp:
            uid = scim.find_user_id(w, deployer)
            member_ok = bool(uid) and uid in scim.group_member_ids(grp)
        result["deployer"] = deployer
    except Exception as exc:
        result["groups_error"] = str(exc)

    try:
        conn = ctx.pg_connection(role="admin", database=database)
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname = '{participant_role}'")
        role_ok = cur.fetchone() is not None
    except Exception as exc:
        result["pg_role_error"] = str(exc)

    result["admin_group_exists"] = admin_ok
    result["deployer_is_admin"] = member_ok
    result["participant_role_exists"] = role_ok
    result["healthy"] = admin_ok and member_ok and role_ok
    result["status"] = "ok" if result["healthy"] else "unhealthy"
    return result

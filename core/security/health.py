"""core/security health check (P1).

Verifies the app + read-only PG roles exist (``pg_roles``).

Needs only a PG connection; when none is injected it returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict

_ROLES_EXIST = "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)"


def health_check(ctx: Any) -> Dict[str, Any]:
    app_role = ctx.resolved_names.get("pg_app_role", f"{ctx.deployment_id}_app")
    ro_role = ctx.resolved_names.get("pg_readonly_role", f"{ctx.deployment_id}_readonly")
    database = ctx.params.get("database") or "databricks_postgres"
    expected = [app_role, ro_role]

    if not ctx.has_pg_connection():
        ctx.logger.info(
            "[stub] core/security.health: no PG connection injected; would confirm "
            "roles %r exist.",
            expected,
        )
        return {"pg_roles": expected, "healthy": None, "status": "stub"}

    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    cur.execute(_ROLES_EXIST, (expected,))
    found = {row[0] for row in (cur.fetchall() or [])}
    healthy = all(role in found for role in expected)

    ctx.logger.info(
        "core/security.health: expected roles %r; found %s.",
        expected,
        sorted(found),
    )
    return {
        "pg_roles": expected,
        "found": sorted(found),
        "healthy": healthy,
        "status": "ok" if healthy else "unhealthy",
    }

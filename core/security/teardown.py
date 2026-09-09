"""core/security teardown step (P1).

Revokes grants and drops the PG roles this component created, then deletes the
role-credential secrets it wrote. ``DROP OWNED BY`` clears any objects/grants the
role still owns so ``DROP ROLE`` succeeds; ``DROP ROLE IF EXISTS`` is idempotent.

The secret **scope** itself is DABs-managed (`bundle destroy`) -- only the keys
this step wrote are removed.

When no live clients are injected it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict, List

# Mirror of ``deploy.ROLE_SECRET_KEYS`` -- kept local because the orchestrator
# loads each step file flat (no package context), so relative imports between
# sibling step files are not available at load time.
ROLE_SECRET_KEYS: List[str] = [
    "app-role-username",
    "app-role-password",
    "readonly-role-username",
    "readonly-role-password",
]


def role_teardown_sql(role: str, schema: str) -> List[str]:
    """Revoke/reassign then drop one role (idempotent)."""

    return [
        f'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema}" FROM "{role}"',
        f'REVOKE ALL PRIVILEGES ON SCHEMA "{schema}" FROM "{role}"',
        f'DROP OWNED BY "{role}"',
        f'DROP ROLE IF EXISTS "{role}"',
    ]


def teardown(ctx: Any) -> Dict[str, Any]:
    app_role = ctx.resolved_names.get("pg_app_role", f"{ctx.deployment_id}_app")
    ro_role = ctx.resolved_names.get("pg_readonly_role", f"{ctx.deployment_id}_readonly")
    database = ctx.params.get("database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/security.teardown: no live clients injected; would revoke "
            "+ DROP ROLE %r + %r and delete role secrets from %r. "
            "P1 live run happens in-workspace.",
            app_role,
            ro_role,
            scope,
        )
        return {"pg_roles": [app_role, ro_role], "secret_scope": scope, "status": "stub"}

    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    executed: List[str] = []
    for role in (app_role, ro_role):
        for stmt in role_teardown_sql(role, schema):
            try:
                cur.execute(stmt)
                executed.append(stmt)
            except Exception as exc:  # role/grant may already be gone -- best effort
                conn.rollback()
                ctx.logger.warning("core/security.teardown: %r failed: %s", stmt, exc)
    conn.commit()

    w = ctx.workspace_client()
    deleted: List[str] = []
    for key in ROLE_SECRET_KEYS:
        try:
            w.secrets.delete_secret(scope=scope, key=key)
            deleted.append(key)
        except Exception as exc:  # secret may already be gone -- best effort
            ctx.logger.warning("core/security.teardown: delete secret %r failed: %s", key, exc)

    ctx.logger.info(
        "core/security.teardown: dropped roles %r + %r; deleted %d secret(s) from %r.",
        app_role,
        ro_role,
        len(deleted),
        scope,
    )
    return {
        "pg_roles": [app_role, ro_role],
        "secret_scope": scope,
        "sql": executed,
        "secrets_deleted": deleted,
        "status": "torn_down",
    }

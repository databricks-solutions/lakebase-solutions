"""core/security deploy step (P1).

Creates the deployment's PG roles and writes their credentials to the standalone
secret scope:

* an **app** role (``${prefix}_app``) -- LOGIN + read/write on the workshop
  schema (DML, sequences, default privileges),
* a **read-only** role (``${prefix}_readonly``) -- LOGIN + SELECT only.

Roles are created via ``CREATE ROLE`` SQL over psycopg -- deliberately NOT the
Beta ``postgres_role`` bundle resource (SPEC section 4). Role creation is
idempotent via a ``DO``-block guard (Postgres has no ``CREATE ROLE IF NOT
EXISTS``); passwords are (re)set on every run to support rotation. Grant SQL is
ported from the FSM permissions script.

When no live clients are injected it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

import secrets as _secrets
import string
from typing import Any, Dict, List, Tuple

_PW_ALPHABET = string.ascii_letters + string.digits + "!@#$%&*"

# Secret keys this step writes (and teardown removes).
ROLE_SECRET_KEYS: List[str] = [
    "app-role-username",
    "app-role-password",
    "readonly-role-username",
    "readonly-role-password",
]


def generate_password(length: int = 24) -> str:
    """Return a random password for a native-auth PG role."""

    return "".join(_secrets.choice(_PW_ALPHABET) for _ in range(length))


def create_role_sql(role: str) -> str:
    """Idempotent ``CREATE ROLE`` via a ``DO``-block guard (no PW in the block)."""

    return (
        "DO $$ BEGIN "
        f"IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN "
        f'CREATE ROLE "{role}" WITH LOGIN; '
        "END IF; END $$;"
    )


def grant_sql(role: str, database: str, schema: str, readonly: bool) -> List[str]:
    """Grant SQL for one role (ported/pared from FSM ``03_setup_permissions``)."""

    stmts = [
        f'GRANT CONNECT ON DATABASE "{database}" TO "{role}"',
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"',
    ]
    if readonly:
        stmts += [
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"',
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT SELECT ON TABLES TO "{role}"',
        ]
    else:
        stmts += [
            f'GRANT CREATE ON SCHEMA "{schema}" TO "{role}"',
            f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"',
            f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "{schema}" TO "{role}"',
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" '
            f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{role}"',
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" '
            f'GRANT ALL PRIVILEGES ON SEQUENCES TO "{role}"',
        ]
    return stmts


def deploy(ctx: Any) -> Dict[str, Any]:
    app_role = ctx.resolved_names.get("pg_app_role", f"{ctx.deployment_id}_app")
    ro_role = ctx.resolved_names.get("pg_readonly_role", f"{ctx.deployment_id}_readonly")
    database = ctx.params.get("database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/security.deploy: no live clients injected; would CREATE "
            "ROLE %r + %r (via DO-block), grant on %s.%s, and write role secrets "
            "to %r. P1 live run happens in-workspace.",
            app_role,
            ro_role,
            database,
            schema,
            scope,
        )
        return {"pg_roles": [app_role, ro_role], "secret_scope": scope, "status": "stub"}

    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    executed: List[str] = []
    passwords = {app_role: generate_password(), ro_role: generate_password()}

    roles: Tuple[Tuple[str, bool], ...] = ((app_role, False), (ro_role, True))
    for role, readonly in roles:
        create = create_role_sql(role)
        cur.execute(create)
        executed.append(create)
        # Parameterized password (identifier is trusted/quoted; value is bound).
        pw_stmt = f'ALTER ROLE "{role}" WITH LOGIN PASSWORD %s'
        cur.execute(pw_stmt, (passwords[role],))
        executed.append(pw_stmt)
        for stmt in grant_sql(role, database, schema, readonly=readonly):
            cur.execute(stmt)
            executed.append(stmt)
    conn.commit()

    w = ctx.workspace_client()
    secret_map = {
        "app-role-username": app_role,
        "app-role-password": passwords[app_role],
        "readonly-role-username": ro_role,
        "readonly-role-password": passwords[ro_role],
    }
    for key, value in secret_map.items():
        w.secrets.put_secret(scope=scope, key=key, string_value=value)

    ctx.logger.info(
        "core/security.deploy: ensured roles %r + %r on %s.%s; wrote %d role "
        "secret(s) to %r.",
        app_role,
        ro_role,
        database,
        schema,
        len(secret_map),
        scope,
    )
    return {
        "pg_roles": [app_role, ro_role],
        "secret_scope": scope,
        "sql": executed,
        "secrets_written": sorted(secret_map),
        "status": "deployed",
    }

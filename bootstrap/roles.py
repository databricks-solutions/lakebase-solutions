"""Shared PG native-auth role provisioning helpers.

``core/`` is loaded flat by file path (it is deliberately NOT a package), so
sibling step files cannot import from one another. Any role/credential helper
that must be shared across components therefore lives here in the ``bootstrap``
package, which every module already imports (``from bootstrap.adapters import
...``). Both ``core/security/deploy.py`` (the admin console's app role) and
``modules/field_service/fs_steps.py`` (the field-service app's own role) build
their per-app native-password PG roles through these helpers.

Passwords are (re)set on every run to support rotation; role creation is
idempotent via a ``DO``-block guard (Postgres has no ``CREATE ROLE IF NOT
EXISTS``). Grant SQL is ported from the FSM permissions script.
"""

from __future__ import annotations

import secrets as _secrets
import string
from typing import Any, List

_PW_ALPHABET = string.ascii_letters + string.digits + "!@#$%&*"


def generate_password(length: int = 32) -> str:
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


def dba_console_grants(role: str) -> List[str]:
    """Extension + monitoring grants so the app's DBA/admin queries work.

    Ported from FSM ``03_setup_permissions``. Best-effort at the call site
    (each runs on its own commit; a missing privilege is logged, not fatal):
    ``pg_monitor`` gives cross-session visibility into ``pg_stat_statements``
    query text, and ``pgstattuple`` powers the table-bloat card.
    """

    return [
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
        "CREATE EXTENSION IF NOT EXISTS pgstattuple",
        f'GRANT SELECT ON pg_stat_statements TO "{role}"',
        # Full pgstattuple/pgstatindex function set the admin table/index-bloat
        # cards call (ported 1:1 from FSM 03_setup_permissions.py).
        f'GRANT EXECUTE ON FUNCTION pgstattuple(regclass) TO "{role}"',
        f'GRANT EXECUTE ON FUNCTION pgstattuple(text) TO "{role}"',
        f'GRANT EXECUTE ON FUNCTION pgstatindex(regclass) TO "{role}"',
        f'GRANT EXECUTE ON FUNCTION pgstatindex(text) TO "{role}"',
        f'GRANT EXECUTE ON FUNCTION pgstattuple_approx(regclass) TO "{role}"',
        f'GRANT pg_monitor TO "{role}"',
    ]


def provision_native_app_role(cur: Any, role: str, password: str) -> List[str]:
    """Create ``role`` (idempotent) + set its native LOGIN password.

    ``ALTER ROLE ... PASSWORD`` is DDL and does NOT accept bind parameters, so
    the password is inlined as a single-quoted SQL literal (the generated
    alphabet excludes quotes/backslashes; any quote is doubled defensively).
    Returns the executed SQL with the password line REDACTED so it never lands
    in logs / step results.
    """

    create = create_role_sql(role)
    cur.execute(create)
    pw_literal = "'" + password.replace("'", "''") + "'"
    cur.execute(f'ALTER ROLE "{role}" WITH LOGIN PASSWORD {pw_literal}')
    return [create, f'ALTER ROLE "{role}" WITH LOGIN PASSWORD <redacted>']


def write_app_secrets(w: Any, scope: str, prefix: str, role: str, password: str) -> List[str]:
    """Write an app's OWN ``{prefix}-pguser`` / ``{prefix}-pgpassword`` secrets.

    Each app gets its own credential keys (never a shared ``pguser``/``pgpassword``)
    so no two apps ever authenticate with the same PG identity. Returns the sorted
    keys written.
    """

    secret_map = {f"{prefix}-pguser": role, f"{prefix}-pgpassword": password}
    for key, value in secret_map.items():
        w.secrets.put_secret(scope=scope, key=key, string_value=value)
    return sorted(secret_map)

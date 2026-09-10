"""core/data_api teardown step.

Undoes the phase-2 configure: revokes the dedicated SP from ``authenticator``,
drops what it owns + the role, deletes the SP and its OAuth secret, and removes
the Data API secrets from the standalone scope. The manual UI *disable* (mirror
of the phase-1 enable) is called out for the operator -- it is not undone here.

Every step is best-effort (resources may already be gone); when no live clients
are injected it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

import base64
from typing import Any, Dict, Optional


def _read_secret(w: Any, scope: str, key: str) -> Optional[str]:
    """Best-effort read + base64-decode of a secret value; ``None`` if absent."""

    try:
        resp = w.secrets.get_secret(scope=scope, key=key)
    except Exception:
        return None
    value = getattr(resp, "value", None)
    if value is None:
        return None
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception:  # pragma: no cover - value already plain
        return str(value)


def teardown(ctx: Any) -> Dict[str, Any]:
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")
    database = ctx.params.get("database") or "databricks_postgres"
    sp_name = ctx.name("data-api-sp")

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/data_api.teardown: no workspace client injected; would revoke + drop "
            "the Data API role, delete SP %r, and remove its secrets from %r.",
            sp_name,
            scope,
        )
        return {"sp": sp_name, "status": "stub"}

    w = ctx.workspace_client()
    keys = [
        "data-api-sp-client-id",
        "data-api-sp-client-secret",
        "data-api-url",
        "data-api-database",
        "data-api-schema",
    ]
    app_id = _read_secret(w, scope, "data-api-sp-client-id")

    # (1) Undo the Postgres wiring (best-effort; role may be gone / never created).
    if app_id and ctx.has_pg_connection():
        try:
            conn = ctx.pg_connection(role="admin", database=database)
            try:
                conn.autocommit = True
            except Exception:  # pragma: no cover
                pass
            cur = conn.cursor()
            for stmt in (
                f'REVOKE "{app_id}" FROM authenticator',
                f'DROP OWNED BY "{app_id}"',
                f'DROP ROLE IF EXISTS "{app_id}"',
            ):
                try:
                    cur.execute(stmt)
                except Exception as exc:
                    ctx.logger.info("core/data_api.teardown: %s -> %s", stmt, exc)
        except Exception as exc:
            ctx.logger.warning("core/data_api.teardown: PG cleanup skipped: %s", exc)

    # (2) Delete the SP (by discovered name) -- best-effort.
    sp_deleted = False
    try:
        for sp in w.service_principals.list(filter=f'displayName eq "{sp_name}"'):
            w.service_principals.delete(id=sp.id)
            sp_deleted = True
    except Exception as exc:
        ctx.logger.warning("core/data_api.teardown: SP delete skipped: %s", exc)

    # (3) Remove the Data API secrets from the scope.
    for key in keys:
        try:
            w.secrets.delete_secret(scope=scope, key=key)
        except Exception:  # pragma: no cover - already absent
            pass

    ctx.logger.info(
        "core/data_api.teardown: removed Data API wiring for SP %r (sp_deleted=%s). "
        "Reminder: disabling the Data API in the UI is a manual step.",
        sp_name,
        sp_deleted,
    )
    return {"sp": sp_name, "sp_deleted": sp_deleted, "status": "torn_down"}

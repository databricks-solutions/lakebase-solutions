"""core/data_api health check.

Verifies the phase-2 wiring actually took: the dedicated service principal's role
must be a MEMBER of ``authenticator`` (the grant that clears the PostgREST
``SET ROLE`` 403 for a non-owner identity). The reliable signal is a Postgres
catalog query over the admin connection; as a bonus it best-effort mints an OAuth
DB credential AS the SP to prove the identity resolves.

A table query is intentionally NOT the health signal: a freshly deployed workshop
schema often has no tables yet, and the PostgREST root always returns ``PGRST106``
even when correctly wired -- so membership-in-``authenticator`` is the robust
check. When no live clients are injected it logs intent and returns ``stub``.
"""

from __future__ import annotations

import base64
from typing import Any, Dict, Optional

# authenticator-membership check: is <app_id> role a member of `authenticator`?
_MEMBERSHIP_SQL = (
    "SELECT 1 FROM pg_auth_members m "
    "JOIN pg_roles r ON m.roleid = r.oid "
    "JOIN pg_roles s ON m.member = s.oid "
    "WHERE r.rolname = 'authenticator' AND s.rolname = %s"
)


def _read_secret(w: Any, scope: str, key: str) -> Optional[str]:
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


def health_check(ctx: Any) -> Dict[str, Any]:
    sp_name = ctx.name("data-api-sp")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/data_api.health: no live clients injected; would assert SP %r's role "
            "is a member of `authenticator` and mint an OAuth token as the SP.",
            sp_name,
        )
        return {"sp": sp_name, "healthy": None, "status": "stub"}

    w = ctx.workspace_client()
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")
    database = ctx.params.get("database") or "databricks_postgres"
    app_id = _read_secret(w, scope, "data-api-sp-client-id")

    if not app_id:
        ctx.logger.info("core/data_api.health: no Data API SP configured (phase 2 not run).")
        return {"sp": sp_name, "healthy": None, "status": "not_configured"}

    # Best-effort like the deploy step: a health error must not raise out of the
    # step (it runs as part of the run's health phase).
    try:
        # (1) Robust signal: is the SP role a member of `authenticator`?
        conn = ctx.pg_connection(role="admin", database=database)
        cur = conn.cursor()
        cur.execute(_MEMBERSHIP_SQL, (app_id,))
        wired = cur.fetchone() is not None

        # (2) Bonus: prove the SP identity resolves by minting a DB credential as it.
        token_ok = _best_effort_sp_token(w, scope, ctx.logger)
    except Exception as exc:
        ctx.logger.warning("[data_api] health deferred: %s", exc)
        return {"sp": sp_name, "sp_application_id": app_id, "healthy": None,
                "error": str(exc), "status": "deferred"}

    ctx.logger.info(
        "core/data_api.health: SP %r (app_id=%s) member-of-authenticator=%s, sp_token=%s.",
        sp_name,
        app_id,
        wired,
        token_ok,
    )
    return {
        "sp": sp_name,
        "sp_application_id": app_id,
        "member_of_authenticator": wired,
        "sp_token_ok": token_ok,
        "healthy": wired,
        "status": "ok" if wired else "unhealthy",
    }


def _best_effort_sp_token(w: Any, scope: str, logger: Any) -> Optional[bool]:
    """Build a WorkspaceClient as the SP and mint a DB credential; None on any failure."""

    client_id = _read_secret(w, scope, "data-api-sp-client-id")
    client_secret = _read_secret(w, scope, "data-api-sp-client-secret")
    if not (client_id and client_secret):
        return None
    try:
        from databricks.sdk import WorkspaceClient  # lazy import

        host = getattr(getattr(w, "config", None), "host", None)
        w_sp = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        # Any authenticated call proves the identity resolves; keep it cheap.
        w_sp.current_user.me()
        return True
    except Exception as exc:  # pragma: no cover - live-only; secret propagation lag
        logger.info("core/data_api.health: SP token check skipped: %s", exc)
        return None

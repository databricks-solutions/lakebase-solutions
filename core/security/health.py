"""core/security health check (P0 stub).

Responsibility: confirm the secret scope + required keys exist and the app role
can authenticate to Postgres.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")
    ctx.logger.info(
        "[stub] would verify secret scope %r has pguser/pgpassword and the app role connects.",
        scope,
    )
    # TODO(P1): list secret keys; test PG login with the app role.
    return {"secret_scope": scope, "healthy": None, "status": "stub"}

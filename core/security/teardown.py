"""core/security teardown step (P0 stub).

Responsibility: revoke/drop PG roles and grants, delete the SP, and remove the
secret scope + keys created at deploy time.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")
    ctx.logger.info(
        "[stub] would drop PG roles/grants (via `DROP ROLE` SQL), delete the app SP, "
        "and remove secret scope %r.",
        scope,
    )
    # TODO(P1): `DROP ROLE`/`REVOKE` over psycopg; delete SP; delete secret scope.
    return {"secret_scope": scope, "status": "stub"}

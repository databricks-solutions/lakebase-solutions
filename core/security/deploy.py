"""core/security deploy step (P0 stub).

Responsibility: the deployment's security posture --
- standalone Databricks secret scope + credential keys (PG user/password),
- service principal(s) for app/Data API access,
- PG roles and grants created via `CREATE ROLE` SQL over psycopg (NOT the Beta
  `postgres_role` bundle resource; SPEC section 4).

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def deploy(ctx: Any) -> Dict[str, Any]:
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")
    ctx.logger.info(
        "[stub] would create secret scope %r, app service principal, and PG roles "
        "(%s_app, %s_app_perms) via `CREATE ROLE` SQL.",
        scope,
        ctx.deployment_id,
        ctx.deployment_id,
    )
    # TODO(P1): create secret scope + keys (DABs secret_scope + SDK put-secret),
    #   create SP, then `CREATE ROLE`/`GRANT` over psycopg. No postgres_role.
    return {"secret_scope": scope, "status": "stub"}

"""core/admin_app health check (P0 stub).

Responsibility: hit the deployed app's `/api/health` endpoint and confirm a 200
with DB connectivity (mirrors lakebase_admin's health contract).

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))
    ctx.logger.info("[stub] would GET %s /api/health and assert status ok + db connected.", app_name)
    # TODO(P2): resolve app URL; GET /api/health with bearer; assert 200.
    return {"app": app_name, "healthy": None, "status": "stub"}

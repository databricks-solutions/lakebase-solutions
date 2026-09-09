"""core/admin_app teardown step (P0 stub).

Responsibility: remove the admin app (`bundle destroy` for the `app` resource).

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))
    ctx.logger.info("[stub] would delete admin app %r (via `databricks bundle destroy`).", app_name)
    # TODO(P2): `databricks bundle destroy` for the app resource.
    return {"app": app_name, "status": "stub"}

"""core/admin_app teardown step.

Removes the admin app via the Databricks Apps **REST API**
(``DELETE /api/2.0/apps/<name>``) through ``w.api_client.do`` -- the same
version-proof surface the deploy step uses. ``databricks bundle destroy`` cannot
run on notebook/job compute, so the delete is REST-driven and best-effort (the
app may already be gone).

Needs a workspace client; when none is injected it logs intent and returns a
``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict

from bootstrap.adapters import APPS_API_BASE


def teardown(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.teardown: no workspace client injected; would "
            "DELETE /api/2.0/apps/%s.",
            app_name,
        )
        return {"app": app_name, "status": "stub"}

    w = ctx.workspace_client()
    deleted = False
    try:
        w.api_client.do("DELETE", f"{APPS_API_BASE}/{app_name}")
        deleted = True
        ctx.logger.info("core/admin_app.teardown: deleted app %r.", app_name)
    except Exception as exc:  # app may already be gone -- best effort
        ctx.logger.warning("core/admin_app.teardown: delete app %r failed: %s", app_name, exc)
    return {"app": app_name, "app_deleted": deleted, "status": "deleted" if deleted else "skipped"}

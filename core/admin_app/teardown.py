"""core/admin_app teardown step.

Removes the admin app. The app is a GA `app` DABs resource, so teardown is a
``databricks bundle destroy`` of that resource, driven from the in-workspace
deploy notebook.

Needs a workspace client; when none is injected it logs intent and returns a
``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict


def teardown(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.teardown: no workspace client injected; would delete "
            "app %r (via `databricks bundle destroy` of the `app` resource).",
            app_name,
        )
        return {"app": app_name, "status": "stub"}

    w = ctx.workspace_client()
    w.apps.delete(name=app_name)
    ctx.logger.info("core/admin_app.teardown: deleted app %r.", app_name)
    return {"app": app_name, "status": "deleted"}

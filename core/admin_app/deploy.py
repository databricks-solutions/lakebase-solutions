"""core/admin_app deploy step.

Deploys the always-on admin app (the Lakebase DBA console) via the GA `app`
DABs resource. The app source is this directory (see databricks.yml
``apps.admin_app.source_code_path: ./core/admin_app``) and its runtime config
lives in ``app.yaml`` (PG connection from the deployment's secret scope via
``valueFrom``, plus ``TARGET_SCHEMA`` / ``ADMIN_GROUP``).

The bundle expresses the app resource itself, so this step's job is to confirm
the app was created and record its identity. It needs only a workspace client;
when none is injected (e.g. the orchestrator smoke tests) it logs intent and
returns a ``stub`` result -- the live run happens in-workspace.
"""

from __future__ import annotations

from typing import Any, Dict


def deploy(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.deploy: no workspace client injected; would confirm "
            "app %r (deployed via the DABs `app` resource, source core/admin_app) is "
            "created and gated by group %r.",
            app_name,
            admin_group,
        )
        return {"app": app_name, "admin_group": admin_group, "status": "stub"}

    w = ctx.workspace_client()
    app = w.apps.get(name=app_name)
    status = getattr(getattr(app, "compute_status", None), "state", None) or getattr(app, "app_status", None)

    ctx.logger.info(
        "core/admin_app.deploy: confirmed app %r (gated by group %r); status=%s.",
        app_name,
        admin_group,
        status,
    )
    return {
        "app": app_name,
        "admin_group": admin_group,
        "url": getattr(app, "url", None),
        "compute_status": str(status) if status is not None else None,
        "status": "deployed",
    }

"""core/admin_app deploy step (P0 stub).

Responsibility: deploy the always-on admin app (Lakebase DBA console) via the GA
`app` DABs resource. Source for the app is this directory (see databricks.yml
`apps.admin_app.source_code_path: ./core/admin_app`). App runtime config lives in
`app.yaml`. The console itself is a fork of `lakebase_admin` (harvested in P2).

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def deploy(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))
    ctx.logger.info(
        "[stub] would deploy admin app %r via the DABs `app` resource "
        "(source: core/admin_app), gated by group %r.",
        app_name,
        ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group"),
    )
    # TODO(P2): harvest lakebase_admin app source here; wire PG creds + Data API
    #   env from the secret scope; `databricks bundle deploy` the app resource.
    return {"app": app_name, "status": "stub"}

"""core/lakebase teardown step.

SDK-deletes the whole Lakebase surface this deployment provisioned. ``databricks
bundle destroy`` cannot run on notebook/job compute, so teardown drives the
Databricks Python SDK (``WorkspaceClient``) instead of the CLI:

1. delete the autoscaling ``postgres`` **project** via REST
   (``DELETE /api/2.0/postgres/projects/<id>`` through ``w.api_client.do`` -- the
   notebook-runtime SDK has no typed autoscaling-postgres service) -- this removes the
   ``production`` branch, the ``primary`` endpoint, and every database/schema
   inside it, so an explicit ``DROP SCHEMA`` is moot,
2. delete the standalone secret **scope** (``w.secrets.delete_scope``) -- this
   removes every connection secret the deploy step wrote in one call.

Both deletes are best-effort (the project or scope may already be gone). Runs
LAST in teardown order because every other component depends on it.

When no live clients are injected it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict

from bootstrap.adapters import POSTGRES_API_BASE

# Connection-info secret keys the deploy step wrote (documented here for
# reference; teardown removes the whole scope rather than deleting keys 1-by-1).
CONN_SECRET_KEYS = ["pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"]


def teardown(ctx: Any) -> Dict[str, Any]:
    project = ctx.resolved_names.get("lakebase_project", ctx.deployment_id)
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/lakebase.teardown: no workspace client injected; would "
            "SDK-delete postgres project %r and secret scope %r.",
            project,
            scope,
        )
        return {"project": project, "secret_scope": scope, "status": "stub"}

    w = ctx.workspace_client()

    # (1) Delete the autoscaling `postgres` project (removes branch/endpoint/DBs)
    #     via REST DELETE.
    project_deleted = False
    try:
        w.api_client.do("DELETE", f"{POSTGRES_API_BASE}/projects/{project}")
        project_deleted = True
    except Exception as exc:  # project may already be gone -- best effort
        ctx.logger.warning("core/lakebase.teardown: delete project %r failed: %s", project, exc)

    # (2) Delete the standalone secret scope (removes all connection secrets).
    scope_deleted = False
    try:
        w.secrets.delete_scope(scope)
        scope_deleted = True
    except Exception as exc:  # scope may already be gone -- best effort
        ctx.logger.warning("core/lakebase.teardown: delete scope %r failed: %s", scope, exc)

    ctx.logger.info(
        "core/lakebase.teardown: SDK-deleted project %r (%s) and scope %r (%s).",
        project,
        "ok" if project_deleted else "skip/err",
        scope,
        "ok" if scope_deleted else "skip/err",
    )
    return {
        "project": project,
        "secret_scope": scope,
        "project_deleted": project_deleted,
        "scope_deleted": scope_deleted,
        "status": "torn_down",
    }

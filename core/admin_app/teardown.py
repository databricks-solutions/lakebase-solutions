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


def _delete_ash_collector_job(w: Any, ctx: Any) -> bool:
    """Best-effort delete the always-on ASH collector job by name (reuse-by-name)."""

    job_name = f"{ctx.deployment_id}-ash-collector"
    try:
        found = w.api_client.do("GET", "/api/2.1/jobs/list", query={"name": job_name})
        existing = found.get("jobs", []) if isinstance(found, dict) else []
    except Exception as exc:  # best effort -- may already be gone
        ctx.logger.warning("core/admin_app.teardown: list job %r failed: %s", job_name, exc)
        return False
    deleted = False
    for job in existing:
        jid = job.get("job_id")
        if jid is None:
            continue
        try:
            w.api_client.do("POST", "/api/2.1/jobs/delete", body={"job_id": int(jid)})
            deleted = True
            ctx.logger.info("core/admin_app.teardown: deleted ASH collector job %r (job_id=%s).",
                            job_name, jid)
        except Exception as exc:  # pragma: no cover - live-only
            ctx.logger.warning("core/admin_app.teardown: delete job %r failed: %s", job_name, exc)
    return deleted


def teardown(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.teardown: no workspace client injected; would "
            "DELETE /api/2.0/apps/%s and the ASH collector job.",
            app_name,
        )
        return {"app": app_name, "status": "stub"}

    w = ctx.workspace_client()
    # Best-effort remove the always-on collector job first (independent of the app).
    collector_deleted = _delete_ash_collector_job(w, ctx)

    deleted = False
    try:
        w.api_client.do("DELETE", f"{APPS_API_BASE}/{app_name}")
        deleted = True
        ctx.logger.info("core/admin_app.teardown: deleted app %r.", app_name)
    except Exception as exc:  # app may already be gone -- best effort
        ctx.logger.warning("core/admin_app.teardown: delete app %r failed: %s", app_name, exc)
    return {"app": app_name, "app_deleted": deleted,
            "ash_collector_deleted": collector_deleted,
            "status": "deleted" if deleted else "skipped"}

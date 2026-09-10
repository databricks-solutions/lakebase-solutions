"""core/admin_app deploy step.

Provisions the always-on admin app (the Lakebase DBA console) via the Databricks
Python SDK (``w.apps``). ``databricks bundle deploy`` cannot run on notebook/job
compute, so the app is created (if missing) and deployed from its workspace
source path through the SDK. Its runtime config lives in ``app.yaml`` (PG
connection from the deployment's secret scope via ``valueFrom``, plus
``TARGET_SCHEMA`` / ``ADMIN_GROUP``).

**Robustness:** the create/deploy is wrapped so any failure -- including
``w.apps`` being ABSENT on the notebook-runtime SDK (``AttributeError``) -- does
NOT abort the whole run. On failure it logs ``[admin_app] deferred: <err>`` and
returns a ``{"status": "deferred", ...}`` result instead of raising -- the infra
(Lakebase/security) validates independently, and the app can be iterated on
separately. The exact ``w.apps`` request shapes are best-effort equivalents of
the apps CLI and are marked "verify at live run".

Needs only a workspace client; when none is injected (e.g. the orchestrator
smoke tests) it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict

# Workspace source path the app is deployed from (the app package in the Repo).
# verify the exact workspace path at live run.
APP_SOURCE_CODE_PATH = (
    "/Workspace/Users/redacted-user/lakebase-solutions/core/admin_app"
)


def _ensure_app(w: Any, app_name: str, logger: Any) -> bool:
    """Create the app if it does not already exist; return True if created.

    verify ``w.apps.get`` / ``w.apps.create`` arg + response shapes at live run.
    """

    try:
        w.apps.get(name=app_name)
        return False
    except Exception as exc:  # not found (or transient) -- attempt create.
        logger.info("core/admin_app.deploy: app %r not found (%s); creating.", app_name, exc)
        w.apps.create(name=app_name)
        return True


def deploy(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")
    source_path = ctx.params.get("admin_app_source_path") or APP_SOURCE_CODE_PATH

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.deploy: no workspace client injected; would create "
            "app %r (if missing) and deploy it from %r, gated by group %r.",
            app_name,
            source_path,
            admin_group,
        )
        return {"app": app_name, "admin_group": admin_group, "status": "stub"}

    w = ctx.workspace_client()
    try:
        # `w.apps` may be entirely absent on the notebook-runtime SDK; touching it
        # raises AttributeError, which the broad except below turns into a defer.
        if getattr(w, "apps", None) is None:
            raise AttributeError("WorkspaceClient has no attribute 'apps'")
        created = _ensure_app(w, app_name, ctx.logger)
        # Deploy the app from its workspace source path. verify arg shape at live run.
        w.apps.deploy(app_name=app_name, source_code_path=source_path)

        app = None
        try:  # best-effort identity read-back (status/url).
            app = w.apps.get(name=app_name)
        except Exception:  # pragma: no cover - live-only shape variance
            pass
        status = None
        if app is not None:
            status = getattr(getattr(app, "compute_status", None), "state", None) or getattr(
                app, "app_status", None
            )

        ctx.logger.info(
            "core/admin_app.deploy: deployed app %r from %r (created=%s, gated by %r); status=%s.",
            app_name,
            source_path,
            created,
            admin_group,
            status,
        )
        return {
            "app": app_name,
            "admin_group": admin_group,
            "source_code_path": source_path,
            "created": created,
            "url": getattr(app, "url", None) if app is not None else None,
            "compute_status": str(status) if status is not None else None,
            "status": "deployed",
        }
    except Exception as exc:
        # Any failure (incl. w.apps absent -> AttributeError) must NOT abort the
        # whole run: log and defer.
        ctx.logger.error("[admin_app] deferred: %s", exc)
        return {
            "app": app_name,
            "admin_group": admin_group,
            "source_code_path": source_path,
            "error": str(exc),
            "status": "deferred",
        }

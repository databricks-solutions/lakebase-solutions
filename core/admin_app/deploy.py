"""core/admin_app deploy step.

Provisions the always-on admin app (the Lakebase DBA console) via the Databricks
Apps **REST API** (``/api/2.0/apps``), called through ``w.api_client.do``.
``databricks bundle deploy`` cannot run on notebook/job compute, and the typed
``w.apps`` service is not present on every notebook-runtime SDK, so -- exactly as
``core/lakebase`` does for the autoscaling ``postgres`` surface -- this step
drives the REST surface directly. ``api_client`` is present on every SDK version.

In order, the step:

1. GETs ``/api/2.0/apps/<name>``; on 404, POSTs ``/api/2.0/apps`` to create the
   app WITH its secret resources (so the ``valueFrom`` env in ``app.yaml`` -- the
   PG connection secrets ``pghost`` / ``pgdatabase`` / ``pguser`` / ``pgpassword``
   -- resolves at launch from this deployment's standalone secret scope), then
   polls until compute is ACTIVE,
2. creates a deployment: ``POST /api/2.0/apps/<name>/deployments`` with the
   workspace ``source_code_path`` (the app package in the synced Repo) and
   ``mode: SNAPSHOT``, then polls the deployment until it SUCCEEDS/FAILS,
3. reads the app back for its URL + compute/app status.

**Robustness:** the whole thing is wrapped so any failure does NOT abort the run
(the infra -- Lakebase/security -- validates independently, and the app can be
iterated on separately). On failure it logs ``[admin_app] deferred: <err>`` and
returns ``{"status": "deferred", ...}`` instead of raising.

Needs only a workspace client; when none is injected (e.g. the orchestrator
smoke tests) it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from bootstrap.adapters import APPS_API_BASE

# The PG connection-secret keys core/lakebase writes to the standalone scope, and
# which app.yaml exposes to the app via ``valueFrom: <key>``. Each becomes an app
# secret RESOURCE named after the key so the launch-time ``valueFrom`` resolves.
_PG_SECRET_KEYS: List[str] = ["pghost", "pgdatabase", "pguser", "pgpassword"]

# App compute + deployment poll budgets (apps can take minutes to go ACTIVE).
_APP_POLL_ATTEMPTS = 60
_APP_POLL_DELAY_SECONDS = 10.0

_ACTIVE_COMPUTE_STATES = {"ACTIVE"}
_TERMINAL_DEPLOY_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "CANCELLED"}


def _default_source_path(w: Any, ctx: Any) -> str:
    """Derive the workspace source path of the admin app from the live identity.

    The Repo is synced to ``/Workspace/Users/<email>/<repo>/`` and the app package
    is ``core/admin_app`` under it. Both the connecting email and the repo folder
    name are resolved at runtime so nothing is hardcoded; either can be overridden
    with the ``admin_app_source_path`` / ``repo_folder`` params.
    """

    repo_folder = ctx.params.get("repo_folder") or "lakebase-solutions"
    try:
        email = w.current_user.me().user_name
    except Exception:  # pragma: no cover - live-only; fall back to a param
        email = ctx.params.get("workspace_user") or "unknown"
    return f"/Workspace/Users/{email}/{repo_folder}/core/admin_app"


def _secret_resources(scope: str) -> List[Dict[str, Any]]:
    """App secret-resource entries mapping each ``valueFrom`` key to the scope."""

    return [
        {
            "name": key,
            "description": f"Lakebase connection secret '{key}' for the admin console",
            "secret": {"scope": scope, "key": key, "permission": "READ"},
        }
        for key in _PG_SECRET_KEYS
    ]


def _app_states(app: Dict[str, Any]) -> Dict[str, Any]:
    """Pull url + compute/app/deployment states out of a GET-app response dict."""

    compute = (app.get("compute_status") or {}).get("state")
    app_state = (app.get("app_status") or {}).get("state")
    active = app.get("active_deployment") or {}
    deploy_state = (active.get("status") or {}).get("state")
    return {
        "url": app.get("url"),
        "compute_state": compute,
        "app_state": app_state,
        "deployment_state": deploy_state,
    }


def _get_app(w: Any, app_name: str) -> Dict[str, Any]:
    """GET ``/api/2.0/apps/<name>`` -> parsed dict (raises on non-404 error)."""

    return w.api_client.do("GET", f"{APPS_API_BASE}/{app_name}")


def _is_not_found(exc: Exception) -> bool:
    """Offline-safe 404/NOT_FOUND detection for ``w.api_client.do`` errors."""

    code = str(getattr(exc, "error_code", "") or "").upper()
    if "NOT_FOUND" in code or "DOES_NOT_EXIST" in code:
        return True
    if getattr(exc, "status_code", None) == 404:
        return True
    text = str(exc).lower()
    return "not found" in text or "does not exist" in text or "404" in text


def _wait_for_compute_active(w: Any, app_name: str, logger: Any) -> Dict[str, Any]:
    """Poll GET app until compute is ACTIVE (or the budget runs out); return last app."""

    app: Dict[str, Any] = {}
    for _ in range(_APP_POLL_ATTEMPTS):
        app = _get_app(w, app_name)
        state = (app.get("compute_status") or {}).get("state")
        if state in _ACTIVE_COMPUTE_STATES:
            return app
        time.sleep(_APP_POLL_DELAY_SECONDS)  # pragma: no cover - live-only wait
    logger.warning("core/admin_app.deploy: compute not ACTIVE within budget for %r.", app_name)
    return app


def _wait_for_deployment(w: Any, app_name: str, deployment_id: str, logger: Any) -> str:
    """Poll a deployment until it reaches a terminal state; return that state."""

    state = ""
    for _ in range(_APP_POLL_ATTEMPTS):
        dep = w.api_client.do(
            "GET", f"{APPS_API_BASE}/{app_name}/deployments/{deployment_id}"
        )
        state = (dep.get("status") or {}).get("state") or ""
        if state in _TERMINAL_DEPLOY_STATES:
            return state
        time.sleep(_APP_POLL_DELAY_SECONDS)  # pragma: no cover - live-only wait
    return state


def deploy(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.deploy: no workspace client injected; would create "
            "app %r (if missing, with PG secret resources from scope %r), deploy it, "
            "and gate it by group %r.",
            app_name,
            scope,
            admin_group,
        )
        return {"app": app_name, "admin_group": admin_group, "status": "stub"}

    w = ctx.workspace_client()
    source_path = ctx.params.get("admin_app_source_path") or _default_source_path(w, ctx)
    try:
        # (1) Create the app (with its secret resources) if it does not exist.
        created = False
        try:
            _get_app(w, app_name)
            ctx.logger.info("core/admin_app.deploy: app %r already exists.", app_name)
        except Exception as exc:
            if not _is_not_found(exc):
                raise
            ctx.logger.info("core/admin_app.deploy: app %r not found; creating.", app_name)
            w.api_client.do(
                "POST",
                APPS_API_BASE,
                body={
                    "name": app_name,
                    "description": "Lakebase Admin console (always-on core component).",
                    "resources": _secret_resources(scope),
                },
            )
            created = True
            _wait_for_compute_active(w, app_name, ctx.logger)

        # (2) Create a deployment from the workspace source path (SNAPSHOT).
        dep = w.api_client.do(
            "POST",
            f"{APPS_API_BASE}/{app_name}/deployments",
            body={"source_code_path": source_path, "mode": "SNAPSHOT"},
        )
        deployment_id = dep.get("deployment_id") or (dep.get("status") or {}).get("deployment_id")
        deploy_state = (dep.get("status") or {}).get("state") or ""
        if deployment_id and deploy_state not in _TERMINAL_DEPLOY_STATES:
            deploy_state = _wait_for_deployment(w, app_name, deployment_id, ctx.logger)

        # (3) Read the app back for URL + states.
        app = _get_app(w, app_name)
        states = _app_states(app)
        # Only claim "deployed" on a genuine success signal: the deployment reached
        # SUCCEEDED and compute is ACTIVE. Anything else (empty/None/other) is
        # surfaced as "unhealthy" rather than a false-positive "deployed".
        effective_deploy_state = deploy_state or states["deployment_state"]
        healthy_deploy = (
            effective_deploy_state == "SUCCEEDED"
            and states["compute_state"] in _ACTIVE_COMPUTE_STATES
        )
        ctx.logger.info(
            "core/admin_app.deploy: app %r deployed from %r (created=%s); url=%s, "
            "compute=%s, deployment=%s.",
            app_name,
            source_path,
            created,
            states["url"],
            states["compute_state"],
            deploy_state or states["deployment_state"],
        )
        return {
            "app": app_name,
            "admin_group": admin_group,
            "source_code_path": source_path,
            "created": created,
            "url": states["url"],
            "compute_status": states["compute_state"],
            "deployment_state": deploy_state or states["deployment_state"],
            "status": "deployed" if healthy_deploy else "unhealthy",
        }
    except Exception as exc:
        # Any failure must NOT abort the whole run: log and defer.
        ctx.logger.error("[admin_app] deferred: %s", exc)
        return {
            "app": app_name,
            "admin_group": admin_group,
            "source_code_path": source_path,
            "error": str(exc),
            "status": "deferred",
        }

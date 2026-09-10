"""core/admin_app health check.

Confirms the admin app is deployed and running. The reliable signal is the app's
own control-plane state, read via the Apps **REST API**
(``GET /api/2.0/apps/<name>``) through ``w.api_client.do``: the app is healthy
when compute is ACTIVE and its active deployment SUCCEEDED. As a bonus, it makes
a best-effort HTTP GET of ``<url>/api/health`` (the app's Flask liveness route,
which also pings the database) -- but that call can be rejected by the app's
OAuth front door with a bare workspace token, so it never decides health on its
own; a failure there is logged and ignored.

Needs a workspace client; when none is injected it logs intent and returns a
``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict

from bootstrap.adapters import APPS_API_BASE


def health_check(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.health: no workspace client injected; would GET "
            "/api/2.0/apps/%s and assert compute ACTIVE + deployment SUCCEEDED.",
            app_name,
        )
        return {"app": app_name, "healthy": None, "status": "stub"}

    # Best-effort: the admin app is a deferred/optional step, so a health failure
    # must NOT abort the run.
    try:
        w = ctx.workspace_client()
        app = w.api_client.do("GET", f"{APPS_API_BASE}/{app_name}")
        compute_state = (app.get("compute_status") or {}).get("state")
        active = app.get("active_deployment") or {}
        deploy_state = (active.get("status") or {}).get("state")
        url = app.get("url")

        healthy = compute_state == "ACTIVE" and deploy_state == "SUCCEEDED"

        # Bonus liveness ping (never decides health -- OAuth may reject a bare token).
        db_status = None
        if url:
            db_status = _best_effort_health_ping(w, url, ctx.logger)

        ctx.logger.info(
            "core/admin_app.health: app %r compute=%s deployment=%s url=%s (db_ping=%s).",
            app_name,
            compute_state,
            deploy_state,
            url,
            db_status,
        )
        return {
            "app": app_name,
            "url": url,
            "compute_state": compute_state,
            "deployment_state": deploy_state,
            "db": db_status,
            "healthy": healthy,
            "status": "ok" if healthy else "unhealthy",
        }
    except Exception as exc:
        ctx.logger.warning("[admin_app] health deferred: %s", exc)
        return {"app": app_name, "healthy": None, "error": str(exc), "status": "deferred"}


def _best_effort_health_ping(w: Any, url: str, logger: Any) -> Any:
    """GET ``<url>/api/health`` with a bearer token; return the ``db`` field or None."""

    import json
    import urllib.request

    try:
        token = w.config.token or ""
        req = urllib.request.Request(
            f"{url.rstrip('/')}/api/health",
            headers={"Authorization": f"Bearer {token}"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode() or "{}")
        return body.get("db")
    except Exception as exc:  # pragma: no cover - live-only; OAuth/network variance
        logger.info("core/admin_app.health: liveness ping skipped: %s", exc)
        return None

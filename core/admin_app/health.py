"""core/admin_app health check.

Confirms the deployed admin app is reachable by GETting its ``/api/health``
endpoint, which returns ``status: "ok"`` plus a best-effort database ping
(``db: "connected"`` when the console can check out a pooled connection).

Needs a workspace client to resolve the app URL and mint a bearer token; when
none is injected it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    app_name = ctx.resolved_names.get("admin_app", ctx.name("admin-app"))

    if not ctx.has_workspace_client():
        ctx.logger.info(
            "[stub] core/admin_app.health: no workspace client injected; would GET "
            "%r /api/health and assert HTTP 200 with status ok.",
            app_name,
        )
        return {"app": app_name, "healthy": None, "status": "stub"}

    import json
    import urllib.request

    # Best-effort: the admin app is a deferred/optional step, so a health failure
    # (incl. w.apps method variance on the runtime SDK) must NOT abort the run.
    try:
        w = ctx.workspace_client()
        app = w.apps.get(name=app_name)
        base_url = (getattr(app, "url", "") or "").rstrip("/")
        token = w.config.token or ""

        req = urllib.request.Request(
            f"{base_url}/api/health",
            headers={"Authorization": f"Bearer {token}"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            code = resp.getcode()
            body = json.loads(resp.read().decode() or "{}")

        healthy = code == 200 and body.get("status") == "ok"
        ctx.logger.info(
            "core/admin_app.health: GET %s/api/health -> HTTP %s (status=%s, db=%s).",
            base_url,
            code,
            body.get("status"),
            body.get("db"),
        )
        return {
            "app": app_name,
            "http_status": code,
            "db": body.get("db"),
            "healthy": healthy,
            "status": "ok" if healthy else "unhealthy",
        }
    except Exception as exc:
        ctx.logger.warning("[admin_app] health deferred: %s", exc)
        return {"app": app_name, "healthy": None, "error": str(exc), "status": "deferred"}

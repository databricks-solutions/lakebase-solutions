"""core/admin_app deploy/teardown/health unit tests (offline, no workspace).

The admin_app step drives the Databricks Apps REST surface (``/api/2.0/apps``)
via ``w.api_client.do`` -- not the typed ``w.apps`` service, which is absent on
some notebook-runtime SDKs. These tests inject the fake api-client and assert the
create-with-secret-resources, deploy-from-source, and read-back behaviour, plus
the best-effort defer + stub guards.
"""

from __future__ import annotations

from bootstrap.context import DeployContext

from _fakes import live_context, load_step

admin_deploy = load_step("admin_app", "deploy.py")
admin_teardown = load_step("admin_app", "teardown.py")
admin_health = load_step("admin_app", "health.py")

APP = "acme-ws-admin-app"


def test_deploy_creates_app_with_secret_resources_then_deploys():
    ctx, _conn, ws = live_context()
    result = admin_deploy.deploy(ctx)

    assert result["status"] == "deployed"
    assert result["app"] == APP
    assert result["url"] == "https://app.example"
    assert result["created"] is True

    api = ws.api_client
    # Created via REST POST /api/2.0/apps, WITH the four PG secret resources so the
    # app.yaml `valueFrom` env resolves from this deployment's scope at launch.
    posts = [c for c in api.calls if c[0] == "POST" and c[1] == "/api/2.0/apps"]
    assert len(posts) == 1
    body = posts[0][2]
    assert body["name"] == APP
    res_names = {r["name"] for r in body["resources"]}
    assert res_names == {"pghost", "pgdatabase", "pguser", "pgpassword"}
    assert all(r["secret"]["scope"] == "acme-ws-secrets" for r in body["resources"])
    assert all(r["secret"]["permission"] == "READ" for r in body["resources"])

    # Deployed from the derived workspace source path with SNAPSHOT mode.
    deps = [c for c in api.calls if c[0] == "POST" and c[1].endswith("/deployments")]
    assert len(deps) == 1
    assert deps[0][2]["mode"] == "SNAPSHOT"
    assert deps[0][2]["source_code_path"] == (
        "/Workspace/Users/admin@example.com/lakebase-solutions/core/admin_app"
    )


def test_deploy_source_path_override_is_honored():
    ctx, _conn, _ws = live_context(params={"admin_app_source_path": "/Workspace/custom/app"})
    result = admin_deploy.deploy(ctx)
    assert result["source_code_path"] == "/Workspace/custom/app"


def test_deploy_defers_on_error_instead_of_raising():
    # A workspace client whose api_client raises on every call must NOT abort the run.
    class Boom:
        def do(self, *a, **k):
            raise RuntimeError("apps API exploded")

    ctx, _conn, ws = live_context()
    ws.api_client = Boom()
    result = admin_deploy.deploy(ctx)
    assert result["status"] == "deferred"
    assert "exploded" in result["error"]


def test_teardown_deletes_app_via_rest():
    ctx, _conn, ws = live_context()
    result = admin_teardown.teardown(ctx)
    assert result["status"] == "deleted"
    assert ("DELETE", f"/api/2.0/apps/{APP}", None) in ws.api_client.calls


def test_health_ok_when_compute_active_and_deployment_succeeded():
    ctx, _conn, ws = live_context()
    ws.api_client._app_exists = True  # GET app returns ACTIVE + SUCCEEDED
    result = admin_health.health_check(ctx)
    assert result["healthy"] is True
    assert result["compute_state"] == "ACTIVE"
    assert result["deployment_state"] == "SUCCEEDED"


def test_no_client_configured_is_stub_not_crash():
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    assert admin_deploy.deploy(ctx)["status"] == "stub"
    assert admin_teardown.teardown(ctx)["status"] == "stub"
    assert admin_health.health_check(ctx)["status"] == "stub"

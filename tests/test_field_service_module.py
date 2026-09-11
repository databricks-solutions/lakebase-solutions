"""field_service module scaffold tests (offline).

Loads the module's deploy/teardown/health entrypoints by path (as the
orchestrator does), and checks the internal sub-pipeline runs, respects gate
params, and tears down in reverse. Steps are stubs today; these tests lock the
contract so the wave-by-wave real implementations keep it.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from bootstrap.context import DeployContext

from _fakes import FakeApiClient, FakeWorkspaceClient, live_context

MOD = Path(__file__).resolve().parents[1] / "modules" / "field_service"


def _load(filename: str):
    path = MOD / filename
    spec = importlib.util.spec_from_file_location(f"fs_test_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fs_deploy = _load("deploy.py")
fs_teardown = _load("teardown.py")
fs_health = _load("health.py")


def _ctx(**params):
    return DeployContext(deployment_id="acme-ws", params=params)


def test_deploy_runs_full_pipeline_as_stubs():
    result = fs_deploy.deploy(_ctx())
    assert result["module"] == "field_service"
    assert result["status"] == "stub"
    names = [s["step"] for s in result["steps"]]
    # All 13 steps run when every gate defaults on.
    assert names == [
        "data", "uc_catalog", "warehouse", "features", "synced", "pipeline",
        "genie", "dashboards", "governance", "ml", "agent", "ops", "app",
    ]


def test_gates_skip_optional_steps():
    result = fs_deploy.deploy(
        _ctx(include_pipeline="false", include_ml="false", include_agent="false", include_ops_jobs="false")
    )
    names = [s["step"] for s in result["steps"]]
    assert names == ["data", "uc_catalog", "warehouse", "features", "synced",
                     "genie", "dashboards", "governance", "app"]
    for gated in ("pipeline", "ml", "agent", "ops"):
        assert gated not in names


def test_teardown_runs_in_reverse():
    result = fs_teardown.teardown(_ctx())
    assert result["status"] == "torn_down"
    names = [s["step"] for s in result["steps"]]
    assert names[0] == "app" and names[-1] == "data"


def test_health_aggregates_stub():
    result = fs_health.health_check(_ctx())
    assert result["module"] == "field_service"
    assert result["status"] == "stub"
    assert result["healthy"] is None


# --- data step (first real step) -------------------------------------------- #
def _data_step():
    import fs_steps  # cached in sys.modules by the entrypoint loads above

    return next(s for s in fs_steps.ORDERED_STEPS if s.name == "data")


def test_data_step_stub_when_not_live():
    assert _data_step().deploy(_ctx())["status"] == "stub"


def test_data_step_deploy_applies_real_sql_assets():
    ctx, conn, _ws = live_context(deployment_id="acme-ws")
    res = _data_step().deploy(ctx)
    assert res["status"] == "deployed"
    assert res["schemas"] == ["field_service", "ai_memory", "monitoring"]
    assert res["statements_failing"] == 0
    assert res["statements_applied"] > 20  # many statements across the seed files
    sql = conn.executed_sql()
    assert any("CREATE SCHEMA IF NOT EXISTS field_service" in s for s in sql)
    assert any("work_orders" in s for s in sql)


def test_data_step_teardown_drops_schemas():
    ctx, conn, _ws = live_context()
    res = _data_step().teardown(ctx)
    assert res["status"] == "torn_down"
    sql = conn.executed_sql()
    assert any('DROP SCHEMA IF EXISTS "field_service" CASCADE' == s for s in sql)
    assert any('DROP SCHEMA IF EXISTS "monitoring" CASCADE' == s for s in sql)


def test_data_step_health_ok_when_table_present():
    ctx, _conn, _ws = live_context()
    res = _data_step().health(ctx)
    assert res["healthy"] is True
    assert res["status"] == "ok"


# --- platform step group (uc_catalog / warehouse / features / synced) ------- #
def _step(name):
    import fs_steps

    return next(s for s in fs_steps.ORDERED_STEPS if s.name == name)


def _live_ctx_with_project(**params):
    # project_exists=True so the catalog step can read the project/branch uids.
    api = FakeApiClient(project_exists=True)
    ws = FakeWorkspaceClient(api_client=api)
    ctx, conn, ws = live_context(ws=ws, deployment_id="acme-ws", params=params)
    return ctx, conn, ws


def test_platform_steps_stub_offline():
    for name in ("warehouse", "uc_catalog", "features", "synced"):
        assert _step(name).deploy(_ctx())["status"] == "stub"


def test_warehouse_step_creates_and_persists_id():
    ctx, _conn, ws = _live_ctx_with_project()
    res = _step("warehouse").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["warehouse"] == "acme-ws-fs-warehouse"
    assert res["warehouse_id"] == "wh-fake-1"
    assert ws.secrets.value_for("fs-warehouse-id") == "wh-fake-1"  # persisted for later steps
    assert _step("warehouse").health(ctx)["status"] == "ok"


def test_uc_catalog_step_creates_linked_catalog():
    ctx, _conn, ws = _live_ctx_with_project()
    res = _step("uc_catalog").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["catalog"] == "acme-ws_field_service"
    posts = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/database/catalogs")]
    assert len(posts) == 1
    body = posts[0][2]
    assert body["database_project_id"] == "project-uid-1"
    assert body["database_branch_id"] == "branch-uid-1"
    assert body["name"] == "acme-ws_field_service"
    assert _step("uc_catalog").health(ctx)["status"] == "ok"


def test_features_step_applies_sql():
    ctx, _conn, _ws = _live_ctx_with_project()
    res = _step("features").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["statements_applied"] > 0
    assert _step("features").health(ctx)["healthy"] is True


def test_synced_step_confirms_registration():
    ctx, _conn, _ws = _live_ctx_with_project()
    assert _step("synced").deploy(ctx)["status"] == "deployed"
    assert _step("synced").health(ctx)["status"] == "ok"


# --- analytics step group (genie / dashboards / governance) ----------------- #
def test_analytics_steps_stub_offline():
    for name in ("genie", "dashboards", "governance"):
        assert _step(name).deploy(_ctx())["status"] == "stub"


def test_genie_step_creates_four_spaces():
    ctx, _conn, ws = _live_ctx_with_project()
    res = _step("genie").deploy(ctx)
    assert res["status"] == "deployed"
    assert len(res["spaces"]) == 4
    posts = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/genie/spaces")]
    assert len(posts) == 4
    body = posts[0][2]
    assert "serialized_space" in body and body["title"].startswith("acme-ws ")
    assert _step("genie").health(ctx)["status"] == "ok"


def test_dashboards_step_creates_and_publishes():
    ctx, _conn, ws = _live_ctx_with_project()
    res = _step("dashboards").deploy(ctx)
    assert res["status"] == "deployed"
    assert len(res["dashboards"]) == 2
    pubs = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/published")]
    assert len(pubs) == 2
    assert _step("dashboards").health(ctx)["status"] == "ok"


def test_governance_step_applies_rls_and_masking():
    ctx, conn, _ws = _live_ctx_with_project()
    res = _step("governance").deploy(ctx)
    assert res["status"] == "deployed"
    sql = conn.executed_sql()
    assert any("ENABLE ROW LEVEL SECURITY" in s for s in sql)
    assert any("v_customers_masked" in s for s in sql)
    assert _step("governance").health(ctx)["healthy"] is True


# --- compute / AI / app step group ------------------------------------------ #
def test_compute_ai_app_steps_stub_offline():
    for name in ("pipeline", "ml", "agent", "ops", "app"):
        assert _step(name).deploy(_ctx())["status"] == "stub"


def test_pipeline_and_ml_submit_jobs():
    ctx, _c, ws = _live_ctx_with_project()
    assert _step("pipeline").deploy(ctx)["status"] == "deployed"
    assert _step("ml").deploy(ctx)["status"] == "deployed"
    submits = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/jobs/runs/submit")]
    assert len(submits) == 2


def test_agent_step_names_endpoint_and_teardown_deletes():
    ctx, _c, _ws = _live_ctx_with_project()
    res = _step("agent").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["endpoint"] == "acme-ws-fs-agent"
    assert _step("agent").teardown(ctx)["status"] == "torn_down"


def test_ops_step_schedules_three_jobs():
    ctx, _c, ws = _live_ctx_with_project()
    res = _step("ops").deploy(ctx)
    assert res["status"] == "deployed"
    assert len(res["jobs"]) == 3
    creates = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/jobs/create")]
    assert len(creates) == 3
    assert "schedule" in creates[0][2]
    assert _step("ops").health(ctx)["status"] == "ok"
    assert _step("ops").teardown(ctx)["status"] == "torn_down"


def test_app_step_renders_yaml_creates_and_deploys():
    ctx, _c, ws = _live_ctx_with_project()
    res = _step("app").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["app"] == "acme-ws-field-service"
    imports = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/workspace/import")]
    assert len(imports) == 1
    creates = [c for c in ws.api_client.calls if c[0] == "POST" and c[1] == "/api/2.0/apps"]
    assert len(creates) == 1
    assert {r["name"] for r in creates[0][2]["resources"]} == {"pguser", "pgpassword"}
    assert any(c[0] == "POST" and c[1].endswith("/deployments") for c in ws.api_client.calls)
    assert _step("app").health(ctx)["status"] == "ok"

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
    # All steps run when every gate defaults on (datagen runs before pipeline).
    assert names == [
        "data", "uc_catalog", "warehouse", "features", "synced", "datagen", "pipeline",
        "governance", "genie", "dashboards", "ml", "agent", "ops", "app",
    ]


def test_gates_skip_optional_steps():
    result = fs_deploy.deploy(
        _ctx(include_pipeline="false", include_ml="false", include_agent="false", include_ops_jobs="false")
    )
    names = [s["step"] for s in result["steps"]]
    assert names == ["data", "uc_catalog", "warehouse", "features", "synced",
                     "governance", "genie", "dashboards", "app"]
    for gated in ("datagen", "pipeline", "ml", "agent", "ops"):
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


def test_synced_step_registers_tables_via_warehouse():
    ctx, _conn, ws = _live_ctx_with_project()
    _step("warehouse").deploy(ctx)  # sets fs_warehouse_id (synced needs it)
    res = _step("synced").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["expected_count"] > 0
    # each expected table was queried through the warehouse to trigger registration.
    stmts = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/sql/statements")]
    assert len(stmts) >= res["expected_count"]
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


def test_governance_step_applies_rls_masking_and_uc_views():
    ctx, conn, ws = _live_ctx_with_project()
    _step("warehouse").deploy(ctx)  # UC views run on the warehouse
    res = _step("governance").deploy(ctx)
    assert res["status"] == "deployed"
    # PG side: RLS + masked view.
    sql = conn.executed_sql()
    assert any("ENABLE ROW LEVEL SECURITY" in s for s in sql)
    assert any("v_customers_masked" in s for s in sql)
    # UC side: the 4 sla_workforce governance views in the network catalog.
    assert res["uc_views_applied"] == res["uc_views_total"]
    stmts = [b["statement"] for m, p, b in ws.api_client.calls
             if m == "POST" and p.endswith("/sql/statements")]
    for v in ("v_regional_work_orders", "v_customers_masked", "v_technician_performance", "v_sla_compliance"):
        assert any(f"acme-ws_network`.`governance`.`{v}`" in s for s in stmts)
    assert _step("governance").health(ctx)["healthy"] is True


def test_governance_runs_before_genie():
    import fs_steps
    names = [s.name for s in fs_steps.ORDERED_STEPS]
    assert names.index("governance") < names.index("genie")


def test_data_step_creates_work_order_indexes():
    ctx, conn, _ws = live_context(deployment_id="acme-ws")
    res = _data_step().deploy(ctx)
    assert res["indexes_applied"] == 3
    sql = conn.executed_sql()
    assert any("idx_wo_tech_active" in s for s in sql)
    assert any(s.strip().startswith("ANALYZE field_service.work_orders") for s in sql)


def test_genie_network_spaces_use_network_catalog():
    import json
    ctx, _conn, ws = _live_ctx_with_project()
    _step("genie").deploy(ctx)
    posts = [c[2] for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/genie/spaces")]
    by_title = {p["title"]: json.loads(p["serialized_space"]) for p in posts}
    # network_health + sla_workforce tables must be namespaced to acme-ws_network.
    for title_suffix in ("Network Health & Telemetry", "SLA & Workforce Analytics"):
        cfg = next(v for k, v in by_title.items() if k.endswith(title_suffix))
        idents = [t["identifier"] for t in cfg["data_sources"]["tables"]]
        assert all(i.startswith("acme-ws_network.") for i in idents), idents
    # field_ops stays on the managed online catalog.
    fo = next(v for k, v in by_title.items() if k.endswith("Field Service Operations"))
    assert all(t["identifier"].startswith("acme-ws_field_service.")
               for t in fo["data_sources"]["tables"])


# --- compute / AI / app step group ------------------------------------------ #
def test_compute_ai_app_steps_stub_offline():
    for name in ("pipeline", "ml", "agent", "ops", "app"):
        assert _step(name).deploy(_ctx())["status"] == "stub"


def test_pipeline_and_ml_submit_jobs():
    ctx, _c, ws = _live_ctx_with_project()
    assert _step("pipeline").deploy(ctx)["status"] == "deployed"
    res = _step("ml").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["scoring_result_state"] == "SUCCESS"  # train then score
    # pipeline(1) + ml train(1) + ml score(1)
    submits = [c for c in ws.api_client.calls if c[0] == "POST" and c[1].endswith("/jobs/runs/submit")]
    assert len(submits) == 3
    assert _submit_body(ws, "score_and_create_work_orders")["catalog"] == "acme-ws_network"


def test_ml_skips_scoring_when_training_fails():
    api = FakeApiClient(project_exists=True, run_result="FAILED")
    ws = FakeWorkspaceClient(api_client=api)
    ctx, _c, ws = live_context(ws=ws, deployment_id="acme-ws")
    res = _step("ml").deploy(ctx)
    assert res["status"] == "failed"
    assert _submit_body(ws, "score_and_create_work_orders") is None  # never submitted


def test_agent_step_names_endpoint_and_teardown_deletes():
    ctx, _c, _ws = _live_ctx_with_project()
    res = _step("agent").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["endpoint"] == "acme-ws-fs-agent"
    assert _step("agent").teardown(ctx)["status"] == "torn_down"


def _submit_body(ws, notebook_substr):
    """Return the base_parameters of the runs/submit whose notebook matches."""
    for method, path, body in ws.api_client.calls:
        if method == "POST" and path.endswith("/jobs/runs/submit"):
            nb = body["tasks"][0]["notebook_task"]
            if notebook_substr in nb["notebook_path"]:
                return nb["base_parameters"]
    return None


def test_pipeline_ml_agent_target_the_standard_network_catalog():
    # Pipeline/ml/agent must NOT point at the managed online catalog
    # (acme-ws_field_service); they use the standard, self-provisioned
    # acme-ws_network catalog for Iceberg + the registered agent model.
    ctx, _c, ws = _live_ctx_with_project()
    _step("warehouse").deploy(ctx)   # stores fs-warehouse-id used by the SQL runner
    _step("pipeline").deploy(ctx)
    _step("ml").deploy(ctx)
    _step("agent").deploy(ctx)

    pipe = _submit_body(ws, "iceberg_streaming_pipeline")
    assert pipe["catalog"] == "acme-ws_network"
    assert pipe["schema"] == "network_data"
    assert pipe["volume_path"] == "/Volumes/acme-ws_network/network_data/raw_files"

    ml = _submit_body(ws, "predictive_maintenance")
    assert ml["catalog"] == "acme-ws_network"

    agent = _submit_body(ws, "deploy_agent_endpoint")
    assert agent["catalog"] == "acme-ws_network"
    assert agent["agent_schema"] == "agents"
    assert agent["endpoint_name"] == "acme-ws-fs-agent"
    # The standard catalog is created via CREATE CATALOG IF NOT EXISTS.
    stmts = [b["statement"] for m, pth, b in ws.api_client.calls
             if m == "POST" and pth.endswith("/sql/statements")]
    assert any("CREATE CATALOG IF NOT EXISTS `acme-ws_network`" in s for s in stmts)


def test_datagen_generates_into_network_volume_before_pipeline():
    ctx, _c, ws = _live_ctx_with_project()
    _step("warehouse").deploy(ctx)
    res = _step("datagen").deploy(ctx)
    assert res["status"] == "deployed"
    assert res["catalog"] == "acme-ws_network"
    assert res["volume_path"] == "/Volumes/acme-ws_network/network_data/raw_files"
    body = _submit_body(ws, "generate_network_data")
    assert body["catalog"] == "acme-ws_network"
    assert body["volume_path"] == "/Volumes/acme-ws_network/network_data/raw_files"
    # datagen is ordered immediately before pipeline.
    import fs_steps
    names = [s.name for s in fs_steps.ORDERED_STEPS]
    assert names.index("datagen") == names.index("pipeline") - 1


def test_agent_passes_created_genie_space_ids():
    ctx, _c, ws = _live_ctx_with_project()
    _step("genie").deploy(ctx)          # creates the four Genie spaces + stores ids
    _step("agent").deploy(ctx)
    import json
    ids = json.loads(_submit_body(ws, "deploy_agent_endpoint")["genie_space_ids"])
    # All four configured spaces were created and forwarded to the agent.
    assert set(ids) == {"postgres", "field_ops", "network_health", "sla_workforce"}
    assert all(v for v in ids.values())


def test_run_failure_reports_failed_not_deployed():
    # The steps must POLL the run and report its REAL result, not "deployed" on submit.
    api = FakeApiClient(project_exists=True, run_result="FAILED")
    ws = FakeWorkspaceClient(api_client=api)
    ctx, _c, ws = live_context(ws=ws, deployment_id="acme-ws")
    assert _step("pipeline").deploy(ctx)["status"] == "failed"
    assert _step("ml").deploy(ctx)["status"] == "failed"
    assert _step("agent").deploy(ctx)["status"] == "failed"


def test_pipeline_teardown_drops_the_network_catalog():
    ctx, _c, ws = _live_ctx_with_project()
    _step("warehouse").deploy(ctx)   # stores fs-warehouse-id used by the SQL runner
    res = _step("pipeline").teardown(ctx)
    assert res["status"] == "torn_down"
    assert res["network_catalog"] == "acme-ws_network"
    stmts = [b["statement"] for m, pth, b in ws.api_client.calls
             if m == "POST" and pth.endswith("/sql/statements")]
    assert any("DROP CATALOG IF EXISTS `acme-ws_network` CASCADE" in s for s in stmts)


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

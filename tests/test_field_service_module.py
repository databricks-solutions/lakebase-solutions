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

from _fakes import live_context

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

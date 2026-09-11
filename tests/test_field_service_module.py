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

"""Orchestrator control-flow smoke tests (stubbed steps; NO workspace).

Proves discover -> validate -> select -> DAG -> iterate works end to end and
that the per-step stubs are actually invoked. No live calls are made.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bootstrap.context import DeployContext
from bootstrap.orchestrator import run, select_components
from bootstrap.discovery import discover

ROOT = Path(__file__).resolve().parents[1]

CORE = {"lakebase", "security", "user_management", "data_api", "admin_app"}


def test_deploy_core_only_excludes_canary():
    ctx = DeployContext(deployment_id="test-ws", mode="deploy")
    results = run("deploy", selected_modules=[], ctx=ctx, root=ROOT)
    deployed = {r["component"] for r in results}
    assert deployed == CORE
    assert "_canary" not in deployed


def test_deploy_with_canary_includes_it_after_core():
    ctx = DeployContext(deployment_id="test-ws", mode="deploy")
    results = run("deploy", selected_modules=["_canary"], ctx=ctx, root=ROOT)
    order = [r["component"] for r in results]
    assert "_canary" in order
    # canary must come after both of its core deps
    assert order.index("lakebase") < order.index("_canary")
    assert order.index("security") < order.index("_canary")


def test_teardown_is_reverse_order():
    ctx = DeployContext(deployment_id="test-ws", mode="teardown")
    deploy_order = [r["component"] for r in run("deploy", [], ctx=ctx, root=ROOT)]
    teardown_order = [r["component"] for r in run("teardown", [], ctx=ctx, root=ROOT)]
    assert teardown_order == list(reversed(deploy_order))


def test_stub_steps_are_invoked():
    ctx = DeployContext(deployment_id="test-ws", mode="deploy")
    results = run("deploy", [], ctx=ctx, root=ROOT, run_health=False)
    # every result carries the stub status from the dynamically-imported step
    assert all(r["status"] == "ok" for r in results)
    assert all(r["result"]["status"] == "stub" for r in results)


def test_unknown_mode_raises():
    with pytest.raises(ValueError):
        run("frobnicate", [], root=ROOT)


def test_unknown_selected_module_raises():
    with pytest.raises(KeyError):
        select_components(discover(ROOT), ["nonexistent_module"])

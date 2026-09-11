"""_canary reference-module tests (offline): stub guard + real deploy/teardown/health.

The canary is now a real, minimal module (creates a schema + heartbeat table).
These tests lock its contract behaviour against the fakes.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from bootstrap.context import DeployContext

from _fakes import live_context

MOD = Path(__file__).resolve().parents[1] / "modules" / "_canary"


def _load(filename: str):
    path = MOD / filename
    spec = importlib.util.spec_from_file_location(f"canary_{path.stem}", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


canary_deploy = _load("deploy.py")
canary_teardown = _load("teardown.py")
canary_health = _load("health.py")


def test_stub_when_not_live():
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    assert canary_deploy.deploy(ctx)["status"] == "stub"
    assert canary_teardown.teardown(ctx)["status"] == "stub"
    assert canary_health.health_check(ctx)["status"] == "stub"


def test_deploy_creates_schema_table_and_row():
    ctx, conn, _ws = live_context(deployment_id="acme-ws")
    res = canary_deploy.deploy(ctx)
    assert res["status"] == "deployed"
    assert res["table"] == "canary.heartbeat"
    sql = conn.executed_sql()
    assert any('CREATE SCHEMA IF NOT EXISTS "canary"' == s for s in sql)
    assert any("canary" in s and "heartbeat" in s and s.startswith("CREATE TABLE") for s in sql)
    assert any(s.startswith('INSERT INTO "canary".heartbeat') for s in sql)


def test_teardown_drops_schema():
    ctx, conn, _ws = live_context(deployment_id="acme-ws")
    res = canary_teardown.teardown(ctx)
    assert res["status"] == "torn_down"
    assert any('DROP SCHEMA IF EXISTS "canary" CASCADE' == s for s in conn.executed_sql())


def test_health_ok_when_rows_present():
    ctx, _conn, _ws = live_context(deployment_id="acme-ws")
    res = canary_health.health_check(ctx)
    assert res["healthy"] is True
    assert res["status"] == "ok"

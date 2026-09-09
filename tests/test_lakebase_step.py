"""core/lakebase deploy/teardown/health unit tests (offline, no workspace).

Injects fake PG connection + workspace client into a ``DeployContext`` and
asserts the step runs the expected idempotent SQL and secret operations. Also
proves that a live call with NO client configured raises the guard error.
"""

from __future__ import annotations

import pytest

from bootstrap.context import DeployContext, LiveClientUnavailable

from _fakes import FakeConnection, FakeCursor, live_context, load_step

lakebase_deploy = load_step("lakebase", "deploy.py")
lakebase_teardown = load_step("lakebase", "teardown.py")
lakebase_health = load_step("lakebase", "health.py")


def test_deploy_creates_schema_idempotently_and_writes_secrets():
    ctx, conn, ws = live_context()
    result = lakebase_deploy.deploy(ctx)

    assert result["status"] == "deployed"
    assert result["schema"] == "workshop"

    # Idempotent schema DDL was executed.
    sql = conn.executed_sql()
    assert any('CREATE SCHEMA IF NOT EXISTS "workshop"' == s for s in sql)
    assert conn.committed == 1

    # A connection credential was obtained (GA generate-database-credential).
    assert ws.database.cred_calls == [["acme-ws-lakebase"]]

    # Connection info written to the standalone secret scope.
    keys = ws.secrets.keys_written()
    assert set(keys) == {"pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"}
    assert all(scope == "acme-ws-secrets" for scope, _, _ in ws.secrets.put)
    # pguser is decoded from the credential JWT ``sub`` claim.
    assert ws.secrets.value_for("pguser") == "admin@example.com"
    assert ws.secrets.value_for("pgdatabase") == "databricks_postgres"
    assert ws.secrets.value_for("pghost") == "host.example"


def test_teardown_drops_schema_and_deletes_secrets():
    ctx, conn, ws = live_context()
    result = lakebase_teardown.teardown(ctx)

    assert result["status"] == "torn_down"
    assert any('DROP SCHEMA IF EXISTS "workshop" CASCADE' == s for s in conn.executed_sql())
    assert conn.committed == 1
    deleted_keys = {key for _, key in ws.secrets.deleted}
    assert deleted_keys == {"pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"}


def test_health_checks_select_one_and_schema_exists():
    # SELECT 1 -> (1,), schema-exists -> (1,)  => healthy
    cursor = FakeCursor(fetchone_results=[(1,), (1,)])
    ctx, _conn, _ws = live_context(conn=FakeConnection(cursor=cursor))
    result = lakebase_health.health_check(ctx)

    assert result["healthy"] is True
    assert result["reachable"] is True
    assert result["schema_exists"] is True
    executed = [sql for sql, _ in cursor.executed]
    assert executed[0] == "SELECT 1"
    assert "information_schema.schemata" in executed[1]


def test_health_unhealthy_when_schema_missing():
    # SELECT 1 -> (1,), schema-exists -> None  => unhealthy
    cursor = FakeCursor(fetchone_results=[(1,), None])
    ctx, _conn, _ws = live_context(conn=FakeConnection(cursor=cursor))
    result = lakebase_health.health_check(ctx)
    assert result["healthy"] is False
    assert result["status"] == "unhealthy"


def test_no_client_configured_is_stub_not_crash():
    # Orchestrator smoke path: nothing injected -> steps log intent, return stub.
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    assert lakebase_deploy.deploy(ctx)["status"] == "stub"
    assert lakebase_teardown.teardown(ctx)["status"] == "stub"
    assert lakebase_health.health_check(ctx)["status"] == "stub"


def test_live_call_without_injection_raises_guard_error():
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    with pytest.raises(LiveClientUnavailable):
        ctx.workspace_client()
    with pytest.raises(LiveClientUnavailable):
        ctx.pg_connection(role="admin")

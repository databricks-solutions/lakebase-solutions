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


def test_deploy_provisions_project_endpoint_scope_then_database_and_schema():
    ctx, conn, ws = live_context(params={"autoscaling_min_cu": "1", "autoscaling_max_cu": "4"})
    result = lakebase_deploy.deploy(ctx)

    assert result["status"] == "deployed"
    assert result["schema"] == "workshop"
    assert result["project"] == "acme-ws"

    # (0) The standalone secret scope is created first (idempotent).
    assert ws.secrets.scopes_created == ["acme-ws-secrets"]
    # (1) The autoscaling `postgres` project is created (auto-creates branch/endpoint).
    assert ws.postgres.create_project_calls == ["acme-ws"]
    # (2) The primary endpoint's autoscaling CU range is set from the params.
    assert ws.postgres.update_endpoint_calls == [
        ("projects/acme-ws/branches/production/endpoints/primary", "1", "4")
    ]
    # (2 cont.) Availability was polled via get_project before connecting.
    assert ws.postgres.get_project_calls and ws.postgres.get_project_calls[0] == "acme-ws"
    # Provisioning summary surfaced on the result.
    assert result["provisioned"]["autoscaling_min_cu"] == "1"
    assert result["provisioned"]["autoscaling_max_cu"] == "4"

    # Workshop DATABASE created first (autoscaling: default `postgres` db has a
    # restricted public schema), then the idempotent workshop schema DDL.
    sql = conn.executed_sql()
    assert any('CREATE DATABASE "databricks_postgres"' == s for s in sql)
    assert any('CREATE SCHEMA IF NOT EXISTS "workshop"' == s for s in sql)
    # One commit (the schema conn); CREATE DATABASE runs autocommit on the
    # maintenance connection.
    assert conn.committed == 1

    # A connection credential was minted for the PRIMARY endpoint of the project.
    assert ws.postgres.cred_calls == [
        "projects/acme-ws/branches/production/endpoints/primary"
    ]
    # The endpoint host was resolved from the production branch's endpoints.
    assert ws.postgres.list_calls == ["projects/acme-ws/branches/production"]

    # Connection info written to the standalone secret scope.
    keys = ws.secrets.keys_written()
    assert set(keys) == {"pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"}
    assert all(scope == "acme-ws-secrets" for scope, _, _ in ws.secrets.put)
    # pguser is the workspace email (autoscaling uses the email as the PG user).
    assert ws.secrets.value_for("pguser") == "admin@example.com"
    assert ws.secrets.value_for("pgdatabase") == "databricks_postgres"
    # host comes from the endpoint's status.hosts.host.
    assert ws.secrets.value_for("pghost") == "host.example"
    # password is the OAuth token from generate_database_credential.
    assert ws.secrets.value_for("pgpassword") == "oauth-token-xyz"


def test_teardown_sdk_deletes_project_and_scope():
    ctx, _conn, ws = live_context()
    result = lakebase_teardown.teardown(ctx)

    assert result["status"] == "torn_down"
    # The whole autoscaling `postgres` project is SDK-deleted (removes branch/
    # endpoint/DBs -- so an explicit DROP SCHEMA is moot).
    assert ws.postgres.delete_project_calls == ["acme-ws"]
    assert result["project_deleted"] is True
    # The standalone secret scope is deleted (removes all connection secrets).
    assert ws.secrets.scopes_deleted == ["acme-ws-secrets"]
    assert result["scope_deleted"] is True


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

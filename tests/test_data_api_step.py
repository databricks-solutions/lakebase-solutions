"""core/data_api deploy/teardown/health unit tests (offline, no workspace).

Data API setup is two-phase. These tests assert:
* phase-1 gate: when the Data API is NOT enabled on the database, deploy stops
  short with ``awaiting_manual_enable`` and creates nothing;
* phase-2 configure: when enabled, deploy registers a dedicated SP, runs the
  ``databricks_auth`` role + grant + grant-to-authenticator SQL, refreshes the
  schema cache, and writes the SP secrets;
* teardown revokes/drops + removes secrets; health asserts authenticator
  membership; and the no-client stub guard.
"""

from __future__ import annotations

from bootstrap.context import DeployContext

from _fakes import FakeApiClient, FakeConnection, FakeCursor, FakeWorkspaceClient, live_context, load_step

data_api_deploy = load_step("data_api", "deploy.py")
data_api_teardown = load_step("data_api", "teardown.py")
data_api_health = load_step("data_api", "health.py")

SP = "acme-ws-data-api-sp"
SCOPE = "acme-ws-secrets"


def test_deploy_awaits_manual_enable_when_data_api_off():
    # Default fake api-client: data_api_enabled=False -> phase-1 gate.
    ctx, _conn, ws = live_context()
    result = data_api_deploy.deploy(ctx)
    assert result["status"] == "awaiting_manual_enable"
    assert result["sp"] == SP
    # Nothing was provisioned: no SP created, no secrets written.
    assert ws.service_principals.created == []
    assert ws.secrets.keys_written() == []


def test_deploy_configures_sp_when_enabled():
    api = FakeApiClient(data_api_enabled=True)
    ws = FakeWorkspaceClient(api_client=api)
    ctx, conn, ws = live_context(ws=ws)

    result = data_api_deploy.deploy(ctx)
    assert result["status"] == "configured"
    app_id = result["sp_application_id"]
    assert app_id and app_id.startswith("app-")

    # A dedicated SP was created and an OAuth secret minted.
    assert ws.service_principals.created == [SP]

    # PG wiring: extension, role creation, grants, and the grant-to-authenticator
    # (the line that clears the owner/403 for the non-owner identity).
    sql = conn.executed_sql()
    assert any("CREATE EXTENSION IF NOT EXISTS databricks_auth" in s for s in sql)
    assert any(f'GRANT USAGE ON SCHEMA "workshop" TO "{app_id}"' == s for s in sql)
    assert any(f'GRANT SELECT ON ALL TABLES IN SCHEMA "workshop" TO "{app_id}"' == s for s in sql)
    assert any(f'GRANT "{app_id}" TO authenticator' == s for s in sql)
    # databricks_create_role runs as a parameterized call (identity as a bind param).
    assert any(
        s == "SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')" and p == (app_id,)
        for s, p in conn.executed
    )

    # Schema cache refreshed via PATCH .../data-api exposing the workshop schema.
    patches = [c for c in api.calls if c[0] == "PATCH" and c[1].endswith("/data-api")]
    assert len(patches) == 1
    assert patches[0][2]["spec"]["db_schemas"] == ["workshop"]

    # SP creds + Data API coordinates written to the standalone scope.
    keys = set(ws.secrets.keys_written())
    assert keys == {
        "data-api-sp-client-id",
        "data-api-sp-client-secret",
        "data-api-url",
        "data-api-database",
        "data-api-schema",
    }
    assert ws.secrets.value_for("data-api-sp-client-id") == app_id


def test_deploy_skipped_when_disabled():
    ctx, _conn, _ws = live_context(params={"enable_data_api": "false"})
    assert data_api_deploy.deploy(ctx)["status"] == "skipped"


def test_teardown_revokes_drops_and_removes_secrets():
    api = FakeApiClient(data_api_enabled=True)
    ws = FakeWorkspaceClient(api_client=api)
    # Pre-seed the SP client-id secret so teardown finds the identity to undo.
    ws.secrets.put_secret(scope=SCOPE, key="data-api-sp-client-id", string_value="app-0001")
    ctx, conn, ws = live_context(ws=ws)

    result = data_api_teardown.teardown(ctx)
    assert result["status"] == "torn_down"

    sql = conn.executed_sql()
    assert any('REVOKE "app-0001" FROM authenticator' == s for s in sql)
    assert any('DROP ROLE IF EXISTS "app-0001"' == s for s in sql)

    deleted = {key for _, key in ws.secrets.deleted}
    assert {
        "data-api-sp-client-id",
        "data-api-sp-client-secret",
        "data-api-url",
        "data-api-database",
        "data-api-schema",
    } <= deleted


def test_health_ok_when_role_member_of_authenticator():
    # membership query -> (1,) => wired.
    cursor = FakeCursor(fetchone_results=[(1,)])
    api = FakeApiClient(data_api_enabled=True)
    ws = FakeWorkspaceClient(api_client=api)
    ws.secrets.put_secret(scope=SCOPE, key="data-api-sp-client-id", string_value="app-0001")
    ctx, _conn, ws = live_context(conn=FakeConnection(cursor=cursor), ws=ws)

    result = data_api_health.health_check(ctx)
    assert result["healthy"] is True
    assert result["member_of_authenticator"] is True
    assert "pg_auth_members" in cursor.executed[0][0]


def test_health_not_configured_when_no_sp_secret():
    ctx, _conn, _ws = live_context()
    result = data_api_health.health_check(ctx)
    assert result["status"] == "not_configured"
    assert result["healthy"] is None


def test_no_client_configured_is_stub_not_crash():
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    assert data_api_deploy.deploy(ctx)["status"] == "stub"
    assert data_api_teardown.teardown(ctx)["status"] == "stub"
    assert data_api_health.health_check(ctx)["status"] == "stub"

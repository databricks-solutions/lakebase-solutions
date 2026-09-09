"""core/security deploy/teardown/health unit tests (offline, no workspace).

Injects fakes and asserts idempotent ``CREATE ROLE`` SQL + role-secret writes on
deploy, drop SQL + secret deletes on teardown, and the role-existence check on
health. Also proves the no-client guard.
"""

from __future__ import annotations

import pytest

from bootstrap.context import DeployContext, LiveClientUnavailable

from _fakes import FakeConnection, FakeCursor, live_context, load_step

security_deploy = load_step("security", "deploy.py")
security_teardown = load_step("security", "teardown.py")
security_health = load_step("security", "health.py")

APP_ROLE = "acme-ws_app"
RO_ROLE = "acme-ws_readonly"


def test_deploy_creates_roles_idempotently_and_writes_secrets():
    ctx, conn, ws = live_context()
    result = security_deploy.deploy(ctx)

    assert result["status"] == "deployed"
    assert result["pg_roles"] == [APP_ROLE, RO_ROLE]

    sql = conn.executed_sql()
    # Idempotent DO-block role creation for both roles.
    assert any("DO $$" in s and f"rolname = '{APP_ROLE}'" in s for s in sql)
    assert any("DO $$" in s and f"rolname = '{RO_ROLE}'" in s for s in sql)
    # Password is set with a bound parameter (not interpolated).
    pw_stmts = [(s, p) for s, p in conn.executed if s.startswith("ALTER ROLE")]
    assert len(pw_stmts) == 2
    assert all(p is not None for _s, p in pw_stmts)
    # Grants are present; read-only role never gets write DML.
    assert any(f'GRANT CONNECT ON DATABASE "databricks_postgres" TO "{APP_ROLE}"' == s for s in sql)
    assert any(f'GRANT SELECT ON ALL TABLES IN SCHEMA "workshop" TO "{RO_ROLE}"' == s for s in sql)
    assert not any("INSERT, UPDATE, DELETE" in s and RO_ROLE in s for s in sql)
    assert conn.committed == 1

    # Role credentials written to the standalone secret scope.
    keys = set(ws.secrets.keys_written())
    assert keys == {
        "app-role-username",
        "app-role-password",
        "readonly-role-username",
        "readonly-role-password",
    }
    assert ws.secrets.value_for("app-role-username") == APP_ROLE
    assert ws.secrets.value_for("readonly-role-username") == RO_ROLE
    # Passwords are non-empty and distinct.
    app_pw = ws.secrets.value_for("app-role-password")
    ro_pw = ws.secrets.value_for("readonly-role-password")
    assert app_pw and ro_pw and app_pw != ro_pw


def test_teardown_drops_roles_and_deletes_secrets():
    ctx, conn, ws = live_context()
    result = security_teardown.teardown(ctx)

    assert result["status"] == "torn_down"
    sql = conn.executed_sql()
    assert any(f'DROP ROLE IF EXISTS "{APP_ROLE}"' == s for s in sql)
    assert any(f'DROP ROLE IF EXISTS "{RO_ROLE}"' == s for s in sql)
    assert any(f'DROP OWNED BY "{APP_ROLE}"' == s for s in sql)
    deleted_keys = {key for _, key in ws.secrets.deleted}
    assert deleted_keys == {
        "app-role-username",
        "app-role-password",
        "readonly-role-username",
        "readonly-role-password",
    }


def test_health_verifies_roles_exist():
    # ANY(%s) query returns both roles => healthy.
    cursor = FakeCursor(fetchall_results=[[(APP_ROLE,), (RO_ROLE,)]])
    ctx, _conn, _ws = live_context(conn=FakeConnection(cursor=cursor))
    result = security_health.health_check(ctx)

    assert result["healthy"] is True
    assert result["found"] == sorted([APP_ROLE, RO_ROLE])
    assert "pg_roles WHERE rolname = ANY(%s)" in cursor.executed[0][0]


def test_health_unhealthy_when_role_missing():
    cursor = FakeCursor(fetchall_results=[[(APP_ROLE,)]])  # readonly missing
    ctx, _conn, _ws = live_context(conn=FakeConnection(cursor=cursor))
    result = security_health.health_check(ctx)
    assert result["healthy"] is False
    assert result["status"] == "unhealthy"


def test_no_client_configured_is_stub_not_crash():
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    assert security_deploy.deploy(ctx)["status"] == "stub"
    assert security_teardown.teardown(ctx)["status"] == "stub"
    assert security_health.health_check(ctx)["status"] == "stub"


def test_live_call_without_injection_raises_guard_error():
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    with pytest.raises(LiveClientUnavailable):
        ctx.workspace_client()
    with pytest.raises(LiveClientUnavailable):
        ctx.pg_connection()

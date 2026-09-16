"""core/user_management step tests (offline): SCIM groups + membership + PG role.

Locks the real behaviour: ensure admin/workshop groups, add the deployer to the
admin group (so the fail-closed admin console is usable), create the participant
PG role, verify via health, and clean up on teardown.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from bootstrap.context import DeployContext

from _fakes import FakeConnection, FakeWorkspaceClient, live_context

MOD = Path(__file__).resolve().parents[1] / "core" / "user_management"


def _load(filename: str):
    path = MOD / filename
    spec = importlib.util.spec_from_file_location(f"um_{path.stem}", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


um_deploy = _load("deploy.py")
um_teardown = _load("teardown.py")
um_health = _load("health.py")


def test_stub_when_not_live():
    ctx = DeployContext(deployment_id="acme-ws", mode="deploy")
    assert um_deploy.deploy(ctx)["status"] == "stub"
    assert um_teardown.teardown(ctx)["status"] == "stub"
    assert um_health.health_check(ctx)["status"] == "stub"


def test_deploy_ensures_groups_adds_admin_and_creates_role():
    ws = FakeWorkspaceClient(email="sa@databricks.com")
    conn = FakeConnection()
    ctx, _c, _w = live_context(deployment_id="acme-ws", conn=conn, ws=ws)
    res = um_deploy.deploy(ctx)

    assert res["status"] == "deployed"
    # Both prefix-derived groups created.
    assert "acme-ws-admins" in ws.api_client._scim_groups
    assert "acme-ws-participants" in ws.api_client._scim_groups
    # Deployer added to the admin group.
    assert res["admin_member_added"] == "sa@databricks.com"
    admin_grp = ws.api_client._scim_groups["acme-ws-admins"]
    assert len(admin_grp["members"]) == 1
    # Participant PG role created NOLOGIN with grants.
    sql = conn.executed_sql()
    assert any("acme-ws_participant" in s and "NOLOGIN" in s for s in sql)
    assert any(s.startswith("GRANT SELECT ON ALL TABLES") for s in sql)


def test_deploy_membership_idempotent():
    ws = FakeWorkspaceClient(email="sa@databricks.com")
    ctx, _c, _w = live_context(deployment_id="acme-ws", ws=ws)
    um_deploy.deploy(ctx)
    res2 = um_deploy.deploy(ctx)
    assert "already a member" in (res2["admin_member_added"] or "")
    # Still exactly one member (no duplicate).
    assert len(ws.api_client._scim_groups["acme-ws-admins"]["members"]) == 1


def test_health_ok_after_deploy():
    ws = FakeWorkspaceClient(email="sa@databricks.com")
    conn = FakeConnection()
    ctx, _c, _w = live_context(deployment_id="acme-ws", conn=conn, ws=ws)
    um_deploy.deploy(ctx)
    res = um_health.health_check(ctx)
    assert res["healthy"] is True
    assert res["deployer_is_admin"] is True
    assert res["admin_group_exists"] is True
    assert res["participant_role_exists"] is True


def test_teardown_deletes_owned_groups_and_drops_role():
    ws = FakeWorkspaceClient(email="sa@databricks.com")
    conn = FakeConnection()
    ctx, _c, _w = live_context(deployment_id="acme-ws", conn=conn, ws=ws)
    um_deploy.deploy(ctx)
    res = um_teardown.teardown(ctx)

    assert res["status"] == "torn_down"
    assert set(res["deleted_groups"]) == {"acme-ws-admins", "acme-ws-participants"}
    assert ws.api_client._scim_groups == {}
    assert any(s.startswith('DROP ROLE IF EXISTS "acme-ws_participant"') for s in conn.executed_sql())


def test_teardown_preserves_custom_admin_group():
    ws = FakeWorkspaceClient(email="sa@databricks.com")
    ctx, _c, _w = live_context(
        deployment_id="acme-ws", ws=ws, params={"admin_group": "corp-admins"}
    )
    um_deploy.deploy(ctx)
    res = um_teardown.teardown(ctx)
    # Custom, pre-existing admin group must NOT be deleted.
    assert "corp-admins" not in res["deleted_groups"]
    assert "corp-admins" in ws.api_client._scim_groups

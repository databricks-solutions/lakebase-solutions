"""Offline unit tests for bootstrap/acls.py (the Databricks-resource ACL plane).

Uses a tiny recording fake WorkspaceClient (no Databricks workspace needed) to
assert that authorize_app / deauthorize_app / verify issue the correct
Permissions + Unity-Catalog API calls at least-privilege levels, and that
missing / unknown resources are handled gracefully. Also asserts the
field_service module wires the `authz` step last.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from bootstrap import acls

SP = "11111111-2222-3333-4444-555555555555"


class _RecordingApiClient:
    """Records (method, path, body) and returns canned GET responses."""

    def __init__(self, get_responses=None):
        self.calls = []
        self._get = get_responses or {}

    def do(self, method, path, body=None, **kwargs):
        self.calls.append((method, path, body))
        if method == "GET":
            return self._get.get(path, {"access_control_list": [], "privilege_assignments": []})
        return {}


class _FakeWC:
    def __init__(self, get_responses=None):
        self.api_client = _RecordingApiClient(get_responses)


def _patches(w, prefix):
    return {p: b for (m, p, b) in w.api_client.calls if m == "PATCH" and p.startswith(prefix)}


def test_authorize_grants_least_privilege_per_kind():
    w = _FakeWC()
    plan = [
        {"kind": "serving-endpoints", "id": "ep1"},
        {"kind": "warehouses", "id": "wh1"},
        {"kind": "genie", "id": "gs1"},
        {"kind": "uc_catalog", "id": "cat1"},
        {"kind": "uc_schema", "id": "cat1.sch1"},
    ]
    audit, failures = acls.authorize_app(w, SP, plan)

    assert not failures
    assert {a["kind"] for a in audit} == {
        "serving-endpoints", "warehouses", "genie", "uc_catalog", "uc_schema"}

    ws = _patches(w, "/api/2.0/permissions/")
    assert ws["/api/2.0/permissions/serving-endpoints/ep1"]["access_control_list"][0]["permission_level"] == "CAN_QUERY"
    assert ws["/api/2.0/permissions/warehouses/wh1"]["access_control_list"][0]["permission_level"] == "CAN_USE"
    assert ws["/api/2.0/permissions/genie/gs1"]["access_control_list"][0]["permission_level"] == "CAN_RUN"
    for body in ws.values():
        ace = body["access_control_list"][0]
        assert ace["service_principal_name"] == SP
        assert ace["permission_level"] != "CAN_MANAGE"  # least privilege

    uc = _patches(w, "/api/2.1/unity-catalog/")
    assert uc["/api/2.1/unity-catalog/permissions/catalog/cat1"]["changes"][0]["add"] == ["USE_CATALOG"]
    assert uc["/api/2.1/unity-catalog/permissions/schema/cat1.sch1"]["changes"][0]["add"] == ["USE_SCHEMA", "SELECT"]


def test_authorize_skips_absent_resource():
    w = _FakeWC()
    audit, failures = acls.authorize_app(
        w, SP, [{"kind": "warehouses", "id": None}, {"kind": "genie", "id": ""}])
    assert audit == [] and failures == []
    assert w.api_client.calls == []  # nothing attempted for absent ids


def test_authorize_unknown_kind_is_failure_not_raise():
    w = _FakeWC()
    audit, failures = acls.authorize_app(w, SP, [{"kind": "mystery", "id": "x"}])
    assert audit == []
    assert len(failures) == 1 and "unknown kind" in failures[0]["error"]


def test_verify_reads_back_the_acl():
    w = _FakeWC(get_responses={
        "/api/2.0/permissions/warehouses/wh1": {"access_control_list": [
            {"service_principal_name": SP, "all_permissions": [{"permission_level": "CAN_USE"}]}]},
        "/api/2.1/unity-catalog/permissions/schema/cat1.sch1": {"privilege_assignments": [
            {"principal": SP, "privileges": ["USE_SCHEMA", "SELECT"]}]},
    })
    assert acls.verify(w, "warehouses", "wh1", SP) is True
    assert acls.verify(w, "genie", "missing", SP) is False       # empty ACL
    assert acls.verify(w, "uc_schema", "cat1.sch1", SP) is True


def test_deauthorize_revokes_without_touching_others():
    w = _FakeWC(get_responses={
        "/api/2.0/permissions/warehouses/wh1": {"access_control_list": [
            {"service_principal_name": SP,
             "all_permissions": [{"permission_level": "CAN_USE", "inherited": False}]},
            {"user_name": "someone@example.com",
             "all_permissions": [{"permission_level": "CAN_MANAGE", "inherited": False}]}]},
    })
    audit = acls.deauthorize_app(
        w, SP, [{"kind": "warehouses", "id": "wh1"}, {"kind": "uc_schema", "id": "cat1.sch1"}])

    # workspace revoke = PUT the remaining ACL, SP dropped, others preserved
    puts = [b for (m, p, b) in w.api_client.calls if m == "PUT" and p.endswith("/warehouses/wh1")]
    assert puts
    kept = puts[0]["access_control_list"]
    assert all(a.get("service_principal_name") != SP for a in kept)
    assert any(a.get("user_name") == "someone@example.com" for a in kept)
    # UC revoke = additive-remove PATCH
    uc = [b for (m, p, b) in w.api_client.calls if m == "PATCH" and "unity-catalog" in p]
    assert uc and uc[0]["changes"][0]["remove"] == ["USE_SCHEMA", "SELECT"]
    assert all(x["result"] in ("revoked", "skipped") for x in audit)


def test_authz_step_wired_last_in_ordered_steps():
    import sys

    path = Path(__file__).resolve().parents[1] / "modules" / "field_service" / "fs_steps.py"
    spec = importlib.util.spec_from_file_location("fs_steps_authz_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # register so @dataclass(Step) can resolve its module
    spec.loader.exec_module(module)
    names = [s.name for s in module.ORDERED_STEPS]
    assert "authz" in names
    assert names[-1] == "authz"                    # runs last
    assert names.index("authz") > names.index("app")  # after the app SP exists

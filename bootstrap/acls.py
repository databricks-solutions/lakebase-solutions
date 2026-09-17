"""Shared Databricks resource-ACL helpers (the workspace-permission plane).

``core/`` is loaded flat by file path (it is deliberately NOT a package), so
sibling step files cannot import from one another. Any authorization helper that
must be shared across components therefore lives here in ``bootstrap`` — right
alongside ``roles.py``.

Where ``roles.py`` grants an app's service principal its **Postgres** access
(native LOGIN role + grants), this module grants the *same* SP its **Databricks
resource** access, the second half of the per-app-credentials contract:

* serving endpoints  -> ``CAN_QUERY``   (invoke the agent endpoint)
* SQL warehouses      -> ``CAN_USE``     (run the app's DBSQL statements)
* Genie spaces        -> ``CAN_RUN``     (Genie AI page + the agent's OBO calls)
* Unity Catalog       -> ``USE_CATALOG`` / ``USE_SCHEMA`` + ``SELECT`` (read the
                         catalogs/schemas the app's DBSQL queries touch)

Design rules (telco/enterprise-grade):

* **Least privilege** — read/query levels only; an app SP is never granted
  ``CAN_MANAGE`` on anything.
* **Idempotent + additive** — workspace ACLs are changed with ``PATCH`` (update
  semantics), so a grant never clobbers an existing ACL entry; UC grants use
  additive ``add`` changes. Safe to re-run on every deploy.
* **Reversible** — ``deauthorize_app`` mirrors ``authorize_app`` for teardown.
  (Teardown also deletes the underlying resources, which removes their ACLs; the
  explicit revoke is belt-and-suspenders and is strictly best-effort.)
* **Best-effort + audited** — a resource that is absent (declared but not
  deployed) is skipped and logged, never fatal; every action returns a
  descriptor for the deploy's audit trail. Secrets/tokens are never logged.

The functions take a Databricks ``WorkspaceClient`` (``w``) and call the
Permissions REST APIs through ``w.api_client.do`` so behaviour is identical to
the rest of the harness's imperative SDK steps (no DABs resource bindings).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

# ── API surfaces ──────────────────────────────────────────────────────────
_WS_PERMISSIONS_API = "/api/2.0/permissions"                 # serving/warehouse/genie/dashboard
_UC_PERMISSIONS_API = "/api/2.1/unity-catalog/permissions"   # catalog/schema grants

# ── Least-privilege default level per workspace-permission object type ──────
DEFAULT_LEVELS: Dict[str, str] = {
    "serving-endpoints": "CAN_QUERY",
    "warehouses": "CAN_USE",
    "genie": "CAN_RUN",
    "dashboards": "CAN_READ",
}
# Read-only UC privileges for the app SP's DBSQL reads.
UC_CATALOG_PRIVS: Tuple[str, ...] = ("USE_CATALOG",)
UC_SCHEMA_PRIVS: Tuple[str, ...] = ("USE_SCHEMA", "SELECT")

# Plan-item kinds this module understands.
_WS_KINDS = set(DEFAULT_LEVELS)
_UC_KINDS = {"uc_catalog": ("catalog", UC_CATALOG_PRIVS), "uc_schema": ("schema", UC_SCHEMA_PRIVS)}


# ── Low-level, per-resource operations ──────────────────────────────────────
def ws_grant(w: Any, object_type: str, object_id: str, sp: str, level: str) -> None:
    """Additively ensure ``sp`` holds ``level`` on a workspace object (idempotent PATCH)."""

    w.api_client.do(
        "PATCH", f"{_WS_PERMISSIONS_API}/{object_type}/{object_id}",
        body={"access_control_list": [
            {"service_principal_name": sp, "permission_level": level}]},
    )


def ws_revoke(w: Any, object_type: str, object_id: str, sp: str) -> None:
    """Remove ``sp`` from a workspace object's ACL (read-modify-PUT).

    The Permissions API expresses removal only by replacing the whole ACL, so we
    read the current direct (non-inherited) entries, drop ``sp``, and PUT the
    remainder. Best-effort at the call site.
    """

    current = w.api_client.do("GET", f"{_WS_PERMISSIONS_API}/{object_type}/{object_id}")
    keep: List[Dict[str, str]] = []
    for ace in current.get("access_control_list", []):
        if ace.get("service_principal_name") == sp:
            continue
        principal = {k: ace[k] for k in ("user_name", "group_name", "service_principal_name") if ace.get(k)}
        if not principal:
            continue
        for perm in ace.get("all_permissions", []):
            if perm.get("inherited"):
                continue  # only re-assert direct grants
            keep.append({**principal, "permission_level": perm["permission_level"]})
    w.api_client.do("PUT", f"{_WS_PERMISSIONS_API}/{object_type}/{object_id}",
                    body={"access_control_list": keep})


def uc_grant(w: Any, securable_type: str, full_name: str, sp: str, privileges: Tuple[str, ...]) -> None:
    """Additively grant read privileges to ``sp`` on a UC securable (idempotent)."""

    w.api_client.do("PATCH", f"{_UC_PERMISSIONS_API}/{securable_type}/{full_name}",
                    body={"changes": [{"principal": sp, "add": list(privileges)}]})


def uc_revoke(w: Any, securable_type: str, full_name: str, sp: str, privileges: Tuple[str, ...]) -> None:
    """Remove read privileges from ``sp`` on a UC securable."""

    w.api_client.do("PATCH", f"{_UC_PERMISSIONS_API}/{securable_type}/{full_name}",
                    body={"changes": [{"principal": sp, "remove": list(privileges)}]})


def verify(w: Any, kind: str, obj_id: str, sp: str, level: Optional[str] = None) -> Optional[bool]:
    """Re-read the ACL and confirm ``sp`` holds the expected level/privileges.

    Returns True/False, or None if the check itself could not run.
    """

    try:
        if kind in _WS_KINDS:
            want = level or DEFAULT_LEVELS[kind]
            resp = w.api_client.do("GET", f"{_WS_PERMISSIONS_API}/{kind}/{obj_id}")
            for ace in resp.get("access_control_list", []):
                if ace.get("service_principal_name") == sp:
                    return any(p.get("permission_level") == want for p in ace.get("all_permissions", []))
            return False
        if kind in _UC_KINDS:
            securable_type, privs = _UC_KINDS[kind]
            resp = w.api_client.do("GET", f"{_UC_PERMISSIONS_API}/{securable_type}/{obj_id}")
            held: set = set()
            for pa in resp.get("privilege_assignments", []):
                if pa.get("principal") == sp:
                    held |= set(pa.get("privileges", []))
            return set(privs) <= held
    except Exception:
        return None
    return None


# ── Plan-driven orchestration ───────────────────────────────────────────────
def authorize_app(w: Any, sp: str, plan: List[Dict[str, Any]],
                  logger: Any = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Grant ``sp`` every ACL in ``plan``; return ``(audit, failures)``.

    ``plan`` items: ``{"kind": <kind>, "id": <object-id-or-full-name>, "level": <optional override>}``.
    A plan item with a falsy ``id`` (a declared-but-absent resource) is skipped.
    Never raises — per-item errors are collected in ``failures``.
    """

    audit: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for item in plan:
        kind, obj_id = item.get("kind"), item.get("id")
        if not obj_id:
            continue  # declared but not deployed -> graceful skip
        try:
            if kind in _WS_KINDS:
                level = item.get("level") or DEFAULT_LEVELS[kind]
                ws_grant(w, kind, obj_id, sp, level)
                audit.append({"kind": kind, "id": obj_id, "level": level, "result": "granted"})
            elif kind in _UC_KINDS:
                securable_type, privs = _UC_KINDS[kind]
                uc_grant(w, securable_type, obj_id, sp, privs)
                audit.append({"kind": kind, "id": obj_id, "level": ",".join(privs), "result": "granted"})
            else:
                failures.append({"kind": kind, "id": obj_id, "error": f"unknown kind {kind!r}"})
        except Exception as exc:  # noqa: BLE001 - best-effort; surface, never abort the deploy
            failures.append({"kind": kind, "id": obj_id, "error": str(exc)[:200]})
            if logger is not None:
                logger.info("acls.authorize_app: %s %s failed: %s", kind, obj_id, str(exc)[:160])
    return audit, failures


def deauthorize_app(w: Any, sp: str, plan: List[Dict[str, Any]],
                    logger: Any = None) -> List[Dict[str, Any]]:
    """Best-effort revoke of every ACL in ``plan`` from ``sp`` (teardown parity).

    Teardown also deletes the underlying resources (which removes their ACLs), so
    a per-item failure here is expected and non-fatal. Returns the revoke audit.
    """

    audit: List[Dict[str, Any]] = []
    for item in plan:
        kind, obj_id = item.get("kind"), item.get("id")
        if not obj_id:
            continue
        try:
            if kind in _WS_KINDS:
                ws_revoke(w, kind, obj_id, sp)
            elif kind in _UC_KINDS:
                securable_type, privs = _UC_KINDS[kind]
                uc_revoke(w, securable_type, obj_id, sp, privs)
            else:
                continue
            audit.append({"kind": kind, "id": obj_id, "result": "revoked"})
        except Exception as exc:  # noqa: BLE001 - resource may already be gone
            audit.append({"kind": kind, "id": obj_id, "result": "skipped", "note": str(exc)[:160]})
            if logger is not None:
                logger.info("acls.deauthorize_app: %s %s skipped: %s", kind, obj_id, str(exc)[:160])
    return audit

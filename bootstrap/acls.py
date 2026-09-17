"""Shared Databricks-resource ACL helpers (the app SP's *resource-plane* grants).

Every app in this harness is deployed under its OWN per-app service principal.
``bootstrap/roles.py`` grants that SP everything it needs on the **Postgres**
plane (native-auth role, schema grants, its own secret keys). But a Databricks
App also has to *reach* the Databricks resources it was wired to — the agent
serving endpoint, the SQL warehouse, the Genie spaces, and the Unity Catalog
data behind them — and none of those are Postgres grants. Those live on the
**Databricks-resource plane** and are governed by two entirely different ACL
surfaces (the workspace Permissions API and the Unity Catalog Permissions API).

This module is the resource-plane counterpart to ``roles.py`` and lives in
``bootstrap/`` for the same reason: ``core/`` and ``modules/`` are loaded flat by
file path (deliberately NOT packages), so sibling step files can't import from
one another. Any ACL helper that must be shared — today ``modules/field_service``
authorizes its app SP through it; other modules will follow — therefore lives
here in the ``bootstrap`` package that every module already imports.

Design, mirroring ``roles.py``:

* **Pure + typed + testable.** Each function takes an injected workspace client
  (``w.api_client.do`` only — no typed SDK service, matching ``adapters.py``) and
  returns a small ``{kind, id, level, result}`` descriptor for audit. No secrets
  or tokens are ever logged.
* **Least privilege.** Default levels are the minimum each resource kind needs:
  ``serving-endpoints`` → ``CAN_QUERY``, ``warehouses`` → ``CAN_USE``,
  ``genie`` → ``CAN_RUN``, ``dashboards`` → ``CAN_READ``; UC is read-only
  (``USE_CATALOG`` on a catalog, ``USE_SCHEMA``+``SELECT`` on a schema).
* **Additive on grant.** Workspace grants use a **PATCH** (never PUT) so every
  pre-existing ACL entry on the object is preserved; UC grants use ``add`` changes.
* **Idempotent.** Re-granting a level the SP already holds is a no-op upsert.
* **Best-effort + reversible.** A missing resource is logged and skipped, never
  fatal; ``deauthorize_app`` reverses each grant for teardown parity.
* **Caller supplies the SP.** The SP application id is always passed in — this
  module hardcodes no principal.

Teardown note (the one place PUT is used): the workspace Permissions API has no
additive *per-principal removal* — PATCH can only add/upsert. So
``revoke_workspace_permission`` reads the object's current ACL and PUTs it back
**with our SP's direct entries removed and every other principal's direct grant
preserved**, which achieves the same "don't clobber other ACLs" guarantee the
additive-PATCH grant path is built around. UC revokes use ``remove`` changes and
need no such reconstruction.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from bootstrap.adapters import is_not_found

__all__ = [
    "WORKSPACE_PERMISSIONS_API",
    "UC_PERMISSIONS_API",
    "WORKSPACE_DEFAULT_LEVELS",
    "UC_DEFAULT_PRIVILEGES",
    "grant_workspace_permission",
    "revoke_workspace_permission",
    "grant_uc_privileges",
    "revoke_uc_privileges",
    "verify",
    "authorize_app",
    "deauthorize_app",
]

# REST bases (driven via w.api_client.do, same version-proofing as adapters.py).
WORKSPACE_PERMISSIONS_API = "/api/2.0/permissions"
UC_PERMISSIONS_API = "/api/2.1/unity-catalog/permissions"

# Least-privilege default permission level per workspace object type. The plan
# "kind" IS the workspace object type for these four.
WORKSPACE_DEFAULT_LEVELS: Dict[str, str] = {
    "serving-endpoints": "CAN_QUERY",
    "warehouses": "CAN_USE",
    "genie": "CAN_RUN",
    "dashboards": "CAN_READ",
}

# UC plan kinds -> (securable_type used in the API path, default privilege list).
# Read-only + schema-granular: the app never needs write on UC.
UC_DEFAULT_PRIVILEGES: Dict[str, Tuple[str, List[str]]] = {
    "uc_catalog": ("catalog", ["USE_CATALOG"]),
    "uc_schema": ("schema", ["USE_SCHEMA", "SELECT"]),
}


def _is_workspace_kind(kind: str) -> bool:
    return kind in WORKSPACE_DEFAULT_LEVELS


def _is_uc_kind(kind: str) -> bool:
    return kind in UC_DEFAULT_PRIVILEGES


def resolve_level(item: Dict[str, Any]) -> Any:
    """Resolve the grant level for a plan item.

    Returns the string permission level for a workspace kind, or the list of
    privileges for a UC kind. ``item["level"]`` overrides the least-privilege
    default (used by the optional ``app_grants`` module.yaml hook). Raises
    ``ValueError`` for an unknown kind.
    """

    kind = item.get("kind", "")
    override = item.get("level")
    if _is_workspace_kind(kind):
        return override or WORKSPACE_DEFAULT_LEVELS[kind]
    if _is_uc_kind(kind):
        if override:
            return override if isinstance(override, list) else [override]
        return list(UC_DEFAULT_PRIVILEGES[kind][1])
    raise ValueError(f"unknown ACL kind: {kind!r}")


# --------------------------------------------------------------------------- #
# Workspace Permissions API (serving-endpoints / warehouses / genie / dashboards)
# --------------------------------------------------------------------------- #
def grant_workspace_permission(
    w: Any, object_type: str, object_id: str, sp_app_id: str, level: str
) -> Dict[str, Any]:
    """Additively grant ``level`` to the SP on a workspace object (PATCH, never PUT).

    PATCH ``/api/2.0/permissions/{object_type}/{object_id}`` adds/updates ONLY the
    named principal's entry, so every pre-existing ACL on the object is preserved.
    Returns an audit descriptor.
    """

    w.api_client.do(
        "PATCH",
        f"{WORKSPACE_PERMISSIONS_API}/{object_type}/{object_id}",
        body={"access_control_list": [
            {"service_principal_name": sp_app_id, "permission_level": level}
        ]},
    )
    return {"kind": object_type, "id": object_id, "level": level, "result": "granted"}


def _read_workspace_acl(w: Any, object_type: str, object_id: str) -> List[Dict[str, Any]]:
    resp = w.api_client.do("GET", f"{WORKSPACE_PERMISSIONS_API}/{object_type}/{object_id}")
    acl = resp.get("access_control_list") if isinstance(resp, dict) else None
    return acl or []


def revoke_workspace_permission(
    w: Any, object_type: str, object_id: str, sp_app_id: str, level: Optional[str] = None
) -> Dict[str, Any]:
    """Remove the SP's DIRECT grants on a workspace object, preserving all others.

    The workspace Permissions API has no additive per-principal removal, so this
    reads the current ACL and PUTs it back with the SP's own direct entries
    dropped and every other principal's DIRECT (non-inherited) grant re-asserted.
    Inherited entries are never re-PUT (they are not settable). Best-effort.
    """

    acl = _read_workspace_acl(w, object_type, object_id)
    rebuilt: List[Dict[str, Any]] = []
    for entry in acl:
        principal_field, principal = _principal_of(entry)
        if principal is None:
            continue
        if principal == sp_app_id:
            continue  # drop our SP entirely
        for perm in entry.get("all_permissions", []) or []:
            if perm.get("inherited"):
                continue  # inherited grants are not settable — never re-PUT them
            lvl = perm.get("permission_level")
            if lvl:
                rebuilt.append({principal_field: principal, "permission_level": lvl})
    w.api_client.do(
        "PUT",
        f"{WORKSPACE_PERMISSIONS_API}/{object_type}/{object_id}",
        body={"access_control_list": rebuilt},
    )
    return {"kind": object_type, "id": object_id, "level": level, "result": "revoked"}


def _principal_of(entry: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """Return the (field-name, value) of whichever principal an ACL entry names."""

    for field_name in ("service_principal_name", "user_name", "group_name"):
        value = entry.get(field_name)
        if value:
            return field_name, value
    return "service_principal_name", None


def verify_workspace_permission(
    w: Any, object_type: str, object_id: str, sp_app_id: str, level: str
) -> bool:
    """True if the SP holds ``level`` on the workspace object (re-reads the ACL)."""

    for entry in _read_workspace_acl(w, object_type, object_id):
        _field, principal = _principal_of(entry)
        if principal != sp_app_id:
            continue
        for perm in entry.get("all_permissions", []) or []:
            if perm.get("permission_level") == level:
                return True
    return False


# --------------------------------------------------------------------------- #
# Unity Catalog Permissions API (read-only, schema-granular)
# --------------------------------------------------------------------------- #
def grant_uc_privileges(
    w: Any, securable_type: str, full_name: str, sp_app_id: str, privileges: List[str]
) -> Dict[str, Any]:
    """Add ``privileges`` for the SP on a UC securable (catalog/schema). Additive."""

    w.api_client.do(
        "PATCH",
        f"{UC_PERMISSIONS_API}/{securable_type}/{full_name}",
        body={"changes": [{"principal": sp_app_id, "add": list(privileges)}]},
    )
    return {"kind": f"uc_{securable_type}", "id": full_name,
            "level": list(privileges), "result": "granted"}


def revoke_uc_privileges(
    w: Any, securable_type: str, full_name: str, sp_app_id: str, privileges: List[str]
) -> Dict[str, Any]:
    """Remove ``privileges`` for the SP on a UC securable (``remove`` changes)."""

    w.api_client.do(
        "PATCH",
        f"{UC_PERMISSIONS_API}/{securable_type}/{full_name}",
        body={"changes": [{"principal": sp_app_id, "remove": list(privileges)}]},
    )
    return {"kind": f"uc_{securable_type}", "id": full_name,
            "level": list(privileges), "result": "revoked"}


def verify_uc_privileges(
    w: Any, securable_type: str, full_name: str, sp_app_id: str, privileges: List[str]
) -> bool:
    """True if the SP holds ALL ``privileges`` on the UC securable (re-reads)."""

    resp = w.api_client.do("GET", f"{UC_PERMISSIONS_API}/{securable_type}/{full_name}")
    assignments = resp.get("privilege_assignments") if isinstance(resp, dict) else None
    for pa in assignments or []:
        if pa.get("principal") == sp_app_id:
            held = set(pa.get("privileges") or [])
            return set(privileges).issubset(held)
    return False


# --------------------------------------------------------------------------- #
# Dispatch + high-level authorize / deauthorize / verify over a plan
# --------------------------------------------------------------------------- #
def _grant_item(w: Any, sp_app_id: str, kind: str, rid: str, level: Any) -> Dict[str, Any]:
    if _is_workspace_kind(kind):
        return grant_workspace_permission(w, kind, rid, sp_app_id, level)
    securable = UC_DEFAULT_PRIVILEGES[kind][0]
    return grant_uc_privileges(w, securable, rid, sp_app_id, level)


def _revoke_item(w: Any, sp_app_id: str, kind: str, rid: str, level: Any) -> Dict[str, Any]:
    if _is_workspace_kind(kind):
        return revoke_workspace_permission(w, kind, rid, sp_app_id, level)
    securable = UC_DEFAULT_PRIVILEGES[kind][0]
    return revoke_uc_privileges(w, securable, rid, sp_app_id, level)


def verify(w: Any, sp_app_id: str, item: Dict[str, Any]) -> bool:
    """Re-read the ACL for one plan item and confirm the SP holds the level."""

    kind = item.get("kind", "")
    rid = item.get("id")
    if not rid:
        return False
    level = resolve_level(item)
    if _is_workspace_kind(kind):
        return verify_workspace_permission(w, kind, rid, sp_app_id, level)
    securable = UC_DEFAULT_PRIVILEGES[kind][0]
    return verify_uc_privileges(w, securable, rid, sp_app_id, level)


def _run_plan(
    w: Any, sp_app_id: str, plan: List[Dict[str, Any]], logger: Any, revoke: bool
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Grant (or revoke) each plan item; return (audit, failures). Never aborts."""

    verb = "deauthorize" if revoke else "authorize"
    audit: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for item in plan:
        kind = item.get("kind", "")
        rid = item.get("id")
        if not rid:
            logger.info("acls.%s: skip %r (no id resolved)", verb, kind)
            continue
        try:
            level = resolve_level(item)
        except ValueError as exc:
            logger.info("acls.%s: %s", verb, exc)
            failures.append({"kind": kind, "id": rid, "level": None, "result": "unknown_kind"})
            continue
        try:
            desc = (_revoke_item if revoke else _grant_item)(w, sp_app_id, kind, rid, level)
            audit.append(desc)
        except Exception as exc:  # best-effort: never abort the whole plan
            if is_not_found(exc):
                logger.info("acls.%s: %s %r not found — skipping", verb, kind, rid)
                audit.append({"kind": kind, "id": rid, "level": level, "result": "skipped_missing"})
            else:
                logger.info("acls.%s: %s %r deferred: %s", verb, kind, rid, str(exc)[:120])
                failures.append({"kind": kind, "id": rid, "level": level, "result": "failed"})
    return audit, failures


def authorize_app(
    w: Any, sp_app_id: str, plan: List[Dict[str, Any]], logger: Any
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Grant the app SP every ACL in ``plan``; return (audit, failures).

    ``plan`` is a list of ``{"kind", "id", "level"?}`` items where ``kind`` is one
    of ``serving-endpoints``/``warehouses``/``genie``/``dashboards``/``uc_catalog``
    /``uc_schema``. Missing resources are skipped (not fatal); ``level`` overrides
    the least-privilege default when present.
    """

    return _run_plan(w, sp_app_id, plan, logger, revoke=False)


def deauthorize_app(
    w: Any, sp_app_id: str, plan: List[Dict[str, Any]], logger: Any
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Reverse :func:`authorize_app` for teardown parity; return (audit, failures)."""

    return _run_plan(w, sp_app_id, plan, logger, revoke=True)

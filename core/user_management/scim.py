"""SCIM helpers for workshop identity plumbing (workspace Groups + Users).

The runtime job SDK does not reliably type ``w.groups`` / ``w.users`` for the
membership operations we need, so -- consistent with the rest of the harness --
group and user lookups go through the workspace SCIM REST surface via
``w.api_client.do(...)``. These helpers are shared by ``deploy`` / ``teardown``
/ ``health``.

SCIM v2 responses use the capitalized ``Resources`` key; we tolerate a
lowercase ``resources`` too for robustness against fakes / proxies.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

SCIM_USERS = "/api/2.0/preview/scim/v2/Users"
SCIM_GROUPS = "/api/2.0/preview/scim/v2/Groups"

_GROUP_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:Group"
_PATCH_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"


def _resources(resp: Any) -> List[Dict[str, Any]]:
    """Return the SCIM ``Resources`` list from a list response (case-tolerant)."""

    if not isinstance(resp, dict):
        return []
    return resp.get("Resources") or resp.get("resources") or []


def _scim_quote(value: str) -> str:
    """Escape a value for a SCIM ``eq`` filter (double-quote delimited)."""

    return value.replace('"', '\\"')


def find_user_id(w: Any, email: str) -> Optional[str]:
    """Return the SCIM id for ``email`` (workspace user), or ``None``."""

    resp = w.api_client.do(
        "GET", SCIM_USERS, query={"filter": f'userName eq "{_scim_quote(email)}"'}
    )
    res = _resources(resp)
    return res[0].get("id") if res else None


def find_group(w: Any, display_name: str) -> Optional[Dict[str, Any]]:
    """Return the SCIM group dict for ``display_name``, or ``None``."""

    resp = w.api_client.do(
        "GET", SCIM_GROUPS, query={"filter": f'displayName eq "{_scim_quote(display_name)}"'}
    )
    res = _resources(resp)
    return res[0] if res else None


def ensure_group(w: Any, display_name: str) -> Dict[str, Any]:
    """Create the workspace group if missing; return the group dict (with id)."""

    existing = find_group(w, display_name)
    if existing:
        return existing
    created = w.api_client.do(
        "POST",
        SCIM_GROUPS,
        body={"schemas": [_GROUP_SCHEMA], "displayName": display_name},
    )
    # A create response should carry the id; fall back to a re-query if not.
    if isinstance(created, dict) and created.get("id"):
        return created
    return find_group(w, display_name) or {"displayName": display_name}


def group_member_ids(group: Dict[str, Any]) -> List[str]:
    """Return the set of member ids on a SCIM group dict."""

    return [m.get("value") for m in (group.get("members") or []) if m.get("value")]


def add_member(w: Any, group_id: str, user_id: str) -> None:
    """Add a user to a group via SCIM PATCH (idempotent server-side)."""

    w.api_client.do(
        "PATCH",
        f"{SCIM_GROUPS}/{group_id}",
        body={
            "schemas": [_PATCH_SCHEMA],
            "Operations": [
                {"op": "add", "path": "members", "value": [{"value": user_id}]}
            ],
        },
    )


def delete_group(w: Any, group_id: str) -> None:
    """Delete a workspace group by SCIM id."""

    w.api_client.do("DELETE", f"{SCIM_GROUPS}/{group_id}")

"""Live-access adapters: the real factories the in-workspace deploy notebook
injects into a :class:`~bootstrap.context.DeployContext`.

Everything here is built on the **autoscaling** Lakebase surface (SPEC
section 4/8): hierarchical ``postgres`` projects -> ``production`` branch ->
``primary`` read-write endpoint (min/max CU + scale-to-zero), NOT the legacy
provisioned ``database_instance`` tier.

* the workspace client is the plain ``databricks.sdk.WorkspaceClient``;
* the endpoint host comes from ``status.hosts.host`` on the endpoint returned by
  ``w.postgres.list_endpoints(projects/<id>/branches/production)``;
* the ADMIN PG credential is ``(workspace email, OAuth token)`` -- the token is
  minted by ``w.postgres.generate_database_credential(<endpoint resource>)`` and
  the connecting user is the workspace email (``w.current_user.me().user_name``),
  NOT a JWT ``sub`` claim; ``sslmode=require``;
* non-admin PG roles (``app`` / ``readonly``) still authenticate with the
  native username/password pair that ``core/security`` wrote to the standalone
  secret scope (unchanged);
* PG roles/grants are created with ``CREATE ROLE`` SQL by the ``core/security``
  step, not a bundle ``postgres_role`` resource.

**Verify live at P1b:** the SDK shapes below are the expected equivalents of the
``databricks postgres`` CLI (``w.postgres.list_endpoints`` /
``w.postgres.generate_database_credential`` / ``w.current_user.me().user_name``);
they cannot be imported/checked offline, so the resolvers are defensive about
response shape (endpoint list vs. ``.endpoints``; ``status.hosts`` list vs.
single object).

**Import safety:** every third-party import (``databricks-sdk``, ``psycopg``) is
deferred inside the function that needs it, so importing this module never
requires those packages. That keeps the offline unit tests -- which inject fakes
and never call these factories -- runnable with neither the SDK nor psycopg
installed.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Optional

__all__ = [
    "default_workspace_client_factory",
    "default_pg_connection_factory",
    "endpoint_resource_name",
    "resolve_endpoint_host",
    "username_from_token",
]

# Autoscaling Lakebase creates these by default when a project is provisioned.
DEFAULT_BRANCH = "production"
DEFAULT_ENDPOINT = "primary"


def default_workspace_client_factory() -> Any:
    """Construct a real Databricks ``WorkspaceClient`` (lazy import).

    Constructing the client makes no API call; auth is resolved from the
    in-workspace environment where the deploy notebook runs.
    """

    from databricks.sdk import WorkspaceClient  # lazy import

    return WorkspaceClient()


def username_from_token(token: Optional[str]) -> str:
    """Best-effort decode of a JWT ``sub`` claim (legacy helper, kept for reuse).

    The autoscaling surface authenticates the admin as the **workspace email**
    (``w.current_user.me().user_name``), not a token ``sub`` claim, so this is no
    longer on the connection path. It is retained as a defensive utility and
    never raises; on any failure it returns ``"databricks"``.
    """

    if not token or "." not in token:
        return "databricks"
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)  # restore base64 padding
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return claims.get("sub") or "databricks"
    except Exception:  # pragma: no cover - defensive; malformed token
        return "databricks"


def endpoint_resource_name(
    project: str, branch: str = DEFAULT_BRANCH, endpoint: str = DEFAULT_ENDPOINT
) -> str:
    """Return the hierarchical endpoint resource path used by the postgres API."""

    return f"projects/{project}/branches/{branch}/endpoints/{endpoint}"


def _host_from_endpoint(endpoint: Any) -> Optional[str]:
    """Extract ``status.hosts.host`` from an endpoint (list or single host)."""

    status = getattr(endpoint, "status", None)
    hosts = getattr(status, "hosts", None)
    if hosts is None:
        return None
    if isinstance(hosts, (list, tuple)):
        if not hosts:
            return None
        first = hosts[0]
    else:
        first = hosts
    if isinstance(first, dict):
        return first.get("host")
    return getattr(first, "host", None)


def resolve_endpoint_host(
    w: Any,
    project: str,
    branch: str = DEFAULT_BRANCH,
    endpoint: str = DEFAULT_ENDPOINT,
) -> Optional[str]:
    """Resolve the read-write endpoint host for a project/branch (autoscaling).

    Lists the branch's endpoints and returns ``status.hosts.host`` for the
    endpoint whose resource name ends in ``endpoints/<endpoint>`` (falling back
    to the first endpoint). Response shape is handled defensively (a list, or an
    object exposing ``.endpoints``).
    """

    resp = w.postgres.list_endpoints(f"projects/{project}/branches/{branch}")
    endpoints = getattr(resp, "endpoints", None)
    if endpoints is None:
        endpoints = list(resp) if resp is not None else []

    chosen = None
    for ep in endpoints:
        name = getattr(ep, "name", "") or ""
        if name.rsplit("/", 1)[-1] == endpoint or name.endswith(f"endpoints/{endpoint}"):
            chosen = ep
            break
    if chosen is None and endpoints:
        chosen = endpoints[0]
    if chosen is None:
        return None
    return _host_from_endpoint(chosen)


def default_pg_connection_factory(
    ctx: Any, role: Optional[str] = None, database: Optional[str] = None
) -> Any:
    """Connect to the autoscaling Lakebase endpoint over psycopg (lazy import).

    ``role`` selects credentials:

    * ``None`` / ``"admin"`` -- authenticate as the **workspace email** with an
      OAuth token minted by ``generate_database_credential`` on the ``primary``
      endpoint; used for DDL (``CREATE DATABASE`` / ``CREATE SCHEMA`` / roles).
    * any other role (``"app"``, ``"readonly"``, ...) -- authenticate with the
      native-password secret pair that ``core/security`` wrote to the standalone
      secret scope at deploy time.
    """

    import psycopg  # lazy import -- absent from the offline test interpreter

    w = ctx.workspace_client()
    project = ctx.resolved_names["lakebase_project"]
    host = resolve_endpoint_host(w, project)
    # The maintenance/default db is `postgres`; the workshop db is created by the
    # lakebase step. Callers pass the target db explicitly.
    dbname = database or ctx.params.get("database") or "databricks_postgres"

    if role in (None, "admin"):
        cred = w.postgres.generate_database_credential(endpoint_resource_name(project))
        pg_user = w.current_user.me().user_name
        pg_password = getattr(cred, "token", None)
    else:
        scope = ctx.params.get("secret_scope") or ctx.resolved_names["secret_scope"]
        pg_user = _read_secret(w, scope, f"{role}-role-username")
        pg_password = _read_secret(w, scope, f"{role}-role-password")

    return psycopg.connect(
        host=host,
        port=5432,
        dbname=dbname,
        user=pg_user,
        password=pg_password,
        sslmode="require",
    )


def _read_secret(w: Any, scope: str, key: str) -> str:
    """Read + base64-decode a Databricks secret value (in-workspace helper)."""

    resp = w.secrets.get_secret(scope=scope, key=key)
    value = getattr(resp, "value", None)
    if value is None:
        return ""
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception:  # pragma: no cover - value already plain
        return str(value)

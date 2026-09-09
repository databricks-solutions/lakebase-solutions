"""Live-access adapters: the real factories the in-workspace deploy notebook
injects into a :class:`~bootstrap.context.DeployContext`.

Everything here is deliberately built on the **Public Preview / GA** Lakebase
surface (SPEC section 4):

* the workspace client is the plain ``databricks.sdk.WorkspaceClient``;
* PG connection credentials come from the GA
  ``generate-database-credential`` surface (``w.database.*``) -- NOT the Beta
  Autoscaling ``/api/2.0/postgres`` API;
* PG roles/grants are created with ``CREATE ROLE`` SQL by the ``core/security``
  step, not the Beta ``postgres_role`` bundle resource.

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
    "username_from_token",
]


def default_workspace_client_factory() -> Any:
    """Construct a real Databricks ``WorkspaceClient`` (lazy import).

    Constructing the client makes no API call; auth is resolved from the
    in-workspace environment where the deploy notebook runs.
    """

    from databricks.sdk import WorkspaceClient  # lazy import

    return WorkspaceClient()


def username_from_token(token: Optional[str]) -> str:
    """Extract the PG username (the JWT ``sub`` claim) from a credential token.

    Lakebase database credentials are short-lived JWTs whose ``sub`` claim is the
    connecting identity. Decoding is best-effort and never raises; on any
    failure it returns ``"databricks"`` so callers always get a usable string.
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


def default_pg_connection_factory(
    ctx: Any, role: Optional[str] = None, database: Optional[str] = None
) -> Any:
    """Connect to the Lakebase instance over psycopg (lazy import).

    ``role`` selects credentials:

    * ``None`` / ``"admin"`` -- authenticate with the OAuth database credential
      (the connecting identity is the credential's ``sub``); used for DDL.
    * any other role (``"app"``, ``"readonly"``, ...) -- authenticate with the
      native-password secret pair that ``core/security`` wrote to the standalone
      secret scope at deploy time.
    """

    import psycopg  # lazy import -- absent from the offline test interpreter

    w = ctx.workspace_client()
    instance = ctx.resolved_names["lakebase_instance"]
    inst = w.database.get_database_instance(name=instance)
    host = getattr(inst, "read_write_dns", None)
    dbname = database or ctx.params.get("database") or "databricks_postgres"

    if role in (None, "admin"):
        cred = w.database.generate_database_credential(instance_names=[instance])
        token = cred.token
        pg_user, pg_password = username_from_token(token), token
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

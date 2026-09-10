"""core/data_api deploy step.

The Lakebase Data API is a managed PostgREST layer. Setup is **two-phase**:

* Phase 1 -- MANUAL: a human enables the Data API on the workshop database in the
  Lakebase UI and exposes the target schema. This step DETECTS whether that has
  happened; if not, it prints a loud, explicit instruction and stops short
  (``status: awaiting_manual_enable``) so nothing downstream assumes it is on.
* Phase 2 -- RE-RUNNABLE: once enabled, this step configures a dedicated service
  principal as the non-owner Data API identity and wires it end-to-end. Safe to
  re-run.

The core gotcha: the project OWNER cannot use the Data API (PostgREST does
``SET ROLE`` to the caller, which the control plane blocks for the owner's own
identity -> HTTP 403). So a dedicated **service principal** is registered as a
non-owner role and granted to ``authenticator``. Phase 2, in order:

1. discover the workshop database's management id (``GET .../branches/production/databases``),
2. confirm the Data API is enabled (``GET .../databases/<dbid>/data-api``; 404 -> phase 1),
3. create/reuse the dedicated SP + mint an OAuth (M2M) secret,
4. as admin over psycopg: ``CREATE EXTENSION databricks_auth`` -> ``databricks_create_role(<app-id>, 'SERVICE_PRINCIPAL')`` -> ``GRANT USAGE/SELECT`` on the workshop schema -> ``GRANT "<app-id>" TO authenticator``,
5. refresh the PostgREST schema cache (``PATCH .../data-api``; else a just-granted role 500s),
6. write the SP client id/secret + Data API URL to the standalone secret scope.

Every control-plane call goes through ``w.api_client.do`` (version-proof; the
notebook-runtime SDK has no typed ``postgres`` service). SP creation/secrets use
the workspace SDK surfaces, each guarded: a missing entitlement DEFERS (logs +
returns ``deferred``) rather than aborting the whole run.

When no live clients are injected it logs intent and returns a ``stub`` result.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bootstrap.adapters import POSTGRES_API_BASE

# Secret keys this step writes (and teardown removes).
DATA_API_SECRET_KEYS: List[str] = [
    "data-api-sp-client-id",
    "data-api-sp-client-secret",
    "data-api-url",
    "data-api-database",
    "data-api-schema",
]

_BRANCH = "production"

_MANUAL_ENABLE_BANNER = """
================================================================================
  ACTION REQUIRED -- enable the Data API MANUALLY (two-phase, phase 1)
--------------------------------------------------------------------------------
  The Data API is NOT enabled on database %(database)r yet, so the configure
  step cannot run. Do this once in the Databricks UI, then re-run deploy:

    1. Open the Lakebase project %(project)r in the Databricks UI.
    2. Open database %(database)r -> Data API tab -> ENABLE it.
    3. Expose the %(schema)r schema.
    4. Re-run this deploy. Phase 2 will register the dedicated service
       principal (%(sp)r), wire it to `authenticator`, and refresh the cache.
================================================================================
"""


def _discover_db_mgmt_id(w: Any, project: str, database: str) -> Optional[str]:
    """Return the management ``database_id`` for the workshop PG database, or None."""

    resp = w.api_client.do(
        "GET", f"{POSTGRES_API_BASE}/projects/{project}/branches/{_BRANCH}/databases"
    )
    databases = resp.get("databases", []) if isinstance(resp, dict) else []
    for db in databases:
        status = db.get("status", {}) if isinstance(db, dict) else {}
        if status.get("postgres_database") == database:
            return status.get("database_id")
    # Fall back to the first database if the exact name isn't matched.
    if databases:
        return (databases[0].get("status", {}) or {}).get("database_id")
    return None


def _is_not_found(exc: Exception) -> bool:
    """Offline-safe 404/NOT_FOUND detection for ``w.api_client.do`` errors."""

    code = str(getattr(exc, "error_code", "") or "").upper()
    if "NOT_FOUND" in code or "DOES_NOT_EXIST" in code:
        return True
    if getattr(exc, "status_code", None) == 404:
        return True
    text = str(exc).lower()
    return "not found" in text or "does not exist" in text or "404" in text


def _get_data_api_config(w: Any, project: str, dbid: str) -> Optional[Dict[str, Any]]:
    """GET the Data API mgmt resource; ``None`` ONLY on a real 404 (= not enabled).

    A 404 means phase 1 hasn't happened. Any OTHER error (5xx, timeout, auth) is
    re-raised so the caller DEFERS with the real reason instead of falsely telling
    the operator to go enable the API in the UI on an already-enabled database.
    """

    try:
        return w.api_client.do(
            "GET",
            f"{POSTGRES_API_BASE}/projects/{project}/branches/{_BRANCH}/databases/{dbid}/data-api",
        )
    except Exception as exc:
        if _is_not_found(exc):
            return None
        raise


def _create_role_sql(app_id: str, schema: str) -> List[str]:
    """SQL to register the SP as a non-owner role and grant it read on the schema.

    ``databricks_create_role`` runs via ``execute``+param below (it takes the
    identity as a string); the GRANTs quote ``app_id`` as an identifier (it is a
    UUID, so hyphens require quoting).
    """

    role = f'"{app_id}"'
    sch = f'"{schema}"'
    return [
        f"GRANT USAGE ON SCHEMA {sch} TO {role}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {sch} TO {role}",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {sch} GRANT SELECT ON TABLES TO {role}",
        f"GRANT {role} TO authenticator",
    ]


def _configure_sp_in_pg(ctx: Any, database: str, schema: str, app_id: str) -> List[str]:
    """Register + grant the SP identity in Postgres (admin connection). Returns SQL run."""

    conn = ctx.pg_connection(role="admin", database=database)
    try:  # extension + role creation run outside a txn block cleanly on autocommit.
        conn.autocommit = True
    except Exception:  # pragma: no cover - fake/driver without the attribute
        pass
    cur = conn.cursor()
    executed: List[str] = []

    cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
    executed.append("CREATE EXTENSION IF NOT EXISTS databricks_auth")
    try:
        cur.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')", (app_id,))
        executed.append(f"SELECT databricks_create_role('{app_id}', 'SERVICE_PRINCIPAL')")
    except Exception as exc:  # role may already exist -- idempotent re-run.
        ctx.logger.info("core/data_api: databricks_create_role note (may exist): %s", exc)
    for stmt in _create_role_sql(app_id, schema):
        cur.execute(stmt)
        executed.append(stmt)
    return executed


def _refresh_schema_cache(
    w: Any, project: str, dbid: str, current: List[str], schema: str, logger: Any
) -> List[str]:
    """PATCH the Data API to re-expose schemas (refreshes PostgREST's role cache)."""

    schemas = sorted(set(current or []) | {schema})
    w.api_client.do(
        "PATCH",
        f"{POSTGRES_API_BASE}/projects/{project}/branches/{_BRANCH}/databases/{dbid}/data-api",
        query={"update_mask": "spec"},
        body={"spec": {"db_schemas": schemas}},
    )
    logger.info("core/data_api: refreshed Data API schema cache; exposed schemas=%s.", schemas)
    return schemas


def deploy(ctx: Any) -> Dict[str, Any]:
    enabled = str(ctx.params.get("enable_data_api", "true")).lower() == "true"
    if not enabled:
        ctx.logger.info("[stub] Data API disabled (enable_data_api=false); skipping configure.")
        return {"status": "skipped"}

    project = ctx.resolved_names.get("lakebase_project", ctx.deployment_id)
    database = ctx.params.get("database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")
    sp_name = ctx.name("data-api-sp")
    secret_lifetime = ctx.params.get("data_api_sp_secret_lifetime") or "86400s"

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/data_api.deploy: no live clients injected; would detect Data API "
            "enablement on %r and, if enabled, register SP %r + `databricks_auth` role and "
            "refresh the cache. Live run happens in-workspace.",
            database,
            sp_name,
        )
        return {"sp": sp_name, "phase": "configure", "status": "stub"}

    # The whole configure is resilient: data_api runs mid-DAG, so a failure here
    # (missing entitlement, transient error) DEFERS instead of aborting the run.
    try:
        w = ctx.workspace_client()

        # (1) Discover the workshop database's management id.
        dbid = _discover_db_mgmt_id(w, project, database)
        if not dbid:
            ctx.logger.warning(
                "core/data_api: could not resolve a management id for database %r on %r; "
                "deferring configure.",
                database,
                project,
            )
            return {"sp": sp_name, "status": "deferred", "reason": "database_not_found"}

        # (2) Phase-1 gate: is the Data API enabled on this database?
        da = _get_data_api_config(w, project, dbid)
        if da is None:
            ctx.logger.warning(
                _MANUAL_ENABLE_BANNER,
                {"project": project, "database": database, "schema": schema, "sp": sp_name},
            )
            return {
                "sp": sp_name,
                "database": database,
                "phase": "manual_enable",
                "status": "awaiting_manual_enable",
            }

        # (3) Create/reuse the dedicated SP. Mint an OAuth secret only when the SP
        #     is newly created (or no usable secret is stored yet): re-running an
        #     existing deployment must NOT rotate the credential out from under a
        #     consumer already holding it.
        sp = _ensure_service_principal(w, sp_name, ctx.logger)
        app_id = sp["application_id"]
        stored_secret = _read_secret(w, scope, "data-api-sp-client-secret")
        if not sp["created"] and stored_secret:
            client_secret = stored_secret
            ctx.logger.info("core/data_api: reusing stored SP OAuth secret (idempotent re-run).")
        else:
            client_secret = _mint_sp_secret(w, sp["id"], secret_lifetime, ctx.logger)

        # (4) Wire the SP identity in Postgres (role + grants + grant-to-authenticator).
        executed = _configure_sp_in_pg(ctx, database, schema, app_id)

        # (5) Refresh the PostgREST schema cache so the new role is recognized.
        current = (da.get("status", {}) or {}).get("db_schemas", []) if isinstance(da, dict) else []
        exposed = _refresh_schema_cache(w, project, dbid, current, schema, ctx.logger)

        # (6) Persist the SP credentials + Data API coordinates to the scope.
        data_api_url = (da.get("status", {}) or {}).get("url", "") if isinstance(da, dict) else ""
        secret_map = {
            "data-api-sp-client-id": app_id,
            "data-api-sp-client-secret": client_secret or "",
            "data-api-url": data_api_url,
            "data-api-database": database,
            "data-api-schema": schema,
        }
        for key, value in secret_map.items():
            w.secrets.put_secret(scope=scope, key=key, string_value=value)

        ctx.logger.info(
            "core/data_api.deploy: configured Data API SP %r (app_id=%s) on %r; exposed=%s; "
            "wrote %d secret(s) to %r.",
            sp_name,
            app_id,
            database,
            exposed,
            len(secret_map),
            scope,
        )
        return {
            "sp": sp_name,
            "sp_application_id": app_id,
            "database": database,
            "schema": schema,
            "exposed_schemas": exposed,
            "data_api_url": data_api_url,
            "sql": executed,
            "secrets_written": sorted(secret_map),
            "phase": "configure",
            "status": "configured",
        }
    except Exception as exc:
        ctx.logger.error("[data_api] deferred: %s", exc)
        return {"sp": sp_name, "error": str(exc), "status": "deferred"}


def _read_secret(w: Any, scope: str, key: str) -> Optional[str]:
    """Best-effort read + base64-decode of a secret value; ``None`` if absent."""

    import base64

    try:
        resp = w.secrets.get_secret(scope=scope, key=key)
    except Exception:
        return None
    value = getattr(resp, "value", None)
    if value is None:
        return None
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception:  # pragma: no cover - value already plain
        return str(value)


def _ensure_service_principal(w: Any, sp_name: str, logger: Any) -> Dict[str, Any]:
    """Create or reuse a workspace service principal named ``sp_name``.

    Returns ``{"id": <internal id>, "application_id": <app-id uuid>, "created": bool}``.
    """

    existing = list(w.service_principals.list(filter=f'displayName eq "{sp_name}"'))
    if existing:
        sp = existing[0]
        logger.info("core/data_api: reusing service principal %r (app_id=%s).", sp_name, sp.application_id)
        created = False
    else:
        sp = w.service_principals.create(display_name=sp_name)
        logger.info("core/data_api: created service principal %r (app_id=%s).", sp_name, sp.application_id)
        created = True
    return {"id": sp.id, "application_id": sp.application_id, "created": created}


def _mint_sp_secret(w: Any, sp_internal_id: Any, lifetime: str, logger: Any) -> Optional[str]:
    """Mint an OAuth M2M secret for the SP (removing the oldest if the cap is hit)."""

    try:
        sec = w.service_principal_secrets_proxy.create(
            service_principal_id=sp_internal_id, lifetime=lifetime
        )
    except Exception:
        old = list(w.service_principal_secrets_proxy.list(service_principal_id=sp_internal_id))
        if old:
            w.service_principal_secrets_proxy.delete(
                service_principal_id=sp_internal_id, secret_id=old[0].id
            )
        sec = w.service_principal_secrets_proxy.create(
            service_principal_id=sp_internal_id, lifetime=lifetime
        )
    logger.info("core/data_api: minted SP OAuth secret (lifetime=%s).", lifetime)
    return getattr(sec, "secret", None)

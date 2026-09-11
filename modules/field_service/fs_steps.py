"""Internal step registry for the field_service module.

The harness module contract is one ``deploy``/``teardown``/``health_check``
entrypoint per module, but field_service provisions many resources with a real
dependency chain. So the module runs an ORDERED internal sub-pipeline: this file
declares the steps (name, optional gate parameter, and deploy/teardown/health
callables); ``deploy.py`` runs them forward, ``teardown.py`` in reverse, and
``health.py`` aggregates. Each step is idempotent and best-effort so a partial
failure defers rather than aborting the module (which itself runs mid-DAG).

Steps start as stubs (log intent + return ``status: "stub"``) and are filled in
wave by wave with live SDK/REST logic behind ``ctx.is_live()`` guards — exactly
like the core components. Sibling-importable via the ``sys.path`` insert the
entrypoints perform (the orchestrator loads module files flat by path).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from bootstrap.adapters import (
    DATABASE_CATALOGS_API,
    SQL_WAREHOUSES_API,
    is_already_exists,
    is_not_found,
)

StepFn = Callable[[Any], Dict[str, Any]]


# --------------------------------------------------------------------------- #
# Cross-step id persistence: steps write provisioned ids to the deployment's
# standalone secret scope so later steps (and re-runs / the app step) can read
# them without relying on in-memory state that a fresh teardown run won't have.
# --------------------------------------------------------------------------- #
def _scope(ctx: Any) -> str:
    return ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")


def _put_id(ctx: Any, key: str, value: str) -> None:
    try:
        ctx.workspace_client().secrets.put_secret(scope=_scope(ctx), key=key, string_value=value)
    except Exception as exc:  # pragma: no cover - best-effort persistence
        ctx.logger.info("field_service: could not persist %s: %s", key, exc)


def _get_id(ctx: Any, key: str) -> Optional[str]:
    import base64

    try:
        resp = ctx.workspace_client().secrets.get_secret(scope=_scope(ctx), key=key)
    except Exception:
        return None
    value = getattr(resp, "value", None)
    if value is None:
        return None
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception:  # pragma: no cover
        return str(value)


@dataclass
class Step:
    """One internal component step of the field_service module."""

    name: str
    deploy: StepFn
    teardown: StepFn
    health: StepFn
    # If set, the step runs only when this boolean module param is truthy.
    gate_param: Optional[str] = None


def _truthy(ctx: Any, param: str) -> bool:
    return str(ctx.params.get(param, "true")).lower() == "true"


def _stub(step: str, verb: str, ctx: Any, detail: str) -> Dict[str, Any]:
    ctx.logger.info("[stub] field_service.%s.%s: %s", step, verb, detail)
    return {"step": step, "status": "stub"}


# --------------------------------------------------------------------------- #
# Step stubs (filled in wave by wave). Each logs intent and returns a stub.
# --------------------------------------------------------------------------- #
def _mk(step: str, deploy_detail: str, teardown_detail: str, health_detail: str):
    def d(ctx: Any) -> Dict[str, Any]:
        return _stub(step, "deploy", ctx, deploy_detail)

    def t(ctx: Any) -> Dict[str, Any]:
        return _stub(step, "teardown", ctx, teardown_detail)

    def h(ctx: Any) -> Dict[str, Any]:
        return _stub(step, "health", ctx, health_detail)

    return d, t, h


def _data_deploy(ctx: Any) -> Dict[str, Any]:
    """Create the field-service schemas + tables + seed from assets/sql/*.sql."""

    import fs_sql

    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.is_live():
        return _stub(
            "data",
            "deploy",
            ctx,
            f"apply {len(fs_sql.DATA_SQL_FILES)} SQL file(s) creating schemas "
            f"{fs_sql.DATA_SCHEMAS} + seed into {database!r}",
        )

    scale_name = ctx.params.get("seed_volume", "demo")
    scale = fs_sql.scale_profile(scale_name)
    conn = ctx.pg_connection(role="admin", database=database)
    try:  # seed DDL/DML runs statement-at-a-time on autocommit (failures isolated).
        conn.autocommit = True
    except Exception:  # pragma: no cover - fake/driver without the attribute
        pass
    cur = conn.cursor()
    applied, failing = fs_sql.apply_sql_files(cur, fs_sql.DATA_SQL_FILES, scale, ctx.logger)
    ctx.logger.info(
        "field_service.data.deploy: applied %d statement(s) across %d file(s) "
        "(seed_volume=%s); %d still failing.",
        applied,
        len(fs_sql.DATA_SQL_FILES),
        scale_name,
        failing,
    )
    return {
        "step": "data",
        "schemas": fs_sql.DATA_SCHEMAS,
        "seed_volume": scale_name,
        "statements_applied": applied,
        "statements_failing": failing,
        "status": "deployed" if failing == 0 else "partial",
    }


def _data_teardown(ctx: Any) -> Dict[str, Any]:
    """Drop the field-service schemas (CASCADE) + the public Data API demo table."""

    import fs_sql

    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.has_pg_connection() or not ctx.has_workspace_client():
        return _stub("data", "teardown", ctx, f"DROP SCHEMA {fs_sql.DATA_SCHEMAS} CASCADE")

    conn = ctx.pg_connection(role="admin", database=database)
    try:
        conn.autocommit = True
    except Exception:  # pragma: no cover
        pass
    cur = conn.cursor()
    dropped: List[str] = []
    for schema in fs_sql.DATA_SCHEMAS:
        stmt = f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        try:
            cur.execute(stmt)
            dropped.append(schema)
        except Exception as exc:
            ctx.logger.info("field_service.data.teardown: %s -> %s", stmt, exc)
    try:  # the Data API demo sandbox table lives in public.
        cur.execute("DROP TABLE IF EXISTS public.data_api_demo CASCADE")
    except Exception as exc:  # pragma: no cover
        ctx.logger.info("field_service.data.teardown: drop public.data_api_demo -> %s", exc)
    return {"step": "data", "schemas_dropped": dropped, "status": "torn_down"}


def _data_health(ctx: Any) -> Dict[str, Any]:
    """Healthy when the central field_service.work_orders table exists."""

    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.is_live():
        return _stub("data", "health", ctx, "assert field_service.work_orders exists")

    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('field_service.work_orders')")
    row = cur.fetchone()
    exists = bool(row and row[0])
    return {
        "step": "data",
        "work_orders_present": exists,
        "healthy": exists,
        "status": "ok" if exists else "unhealthy",
    }


_data = (_data_deploy, _data_teardown, _data_health)
def _warehouse_name(ctx: Any) -> str:
    return ctx.name("fs-warehouse")


def _warehouse_deploy(ctx: Any) -> Dict[str, Any]:
    """Create (or reuse) a serverless SQL warehouse for Genie + dashboards."""

    name = _warehouse_name(ctx)
    if not ctx.has_workspace_client():
        return _stub("warehouse", "deploy", ctx, f"create serverless SQL warehouse {name!r}")
    w = ctx.workspace_client()
    wh_id = None
    try:  # idempotent: reuse an existing warehouse with this name.
        listed = w.api_client.do("GET", SQL_WAREHOUSES_API)
        for wh in (listed.get("warehouses", []) if isinstance(listed, dict) else []):
            if wh.get("name") == name:
                wh_id = wh.get("id")
                break
    except Exception as exc:  # pragma: no cover - best-effort listing
        ctx.logger.info("field_service.warehouse: list failed: %s", exc)
    if wh_id is None:
        created = w.api_client.do(
            "POST",
            SQL_WAREHOUSES_API,
            body={
                "name": name,
                "cluster_size": ctx.params.get("warehouse_size") or "Small",
                "min_num_clusters": 1,
                "max_num_clusters": 2,
                "auto_stop_mins": 15,
                "warehouse_type": "PRO",
                "enable_serverless_compute": True,
            },
        )
        wh_id = created.get("id") if isinstance(created, dict) else None
    if wh_id:
        ctx.resolved_names["fs_warehouse_id"] = wh_id
        _put_id(ctx, "fs-warehouse-id", wh_id)
    ctx.logger.info("field_service.warehouse.deploy: warehouse %r id=%s.", name, wh_id)
    return {"step": "warehouse", "warehouse": name, "warehouse_id": wh_id, "status": "deployed"}


def _warehouse_teardown(ctx: Any) -> Dict[str, Any]:
    name = _warehouse_name(ctx)
    if not ctx.has_workspace_client():
        return _stub("warehouse", "teardown", ctx, f"delete SQL warehouse {name!r}")
    w = ctx.workspace_client()
    wh_id = _get_id(ctx, "fs-warehouse-id")
    if not wh_id:  # discover by name
        try:
            listed = w.api_client.do("GET", SQL_WAREHOUSES_API)
            for wh in (listed.get("warehouses", []) if isinstance(listed, dict) else []):
                if wh.get("name") == name:
                    wh_id = wh.get("id")
                    break
        except Exception:  # pragma: no cover
            pass
    deleted = False
    if wh_id:
        try:
            w.api_client.do("DELETE", f"{SQL_WAREHOUSES_API}/{wh_id}")
            deleted = True
        except Exception as exc:
            ctx.logger.info("field_service.warehouse.teardown: %s", exc)
    return {"step": "warehouse", "warehouse_deleted": deleted, "status": "torn_down"}


def _warehouse_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("warehouse", "health", ctx, "assert the warehouse is available")
    w = ctx.workspace_client()
    wh_id = ctx.resolved_names.get("fs_warehouse_id") or _get_id(ctx, "fs-warehouse-id")
    if not wh_id:
        return {"step": "warehouse", "healthy": None, "status": "not_configured"}
    try:
        wh = w.api_client.do("GET", f"{SQL_WAREHOUSES_API}/{wh_id}")
        state = wh.get("state") if isinstance(wh, dict) else None
        healthy = state in ("RUNNING", "STARTING", "STOPPED")  # exists + valid state
        return {"step": "warehouse", "state": state, "healthy": healthy,
                "status": "ok" if healthy else "unhealthy"}
    except Exception as exc:
        return {"step": "warehouse", "healthy": None, "error": str(exc), "status": "deferred"}


_warehouse = (_warehouse_deploy, _warehouse_teardown, _warehouse_health)


def _catalog_name(ctx: Any) -> str:
    return ctx.resolved_names.setdefault("fs_catalog", f"{ctx.deployment_id}_field_service")


def _uc_catalog_deploy(ctx: Any) -> Dict[str, Any]:
    """Create a MANAGED_ONLINE_CATALOG linked to the Lakebase project (idempotent)."""

    name = _catalog_name(ctx)
    database = ctx.params.get("database") or "databricks_postgres"
    project = ctx.resolved_names.get("lakebase_project", ctx.deployment_id)
    if not ctx.has_workspace_client():
        return _stub("uc_catalog", "deploy", ctx, f"create managed online catalog {name!r}")
    w = ctx.workspace_client()
    try:
        proj = w.api_client.do("GET", f"/api/2.0/postgres/projects/{project}")
        branch = w.api_client.do("GET", f"/api/2.0/postgres/projects/{project}/branches/production")
        body = {
            "name": name,
            "database_project_id": proj["uid"],
            "database_branch_id": branch["uid"],
            "database_name": database,
        }
        try:
            w.api_client.do("POST", DATABASE_CATALOGS_API, body=body)
            created = True
        except Exception as exc:
            if is_already_exists(exc):
                created = False
                ctx.logger.info("field_service.uc_catalog: catalog %r already exists.", name)
            else:
                raise
        _put_id(ctx, "fs-catalog", name)
        ctx.logger.info("field_service.uc_catalog.deploy: catalog %r (created=%s).", name, created)
        return {"step": "uc_catalog", "catalog": name, "created": created, "status": "deployed"}
    except Exception as exc:  # best-effort: defer (project may still be provisioning)
        ctx.logger.error("[field_service.uc_catalog] deferred: %s", exc)
        return {"step": "uc_catalog", "catalog": name, "error": str(exc), "status": "deferred"}


def _uc_catalog_teardown(ctx: Any) -> Dict[str, Any]:
    name = _catalog_name(ctx)
    if not ctx.has_workspace_client():
        return _stub("uc_catalog", "teardown", ctx, f"delete managed online catalog {name!r}")
    w = ctx.workspace_client()
    deleted = False
    try:
        w.api_client.do("DELETE", f"{DATABASE_CATALOGS_API}/{name}")
        deleted = True
    except Exception as exc:
        if not is_not_found(exc):
            ctx.logger.info("field_service.uc_catalog.teardown: %s", exc)
    return {"step": "uc_catalog", "catalog_deleted": deleted, "status": "torn_down"}


def _uc_catalog_health(ctx: Any) -> Dict[str, Any]:
    name = _catalog_name(ctx)
    if not ctx.has_workspace_client():
        return _stub("uc_catalog", "health", ctx, "assert the managed online catalog exists")
    w = ctx.workspace_client()
    try:
        cat = w.api_client.do("GET", f"{DATABASE_CATALOGS_API}/{name}")
        exists = bool(cat.get("name")) if isinstance(cat, dict) else False
        return {"step": "uc_catalog", "catalog": name, "healthy": exists,
                "status": "ok" if exists else "unhealthy"}
    except Exception as exc:
        return {"step": "uc_catalog", "healthy": None, "error": str(exc), "status": "deferred"}


_uc_catalog = (_uc_catalog_deploy, _uc_catalog_teardown, _uc_catalog_health)
_FEATURES_SQL = "lakebase_features.sql"


def _features_deploy(ctx: Any) -> Dict[str, Any]:
    """Apply the SLA engine, events, triggers, and materialized views."""

    import fs_sql

    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.is_live():
        return _stub("features", "deploy", ctx, f"apply {_FEATURES_SQL} (SLA engine + MVs)")
    conn = ctx.pg_connection(role="admin", database=database)
    try:
        conn.autocommit = True
    except Exception:  # pragma: no cover
        pass
    cur = conn.cursor()
    scale = fs_sql.scale_profile(ctx.params.get("seed_volume", "demo"))
    applied, failing = fs_sql.apply_sql_files(cur, [_FEATURES_SQL], scale, ctx.logger)
    ctx.logger.info(
        "field_service.features.deploy: applied %d statement(s); %d failing.", applied, failing
    )
    return {"step": "features", "statements_applied": applied, "statements_failing": failing,
            "status": "deployed" if failing == 0 else "partial"}


def _features_teardown(ctx: Any) -> Dict[str, Any]:
    # The SLA engine objects live in the field_service schema, which the data
    # step drops CASCADE; nothing extra to remove here.
    return {"step": "features", "status": "torn_down",
            "note": "removed with the field_service schema (data step)"}


def _features_health(ctx: Any) -> Dict[str, Any]:
    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.is_live():
        return _stub("features", "health", ctx, "assert the SLA materialized view exists")
    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('field_service.mv_technician_leaderboard')")
    row = cur.fetchone()
    exists = bool(row and row[0])
    return {"step": "features", "sla_mv_present": exists, "healthy": exists,
            "status": "ok" if exists else "unhealthy"}


_features = (_features_deploy, _features_teardown, _features_health)


def _synced_deploy(ctx: Any) -> Dict[str, Any]:
    """Confirm the managed catalog auto-registers the Lakebase tables as foreign tables.

    A MANAGED_ONLINE_CATALOG surfaces the Lakebase database's tables in Unity
    Catalog automatically; there is no separate resource to create, so this step
    just records the linkage (a live poll for a specific foreign table can be
    added when the catalog + data are both present).
    """

    catalog = _catalog_name(ctx)
    if not ctx.has_workspace_client():
        return _stub("synced", "deploy", ctx, f"await foreign-table registration in {catalog!r}")
    return {"step": "synced", "catalog": catalog, "status": "deployed",
            "note": "managed online catalog auto-registers Lakebase tables as foreign tables"}


def _synced_teardown(ctx: Any) -> Dict[str, Any]:
    # Foreign tables are removed when the managed catalog is deleted (uc_catalog step).
    return {"step": "synced", "status": "torn_down", "note": "removed with the managed catalog"}


def _synced_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("synced", "health", ctx, "assert foreign tables are queryable")
    return {"step": "synced", "healthy": True, "status": "ok"}


_synced = (_synced_deploy, _synced_teardown, _synced_health)
_pipeline = _mk(
    "pipeline",
    "deploy + run the DLT/Iceberg streaming pipeline (network/IoT gold tables)",
    "delete the pipeline + its output tables",
    "assert the pipeline's target gold tables exist",
)
_genie = _mk(
    "genie",
    "create 4 Genie spaces from assets/genie/*.json (POST /api/2.0/genie/spaces)",
    "trash the 4 Genie spaces",
    "assert the 4 spaces exist and are queryable",
)
_dashboards = _mk(
    "dashboards",
    "create + publish 2 Lakeview dashboards from assets/dashboards/*.json",
    "delete the 2 dashboards",
    "assert the dashboards are published",
)
_governance = _mk(
    "governance",
    "apply UC tags, row-level security, masking views",
    "remove governance tags/policies",
    "assert masking views + RLS policies exist",
)
_ml = _mk(
    "ml",
    "train + register (UC) + serve the predictive-maintenance model",
    "delete the model serving endpoint + UC model",
    "assert the serving endpoint is READY",
)
_agent = _mk(
    "agent",
    "build + register + serve the LangGraph multi-Genie supervisor agent",
    "delete the agent serving endpoint + UC model",
    "assert the agent endpoint is READY",
)
_ops = _mk(
    "ops",
    "schedule ops jobs (credential rotation, ASH sampler, Genie-conversation cleanup)",
    "delete the scheduled ops jobs",
    "assert the scheduled jobs exist",
)
_app = _mk(
    "app",
    "deploy the field-service Databricks App (Apps REST) with injected env",
    "delete the field-service app",
    "assert the app compute is ACTIVE and its deployment SUCCEEDED",
)


ORDERED_STEPS: List[Step] = [
    Step("data", *_data),
    Step("uc_catalog", *_uc_catalog),
    Step("warehouse", *_warehouse),
    Step("features", *_features),
    Step("synced", *_synced),
    Step("pipeline", *_pipeline, gate_param="include_pipeline"),
    Step("genie", *_genie),
    Step("dashboards", *_dashboards),
    Step("governance", *_governance),
    Step("ml", *_ml, gate_param="include_ml"),
    Step("agent", *_agent, gate_param="include_agent"),
    Step("ops", *_ops, gate_param="include_ops_jobs"),
    Step("app", *_app),
]


def steps_for(ctx: Any) -> List[Step]:
    """Return the steps enabled for this run (gated steps whose param is off are skipped)."""

    return [s for s in ORDERED_STEPS if s.gate_param is None or _truthy(ctx, s.gate_param)]

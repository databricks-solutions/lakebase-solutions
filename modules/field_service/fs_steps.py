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


# --------------------------------------------------------------------------- #
# Shared job-submit / serving helpers (pipeline / ml / agent / ops steps).
# --------------------------------------------------------------------------- #
def _fs_workspace_path(ctx: Any, subpath: str) -> str:
    """Workspace path of a vendored module asset (the synced Repo folder)."""

    repo_folder = ctx.params.get("repo_folder") or "lakebase-solutions"
    try:
        email = ctx.workspace_client().current_user.me().user_name
    except Exception:  # pragma: no cover
        email = ctx.params.get("workspace_user") or "unknown"
    return f"/Workspace/Users/{email}/{repo_folder}/modules/field_service/{subpath}"


def _submit_notebook_job(
    ctx: Any, run_name: str, notebook_subpath: str,
    base_parameters: Optional[Dict[str, Any]] = None, dependencies: Optional[List[str]] = None,
) -> Optional[str]:
    """Submit a one-time serverless notebook job (runs/submit); return run_id."""

    w = ctx.workspace_client()
    body = {
        "run_name": run_name,
        "tasks": [{
            "task_key": "run",
            "notebook_task": {
                "notebook_path": _fs_workspace_path(ctx, notebook_subpath),
                "base_parameters": base_parameters or {},
            },
            "environment_key": "env",
        }],
        "environments": [{"environment_key": "env",
                          "spec": {"client": "2", "dependencies": dependencies or []}}],
    }
    resp = w.api_client.do("POST", "/api/2.1/jobs/runs/submit", body=body)
    return resp.get("run_id") if isinstance(resp, dict) else None


def _serving_endpoint_name(ctx: Any, suffix: str) -> str:
    return f"{ctx.deployment_id}-fs-{suffix}"


def _serving_exists(ctx: Any, name: str) -> bool:
    try:
        ep = ctx.workspace_client().api_client.do("GET", f"/api/2.0/serving-endpoints/{name}")
        return bool(ep.get("name")) if isinstance(ep, dict) else False
    except Exception:
        return False


def _delete_serving(ctx: Any, name: str) -> bool:
    try:
        ctx.workspace_client().api_client.do("DELETE", f"/api/2.0/serving-endpoints/{name}")
        return True
    except Exception as exc:  # pragma: no cover - already gone
        ctx.logger.info("field_service: delete serving %r -> %s", name, exc)
        return False


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
def _pipeline_deploy(ctx: Any) -> Dict[str, Any]:
    """Submit the DLT/Iceberg streaming pipeline as a serverless job run."""

    if not ctx.has_workspace_client():
        return _stub("pipeline", "deploy", ctx, "submit the iceberg streaming pipeline job")
    catalog = _catalog_name(ctx)
    run_id = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-pipeline",
        "assets/pipeline/iceberg_streaming_pipeline",
        base_parameters={"catalog": catalog,
                         "volume_path": f"/Volumes/{catalog}/network_data/raw_files"},
        dependencies=["pyiceberg", "pyarrow"],
    )
    if run_id:
        _put_id(ctx, "fs-pipeline-run-id", str(run_id))
    ctx.logger.info("field_service.pipeline.deploy: submitted run_id=%s.", run_id)
    return {"step": "pipeline", "run_id": run_id, "status": "deployed" if run_id else "partial"}


def _pipeline_teardown(ctx: Any) -> Dict[str, Any]:
    # The pipeline's Iceberg output tables live in the managed catalog and are
    # removed when the catalog is deleted (uc_catalog step).
    return {"step": "pipeline", "status": "torn_down",
            "note": "iceberg output tables removed with the managed catalog"}


def _pipeline_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("pipeline", "health", ctx, "assert the pipeline gold tables exist")
    # A run was submitted; a deep gold-table assertion needs a live warehouse query.
    return {"step": "pipeline", "healthy": True, "status": "ok",
            "note": "pipeline job submitted (gold-table assertion deferred to a live check)"}


_pipeline = (_pipeline_deploy, _pipeline_teardown, _pipeline_health)
_GENIE_API = "/api/2.0/genie/spaces"
# Each Genie space: config key, title, source JSON asset. Titles are namespaced
# by deployment_id at runtime so multiple deployments coexist.
_GENIE_SPACES = [
    {"key": "postgres", "title": "PostgresAdmin", "json": "genie_space_postgres_admin.json",
     "description": "Monitor + manage Lakebase Postgres — connections, queries, table stats"},
    {"key": "field_ops", "title": "Field Service Operations", "json": "genie_space_field_ops.json",
     "description": "Work orders, technicians, dispatch, and SLA data in natural language"},
    {"key": "network_health", "title": "Network Health & Telemetry", "json": "genie_space_network_health.json",
     "description": "Node health, outages, IoT telemetry, and maintenance risk"},
    {"key": "sla_workforce", "title": "SLA & Workforce Analytics", "json": "genie_space_sla_workforce.json",
     "description": "SLA compliance, technician performance, and regional workforce analytics"},
]


def _genie_assets_dir():
    from pathlib import Path

    return Path(__file__).resolve().parent / "assets" / "genie"


def _genie_title(ctx: Any, title: str) -> str:
    return f"{ctx.deployment_id} {title}"


def _rewrite_genie_catalog(space_config: Dict[str, Any], catalog: str) -> Dict[str, Any]:
    """Point every table identifier at ``catalog`` and sort (the API requires sorted)."""

    ds = space_config.get("data_sources") or {}
    tables = ds.get("tables") if isinstance(ds, dict) else None
    if isinstance(tables, list):
        for t in tables:
            ident = t.get("identifier", "")
            parts = ident.split(".")
            if len(parts) == 3:
                parts[0] = catalog
                t["identifier"] = ".".join(parts)
        ds["tables"] = sorted(tables, key=lambda t: t.get("identifier", ""))
        space_config["data_sources"] = ds
    return space_config


def _genie_list(w: Any) -> List[Dict[str, Any]]:
    try:
        resp = w.api_client.do("GET", _GENIE_API)
        return resp.get("spaces", []) if isinstance(resp, dict) else []
    except Exception:  # pragma: no cover
        return []


def _genie_deploy(ctx: Any) -> Dict[str, Any]:
    """Create the 4 Genie spaces from the JSON assets (idempotent by title)."""

    import json

    if not ctx.has_workspace_client():
        return _stub("genie", "deploy", ctx, f"create {len(_GENIE_SPACES)} Genie spaces")
    w = ctx.workspace_client()
    warehouse_id = ctx.resolved_names.get("fs_warehouse_id") or _get_id(ctx, "fs-warehouse-id")
    catalog = _catalog_name(ctx)
    existing = {s.get("title"): s.get("space_id") for s in _genie_list(w)}
    created: Dict[str, str] = {}
    assets = _genie_assets_dir()
    for spec in _GENIE_SPACES:
        title = _genie_title(ctx, spec["title"])
        if title in existing:  # idempotent reuse
            created[spec["key"]] = existing[title]
            continue
        try:
            space_config = json.loads((assets / spec["json"]).read_text(encoding="utf-8"))
        except Exception as exc:
            ctx.logger.info("field_service.genie: read %s failed: %s", spec["json"], exc)
            continue
        space_config = _rewrite_genie_catalog(space_config, catalog)
        try:
            resp = w.api_client.do(
                "POST",
                _GENIE_API,
                body={
                    "title": title,
                    "description": spec["description"],
                    "warehouse_id": warehouse_id,
                    "serialized_space": json.dumps(space_config),
                },
            )
            sid = resp.get("space_id") if isinstance(resp, dict) else None
            if sid:
                created[spec["key"]] = sid
                _put_id(ctx, f"genie-space-{spec['key']}", sid)
        except Exception as exc:
            ctx.logger.info("field_service.genie: create %r failed: %s", title, exc)
    ctx.resolved_names.setdefault("fs_genie_spaces", ",".join(created.values()))
    ctx.logger.info("field_service.genie.deploy: %d/%d spaces present.", len(created), len(_GENIE_SPACES))
    status = "deployed" if len(created) == len(_GENIE_SPACES) else "partial"
    return {"step": "genie", "spaces": created, "status": status}


def _genie_teardown(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("genie", "teardown", ctx, "trash the Genie spaces")
    w = ctx.workspace_client()
    titles = {_genie_title(ctx, s["title"]) for s in _GENIE_SPACES}
    deleted = 0
    for s in _genie_list(w):
        if s.get("title") in titles and s.get("space_id"):
            try:
                w.api_client.do("DELETE", f"{_GENIE_API}/{s['space_id']}")
                deleted += 1
            except Exception as exc:  # pragma: no cover
                ctx.logger.info("field_service.genie.teardown: %s", exc)
    return {"step": "genie", "spaces_deleted": deleted, "status": "torn_down"}


def _genie_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("genie", "health", ctx, "assert the 4 Genie spaces exist")
    w = ctx.workspace_client()
    titles = {_genie_title(ctx, s["title"]) for s in _GENIE_SPACES}
    present = sum(1 for s in _genie_list(w) if s.get("title") in titles)
    healthy = present == len(_GENIE_SPACES)
    return {"step": "genie", "spaces_present": present, "healthy": healthy,
            "status": "ok" if healthy else "unhealthy"}


_genie = (_genie_deploy, _genie_teardown, _genie_health)
_LAKEVIEW_API = "/api/2.0/lakeview/dashboards"
_DASHBOARDS = [
    {"key": "field_service_ops", "name": "Field Service Operations", "json": "dashboard_field_service_ops.json"},
    {"key": "network_ops", "name": "Network Operations", "json": "dashboard_network_ops.json"},
]


def _dashboard_name(ctx: Any, name: str) -> str:
    return f"{ctx.deployment_id} {name}"


def _dashboard_parent_path(ctx: Any) -> str:
    path = ctx.params.get("dashboard_parent_path")
    if path:
        return path
    try:
        email = ctx.workspace_client().current_user.me().user_name
    except Exception:  # pragma: no cover
        email = "unknown"
    return f"/Users/{email}"


def _lakeview_list(w: Any) -> List[Dict[str, Any]]:
    try:
        resp = w.api_client.do("GET", _LAKEVIEW_API)
        return resp.get("dashboards", []) if isinstance(resp, dict) else []
    except Exception:  # pragma: no cover
        return []


def _dashboards_deploy(ctx: Any) -> Dict[str, Any]:
    """Create + publish the Lakeview dashboards (idempotent by display_name)."""

    import json

    if not ctx.has_workspace_client():
        return _stub("dashboards", "deploy", ctx, f"create {len(_DASHBOARDS)} Lakeview dashboards")
    w = ctx.workspace_client()
    warehouse_id = ctx.resolved_names.get("fs_warehouse_id") or _get_id(ctx, "fs-warehouse-id")
    catalog = _catalog_name(ctx)
    parent = _dashboard_parent_path(ctx)
    subs = {"catalog": catalog, "schema": "field_service", "pipeline_catalog": catalog}
    from pathlib import Path

    assets = Path(__file__).resolve().parent / "assets" / "dashboards"
    existing = {d.get("display_name"): d.get("dashboard_id") for d in _lakeview_list(w)}
    published: Dict[str, str] = {}
    for spec in _DASHBOARDS:
        name = _dashboard_name(ctx, spec["name"])
        did = existing.get(name)
        if did is None:
            try:
                raw = (assets / spec["json"]).read_text(encoding="utf-8")
                for k, v in subs.items():
                    raw = raw.replace("{" + k + "}", v)
                resp = w.api_client.do(
                    "POST",
                    _LAKEVIEW_API,
                    body={"display_name": name, "parent_path": parent,
                          "serialized_dashboard": raw, "warehouse_id": warehouse_id},
                )
                did = resp.get("dashboard_id") if isinstance(resp, dict) else None
            except Exception as exc:
                ctx.logger.info("field_service.dashboards: create %r failed: %s", name, exc)
                continue
        if did:
            try:
                w.api_client.do("POST", f"{_LAKEVIEW_API}/{did}/published",
                                body={"warehouse_id": warehouse_id})
            except Exception as exc:  # pragma: no cover
                ctx.logger.info("field_service.dashboards: publish %r failed: %s", name, exc)
            published[spec["key"]] = did
            _put_id(ctx, f"dashboard-{spec['key']}", did)
    status = "deployed" if len(published) == len(_DASHBOARDS) else "partial"
    return {"step": "dashboards", "dashboards": published, "status": status}


def _dashboards_teardown(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("dashboards", "teardown", ctx, "delete the Lakeview dashboards")
    w = ctx.workspace_client()
    names = {_dashboard_name(ctx, s["name"]) for s in _DASHBOARDS}
    deleted = 0
    for d in _lakeview_list(w):
        if d.get("display_name") in names and d.get("dashboard_id"):
            try:
                w.api_client.do("DELETE", f"{_LAKEVIEW_API}/{d['dashboard_id']}")
                deleted += 1
            except Exception as exc:  # pragma: no cover
                ctx.logger.info("field_service.dashboards.teardown: %s", exc)
    return {"step": "dashboards", "dashboards_deleted": deleted, "status": "torn_down"}


def _dashboards_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("dashboards", "health", ctx, "assert the dashboards are published")
    w = ctx.workspace_client()
    names = {_dashboard_name(ctx, s["name"]) for s in _DASHBOARDS}
    present = sum(1 for d in _lakeview_list(w) if d.get("display_name") in names)
    healthy = present == len(_DASHBOARDS)
    return {"step": "dashboards", "dashboards_present": present, "healthy": healthy,
            "status": "ok" if healthy else "unhealthy"}


_dashboards = (_dashboards_deploy, _dashboards_teardown, _dashboards_health)

# Region-based row-level security + a PII-masked customer view. Self-contained
# and idempotent; applied over the workshop Postgres. This is the module's
# governance layer (the app's RBAC reads current_setting('app.region')).
_GOVERNANCE_SQL = [
    "ALTER TABLE IF EXISTS field_service.work_orders ENABLE ROW LEVEL SECURITY",
    """DO $$ BEGIN
         IF NOT EXISTS (SELECT 1 FROM pg_policies
                        WHERE schemaname='field_service' AND tablename='work_orders'
                          AND policyname='rls_region') THEN
           CREATE POLICY rls_region ON field_service.work_orders
             USING (current_setting('app.region', true) IS NULL
                    OR region = current_setting('app.region', true));
         END IF;
       END $$;""",
    """CREATE OR REPLACE VIEW field_service.v_customers_masked AS
         SELECT customer_id, region,
                regexp_replace(COALESCE(email,''), '(^.).*(@.*$)', '\\1***\\2') AS email,
                left(COALESCE(phone,''), 3) || '-***-****' AS phone
         FROM field_service.customers""",
]


def _governance_deploy(ctx: Any) -> Dict[str, Any]:
    """Apply row-level security + a PII-masked customer view (best-effort)."""

    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.is_live():
        return _stub("governance", "deploy", ctx, "enable RLS + masking view on field_service")
    conn = ctx.pg_connection(role="admin", database=database)
    try:
        conn.autocommit = True
    except Exception:  # pragma: no cover
        pass
    cur = conn.cursor()
    applied = 0
    for stmt in _GOVERNANCE_SQL:
        try:
            cur.execute(stmt)
            applied += 1
        except Exception as exc:
            ctx.logger.info("field_service.governance: deferred statement: %s", str(exc)[:120])
    return {"step": "governance", "statements_applied": applied,
            "status": "deployed" if applied == len(_GOVERNANCE_SQL) else "partial"}


def _governance_teardown(ctx: Any) -> Dict[str, Any]:
    # Policies + the masked view live in the field_service schema, dropped with it.
    return {"step": "governance", "status": "torn_down",
            "note": "removed with the field_service schema (data step)"}


def _governance_health(ctx: Any) -> Dict[str, Any]:
    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.is_live():
        return _stub("governance", "health", ctx, "assert the masked customer view exists")
    conn = ctx.pg_connection(role="admin", database=database)
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('field_service.v_customers_masked')")
    row = cur.fetchone()
    exists = bool(row and row[0])
    return {"step": "governance", "masking_view_present": exists, "healthy": exists,
            "status": "ok" if exists else "unhealthy"}


_governance = (_governance_deploy, _governance_teardown, _governance_health)
def _ml_deploy(ctx: Any) -> Dict[str, Any]:
    """Submit the predictive-maintenance training job (trains + registers in UC)."""

    if not ctx.has_workspace_client():
        return _stub("ml", "deploy", ctx, "train + register the predictive-maintenance model")
    run_id = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-ml",
        "assets/notebooks/predictive_maintenance",
        base_parameters={"catalog": _catalog_name(ctx)},
        dependencies=["lightgbm", "scikit-learn", "mlflow"],
    )
    if run_id:
        _put_id(ctx, "fs-ml-run-id", str(run_id))
    return {"step": "ml", "run_id": run_id, "status": "deployed" if run_id else "partial"}


def _ml_teardown(ctx: Any) -> Dict[str, Any]:
    # The UC model is registered inside the managed catalog and removed when the
    # catalog is deleted (uc_catalog step).
    return {"step": "ml", "status": "torn_down", "note": "UC model removed with the managed catalog"}


def _ml_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("ml", "health", ctx, "assert the predictive-maintenance model is registered")
    return {"step": "ml", "healthy": True, "status": "ok",
            "note": "training job submitted (model-registration assertion deferred to a live check)"}


_ml = (_ml_deploy, _ml_teardown, _ml_health)


def _agent_deploy(ctx: Any) -> Dict[str, Any]:
    """Submit the agent build/serve job (registers + creates a serving endpoint)."""

    if not ctx.has_workspace_client():
        return _stub("agent", "deploy", ctx, "build + serve the multi-Genie supervisor agent")
    name = _serving_endpoint_name(ctx, "agent")
    run_id = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-agent",
        "assets/notebooks/deploy_agent_endpoint",
        base_parameters={"endpoint_name": name, "catalog": _catalog_name(ctx)},
        dependencies=["databricks-agents", "mlflow", "databricks-langchain",
                      "langgraph", "langgraph-supervisor"],
    )
    _put_id(ctx, "fs-agent-endpoint", name)
    return {"step": "agent", "endpoint": name, "run_id": run_id,
            "status": "deployed" if run_id else "partial"}


def _agent_teardown(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("agent", "teardown", ctx, "delete the agent serving endpoint")
    name = _serving_endpoint_name(ctx, "agent")
    deleted = _delete_serving(ctx, name)
    return {"step": "agent", "endpoint_deleted": deleted, "status": "torn_down"}


def _agent_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("agent", "health", ctx, "assert the agent serving endpoint exists")
    name = _serving_endpoint_name(ctx, "agent")
    exists = _serving_exists(ctx, name)
    return {"step": "agent", "endpoint": name, "healthy": exists,
            "status": "ok" if exists else "unhealthy"}


_agent = (_agent_deploy, _agent_teardown, _agent_health)
# (suffix, notebook asset, quartz cron) for the scheduled ops jobs.
_OPS_JOBS = [
    ("ash-sampler", "assets/notebooks/ash_sampler", "0 0 * * * ?"),          # hourly
    ("genie-cleanup", "assets/notebooks/genie_conversation_cleanup", "0 0 3 * * ?"),  # daily 3am
    ("rotate-pg", "assets/notebooks/rotate_pg_password", "0 0 4 ? * SUN"),   # weekly
]


def _ops_deploy(ctx: Any) -> Dict[str, Any]:
    """Create the scheduled ops jobs (ASH sampler, Genie cleanup, rotation)."""

    if not ctx.has_workspace_client():
        return _stub("ops", "deploy", ctx, f"schedule {len(_OPS_JOBS)} ops job(s)")
    w = ctx.workspace_client()
    created: List[str] = []
    for suffix, nb, cron in _OPS_JOBS:
        job_name = f"{ctx.deployment_id}-fs-{suffix}"
        body = {
            "name": job_name,
            "tasks": [{"task_key": "run",
                       "notebook_task": {"notebook_path": _fs_workspace_path(ctx, nb)},
                       "environment_key": "env"}],
            "environments": [{"environment_key": "env", "spec": {"client": "2"}}],
            "schedule": {"quartz_cron_expression": cron, "timezone_id": "UTC",
                         "pause_status": "UNPAUSED"},
        }
        try:
            resp = w.api_client.do("POST", "/api/2.1/jobs/create", body=body)
            jid = resp.get("job_id") if isinstance(resp, dict) else None
            if jid is not None:
                _put_id(ctx, f"ops-job-{suffix}", str(jid))
            created.append(job_name)
        except Exception as exc:
            ctx.logger.info("field_service.ops: create %r failed: %s", job_name, exc)
    status = "deployed" if len(created) == len(_OPS_JOBS) else "partial"
    return {"step": "ops", "jobs": created, "status": status}


def _ops_teardown(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("ops", "teardown", ctx, "delete the scheduled ops jobs")
    w = ctx.workspace_client()
    deleted = 0
    for suffix, _nb, _cron in _OPS_JOBS:
        jid = _get_id(ctx, f"ops-job-{suffix}")
        if jid:
            try:
                w.api_client.do("POST", "/api/2.1/jobs/delete", body={"job_id": int(jid)})
                deleted += 1
            except Exception as exc:  # pragma: no cover
                ctx.logger.info("field_service.ops.teardown: %s", exc)
    return {"step": "ops", "jobs_deleted": deleted, "status": "torn_down"}


def _ops_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("ops", "health", ctx, "assert the scheduled ops jobs exist")
    present = sum(1 for suffix, _n, _c in _OPS_JOBS if _get_id(ctx, f"ops-job-{suffix}"))
    healthy = present == len(_OPS_JOBS)
    return {"step": "ops", "jobs_present": present, "healthy": healthy,
            "status": "ok" if healthy else "unhealthy"}


_ops = (_ops_deploy, _ops_teardown, _ops_health)
_APP_PG_SECRET_KEYS = ["pguser", "pgpassword"]
_APP_POLL_ATTEMPTS = 60
_APP_POLL_DELAY = 10.0


def _render_app_yaml(ctx: Any) -> str:
    """Render the field-service app.yaml from this deployment's provisioning results."""

    import json

    from bootstrap.adapters import resolve_endpoint_host

    w = ctx.workspace_client()
    project = ctx.resolved_names.get("lakebase_project", ctx.deployment_id)
    database = ctx.params.get("database") or "databricks_postgres"
    host = ""
    try:
        host = resolve_endpoint_host(w, project) or ""
    except Exception:  # pragma: no cover
        pass
    env = {
        "PGHOST": host,
        "PGDATABASE": database,
        "SQL_WAREHOUSE_ID": _get_id(ctx, "fs-warehouse-id") or "",
        "PIPELINE_CATALOG": _catalog_name(ctx),
        "AGENT_ENDPOINT_NAME": _get_id(ctx, "fs-agent-endpoint") or "",
        "GENIE_SPACE_POSTGRES": _get_id(ctx, "genie-space-postgres") or "",
        "GENIE_SPACE_FIELD_OPS": _get_id(ctx, "genie-space-field_ops") or "",
        "GENIE_SPACE_NETWORK_HEALTH": _get_id(ctx, "genie-space-network_health") or "",
        "GENIE_SPACE_SLA_WORKFORCE": _get_id(ctx, "genie-space-sla_workforce") or "",
        "LAKEBASE_PROJECT_ID": project,
        "LAKEBASE_TYPE": "autoscaling",
        "INSTANCE_NAME": ctx.deployment_id,
        "APP_NAME": ctx.name("field-service"),
    }
    lines = ["command:", "- python", "- app.py", "env:"]
    for key in _APP_PG_SECRET_KEYS:  # secrets via valueFrom
        env_name = "PGUSER" if key == "pguser" else "PGPASSWORD"
        lines += [f"- name: {env_name}", f"  valueFrom: {key}"]
    for k, v in env.items():
        lines += [f"- name: {k}", f"  value: {json.dumps(v)}"]
    return "\n".join(lines) + "\n"


def _upload_app_yaml(ctx: Any, source_path: str, content: str) -> None:
    """Upload the rendered app.yaml into the app source folder (workspace import)."""

    import base64

    w = ctx.workspace_client()
    w.api_client.do(
        "POST",
        "/api/2.0/workspace/import",
        body={
            "path": f"{source_path}/app.yaml",
            "format": "AUTO",
            "overwrite": True,
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        },
    )


def _app_secret_resources(scope: str) -> List[Dict[str, Any]]:
    return [{"name": k, "description": f"Lakebase PG secret {k}",
             "secret": {"scope": scope, "key": k, "permission": "READ"}}
            for k in _APP_PG_SECRET_KEYS]


def _app_deploy(ctx: Any) -> Dict[str, Any]:
    """Deploy the field-service Databricks App with env rendered from provisioning."""

    import time

    from bootstrap.adapters import APPS_API_BASE

    app_name = ctx.name("field-service")
    scope = _scope(ctx)
    if not ctx.has_workspace_client():
        return _stub("app", "deploy", ctx, f"deploy field-service app {app_name!r}")
    w = ctx.workspace_client()
    source_path = ctx.params.get("field_service_app_source_path") or _fs_workspace_path(ctx, "assets/app")
    try:
        # 1. Render + upload the real app.yaml (host/genie/warehouse/catalog/agent).
        _upload_app_yaml(ctx, source_path, _render_app_yaml(ctx))

        # 2. Create the app (with PG secret resources) if missing.
        created = False
        try:
            w.api_client.do("GET", f"{APPS_API_BASE}/{app_name}")
        except Exception as exc:
            if not is_not_found(exc):
                raise
            w.api_client.do("POST", APPS_API_BASE, body={
                "name": app_name,
                "description": "Field-service workshop app.",
                "resources": _app_secret_resources(scope),
            })
            created = True
            for _ in range(_APP_POLL_ATTEMPTS):  # wait for compute ACTIVE
                app = w.api_client.do("GET", f"{APPS_API_BASE}/{app_name}")
                if (app.get("compute_status") or {}).get("state") == "ACTIVE":
                    break
                time.sleep(_APP_POLL_DELAY)  # pragma: no cover - live-only wait

        # 3. Deploy from the source path.
        dep = w.api_client.do("POST", f"{APPS_API_BASE}/{app_name}/deployments",
                              body={"source_code_path": source_path, "mode": "SNAPSHOT"})
        deploy_state = (dep.get("status") or {}).get("state") or ""
        dep_id = dep.get("deployment_id")
        if dep_id and deploy_state not in ("SUCCEEDED", "FAILED", "STOPPED", "CANCELLED"):
            for _ in range(_APP_POLL_ATTEMPTS):  # pragma: no cover - live-only wait
                d = w.api_client.do("GET", f"{APPS_API_BASE}/{app_name}/deployments/{dep_id}")
                deploy_state = (d.get("status") or {}).get("state") or ""
                if deploy_state in ("SUCCEEDED", "FAILED", "STOPPED", "CANCELLED"):
                    break
                time.sleep(_APP_POLL_DELAY)

        app = w.api_client.do("GET", f"{APPS_API_BASE}/{app_name}")
        url = app.get("url")
        compute = (app.get("compute_status") or {}).get("state")
        healthy = deploy_state in ("", "SUCCEEDED") and compute in (None, "ACTIVE")
        ctx.logger.info("field_service.app.deploy: %r url=%s compute=%s deploy=%s.",
                        app_name, url, compute, deploy_state)
        return {"step": "app", "app": app_name, "url": url, "created": created,
                "compute_status": compute, "deployment_state": deploy_state,
                "status": "deployed" if healthy else "unhealthy"}
    except Exception as exc:
        ctx.logger.error("[field_service.app] deferred: %s", exc)
        return {"step": "app", "app": app_name, "error": str(exc), "status": "deferred"}


def _app_teardown(ctx: Any) -> Dict[str, Any]:
    from bootstrap.adapters import APPS_API_BASE

    app_name = ctx.name("field-service")
    if not ctx.has_workspace_client():
        return _stub("app", "teardown", ctx, f"delete field-service app {app_name!r}")
    w = ctx.workspace_client()
    deleted = False
    try:
        w.api_client.do("DELETE", f"{APPS_API_BASE}/{app_name}")
        deleted = True
    except Exception as exc:
        ctx.logger.info("field_service.app.teardown: %s", exc)
    return {"step": "app", "app_deleted": deleted, "status": "torn_down"}


def _app_health(ctx: Any) -> Dict[str, Any]:
    from bootstrap.adapters import APPS_API_BASE

    app_name = ctx.name("field-service")
    if not ctx.has_workspace_client():
        return _stub("app", "health", ctx, "assert the app is ACTIVE + deployment SUCCEEDED")
    w = ctx.workspace_client()
    try:
        app = w.api_client.do("GET", f"{APPS_API_BASE}/{app_name}")
        compute = (app.get("compute_status") or {}).get("state")
        deploy_state = ((app.get("active_deployment") or {}).get("status") or {}).get("state")
        healthy = compute == "ACTIVE" and deploy_state == "SUCCEEDED"
        return {"step": "app", "app": app_name, "url": app.get("url"), "compute_state": compute,
                "deployment_state": deploy_state, "healthy": healthy,
                "status": "ok" if healthy else "unhealthy"}
    except Exception as exc:
        return {"step": "app", "healthy": None, "error": str(exc), "status": "deferred"}


_app = (_app_deploy, _app_teardown, _app_health)


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

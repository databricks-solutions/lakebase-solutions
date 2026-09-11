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


# --------------------------------------------------------------------------- #
# Network catalog (pipeline Iceberg + agent model registry).
#
# The pipeline's Iceberg tables and the agent's registered model CANNOT live in
# the MANAGED_ONLINE_CATALOG (`_catalog_name`, which only surfaces Lakebase
# foreign tables). They need a STANDARD UC catalog, self-provisioned here and
# namespaced per deployment. This is deliberately separate from `fs_catalog`.
# --------------------------------------------------------------------------- #
_NETWORK_SCHEMA = "network_data"
_AGENT_SCHEMA = "agents"


def _network_catalog_name(ctx: Any) -> str:
    return ctx.resolved_names.setdefault("fs_network_catalog", f"{ctx.deployment_id}_network")


def _run_statement(ctx: Any, statement: str) -> bool:
    """Run one SQL statement on the module warehouse; True on SUCCEEDED."""

    warehouse_id = ctx.resolved_names.get("fs_warehouse_id") or _get_id(ctx, "fs-warehouse-id")
    if not warehouse_id:
        return False
    resp = ctx.workspace_client().api_client.do(
        "POST", "/api/2.0/sql/statements",
        body={"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s"},
    )
    return (resp.get("status", {}) or {}).get("state") == "SUCCEEDED" if isinstance(resp, dict) else False


def _ensure_network_catalog(ctx: Any) -> str:
    """Idempotently create the standard network catalog + schemas + raw volume.

    Returns the catalog name. Best-effort per statement (a missing CREATE
    privilege defers rather than aborting the whole run).
    """

    cat = _network_catalog_name(ctx)
    stmts = [
        f"CREATE CATALOG IF NOT EXISTS `{cat}`",
        f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{_NETWORK_SCHEMA}`",
        f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{_AGENT_SCHEMA}`",
        f"CREATE VOLUME IF NOT EXISTS `{cat}`.`{_NETWORK_SCHEMA}`.raw_files",
    ]
    for s in stmts:
        try:
            _run_statement(ctx, s)
        except Exception as exc:  # pragma: no cover - live-only
            ctx.logger.info("field_service: ensure-network-catalog stmt failed (%s): %s", s, exc)
    return cat


def _pg_host(ctx: Any) -> str:
    """Resolve the Lakebase primary-endpoint host for this deployment."""

    from bootstrap.adapters import resolve_endpoint_host

    project = ctx.resolved_names.get("lakebase_project", ctx.deployment_id)
    try:
        return resolve_endpoint_host(ctx.workspace_client(), project) or ""
    except Exception:  # pragma: no cover - live-only
        return ""


def _pg_base_params(ctx: Any) -> Dict[str, str]:
    """Common base_params for notebooks that connect to Lakebase.

    The notebook reads pguser/pgpassword from ``secret_scope`` via
    ``dbutils.secrets.get`` (the ash_sampler pattern) — creds never travel as
    plaintext job parameters.
    """

    return {
        "secret_scope": _scope(ctx),
        "pg_host": _pg_host(ctx),
        "pg_database": ctx.params.get("database") or "databricks_postgres",
    }


def _genie_ids_map(ctx: Any) -> Dict[str, str]:
    """Collect the created Genie space ids by config key (for the agent)."""

    out: Dict[str, str] = {}
    for spec in _GENIE_SPACES:
        sid = _get_id(ctx, f"genie-space-{spec['key']}")
        if sid:
            out[spec["key"]] = sid
    return out


def _wait_for_run(ctx: Any, run_id: Any, timeout_s: int = 2400, poll_s: int = 20) -> tuple:
    """Poll jobs/runs/get until terminal; return (life_cycle_state, result_state).

    A step should report success ONLY on result_state == "SUCCESS". Off-Databricks
    the fake returns TERMINATED/SUCCESS immediately (no wait).
    """

    import time

    life, result = "", ""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = ctx.workspace_client().api_client.do(
            "GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id}
        )
        state = (resp.get("state") or {}) if isinstance(resp, dict) else {}
        life = state.get("life_cycle_state", "") or ""
        result = state.get("result_state", "") or ""
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(poll_s)  # pragma: no cover - live-only wait
    return life, result


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


# Performance indexes on work_orders (ported from FSM create_indexes.py).
_WORK_ORDER_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_wo_tech_active ON field_service.work_orders(assigned_technician_id) "
    "WHERE status NOT IN ('completed', 'cancelled')",
    "CREATE INDEX IF NOT EXISTS idx_wo_completed_sla ON field_service.work_orders(region_id, priority, category, sla_met) "
    "WHERE status = 'completed'",
    "ANALYZE field_service.work_orders",
]


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
    # Performance indexes on the hot work_orders paths (ported from create_indexes.py).
    idx = 0
    for stmt in _WORK_ORDER_INDEXES:
        try:
            cur.execute(stmt)
            idx += 1
        except Exception as exc:
            ctx.logger.info("field_service.data.deploy: index/analyze deferred: %s", str(exc)[:120])
    ctx.logger.info(
        "field_service.data.deploy: applied %d statement(s) across %d file(s) "
        "(seed_volume=%s); %d still failing; %d index/analyze stmts.",
        applied,
        len(fs_sql.DATA_SQL_FILES),
        scale_name,
        failing,
        idx,
    )
    return {
        "step": "data",
        "schemas": fs_sql.DATA_SCHEMAS,
        "seed_volume": scale_name,
        "statements_applied": applied,
        "statements_failing": failing,
        "indexes_applied": idx,
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


_SYNC_ROUNDS = 12
_SYNC_DELAY = 15.0


# Schemas the managed online catalog surfaces from Lakebase. network_data is
# the pipeline's Iceberg output (a separate concern), so it is NOT triggered here.
_SYNC_SKIP_SCHEMAS = {"network_data", "governance"}


def _expected_genie_tables(ctx: Any) -> List[tuple]:
    """(schema, table) pairs the Genie spaces reference, minus pipeline schemas."""

    import json
    from pathlib import Path

    pairs = set()
    gdir = Path(__file__).resolve().parent / "assets" / "genie"
    for spec in _GENIE_SPACES:
        try:
            cfg = json.loads((gdir / spec["json"]).read_text(encoding="utf-8"))
        except Exception:  # pragma: no cover
            continue
        for t in (cfg.get("data_sources") or {}).get("tables", []):
            parts = t.get("identifier", "").split(".")
            if len(parts) == 3 and parts[1] not in _SYNC_SKIP_SCHEMAS:
                pairs.add((parts[1], parts[2]))
    return sorted(pairs)


def _synced_deploy(ctx: Any) -> Dict[str, Any]:
    """Trigger + wait for the managed catalog to register the Lakebase foreign tables.

    A MANAGED_ONLINE_CATALOG registers a Lakebase table as a foreign table
    LAZILY -- on first query. So (as FSM's sync step does) we query each expected
    table through the SQL warehouse to trigger registration, and poll until they
    are all visible. Genie + dashboards, which run after this, validate their
    tables against Unity Catalog and fail if the tables aren't registered yet.
    """

    import time

    catalog = _catalog_name(ctx)
    if not ctx.has_workspace_client():
        return _stub("synced", "deploy", ctx, f"trigger + await foreign-table registration in {catalog!r}")
    w = ctx.workspace_client()
    warehouse_id = ctx.resolved_names.get("fs_warehouse_id") or _get_id(ctx, "fs-warehouse-id")
    expected = _expected_genie_tables(ctx)  # (schema, table) pairs
    if not warehouse_id or not expected:
        return {"step": "synced", "catalog": catalog, "status": "partial",
                "note": "no warehouse id or no expected tables resolved"}

    registered: set = set()
    for _round in range(_SYNC_ROUNDS):
        for schema, tbl in expected:
            if (schema, tbl) in registered:
                continue
            fqn = f"`{catalog}`.`{schema}`.`{tbl}`"
            try:
                resp = w.api_client.do(
                    "POST", "/api/2.0/sql/statements",
                    body={"warehouse_id": warehouse_id,
                          "statement": f"SELECT 1 FROM {fqn} LIMIT 1", "wait_timeout": "30s"},
                )
                if (resp.get("status", {}) or {}).get("state") == "SUCCEEDED":
                    registered.add((schema, tbl))
            except Exception:  # not registered yet -- keep polling
                pass
        if len(registered) == len(expected):
            break
        time.sleep(_SYNC_DELAY)  # pragma: no cover - live-only wait

    ctx.logger.info(
        "field_service.synced.deploy: %d/%d foreign tables registered in %r (schemas: %s).",
        len(registered), len(expected), catalog,
        sorted({s for s, _ in expected}),
    )
    status = "deployed" if len(registered) == len(expected) else "partial"
    return {"step": "synced", "catalog": catalog,
            "registered_count": len(registered), "expected_count": len(expected),
            "status": status}


def _synced_teardown(ctx: Any) -> Dict[str, Any]:
    # Foreign tables are removed when the managed catalog is deleted (uc_catalog step).
    return {"step": "synced", "status": "torn_down", "note": "removed with the managed catalog"}


def _synced_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("synced", "health", ctx, "assert foreign tables are queryable")
    return {"step": "synced", "healthy": True, "status": "ok"}


_synced = (_synced_deploy, _synced_teardown, _synced_health)
def _datagen_deploy(ctx: Any) -> Dict[str, Any]:
    """Generate the pipeline's raw source files into the network UC Volume.

    Runs BEFORE the pipeline so Auto Loader has files to ingest (the FSM
    pipeline reads network_nodes.csv / network_performance.csv / outages from
    the volume). Gated with the pipeline (no point generating if it won't run).
    """

    if not ctx.has_workspace_client():
        return _stub("datagen", "deploy", ctx, "generate network raw files into the volume")
    catalog = _ensure_network_catalog(ctx)
    volume_path = f"/Volumes/{catalog}/{_NETWORK_SCHEMA}/raw_files"

    # (1) Network raw files (pure-python generator into the volume).
    net_run = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-datagen",
        "assets/notebooks/generate_network_data",
        base_parameters={"catalog": catalog, "schema": _NETWORK_SCHEMA, "volume_path": volume_path},
    )
    if not net_run:
        return {"step": "datagen", "catalog": catalog, "status": "partial",
                "note": "runs/submit returned no run_id"}
    _put_id(ctx, "fs-datagen-run-id", str(net_run))
    net_life, net_result = _wait_for_run(ctx, net_run)

    # (2) Fleet telemetry backfill: export Lakebase vehicle_telemetry -> volume CSV
    # so the pipeline's vehicle medallion (gold_vehicle_health) has input.
    fleet_run = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-datagen-fleet",
        "assets/notebooks/generate_fleet_telemetry",
        base_parameters={"catalog": catalog, "schema": _NETWORK_SCHEMA, **_pg_base_params(ctx)},
        dependencies=["psycopg2-binary"],
    )
    fleet_result = None
    if fleet_run:
        _put_id(ctx, "fs-datagen-fleet-run-id", str(fleet_run))
        _f_life, fleet_result = _wait_for_run(ctx, fleet_run)

    ctx.logger.info("field_service.datagen.deploy: network=%s fleet=%s (volume=%s).",
                    net_result, fleet_result, volume_path)
    ok = net_result == "SUCCESS" and fleet_result in (None, "SUCCESS")
    return {"step": "datagen", "run_id": net_run, "fleet_run_id": fleet_run,
            "catalog": catalog, "volume_path": volume_path,
            "result_state": net_result, "fleet_result_state": fleet_result,
            "status": "deployed" if ok else ("failed" if net_result != "SUCCESS" else "partial")}


def _datagen_teardown(ctx: Any) -> Dict[str, Any]:
    # Raw files live in the network volume, dropped with the network catalog
    # (pipeline teardown drops that catalog CASCADE).
    return {"step": "datagen", "status": "torn_down",
            "note": "raw files removed with the network catalog"}


def _datagen_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("datagen", "health", ctx, "assert raw files exist in the volume")
    return {"step": "datagen", "healthy": True, "status": "ok",
            "note": "generation job submitted (file-existence assertion deferred to a live check)"}


_datagen = (_datagen_deploy, _datagen_teardown, _datagen_health)


def _pipeline_deploy(ctx: Any) -> Dict[str, Any]:
    """Submit the DLT/Iceberg streaming pipeline as a serverless job run."""

    if not ctx.has_workspace_client():
        return _stub("pipeline", "deploy", ctx, "submit the iceberg streaming pipeline job")
    # Self-provision the STANDARD network catalog (not the managed online catalog).
    catalog = _ensure_network_catalog(ctx)
    run_id = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-pipeline",
        "assets/pipeline/iceberg_streaming_pipeline",
        base_parameters={"catalog": catalog, "schema": _NETWORK_SCHEMA,
                         "volume_path": f"/Volumes/{catalog}/{_NETWORK_SCHEMA}/raw_files"},
        dependencies=["pyiceberg", "pyarrow"],
    )
    if not run_id:
        return {"step": "pipeline", "catalog": catalog, "status": "partial",
                "note": "runs/submit returned no run_id"}
    _put_id(ctx, "fs-pipeline-run-id", str(run_id))
    life, result = _wait_for_run(ctx, run_id)
    ctx.logger.info("field_service.pipeline.deploy: run_id=%s -> %s/%s.", run_id, life, result)
    return {"step": "pipeline", "run_id": run_id, "catalog": catalog,
            "life_cycle_state": life, "result_state": result,
            "status": "deployed" if result == "SUCCESS" else "failed"}


def _pipeline_teardown(ctx: Any) -> Dict[str, Any]:
    # The standard network catalog is NOT the managed online catalog, so it is
    # not removed by the uc_catalog step -- drop it here (pipeline teardown runs
    # last among the network-catalog consumers, in reverse order).
    if not ctx.has_workspace_client():
        return _stub("pipeline", "teardown", ctx, "drop the standard network catalog")
    cat = _network_catalog_name(ctx)
    dropped = False
    try:
        dropped = _run_statement(ctx, f"DROP CATALOG IF EXISTS `{cat}` CASCADE")
    except Exception as exc:  # pragma: no cover - live-only
        ctx.logger.info("field_service.pipeline.teardown: drop catalog %r -> %s", cat, exc)
    return {"step": "pipeline", "network_catalog": cat, "catalog_dropped": dropped,
            "status": "torn_down"}


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
# Genie spaces whose tables live in the STANDARD network catalog (Iceberg
# network_data + the governance views), not the managed online catalog.
_GENIE_NETWORK_KEYS = {"network_health", "sla_workforce"}


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
    managed_catalog = _catalog_name(ctx)
    network_catalog = _network_catalog_name(ctx)
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
        # network_health (Iceberg network_data) + sla_workforce (governance views)
        # live in the STANDARD network catalog; postgres + field_ops use the
        # managed online catalog (Lakebase foreign tables).
        catalog = network_catalog if spec["key"] in _GENIE_NETWORK_KEYS else managed_catalog
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
             USING (COALESCE(current_setting('app.region_id', true), '') = ''
                    OR region_id = current_setting('app.region_id', true)::int);
         END IF;
       END $$;""",
    """CREATE OR REPLACE VIEW field_service.v_customers_masked AS
         SELECT customer_id, region_id,
                regexp_replace(COALESCE(email,''), '(^.).*(@.*$)', '\\1***\\2') AS email,
                left(COALESCE(phone,''), 3) || '-***-****' AS phone
         FROM field_service.customers""",
]


def _governance_uc_view_statements(ctx: Any) -> List[str]:
    """UC governance views (in ``<network>.governance``) the sla_workforce Genie
    space reads. They reference the field_service foreign tables in the managed
    online catalog (cross-catalog), so ``synced`` must have registered them and
    this step must run BEFORE ``genie`` (which validates tables at creation).
    """

    net = _network_catalog_name(ctx)
    src = f"`{_catalog_name(ctx)}`.`field_service`"  # managed online catalog foreign tables
    gov = f"`{net}`.`governance`"
    return [
        f"CREATE CATALOG IF NOT EXISTS `{net}`",
        f"CREATE SCHEMA IF NOT EXISTS `{net}`.`governance`",
        f"""CREATE OR REPLACE VIEW {gov}.`v_regional_work_orders` AS
            SELECT wo.work_order_id, wo.status, wo.priority, wo.category, wo.subcategory,
                   wo.reported_issue, c.service_type, wo.sla_due_at, wo.created_at, wo.resolved_at,
                   c.first_name || ' ' || c.last_name AS customer_name, c.city,
                   c.state_province AS state, sr.region_name, sr.region_code,
                   t.first_name || ' ' || t.last_name AS technician_name
            FROM {src}.`work_orders` wo
            JOIN {src}.`customers` c ON wo.customer_id = c.customer_id
            JOIN {src}.`service_regions` sr ON wo.region_id = sr.region_id
            LEFT JOIN {src}.`technicians` t ON wo.assigned_technician_id = t.technician_id""",
        f"""CREATE OR REPLACE VIEW {gov}.`v_customers_masked` AS
            SELECT c.customer_id, LEFT(c.first_name, 1) || '***' AS first_name,
                   LEFT(c.last_name, 1) || '***' AS last_name,
                   REGEXP_REPLACE(c.email, '(.).*@', '\\1***@') AS email,
                   'XXX-XXX-' || RIGHT(c.phone, 4) AS phone,
                   c.city, c.state_province, c.account_status, c.service_type,
                   c.customer_tier, sr.region_name, c.created_at
            FROM {src}.`customers` c
            JOIN {src}.`service_regions` sr ON c.region_id = sr.region_id""",
        f"""CREATE OR REPLACE VIEW {gov}.`v_technician_performance` AS
            SELECT t.employee_id, t.certification_level, sr.region_name, t.status,
                   COUNT(CASE WHEN wo.status = 'completed' THEN 1 END) AS total_completed,
                   COUNT(CASE WHEN wo.status IN ('assigned','en_route','in_progress') THEN 1 END) AS active_orders,
                   AVG(CASE WHEN wo.resolved_at IS NOT NULL
                       THEN TIMESTAMPDIFF(HOUR, wo.created_at, wo.resolved_at) END) AS avg_completion_hours,
                   COUNT(CASE WHEN wo.resolved_at > wo.sla_due_at THEN 1 END) AS sla_breaches
            FROM {src}.`technicians` t
            JOIN {src}.`service_regions` sr ON t.region_id = sr.region_id
            LEFT JOIN {src}.`work_orders` wo ON t.technician_id = wo.assigned_technician_id
            WHERE t.is_active = TRUE
            GROUP BY t.employee_id, t.certification_level, sr.region_name, t.status""",
        f"""CREATE OR REPLACE VIEW {gov}.`v_sla_compliance` AS
            SELECT sr.region_name, wo.category, wo.priority,
                   sp.response_hours AS sla_response_hours, sp.resolution_hours AS sla_resolution_hours,
                   COUNT(*) AS total_orders,
                   COUNT(CASE WHEN wo.status = 'completed' THEN 1 END) AS completed,
                   COUNT(CASE WHEN wo.resolved_at IS NOT NULL AND wo.resolved_at <= wo.sla_due_at THEN 1 END) AS within_sla,
                   COUNT(CASE WHEN wo.resolved_at IS NOT NULL AND wo.resolved_at > wo.sla_due_at THEN 1 END) AS sla_breached,
                   ROUND(COUNT(CASE WHEN wo.resolved_at IS NOT NULL AND wo.resolved_at <= wo.sla_due_at THEN 1 END) * 100.0 /
                         NULLIF(COUNT(CASE WHEN wo.resolved_at IS NOT NULL THEN 1 END), 0), 1) AS sla_compliance_pct
            FROM {src}.`work_orders` wo
            JOIN {src}.`customers` c ON wo.customer_id = c.customer_id
            JOIN {src}.`service_regions` sr ON wo.region_id = sr.region_id
            LEFT JOIN {src}.`sla_policies` sp ON wo.sla_id = sp.sla_id
            GROUP BY sr.region_name, wo.category, wo.priority, sp.response_hours, sp.resolution_hours""",
    ]


def _governance_deploy(ctx: Any) -> Dict[str, Any]:
    """Apply PG RLS + PII masking, AND create the UC governance views the
    sla_workforce Genie space reads (best-effort)."""

    database = ctx.params.get("database") or "databricks_postgres"
    if not ctx.is_live():
        return _stub("governance", "deploy", ctx, "enable RLS + masking (PG) + UC governance views")
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
            ctx.logger.info("field_service.governance: deferred PG statement: %s", str(exc)[:120])

    # UC governance views (for the sla_workforce Genie space) via the warehouse.
    uc_stmts = _governance_uc_view_statements(ctx)
    uc_applied = 0
    for stmt in uc_stmts:
        try:
            if _run_statement(ctx, stmt):
                uc_applied += 1
        except Exception as exc:
            ctx.logger.info("field_service.governance: deferred UC view: %s", str(exc)[:120])

    ok = applied == len(_GOVERNANCE_SQL) and uc_applied == len(uc_stmts)
    return {"step": "governance", "statements_applied": applied,
            "uc_views_applied": uc_applied, "uc_views_total": len(uc_stmts),
            "status": "deployed" if ok else "partial"}


def _governance_teardown(ctx: Any) -> Dict[str, Any]:
    # PG policies + masked view live in field_service (dropped by the data step);
    # the UC governance views live in the network catalog (dropped CASCADE by the
    # pipeline teardown). Nothing extra to remove here.
    return {"step": "governance", "status": "torn_down",
            "note": "PG objects removed with field_service; UC views with the network catalog"}


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
    catalog = _ensure_network_catalog(ctx)
    run_id = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-ml",
        "assets/notebooks/predictive_maintenance",
        base_parameters={"catalog": catalog, "schema": _NETWORK_SCHEMA},
        dependencies=["lightgbm", "scikit-learn", "mlflow"],
    )
    if not run_id:
        return {"step": "ml", "catalog": catalog, "status": "partial",
                "note": "runs/submit returned no run_id"}
    _put_id(ctx, "fs-ml-run-id", str(run_id))
    life, result = _wait_for_run(ctx, run_id)
    ctx.logger.info("field_service.ml.deploy: train run_id=%s -> %s/%s.", run_id, life, result)

    # After training succeeds, batch-score assets + write predictive work orders
    # back to Lakebase (score_and_create_work_orders).
    score_result = None
    if result == "SUCCESS":
        score_run = _submit_notebook_job(
            ctx,
            f"{ctx.deployment_id}-fs-ml-score",
            "assets/notebooks/score_and_create_work_orders",
            base_parameters={"catalog": catalog, "schema": _NETWORK_SCHEMA, **_pg_base_params(ctx)},
            dependencies=["mlflow[databricks]", "lightgbm", "scikit-learn",
                          "psycopg2-binary", "databricks-sdk>=0.87.0"],
        )
        if score_run:
            _put_id(ctx, "fs-ml-score-run-id", str(score_run))
            _s_life, score_result = _wait_for_run(ctx, score_run)
            ctx.logger.info("field_service.ml.deploy: score run_id=%s -> %s.", score_run, score_result)

    if result != "SUCCESS":
        status = "failed"
    elif score_result in (None, "SUCCESS"):
        status = "deployed"
    else:
        status = "partial"
    return {"step": "ml", "run_id": run_id, "catalog": catalog,
            "life_cycle_state": life, "result_state": result,
            "scoring_result_state": score_result,
            "status": status}


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


def _ml_fleet_deploy(ctx: Any) -> Dict[str, Any]:
    """Fleet predictive maintenance: train (fleet_maintenance_model @production)
    then batch-score + write predictive fleet work orders to Lakebase."""

    if not ctx.has_workspace_client():
        return _stub("ml_fleet", "deploy", ctx, "train + score the fleet maintenance model")
    catalog = _ensure_network_catalog(ctx)
    train_run = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-ml-fleet",
        "assets/notebooks/fleet_predictive_maintenance",
        base_parameters={"catalog": catalog, "schema": _NETWORK_SCHEMA, **_pg_base_params(ctx)},
        dependencies=["mlflow[databricks]", "lightgbm", "scikit-learn",
                      "psycopg2-binary", "databricks-sdk>=0.87.0"],
    )
    if not train_run:
        return {"step": "ml_fleet", "catalog": catalog, "status": "partial",
                "note": "runs/submit returned no run_id"}
    _put_id(ctx, "fs-ml-fleet-run-id", str(train_run))
    _t_life, train_result = _wait_for_run(ctx, train_run)

    score_result = None
    if train_result == "SUCCESS":
        score_run = _submit_notebook_job(
            ctx,
            f"{ctx.deployment_id}-fs-ml-fleet-score",
            "assets/notebooks/score_fleet_work_orders",
            base_parameters={"catalog": catalog, "schema": _NETWORK_SCHEMA, **_pg_base_params(ctx)},
            dependencies=["mlflow[databricks]", "lightgbm", "scikit-learn",
                          "psycopg2-binary", "databricks-sdk>=0.87.0"],
        )
        if score_run:
            _put_id(ctx, "fs-ml-fleet-score-run-id", str(score_run))
            _s_life, score_result = _wait_for_run(ctx, score_run)
    ctx.logger.info("field_service.ml_fleet.deploy: train=%s score=%s.", train_result, score_result)

    if train_result != "SUCCESS":
        status = "failed"
    elif score_result in (None, "SUCCESS"):
        status = "deployed"
    else:
        status = "partial"
    return {"step": "ml_fleet", "run_id": train_run, "catalog": catalog,
            "result_state": train_result, "scoring_result_state": score_result, "status": status}


def _ml_fleet_teardown(ctx: Any) -> Dict[str, Any]:
    return {"step": "ml_fleet", "status": "torn_down",
            "note": "fleet UC model removed with the network catalog"}


def _ml_fleet_health(ctx: Any) -> Dict[str, Any]:
    if not ctx.has_workspace_client():
        return _stub("ml_fleet", "health", ctx, "assert the fleet maintenance model is registered")
    return {"step": "ml_fleet", "healthy": True, "status": "ok",
            "note": "fleet training submitted (registration assertion deferred to a live check)"}


_ml_fleet = (_ml_fleet_deploy, _ml_fleet_teardown, _ml_fleet_health)


def _make_job_step(name: str, notebook: str, deps: List[str], needs_catalog: bool = True):
    """Build a simple (deploy, teardown, health) tuple for a single-notebook step.

    Used for the dispatch / DTC / fuel steps: each submits one notebook (with the
    common PG base params), polls the run, and reports the real result.
    """

    def d(ctx: Any) -> Dict[str, Any]:
        if not ctx.has_workspace_client():
            return _stub(name, "deploy", ctx, f"submit {notebook}")
        params = dict(_pg_base_params(ctx))
        if needs_catalog:
            params["catalog"] = _ensure_network_catalog(ctx)
            params["schema"] = _NETWORK_SCHEMA
        run_id = _submit_notebook_job(
            ctx, f"{ctx.deployment_id}-fs-{name}", notebook,
            base_parameters=params, dependencies=deps,
        )
        if not run_id:
            return {"step": name, "status": "partial", "note": "runs/submit returned no run_id"}
        _put_id(ctx, f"fs-{name}-run-id", str(run_id))
        life, result = _wait_for_run(ctx, run_id)
        ctx.logger.info("field_service.%s.deploy: run_id=%s -> %s/%s.", name, run_id, life, result)
        return {"step": name, "run_id": run_id, "result_state": result,
                "status": "deployed" if result == "SUCCESS" else "failed"}

    def t(ctx: Any) -> Dict[str, Any]:
        return {"step": name, "status": "torn_down",
                "note": "outputs removed with their catalog/schema"}

    def h(ctx: Any) -> Dict[str, Any]:
        if not ctx.has_workspace_client():
            return _stub(name, "health", ctx, f"assert {name} ran")
        return {"step": name, "healthy": True, "status": "ok",
                "note": "job submitted (deep assertion deferred to a live check)"}

    return (d, t, h)


_ML_DEPS = ["mlflow[databricks]", "lightgbm", "scikit-learn", "psycopg2-binary", "databricks-sdk>=0.87.0"]
_PG_DEPS = ["psycopg2-binary", "databricks-sdk>=0.87.0"]

# Dispatch scoring model (LightGBM), DTC interpretation (ai_query), fuel/external
# ingest (Auto Loader -> Iceberg). Ported 1:1 from the FSM notebooks.
_dispatch = _make_job_step("dispatch", "assets/notebooks/train_dispatch_model", _ML_DEPS)
_dtc = _make_job_step("dtc", "assets/notebooks/interpret_dtc_codes", _PG_DEPS, needs_catalog=False)
_fuel = _make_job_step("fuel", "assets/notebooks/ingest_fuel_external", _PG_DEPS)


def _agent_deploy(ctx: Any) -> Dict[str, Any]:
    """Submit the agent build/serve job (registers + creates a serving endpoint)."""

    if not ctx.has_workspace_client():
        return _stub("agent", "deploy", ctx, "build + serve the multi-Genie supervisor agent")
    import json as _json

    name = _serving_endpoint_name(ctx, "agent")
    catalog = _ensure_network_catalog(ctx)
    genie_ids = _genie_ids_map(ctx)
    run_id = _submit_notebook_job(
        ctx,
        f"{ctx.deployment_id}-fs-agent",
        "assets/notebooks/deploy_agent_endpoint",
        base_parameters={
            "endpoint_name": name,
            "catalog": catalog,
            "agent_schema": _AGENT_SCHEMA,
            "genie_space_ids": _json.dumps(genie_ids),
        },
        # Pin the langgraph family — unpinned deps make pip's serverless resolver
        # give up (ResolutionTooDeep). Mirrors FSM's working set.
        dependencies=["databricks-langchain", "databricks-agents",
                      "langgraph>=1.0.13", "langgraph-prebuilt>=1.0.13",
                      "langgraph-checkpoint", "langgraph-supervisor",
                      "mlflow[databricks]", "databricks-sdk>=0.87.0"],
    )
    _put_id(ctx, "fs-agent-endpoint", name)
    if not run_id:
        return {"step": "agent", "endpoint": name, "status": "partial",
                "note": "runs/submit returned no run_id"}
    _put_id(ctx, "fs-agent-run-id", str(run_id))
    life, result = _wait_for_run(ctx, run_id)
    # The endpoint provisions asynchronously; a SUCCESS run means it was created.
    endpoint_ok = result == "SUCCESS"
    ctx.logger.info("field_service.agent.deploy: run_id=%s -> %s/%s (genie spaces=%d).",
                    run_id, life, result, len(genie_ids))
    return {"step": "agent", "endpoint": name, "run_id": run_id, "catalog": catalog,
            "genie_spaces": len(genie_ids),
            "life_cycle_state": life, "result_state": result,
            "status": "deployed" if endpoint_ok else "failed"}


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
        # Idempotent: reuse an existing job with this exact name (no duplicates).
        try:
            found = w.api_client.do("GET", "/api/2.1/jobs/list", query={"name": job_name})
            existing = found.get("jobs", []) if isinstance(found, dict) else []
        except Exception:  # pragma: no cover
            existing = []
        if existing:
            jid = existing[0].get("job_id")
            if jid is not None:
                _put_id(ctx, f"ops-job-{suffix}", str(jid))
            created.append(job_name)
            continue
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
    Step("datagen", *_datagen, gate_param="include_pipeline"),
    Step("pipeline", *_pipeline, gate_param="include_pipeline"),
    Step("governance", *_governance),
    Step("genie", *_genie),
    Step("dashboards", *_dashboards),
    Step("ml", *_ml, gate_param="include_ml"),
    Step("ml_fleet", *_ml_fleet, gate_param="include_ml"),
    Step("dispatch", *_dispatch, gate_param="include_ml"),
    Step("dtc", *_dtc, gate_param="include_ml"),
    Step("fuel", *_fuel, gate_param="include_pipeline"),
    Step("agent", *_agent, gate_param="include_agent"),
    Step("ops", *_ops, gate_param="include_ops_jobs"),
    Step("app", *_app),
]


def steps_for(ctx: Any) -> List[Step]:
    """Return the steps enabled for this run (gated steps whose param is off are skipped)."""

    return [s for s in ORDERED_STEPS if s.gate_param is None or _truthy(ctx, s.gate_param)]

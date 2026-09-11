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

StepFn = Callable[[Any], Dict[str, Any]]


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
_uc_catalog = _mk(
    "uc_catalog",
    "create MANAGED_ONLINE_CATALOG linked to the Lakebase project",
    "delete the managed online catalog",
    "assert the catalog exists and foreign tables are registered",
)
_warehouse = _mk(
    "warehouse",
    "create a serverless SQL warehouse for Genie + dashboards",
    "delete the SQL warehouse",
    "assert the warehouse is running/available",
)
_features = _mk(
    "features",
    "apply SLA engine, events, triggers, materialized views",
    "drop the Lakebase feature objects",
    "assert the SLA views/materialized views exist",
)
_synced = _mk(
    "synced",
    "wait for the managed catalog to register synced foreign tables",
    "(no-op: removed with the catalog)",
    "assert synced tables are queryable",
)
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

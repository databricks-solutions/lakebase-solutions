# Modules

Optional, opt-in add-ons on top of the always-on `core/`. Select them at deploy
time with the `modules` widget (comma-separated), e.g. `modules = field_service`.
Each module lives in `modules/<name>/`, owns its **own** app + resources, is
namespaced by `deployment_id`, and is fully torn down in reverse on
`mode = teardown`.

> **Keep this inventory current:** when you add a module, add a row here (and a
> short section below). The top-level README links here instead of listing modules,
> so this file is the single source of truth for "what modules exist."

| Module | Personas | Summary | Maturity notes |
|---|---|---|---|
| [`field_service`](field_service/) | DBA, ETL, App Dev, AI Eng, Tech Leadership | Full Telco field-service solution — the flagship, exercises the whole platform end-to-end. | All GA except Lakebase→UC synced online tables (**Public Preview**) |
| [`_canary`](_canary/) | — | Minimal reference / authoring template. Copy it to start a new module. | GA |

## `field_service`
A complete field-service management solution (work orders, dispatch, technicians,
fleet telemetry, SLA tracking). Self-provisioning: it stands up every dependency
it needs and removes them on teardown. Deploys a 19-step pipeline covering:
Lakebase schema + seed, a managed online catalog, a serverless SQL warehouse,
SLA features, a Lakeflow/Iceberg medallion pipeline, 4 Genie spaces, 2 Lakeview
dashboards, UC governance (RLS + masked/analytics views), predictive-maintenance
+ fleet + dispatch ML, DTC `ai_query` enrichment, fuel/external ingest, Lakehouse
Monitoring, a multi-Genie supervisor agent, scheduled ops jobs, and a live
Databricks App. Gated toggles: `include_pipeline` / `include_ml` / `include_agent`
/ `include_ops_jobs` (all default on). Full detail:
[`field_service/AGENTS.md`](field_service/AGENTS.md) and
[`field_service/README.md`](field_service/README.md).

## `_canary`
The reference module and authoring template — intentionally tiny (creates one
schema + a heartbeat table) but exercises the entire module contract
(`module.yaml` + `deploy`/`teardown`/`health`). A green canary proves discovery →
dependency ordering → deploy → health → teardown with zero deploy-notebook edits.
See [`_canary/README.md`](_canary/README.md).

## Add a new module
Drop a folder under `modules/` with a `module.yaml`; discovery + the dependency
DAG pick it up automatically. See [`../docs/MODULE_AUTHORING.md`](../docs/MODULE_AUTHORING.md)
and copy `_canary/`. Then add it to the table above.

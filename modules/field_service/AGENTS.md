# AGENTS.md — field_service module

Scoped guidance for anyone (agent or human) working in this module. Read the
repo-root [`AGENTS.md`](../../AGENTS.md) first for the platform-wide rules.

## What it is
A full Telco **field-service** solution reworked from the Lakebase FSM demo — the
first real, fully self-provisioning module. `deploy.py` runs an **ordered internal
sub-pipeline** (`fs_steps.ORDERED_STEPS`, 19 steps); each step is idempotent,
best-effort (defers rather than aborting), and gated where noted. `teardown.py`
runs it in reverse.

Deploy order: `data → uc_catalog → warehouse → features → synced → datagen ✱ →
pipeline ✱ → governance → genie → dashboards → ml ✱ → ml_fleet ✱ → dispatch ✱ →
dtc ✱ → fuel ✱ → monitoring ✱ → agent ✱ → ops ✱ → app`.
**✱ gated** by `include_pipeline` / `include_ml` / `include_agent` /
`include_ops_jobs` (all default on).

## Two catalogs — do not conflate them
- **`<id>_field_service`** — a **MANAGED_ONLINE_CATALOG** surfacing Lakebase tables
  as **foreign tables** (registered LAZILY on first query). You **cannot** create
  arbitrary schemas / views / Iceberg tables / registered models here.
- **`<id>_network`** — a **standard UC catalog** the module self-provisions
  (`_ensure_network_catalog`) for everything the managed catalog can't hold:
  the pipeline's **Iceberg** medallion (`network_data`), the **`agents`** schema
  (registered ML + agent models), and the **`governance`** schema (the
  `sla_workforce` UC views). Dropped CASCADE on teardown.

Genie spaces are pointed per-space: `postgres` + `field_ops` → managed catalog;
`network_health` + `sla_workforce` → `<id>_network`.

## Notebook conventions (all vendored notebooks follow these)
- **Widget-first** — read `catalog` / `schema` / PG params from **job base params
  (`dbutils.widgets`)**, then fall back. **No `deployment/config.yaml` or
  `config.py`** — that FSM lineage was removed; don't reintroduce it.
- **PG creds come from the secret scope, per-app** — this app has its **own**
  native-password role `<id>_fs_app` and its **own** keys
  `field_service-pguser`/`field_service-pgpassword` (NOT the shared `pguser`/`pgpassword`);
  the role + keys are provisioned in the **`data`** step (`_fs_app_role`), first, so
  every later job/notebook/app has creds. Read them with
  `dbutils.secrets.get(scope, "field_service-pguser" | "field_service-pgpassword")`
  (the `ash_sampler` pattern). Never pass creds as plaintext job params.
  `fs_steps._pg_base_params(ctx)` supplies `secret_scope`/`pg_host`/`pg_database` (the
  shared **connection-info** keys `pghost`/`pgdatabase`/`pgschema` are not credentials).
- Steps that submit notebook jobs **poll the run and report the real result**
  (`_wait_for_run` → status `deployed` only on `result_state == SUCCESS`), never
  "deployed" on submit.

## Ordering rules (why the sequence matters)
- `synced` runs before `genie`/`dashboards` — it triggers the lazy foreign-table
  registration their table-validation depends on.
- `governance` runs **before `genie`** — Genie validates tables at creation, so the
  `sla_workforce` governance views must already exist.
- `datagen` runs **before `pipeline`** — it writes the raw source files the pipeline
  ingests.

## Live landmines already fixed — don't reintroduce
- **Agent** (`deploy_agent_endpoint.py`): pin the langgraph family
  (`langgraph>=1.0.13`, `langgraph-prebuilt>=1.0.13`, `langgraph-checkpoint`,
  `langgraph-supervisor`, `mlflow[databricks]`) or pip's serverless resolver gives
  up (`ResolutionTooDeep`). It must **log + register** the ResponsesAgent (do not
  assume a pre-registered model). `input_example` uses the ResponsesAgent schema:
  `{"input": [...]}`, not `{"messages": [...]}`.
- **Pipeline** (`iceberg_streaming_pipeline.py`): IoT telemetry is generated LIVE by
  the app simulator — on a fresh deploy there are no `iot_telemetry_*.csv`, so the
  IoT bronze ingest **skips cleanly** (a precheck raises a "Path does not exist"
  the graceful branch handles). Don't hard-read that glob.
- **ML notebooks**: `mlflow.sklearn.log_model(...)` for LightGBM must pass
  `skops_trusted_types=[...]` or register fails on "untrusted types". Models are
  registered with a **`@production`** alias; scoring loads `models:/<name>@production`.
- **Monitoring** (`setup_lakehouse_monitoring.py`): best-effort — only monitors
  tables that exist AND have rows, uses the classification **enum** (not a string),
  and always exits success so it never blocks the deploy. `MonitorInferenceLog`
  requires `model_id_col` in the current SDK.

## Feature maturity
Everything is GA except **Lakebase → Unity Catalog synced online tables**
(Public Preview) — declared in `module.yaml` `features` and surfaced in the matrix.

## Layout
```
module.yaml   manifest (depends_on core[lakebase,security,data_api], params, features, provides)
deploy.py     runs the sub-pipeline forward   teardown.py  reverse   health.py  aggregate
fs_steps.py   ORDERED_STEPS registry + step implementations  (hot path)
fs_sql.py     SQL splitter / scale / apply helpers (seeding)
assets/       vendored source: sql/ genie/ dashboards/ pipeline/ notebooks/ app/
```
Offline tests: `tests/test_field_service_module.py` (+ `tests/_fakes.py`).

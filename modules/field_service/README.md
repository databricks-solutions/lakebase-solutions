# `field_service` — full field-service workshop solution

The first **real** module: a complete Telco field-service solution reworked from
the Lakebase FSM demo. It exercises the module contract end-to-end with real
assets and covers **all five personas** (DBA, ETL, App Dev, AI Engineer,
Technical Leadership).

It depends only on core (`lakebase`, `security`, `data_api`) and is **fully
self-provisioning** — it stands up every dependency it needs (UC catalog, SQL
warehouse, data, pipeline, Genie, dashboards, ML, agent, app) and tears it all
down. Nothing pre-existing is assumed.

## What it provisions (internal step pipeline)

The module runs an ordered internal sub-pipeline (`fs_steps.ORDERED_STEPS`).
Each step is idempotent, best-effort (defers rather than aborting), and gated
where noted; teardown runs the pipeline in reverse.

Deploy runs the steps top-to-bottom (teardown runs them bottom-to-top):

```mermaid
flowchart TB
    PRE["prereq · core: lakebase → security → data_api"]:::pre --> s1
    s1["1 · data — schemas + ~30 tables + seed + indexes"]:::d --> s2["2 · uc_catalog — managed online catalog"]:::d
    s2 --> s3["3 · warehouse — serverless SQL"]:::d
    s3 --> s4["4 · features — SLA engine + materialized views"]:::d
    s4 --> s5["5 · synced — trigger UC foreign-table registration"]:::d
    s5 --> s6["6 · datagen ✱ — network + fleet raw files → volume"]:::d
    s6 --> s7["7 · pipeline ✱ — Lakeflow / Iceberg medallion"]:::g
    s7 --> s8["8 · governance — PG RLS/masking + 4 UC views"]:::a
    s8 --> s9["9 · genie — 4 Genie spaces"]:::a
    s9 --> s10["10 · dashboards — 2 Lakeview"]:::a
    s10 --> s11["11 · ml ✱ — network PdM (train → score → work orders)"]:::g
    s11 --> s12["12 · ml_fleet ✱ — fleet PdM (train → score → work orders)"]:::g
    s12 --> s13["13 · dispatch ✱ — dispatch scoring model"]:::g
    s13 --> s14["14 · dtc ✱ — DTC interpretation (ai_query)"]:::g
    s14 --> s15["15 · fuel ✱ — fuel/external ingest (Auto Loader → Iceberg)"]:::g
    s15 --> s16["16 · monitoring ✱ — Lakehouse Monitoring"]:::g
    s16 --> s17["17 · agent ✱ — multi-Genie supervisor (log → register → serve)"]:::g
    s17 --> s18["18 · ops ✱ — ASH / cleanup / rotation jobs"]:::g
    s18 --> s19["19 · app — field-service Databricks App"]:::app

    classDef pre fill:#455a64,color:#fff,stroke:#263238;
    classDef d fill:#1168bd,color:#fff,stroke:#0b3d91;
    classDef a fill:#6a1b9a,color:#fff,stroke:#4a148c;
    classDef g fill:#2e7d32,color:#fff,stroke:#1b5e20;
    classDef app fill:#b8860b,color:#fff,stroke:#8a6508;
```

**✱ gated** by `include_pipeline` (datagen, pipeline, fuel, monitoring),
`include_ml` (ml, ml_fleet, dispatch, dtc), `include_agent`, `include_ops_jobs`
— all default on. `governance` runs **before** `genie` so the `sla_workforce`
governance views exist when Genie validates its tables; `genie` + `dashboards`
run **after** `synced`/`pipeline` because the managed online catalog registers
Lakebase tables as foreign tables lazily (on first query) and the Iceberg tables
must exist. The real-time data generator (`run_data_generator`) is an **on-demand**
notebook (duration-bounded), not part of the deploy pipeline.

## Feature maturity

Declared in `module.yaml` (`features`) and surfaced in the feature matrix:

| Feature | Maturity |
|---|---|
| Genie spaces, Lakeview dashboards, Lakeflow pipelines, Managed Iceberg, Model Serving, Agent Framework, Databricks Apps, AI Functions (`ai_query`), Lakehouse Monitoring | GA |
| Lakebase → Unity Catalog synced online tables | **Public Preview** |

## Parameters

`warehouse_size`, `seed_volume` (`demo`/`scale`), and the gate toggles
`include_pipeline` / `include_ml` / `include_agent` / `include_ops_jobs`.

## Layout

```
module.yaml     manifest (depends_on, params, features, provides)
deploy.py       runs the sub-pipeline forward (best-effort per step)
teardown.py     runs it in reverse
health.py       aggregates per-step health
fs_steps.py     the ORDERED_STEPS registry + step implementations
fs_sql.py       SQL splitter/scale + apply helpers (seeding)
assets/         vendored source: sql/ genie/ dashboards/ pipeline/ notebooks/ app/
```

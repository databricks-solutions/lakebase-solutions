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
    s1["1 · data — schemas + ~30 tables + seed"]:::d --> s2["2 · uc_catalog — managed online catalog"]:::d
    s2 --> s3["3 · warehouse — serverless SQL"]:::d
    s3 --> s4["4 · features — SLA engine + materialized views"]:::d
    s4 --> s5["5 · synced — trigger UC foreign-table registration"]:::d
    s5 --> s6["6 · pipeline ✱ — DLT / Iceberg job"]:::g
    s6 --> s7["7 · genie — 4 Genie spaces"]:::a
    s7 --> s8["8 · dashboards — 2 Lakeview"]:::a
    s8 --> s9["9 · governance — RLS + masked views"]:::a
    s9 --> s10["10 · ml ✱ — predictive maintenance (train + register)"]:::g
    s10 --> s11["11 · agent ✱ — multi-Genie supervisor (serve)"]:::g
    s11 --> s12["12 · ops ✱ — ASH / cleanup / rotation jobs"]:::g
    s12 --> s13["13 · app — field-service Databricks App"]:::app

    classDef pre fill:#455a64,color:#fff,stroke:#263238;
    classDef d fill:#1168bd,color:#fff,stroke:#0b3d91;
    classDef a fill:#6a1b9a,color:#fff,stroke:#4a148c;
    classDef g fill:#2e7d32,color:#fff,stroke:#1b5e20;
    classDef app fill:#b8860b,color:#fff,stroke:#8a6508;
```

**✱ gated** by `include_pipeline` / `include_ml` / `include_agent` /
`include_ops_jobs` (all default on). `genie` + `dashboards` run **after**
`synced` because the managed online catalog registers Lakebase tables as foreign
tables lazily (on first query); `synced` triggers + waits for that so Genie's
table validation passes.

## Feature maturity

Declared in `module.yaml` (`features`) and surfaced in the feature matrix:

| Feature | Maturity |
|---|---|
| Genie spaces, Lakeview dashboards, DLT pipelines, Managed Iceberg, Model Serving, Agent Framework, Databricks Apps | GA |
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

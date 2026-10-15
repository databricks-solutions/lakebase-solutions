# Lakebase performance diagnostic — internal spec (dev-facing)

The data contract that lets the collector, the ongoing-sampler job, and the report card be built
and evolved independently. The user-facing framing is in `README.md`; this file is for contributors.

Goal: a First-Responder-style diagnostic for Lakebase — collect metrics, optionally sample over a
window, run the benchmarks, then emit a **prioritized report card** whose findings each link to a
section of the performance configuration runbook.

## Non-negotiables (apply to every tool here)

- **Notebook-source, widget-driven, immutable.** `# Databricks notebook source`; every input is a
  widget; nothing hardcoded. The same file runs unchanged against any project/workspace.
- **Auto-auth, auto-discover.** Resolve project/endpoint and mint the OAuth token from the workspace
  identity exactly as `lakebase_load_harness.py` does (copy its *Connect to Lakebase* cell verbatim —
  `WorkspaceClient`, `w.postgres.get_endpoint` / `list_endpoints`, `generate_database_credential`,
  `sslmode=require`). No hardcoded host/token/project/secret.
- **No customer specifics.** Operate on `pg_stat_*` / metric *shapes*, never named schemas, tables,
  IDs, or values. Generic neutral defaults only.
- **Extensible by data, not by edits.** New metric = new row (tall format). New check = new rule file.
  Never require editing engine code to add a metric or a check.
- **Read-only by default.** Collectors and the analyzer must not write to the target database. Their
  only writes are to the UC output tables below. psycopg3; `databricks-sdk>=0.81`.

## Output tables (UC Delta; names are widgets with these defaults)

All tables carry `run_id STRING` (uuid per invocation) + `run_ts TIMESTAMP` + `project_id STRING`
+ `endpoint STRING` so a run is self-identifying and multiple runs append cleanly.

### `lakebase_perf_metrics`  (P1 — point-in-time inputs + baseline, TALL format)
Fills runbook §2 (workload inputs) + §3 (baseline). One row per metric.
| column | type | notes |
|---|---|---|
| run_id, run_ts, project_id, endpoint | — | run identity |
| section | STRING | runbook section the metric informs, e.g. `2.traffic`, `3.baseline`, `4.1.compute` |
| metric_name | STRING | e.g. `connections_active`, `cache_hit_pct`, `db_size_bytes` |
| metric_value | DOUBLE | numeric value (NULL if textual) |
| metric_text | STRING | textual value (NULL if numeric) |
| unit | STRING | e.g. `ms`, `pct`, `bytes`, `conns`, `qps` |
| source | STRING | provenance, e.g. `pg_stat_database`, `pg_stat_activity`, `pg_class` |

### `lakebase_perf_tables`  (P1 — per-table size / working set)
| run_id, run_ts, project_id, endpoint | schema_name | table_name | total_bytes | heap_bytes | index_bytes | heap_blks_hit | heap_blks_read |
(sourced from `pg_total_relation_size`/`pg_relation_size`/`pg_indexes_size` + `pg_statio_user_tables`).

### `lakebase_perf_queries`  (P1 — top-N statements)
| run_id, run_ts, project_id, endpoint | queryid | calls | mean_exec_ms | total_exec_ms | rows | shared_blks_hit | shared_blks_read | query_sample |
(sourced from `pg_stat_statements`; `query_sample` truncated, redact literals). Degrade gracefully if
the extension is absent (write a single `metric_text='pg_stat_statements not installed'` note row).

### `lakebase_perf_samples`  (P2 — ongoing time-series)
Same tall shape as `lakebase_perf_metrics` but one row set **per sample tick**:
| sample_id, sample_ts, project_id, endpoint | metric_name | metric_value | source |
Raw counters are stored as-is; deltas/rates are computed at analysis time (consecutive ticks).
Sampled counters (minimum): `pg_stat_database` (xact_commit/rollback, blks_hit/read, deadlocks,
tup_*), `pg_stat_activity` state counts, and (if present) `pg_stat_statements` top movers.

### `lakebase_perf_findings`  (P3 — the report card)
| run_id, run_ts, project_id, endpoint | priority INT (1=highest..5) | category | finding | details | runbook_section | fix | rule_id |

## Rule format (P3 — declarative, one entry per check)

Rules live in `tools/performance/rules/*.yaml` (a rule pack). The engine loads every `.yaml`, each a
list of rule objects. Adding a check = adding a rule, never editing the engine.

```yaml
- id: cache_hit_low                      # stable slug, unique
  title: "Cache hit rate below target under load"
  priority: 2                            # 1=critical .. 5=informational (sp_Blitz-style)
  category: "Memory / working set"
  source_table: lakebase_perf_metrics    # which output table to read (metrics|tables|queries|samples)
  detect: "metric_name == 'cache_hit_pct' and metric_value < threshold"   # safe expr over row fields
  threshold: 99.0
  runbook_section: "4. Primary configuration workflow — Step 1 (size compute for the working set)"
  finding: "Compute cache hit rate is {metric_value}% (target ≥ {threshold}%)."
  fix: "Working set likely exceeds compute cache. Increase min CU or reduce served data before lower-level tuning. See runbook Step 1."
```

Engine contract:
- Load rules → for each rule, read `source_table` for this `run_id` → evaluate `detect` safely
  (restricted eval over the row's columns + `threshold`; no builtins) → on match, emit a
  `lakebase_perf_findings` row with `finding`/`fix` string-formatted from the matched row.
- Unknown/missing fields in a rule → skip that rule with a warning row, never crash.
- Output: `display()` the findings sorted by `priority`, and append to `lakebase_perf_findings`.
  Also render a Markdown report-card string (grouped by priority) for export.

## Runbook section anchors (targets for `runbook_section`)

`2` inputs · `3` baseline · `4.1` size compute / working set · `4.2` scale-to-zero · `4.3`
connection pressure / pooler · `4.4` read replicas vs HA · `4.5` synced tables · `4.6` query & schema
(indexes, EXPLAIN) · `4.7` network placement · `5` acceptance criteria.

## File layout under `tools/performance/`

```
lakebase_collect_state.py       # P1 — point-in-time collector → metrics/tables/queries
lakebase_collect_samples.py     # P2 — one sample tick (callable standalone or by the Job)
lakebase_schedule_sampler.py    # P2 — creates/updates a Databricks Job that runs the sampler on a schedule
lakebase_report_card.py         # P3 — loads rules, evaluates vs collected tables, emits findings
rules/                          # P3 — declarative rule packs (*.yaml)
lakebase_load_harness.py        # (built) concurrent load → p50/p95/p99/QPS
lakebase_latency_probe.py       # single-query latency decomposition
lakebase_index_benchmark.py     # before/after EXPLAIN index impact
oracle_latency_probe.sh         # Oracle counterpart
```

An orchestrator (`lakebase_diagnostic.py`, later) chains: collect → [schedule sampler over a window]
→ benchmark → report card. Each tool must also stand alone.

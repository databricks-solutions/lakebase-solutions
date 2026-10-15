# performance/

Tools for measuring and benchmarking **Lakebase** performance — the hands-on companion to the
performance configuration runbook. Each runs unchanged against any project (all behavior comes from
widgets; nothing is hardcoded, OAuth and the endpoint are auto-resolved from the workspace identity).

### Diagnostic pipeline (First-Responder-style health check)

Run `lakebase_diagnostic.py` for the full flow, or any tool on its own.

| File | Role |
|------|------|
| `lakebase_diagnostic.py` | **Orchestrator** — runs the steps below in order and leaves you a report card |
| `lakebase_collect_state.py` | Point-in-time collector → fills runbook §2 inputs + §3 baseline (`lakebase_perf_metrics`/`_tables`/`_queries`) |
| `lakebase_collect_samples.py` | Ongoing time-series sampler (one or more ticks) → `lakebase_perf_samples` |
| `lakebase_schedule_sampler.py` | Creates a Databricks **Job** that runs the sampler on a schedule (dry-run by default) |
| `lakebase_report_card.py` | **Rules engine** → prioritized findings, each linking a runbook section (`lakebase_perf_findings`) |
| `rules/*.yaml` | Declarative rule packs — add a check by adding a rule, never by editing engine code |

### Benchmarks

| File | Measures |
|------|----------|
| `lakebase_load_harness.py` | Behavior under **concurrent load**: p50/p95/p99, QPS, errors |
| `lakebase_latency_probe.py` | **Single-query** latency, decomposed into line items |
| `oracle_latency_probe.sh` | Oracle (same ledger, for engine comparison) |
| `lakebase_index_benchmark.py` | Index impact — before/after `EXPLAIN` on Lakebase |

> **How it fits together.** The collectors write metrics to UC Delta tables (tall format — new
> metric = new row); the report card reads those tables, evaluates the declarative rule pack, and
> emits a **prioritized "what to improve and how"** list whose findings link straight into the
> performance configuration runbook. The data contract between the pieces is documented in
> `DIAGNOSTIC_SPEC.md` (dev-facing). Think a First Responder health check for Lakebase.

## Why a single "latency" number misleads

Different clients stop their stopwatch at different points on the same journey:

```
[client] ─TCP+TLS─ [server] ─parse─plan─execute─fetch(×roundtrips)─serialize─ [wire] ─deserialize─ [render]
```

For example, a desktop SQL tool might report `0.065 s` for a query (a warm, pooled, server-adjacent
fetch of a few rows), while a browser query editor reports `499 ms total / 142 ms execution` for the
*same* work — because the editor's number includes the browser → web-app round trip and rendering.
Neither is "wrong"; they measure different spans. Comparing them directly is apples to oranges.

These probes fix that by decomposing each run into named line items you can line up side by side.

## The ledger (both probes emit these)

```
CLIENT TOTAL (wall)
  = Connect (TCP+TLS+auth)        [amortized to ~0 with connection pooling]
  + Network RTT  × round trips    [depends on where you measure from; PrivateLink lives here]
  + Server planning + execution   [the engine's true work — the fairest comparison line]
  + Transfer + client (residual)  [serialize → stream result bytes → deserialize/render]
```

| Ledger line | Lakebase (Postgres) source | Oracle source |
|---|---|---|
| Connect | wall around `psycopg.connect()` | wall of a bare `sqlplus` login |
| Network RTT (empty) | min of `SELECT 1` | bulk `SELECT 1 FROM DUAL` ÷ N |
| Round trips for query | 1 (client cursor) / ⌈rows/fetch⌉ (server cursor) | AUTOTRACE `SQL*Net roundtrips to/from client` |
| Server planning | `EXPLAIN (ANALYZE)` → *Planning Time* | V$SQL parse / 10046 Parse |
| Server execution | `pg_stat_statements` mean (clean) + `EXPLAIN` exec | V$SQL `ELAPSED_TIME`/`EXECUTIONS` |
| Logical vs physical reads | buffers: shared **hit** vs **read** | `consistent gets` vs `physical reads` |
| Bytes on wire | estimated | AUTOTRACE `bytes sent via SQL*Net to client` (exact) |
| Client total (wall) | `perf_counter` execute→last row | `SET TIMING` |
| Transfer + client (residual) | total − RTT − server | total − server − (roundtrips × RTT) |

> Oracle exposes *more* wire detail (exact bytes + round trips) to the client than Postgres does.
> For Postgres, bytes-on-wire are estimated; everything else is measured directly.

## Running the load harness

`lakebase_load_harness.py` answers "how does this hold up under concurrency?" — the question the
single-query probe can't.

1. Import it into a workspace (notebook source) and attach to compute (serverless is fine).
2. Set the **`workload_spec`** widget — a JSON list of weighted statements, e.g.
   `[{"name":"lookup","weight":9,"kind":"read","sql":"SELECT ..."},{"name":"ins","weight":1,"kind":"write","sql":"INSERT ..."}]`.
   The default is a single read-only `SELECT 1` so a plain Run All is safe. Define any read/write mix
   here without editing code.
3. Set `concurrency_levels` (the sweep, e.g. `1,2,4,8,16`), `duration_seconds`, and optionally a
   `target_qps` to pace arrivals. Run all; read the **results ledger** — one row per level with
   p50/p95/p99, achieved QPS, error rate, and server-side TPS/cache-hit/deadlocks.

**Write safety:** the harness refuses `kind: "write"` statements unless `allow_writes = yes`, and the
right place to run them is a **copy-on-write branch** (`databricks postgres create-branch`) — not
`production`. Branches are cheap, isolated, and disposable: the safe way to load-test writes.

**Reading it:** walk the sweep for the *knee* — the worker count where QPS stops rising while p95/p99
climb. That's the practical concurrency ceiling for this compute size. Falling `cache_hit_pct` under
load means the working set outgrew the compute cache (size compute up); a rising `error_rate` near the
top usually means connection saturation (check the pooling model).

## Running the Lakebase probe

1. Import `lakebase_latency_probe.py` into a Databricks workspace (it's notebook source) and
   attach it to compute (serverless is fine).
2. Set **two widgets**: `project_id` (your Lakebase project id; leave blank to auto-pick if the
   workspace has only one) and `target_sql` (an example join is the default — replace with your
   query). Host, OAuth token, username, and branch/endpoint (`production`/`primary`) are
   auto-discovered — no hostname, password, or connection string needed.
3. Run all. Read the **latency ledger** table at the bottom.

Server-time precedence: the probe prefers `pg_stat_statements` (no instrumentation overhead). If
the extension isn't present it falls back to `EXPLAIN ANALYZE` execution time and labels it as an
upper bound (per-row `TIMING` inflates it).

## Running the Oracle probe

An example query is the built-in default, so the minimum is just the connection:

```bash
export ORA_CONN='USERNAME/PASSWORD@MYDB'   # same TNS alias you use in SQL Developer
./oracle_latency_probe.sh --no-result-cache
```

To profile your own query or tune the run:

```bash
./oracle_latency_probe.sh \
  --sql-inline "select count(1) from orders o, order_lines l
                where o.order_id = l.order_id
                  and o.region_id = l.region_id
                  and o.customer_id = '12345'" \
  --iters 30 --warmup 5 --rtt 1000 --arraysize 15 --no-result-cache
```

It auto-detects what your Oracle login can do and degrades gracefully:

| Tier | Needs | Gives |
|------|-------|-------|
| A | CONNECT only | connect time, bulk RTT, client total |
| B | PLUSTRACE role / V$ access | AUTOTRACE: round trips, bytes, logical/physical reads |
| C | SELECT on V$SQL | clean server `ELAPSED_TIME`/exec (no instrumentation) |
| D | `ALTER SESSION` + trace-file read (`--trace`) | 10046 + tkprof: parse/exec/fetch + SQL*Net waits |

**Security:** never hardcode a password. `user/pass@tns` is visible in `ps`; prefer an Oracle
Wallet (`/@alias`). The script reads `ORA_CONN` from your shell only.

## Running the index-remediation notebook

`lakebase_index_benchmark.py` creates (or verifies) an index on a Lakebase table **from inside the
workspace** and shows its impact with `EXPLAIN (ANALYZE, BUFFERS)` before and after. Running from a
notebook sidesteps two blockers: no external connectivity (private networking not set up → can't use
`psql`/DBeaver from a laptop) and the SQL-editor statement timeout that kills a long `CREATE INDEX`
(the notebook owns the session and sets `statement_timeout = 0`).

1. Import the notebook into a workspace and attach it to compute (serverless is fine).
2. Set the **Connection** widgets (`project_id` blank = auto-pick; `branch`/`endpoint`/`database`
   default to `production`/`primary`/`databricks_postgres`) and the **Index** widgets
   (`target_schema`, `target_table`, `index_name`, `index_columns`, plus a `benchmark_sql` to
   profile). Host, user, and a short-lived OAuth token are resolved automatically.
3. Run top-to-bottom: baseline plan → build index + `ANALYZE` → after plan → confirm.

**Leading-column reminder:** a composite B-tree index is only usable when the query filters on its
leading column; Postgres has no skip-scan, so a filter on a non-leading column alone forces a
sequential scan. Put the filtered column **first** in `index_columns` (and add join columns after it
to keep the scan index-only). This closes a tuning gap, not an engine gap.

## Keep it fair — the apples-to-apples checklist

1. **Identical SQL text and predicate values** → identical result cardinality. If your two runs
   filter on different values (and return different row counts), you're not comparing the same work.
2. **Equivalent indexes** present on both engines for the join/filter columns.
3. **Same data volume** in both tables.
4. **Warm vs cold is a dimension, not an accident.** Report both:
   - Cold: Postgres shows `shared read` blocks > 0; Oracle shows `physical reads` > 0.
   - Warm: served from cache.
   - To force cold on Oracle (DBA): `ALTER SYSTEM FLUSH BUFFER_CACHE;` (+ shared pool). On
     Lakebase you can't flush cheaply — label the first run as cold and discard it from "warm".
5. **Bypass result caches** when comparing engines (Oracle: `--no-result-cache` injects a
   `NO_RESULT_CACHE` hint; a `COUNT` can otherwise return from the result cache in microseconds).
   Measure *with* caches on for a real-world number.
6. **Match fetch size.** Oracle `arraysize` (default 15) sets rows per fetch round trip — a big
   result at arraysize 15 is dominated by round trips. Set `--arraysize` to whatever the real
   client uses; set the Postgres `fetch_mode`/batch to match.
7. **Same vantage point** when comparing *totals*. Server-execution lines are vantage-independent
   and always comparable; totals are only comparable when measured from equivalent network positions.

## PrivateLink and the vantage point

If Lakebase is reached over **PrivateLink**, that affects only the **Network RTT** line (the data
path a client/compute takes to the Postgres endpoint) — not server execution, and not the
browser query-editor overhead. Don't infer end-to-end latency from the query editor: run the
notebook from compute on the **same network path your real app uses**. The notebook prints the
resolved endpoint IP and flags whether it's private (RFC1918), so you can confirm the path. The
Oracle equivalent is whether SQL*Net rides a private link/TCPS — measure that probe from the real
app host too.

## Interpreting the result

- Line up the two **Server planning + execution** numbers first — the cleanest engine comparison,
  immune to network/vantage differences.
- Then use **Network RTT** and **round trips** to explain any total-latency gap: a chatty fetch
  (small arraysize, many rows) or a longer network path inflates totals without the engine being
  "slower".
- Report **min / median / p95**, never a single shot.

# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Load Harness — concurrency, throughput, and latency under load
# MAGIC
# MAGIC The latency probe (`lakebase_latency_probe.py`) measures **one query, one thread** and
# MAGIC decomposes where the time goes. This harness answers the other half of the question:
# MAGIC **how does the workload behave under concurrent load?** It drives a configurable number of
# MAGIC concurrent clients against a workload you define and reports the numbers the performance
# MAGIC runbook's acceptance criteria are written in: true **p50 / p95 / p99** latency, achieved
# MAGIC **QPS/throughput**, and **error rate** — none of which `pg_stat_statements` can give you,
# MAGIC and which a UI query editor can't produce in a controlled, repeatable way.
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Import this file into a Databricks workspace (it's notebook source), attach to compute
# MAGIC    (serverless is fine), and **Run all**. The first cell `%pip`-installs deps and restarts
# MAGIC    Python.
# MAGIC 2. Set the widgets. The only one you'll usually change is **`workload_spec`** — a JSON list
# MAGIC    of weighted statements (see below). Everything else has a working default.
# MAGIC 3. Read the **results ledger** at the bottom: one row per concurrency level.
# MAGIC
# MAGIC You do **not** supply a hostname, password, port, or connection string. The notebook resolves
# MAGIC the endpoint host and mints a short-lived OAuth token from the workspace identity, and defaults
# MAGIC the branch/endpoint to `production`/`primary` (auto-discovering if those differ). The same file
# MAGIC runs unchanged against any project in any workspace — all behavior comes from the widgets.
# MAGIC
# MAGIC ## The workload spec (the extension point)
# MAGIC `workload_spec` is a JSON array; each entry is one statement the harness may run:
# MAGIC ```json
# MAGIC [
# MAGIC   {"name": "lookup",  "weight": 9, "kind": "read",  "sql": "SELECT 1"},
# MAGIC   {"name": "insert",  "weight": 1, "kind": "write", "sql": "SELECT 1"}
# MAGIC ]
# MAGIC ```
# MAGIC - `weight` sets the read/write mix (relative probability a worker picks that statement).
# MAGIC - `kind` is `read` or `write` — only used to label results and to gate the write-safety check.
# MAGIC - `sql` is any statement. Parameterize it yourself; the harness runs the text as given.
# MAGIC
# MAGIC Define any mix without touching code. The default is a single read-only `SELECT 1` so a plain
# MAGIC Run All is safe and self-contained.
# MAGIC
# MAGIC > ### ⚠️ Write workloads mutate real data
# MAGIC > A statement with side effects changes the database it runs against. For write/mixed load
# MAGIC > tests, point the harness at a **copy-on-write branch** (`databricks postgres create-branch`),
# MAGIC > not `production`. Branches share storage, cost almost nothing, and are disposable — the
# MAGIC > safe, repeatable way to load-test writes. The notebook refuses to run `write` statements
# MAGIC > unless `allow_writes = yes`.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "psycopg[binary]>=3.1" "databricks-sdk>=0.81" pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

import json

dbutils.widgets.text("project_id", "", "1. Lakebase project id (blank = auto-pick if only one)")
dbutils.widgets.text("branch", "production", "2. Branch (default production)")
dbutils.widgets.text("endpoint", "primary", "3. Endpoint/compute (default primary)")
dbutils.widgets.text("database", "databricks_postgres", "4. Postgres database name")
dbutils.widgets.text(
    "workload_spec",
    json.dumps([{"name": "ping", "weight": 1, "kind": "read", "sql": "SELECT 1"}]),
    "5. Workload spec (JSON list of {name, weight, kind, sql})",
)
dbutils.widgets.text("concurrency_levels", "1,2,4,8,16", "6. Concurrency sweep (comma-separated worker counts)")
dbutils.widgets.text("duration_seconds", "20", "7. Measured seconds per level")
dbutils.widgets.text("warmup_seconds", "5", "8. Warmup seconds per level (discarded)")
dbutils.widgets.text("target_qps", "0", "9. Target QPS per level (0 = closed-loop, run flat out)")
dbutils.widgets.dropdown("allow_writes", "no", ["no", "yes"], "10. Allow write-kind statements (point at a BRANCH)")
dbutils.widgets.dropdown("measure_wake", "no", ["no", "yes"], "11. Measure scale-to-zero wake-up latency")
dbutils.widgets.text("wake_idle_seconds", "360", "12. Idle seconds before the wake-up probe (> suspend timeout)")
dbutils.widgets.text("results_table", "", "13. UC table to append results to (blank = don't persist)")
dbutils.widgets.text("max_workers", "64", "14. Safety cap on concurrent workers")

PROJECT_ID = dbutils.widgets.get("project_id").strip()
BRANCH = dbutils.widgets.get("branch").strip() or "production"
ENDPOINT = dbutils.widgets.get("endpoint").strip() or "primary"
DATABASE = dbutils.widgets.get("database").strip()
WORKLOAD = json.loads(dbutils.widgets.get("workload_spec"))
LEVELS = [int(x) for x in dbutils.widgets.get("concurrency_levels").split(",") if x.strip()]
DURATION = float(dbutils.widgets.get("duration_seconds"))
WARMUP = float(dbutils.widgets.get("warmup_seconds"))
TARGET_QPS = float(dbutils.widgets.get("target_qps"))
ALLOW_WRITES = dbutils.widgets.get("allow_writes") == "yes"
MEASURE_WAKE = dbutils.widgets.get("measure_wake") == "yes"
WAKE_IDLE = float(dbutils.widgets.get("wake_idle_seconds"))
RESULTS_TABLE = dbutils.widgets.get("results_table").strip()
MAX_WORKERS = int(dbutils.widgets.get("max_workers"))

# Validate the workload spec and enforce the write-safety gate.
assert WORKLOAD, "workload_spec must list at least one statement."
for w in WORKLOAD:
    assert {"name", "weight", "kind", "sql"} <= set(w), f"each workload entry needs name/weight/kind/sql: {w}"
    assert w["kind"] in ("read", "write"), f"kind must be 'read' or 'write': {w}"
has_writes = any(w["kind"] == "write" for w in WORKLOAD)
if has_writes and not ALLOW_WRITES:
    raise ValueError(
        "Workload contains 'write' statements but allow_writes='no'. "
        "Point this harness at a copy-on-write BRANCH and set allow_writes='yes' to proceed."
    )
LEVELS = [min(n, MAX_WORKERS) for n in LEVELS]
print(f"Concurrency levels: {LEVELS}")
print(f"Workload: {[(w['name'], w['kind'], w['weight']) for w in WORKLOAD]}")
print(f"Per level: {WARMUP}s warmup + {DURATION}s measured; target_qps={TARGET_QPS or 'unbounded'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase
# MAGIC Host from `get_endpoint`, OAuth token (valid ~1h) as the Postgres password, `sslmode=require`.
# MAGIC Nothing here is hardcoded — host, token, user, and endpoint path are all resolved from the
# MAGIC workspace identity, so the same notebook targets any project.

# COMMAND ----------

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
USERNAME = w.current_user.me().user_name

proj = PROJECT_ID
if not proj:
    projects = list(w.postgres.list_projects())
    if not projects:
        raise ValueError("No Lakebase projects found; set the 'project_id' widget.")
    proj = projects[0].project_id
    print(f"No project_id given — auto-selected: {proj}")

ENDPOINT_PATH = f"projects/{proj}/branches/{BRANCH}/endpoints/{ENDPOINT}"
try:
    endpoint = w.postgres.get_endpoint(name=ENDPOINT_PATH)
except Exception:
    parent = f"projects/{proj}/branches/{BRANCH}"
    try:
        eps = list(w.postgres.list_endpoints(parent))
    except TypeError:
        eps = list(w.postgres.list_endpoints(parent=parent))
    if not eps:
        raise ValueError(f"No endpoints found under {parent}.")
    endpoint = eps[0]
    ENDPOINT_PATH = endpoint.name
    print(f"Endpoint '{ENDPOINT}' not found; using discovered endpoint: {ENDPOINT_PATH}")

HOST = endpoint.status.hosts.host
TOKEN = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH).token  # ~1h OAuth as PG password

CONNECT_KWARGS = dict(
    host=HOST,
    dbname=DATABASE,
    user=USERNAME,
    password=TOKEN,
    sslmode="require",
    autocommit=True,  # each statement is its own transaction — clean, isolated round trips
)

# Smoke-test one connection before spinning up the pool.
with psycopg.connect(**CONNECT_KWARGS) as _c:
    ver = _c.execute("select version()").fetchone()[0]
print(f"Connected as {USERNAME} to {HOST}/{DATABASE}")
print(f"Endpoint: {ENDPOINT_PATH}")
print(f"Server: {ver}")

# COMMAND ----------

# MAGIC %md ## Measurement helpers

# COMMAND ----------

import time
import random
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor


def _pct(sorted_vals, p):
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_vals[0]
    k = (n - 1) * p
    f = int(k)
    c = min(f + 1, n - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def summarize(samples_ms):
    s = sorted(samples_ms)
    if not s:
        return {"n": 0, "min_ms": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "max_ms": 0, "mean_ms": 0}
    return {
        "n": len(s),
        "min_ms": round(s[0], 3),
        "p50_ms": round(_pct(s, 0.50), 3),
        "p95_ms": round(_pct(s, 0.95), 3),
        "p99_ms": round(_pct(s, 0.99), 3),
        "max_ms": round(s[-1], 3),
        "mean_ms": round(sum(s) / len(s), 3),
    }


def _weighted_picker(workload):
    """Return a function that picks a statement by weight on each call."""
    names = [x["name"] for x in workload]
    sqls = {x["name"]: x["sql"] for x in workload}
    weights = [max(float(x["weight"]), 0.0) for x in workload]
    def pick():
        name = random.choices(names, weights=weights, k=1)[0]
        return name, sqls[name]
    return pick


def _worker(stop_evt, pick, pacing_sleep, out_latencies, out_errors):
    """One concurrent client: own connection, loop until stop_evt, record per-request latency."""
    local = []
    errs = 0
    try:
        conn = psycopg.connect(**CONNECT_KWARGS)
    except Exception:
        out_errors.append(1)
        return
    try:
        while not stop_evt.is_set():
            _, sql = pick()
            t = time.perf_counter_ns()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    if cur.description is not None:
                        cur.fetchall()
                local.append((time.perf_counter_ns() - t) / 1e6)
            except Exception:
                errs += 1
            if pacing_sleep:
                time.sleep(pacing_sleep)
    finally:
        conn.close()
    out_latencies.extend(local)
    out_errors.append(errs)


def run_level(workers, duration_s, warmup_s, target_qps):
    """Run `workers` concurrent clients for warmup_s + duration_s. Returns a result dict.

    Closed-loop (target_qps<=0): each worker runs flat out; achieved QPS is an outcome.
    Paced (target_qps>0): each worker sleeps to approximate target_qps/workers arrivals.
    """
    pick = _weighted_picker(WORKLOAD)
    pacing_sleep = 0.0
    if target_qps and target_qps > 0:
        pacing_sleep = max(workers / target_qps, 0.0)

    stop_evt = threading.Event()
    latencies, errors = [], []

    # Warmup: run and discard.
    if warmup_s > 0:
        warm_lat, warm_err = [], []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for _ in range(workers):
                ex.submit(_worker, stop_evt, pick, pacing_sleep, warm_lat, warm_err)
            time.sleep(warmup_s)
            stop_evt.set()
        stop_evt = threading.Event()

    # Measured window.
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for _ in range(workers):
            ex.submit(_worker, stop_evt, pick, pacing_sleep, latencies, errors)
        time.sleep(duration_s)
        stop_evt.set()
    elapsed = time.perf_counter() - t0

    stats = summarize(latencies)
    total_err = sum(errors)
    completed = stats["n"]
    achieved_qps = round(completed / elapsed, 1) if elapsed > 0 else 0.0
    err_rate = round(total_err / max(completed + total_err, 1), 4)
    return {
        "workers": workers,
        "achieved_qps": achieved_qps,
        "completed": completed,
        "errors": total_err,
        "error_rate": err_rate,
        **{k: stats[k] for k in ("p50_ms", "p95_ms", "p99_ms", "max_ms")},
    }


def snapshot_db_stats(conn):
    """Server-side counters for the current database (for TPS / cache-hit / deadlock deltas)."""
    row = conn.execute(
        "SELECT xact_commit, xact_rollback, blks_hit, blks_read, deadlocks "
        "FROM pg_stat_database WHERE datname = current_database()"
    ).fetchone()
    return dict(zip(("xact_commit", "xact_rollback", "blks_hit", "blks_read", "deadlocks"), map(float, row)))


def diff_db_stats(before, after, elapsed_s):
    d = {k: after[k] - before[k] for k in before}
    reads = d["blks_hit"] + d["blks_read"]
    return {
        "server_tps": round((d["xact_commit"] + d["xact_rollback"]) / elapsed_s, 1) if elapsed_s else 0.0,
        "cache_hit_pct": round(100 * d["blks_hit"] / reads, 2) if reads > 0 else None,
        "deadlocks": int(d["deadlocks"]),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Scale-to-zero wake-up latency
# MAGIC If the endpoint has scale-to-zero enabled, the first request after an idle period pays a
# MAGIC wake-up cost (compute resumes, cache is cold, session state is reset). This measures that
# MAGIC penalty: close all connections, idle past the suspend timeout, then time a fresh
# MAGIC connect + first query. Set `measure_wake='yes'` and `wake_idle_seconds` above the endpoint's
# MAGIC suspend timeout.

# COMMAND ----------

wake_result = None
if MEASURE_WAKE:
    probe_sql = next((x["sql"] for x in WORKLOAD if x["kind"] == "read"), "SELECT 1")
    print(f"Idling {WAKE_IDLE:.0f}s to let the endpoint suspend...")
    time.sleep(WAKE_IDLE)
    t = time.perf_counter_ns()
    c = psycopg.connect(**CONNECT_KWARGS)
    connect_ms = (time.perf_counter_ns() - t) / 1e6
    t = time.perf_counter_ns()
    c.execute(probe_sql).fetchall()
    first_query_ms = (time.perf_counter_ns() - t) / 1e6
    c.close()
    wake_result = {"cold_connect_ms": round(connect_ms, 1), "cold_first_query_ms": round(first_query_ms, 1)}
    print(f"Wake-up — connect: {connect_ms:.1f} ms | first query: {first_query_ms:.1f} ms")

# COMMAND ----------

# MAGIC %md ## Run the concurrency sweep

# COMMAND ----------

mon = psycopg.connect(**CONNECT_KWARGS)  # a side connection for server-stat snapshots
rows = []
for n in LEVELS:
    print(f"Level: {n} worker(s)...")
    before = snapshot_db_stats(mon)
    t0 = time.perf_counter()
    res = run_level(n, DURATION, WARMUP, TARGET_QPS)
    elapsed = time.perf_counter() - t0
    after = snapshot_db_stats(mon)
    res.update(diff_db_stats(before, after, DURATION))
    rows.append(res)
    print(
        f"  qps={res['achieved_qps']}  p50={res['p50_ms']}ms  p95={res['p95_ms']}ms  "
        f"p99={res['p99_ms']}ms  err={res['error_rate']}  cache_hit={res.get('cache_hit_pct')}%"
    )
mon.close()
print("Sweep complete.")

# COMMAND ----------

# MAGIC %md ## Results ledger

# COMMAND ----------

import pandas as pd

cols = ["workers", "achieved_qps", "p50_ms", "p95_ms", "p99_ms", "max_ms",
        "error_rate", "completed", "errors", "server_tps", "cache_hit_pct", "deadlocks"]
ledger = pd.DataFrame(rows)[cols]
if wake_result:
    print(f"Scale-to-zero wake-up — cold connect: {wake_result['cold_connect_ms']} ms | "
          f"cold first query: {wake_result['cold_first_query_ms']} ms")
display(ledger)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to read this
# MAGIC - **Find the knee.** Walk the concurrency sweep from low to high: QPS should rise while p95/p99
# MAGIC   stay roughly flat, then at some worker count latency climbs sharply while QPS stops rising —
# MAGIC   that knee is the practical concurrency ceiling for this compute size and workload.
# MAGIC - **p99 is the SLA number.** The runbook's acceptance criteria are written in p95/p99 under peak
# MAGIC   conditions; read them off the level that matches your expected peak concurrency.
# MAGIC - **`cache_hit_pct` falling as load rises** means the working set no longer fits the compute
# MAGIC   cache — size compute up (runbook Step 1) rather than tuning queries.
# MAGIC - **`error_rate` climbing** near the top of the sweep usually means connection saturation —
# MAGIC   check the connection model (runbook Step 3) before adding compute.
# MAGIC - **Vantage point matters.** This runs on Databricks compute (an in-cloud hop to Lakebase), so
# MAGIC   its latency floor is lower than a browser or a cross-region app. Run it from the same network
# MAGIC   path your real client uses for an end-to-end number, and use the single-query probe to
# MAGIC   decompose any gap.

# COMMAND ----------

# MAGIC %md ### (Optional) Persist results for before/after comparison

# COMMAND ----------

if RESULTS_TABLE:
    sdf = spark.createDataFrame(ledger.assign(
        run_ts=pd.Timestamp.utcnow().isoformat(),
        project_id=proj,
        endpoint=ENDPOINT_PATH,
        target_qps=TARGET_QPS,
    ))
    sdf.write.mode("append").saveAsTable(RESULTS_TABLE)
    print(f"Appended {len(ledger)} row(s) to {RESULTS_TABLE}")
else:
    print("results_table blank — not persisting (set widget 13 to append to a UC table).")

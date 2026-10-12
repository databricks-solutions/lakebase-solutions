# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Latency Probe — apples-to-apples decomposition
# MAGIC
# MAGIC ## How to run
# MAGIC 1. **Attach** this notebook to compute (serverless is fine) and click **Run all**. The first
# MAGIC    cell `%pip`-installs dependencies and restarts Python.
# MAGIC 2. **Set two widgets** at the top:
# MAGIC    - `project_id` — your Lakebase project id (e.g. `my-lakebase-project`). Leave blank to
# MAGIC      auto-pick if the workspace has only one project.
# MAGIC    - `target_sql` — defaults to an example join `count(1)`; replace with your own query.
# MAGIC 3. Read the **latency ledger** table at the bottom.
# MAGIC
# MAGIC You do **not** need a hostname, password, port, or connection string: the notebook resolves
# MAGIC the endpoint host and mints a short-lived OAuth token from the workspace identity, and
# MAGIC defaults the branch/endpoint to `production`/`primary` (auto-discovering if those differ).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Breaks a single query's **client-perceived latency** into named line items so it can be
# MAGIC compared like-for-like against the Oracle probe (`oracle_latency_probe.sh`).
# MAGIC
# MAGIC The Databricks/Lakebase query editor reports three numbers (e.g. *499 ms total /
# MAGIC 142 ms execution / 0.212 ms planning*). The gap between "total" and "execution" is
# MAGIC network + fetch + driver/UI overhead — exactly what this notebook isolates.
# MAGIC
# MAGIC **Ledger produced per query (min / p50 / p95 over N runs):**
# MAGIC
# MAGIC | Line item | Source |
# MAGIC |---|---|
# MAGIC | Connect (TCP+TLS+auth) | wall-clock around `psycopg.connect()` |
# MAGIC | Network RTT (empty round trip) | min of `SELECT 1` |
# MAGIC | Server planning | `EXPLAIN (ANALYZE)` → *Planning Time* |
# MAGIC | Server execution | `pg_stat_statements` mean (no instrumentation overhead), with `EXPLAIN` exec as fallback |
# MAGIC | Logical vs physical reads | `EXPLAIN (BUFFERS)` → shared **hit** vs **read** (+ I/O time if `track_io_timing`) |
# MAGIC | Client total (wall) | `perf_counter` around `execute()` + full fetch |
# MAGIC | Transfer + client (residual) | total − RTT − server |
# MAGIC
# MAGIC > **Vantage point matters.** This runs on Databricks compute, so Compute→Lakebase is an
# MAGIC > in-cloud hop and will be *faster* than the browser query editor (which adds
# MAGIC > browser→control-plane→Postgres). That is expected: compare the **line items**, not
# MAGIC > just the totals, and run the probe from the same network location as the real client
# MAGIC > when you want an end-to-end user-perceived number.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "psycopg[binary]>=3.1" "databricks-sdk>=0.81" pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("project_id", "", "1. Lakebase project id (e.g. my-lakebase-project; blank = auto-pick if only one)")
dbutils.widgets.text("branch", "production", "2. Branch (default production)")
dbutils.widgets.text("endpoint", "primary", "3. Endpoint/compute (default primary)")
dbutils.widgets.text("database", "databricks_postgres", "4. Postgres database name")
dbutils.widgets.text(
    "target_sql",
    # Example join count(1) — replace with the query you want to benchmark.
    "select count(1)\n"
    "from demo.orders o,\n"
    "     demo.order_lines l\n"
    "where o.order_id = l.order_id\n"
    "  and o.region_id = l.region_id\n"
    "  and o.customer_id = '12345'",
    "5. Target SQL to profile",
)
dbutils.widgets.text("pgss_match", "order_lines", "6. pg_stat_statements match substring (a token unique to the query)")
dbutils.widgets.text("iterations", "30", "7. Measured iterations")
dbutils.widgets.text("warmup", "5", "8. Warmup iterations (discarded)")
dbutils.widgets.text("rtt_iterations", "50", "9. RTT (SELECT 1) iterations")
dbutils.widgets.text("connect_iterations", "10", "10. Fresh-connection iterations")
dbutils.widgets.dropdown("fetch_mode", "all", ["all", "stream"], "11. Fetch mode (all=client cursor, stream=server cursor)")

PROJECT_ID = dbutils.widgets.get("project_id").strip()
BRANCH = dbutils.widgets.get("branch").strip() or "production"
ENDPOINT = dbutils.widgets.get("endpoint").strip() or "primary"
DATABASE = dbutils.widgets.get("database").strip()
TARGET_SQL = dbutils.widgets.get("target_sql").strip().rstrip(";")
PGSS_MATCH = dbutils.widgets.get("pgss_match").strip()
ITERS = int(dbutils.widgets.get("iterations"))
WARMUP = int(dbutils.widgets.get("warmup"))
RTT_ITERS = int(dbutils.widgets.get("rtt_iterations"))
CONNECT_ITERS = int(dbutils.widgets.get("connect_iterations"))
FETCH_MODE = dbutils.widgets.get("fetch_mode")

print("Target SQL:\n" + TARGET_SQL)

# COMMAND ----------

# MAGIC %md ## Connect to Lakebase
# MAGIC Host from `get_endpoint`, OAuth token (valid ~1h) as the Postgres password, `sslmode=require`.

# COMMAND ----------

import time
import statistics
import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
USERNAME = w.current_user.me().user_name

# Resolve the project (auto-pick if the widget is blank and there's only one).
proj = PROJECT_ID
if not proj:
    try:
        projects = list(w.postgres.list_projects())
        if not projects:
            raise ValueError("No Lakebase projects found in this workspace.")
        proj = projects[0].project_id
        print(f"No project_id given — auto-selected: {proj}")
    except Exception as e:
        raise ValueError("Set the 'project_id' widget (e.g. my-lakebase-project).") from e

# Build the endpoint path from branch/endpoint defaults; fall back to discovery if they differ.
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

host = endpoint.status.hosts.host                                    # no hostname to supply
token = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH).token  # short-lived OAuth as PG password

CONNECT_KWARGS = dict(
    host=host,
    dbname=DATABASE,
    user=USERNAME,
    password=token,
    sslmode="require",
    # autocommit so each measured statement is a clean, isolated round trip (no implicit BEGIN/COMMIT noise)
    autocommit=True,
)

conn = psycopg.connect(**CONNECT_KWARGS)
with conn.cursor() as c:
    c.execute("select version(), current_setting('track_io_timing')")
    ver, io_timing = c.fetchone()
print(f"Connected as {USERNAME} to {host}/{DATABASE}")
print(f"Endpoint: {ENDPOINT_PATH}")
print(f"Server: {ver}")
print(f"track_io_timing: {io_timing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm the network path (PrivateLink check)
# MAGIC PrivateLink changes the **data-path RTT** that a real app/compute sees — i.e. the
# MAGIC *Network RTT* line in the ledger below. It does **not** change server execution, and it
# MAGIC does **not** change the browser query-editor overhead. Resolve the endpoint host to see
# MAGIC which path this compute is actually using: a private (RFC1918) IP means the probe is on
# MAGIC the PrivateLink path and its RTT is representative of a private-networked client.

# COMMAND ----------

import socket
import ipaddress

try:
    resolved = socket.getaddrinfo(host, 5432, proto=socket.IPPROTO_TCP)
    ips = sorted({r[4][0] for r in resolved})
    for ip in ips:
        try:
            addr = ipaddress.ip_address(ip)
            kind = "PRIVATE (RFC1918 — PrivateLink/VPC path)" if addr.is_private else "PUBLIC (internet-routable path)"
        except ValueError:
            kind = "unparsed"
        print(f"{host} -> {ip}  [{kind}]")
    print("\nNote: run this notebook from the SAME compute/network the real app uses, so this RTT reflects the real client path.")
except Exception as e:
    print(f"Could not resolve {host}: {e}")

# COMMAND ----------

# MAGIC %md ## Measurement helpers

# COMMAND ----------

def _pct(sorted_vals, p):
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    k = (n - 1) * p
    f = int(k)
    c = min(f + 1, n - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def summarize(samples_ms):
    s = sorted(samples_ms)
    return {
        "n": len(s),
        "min_ms": round(s[0], 3),
        "p50_ms": round(_pct(s, 0.50), 3),
        "p95_ms": round(_pct(s, 0.95), 3),
        "max_ms": round(s[-1], 3),
        "mean_ms": round(sum(s) / len(s), 3),
    }


def measure_rtt(conn, iters):
    """Empty round trip: RTT + minimal server + protocol framing. The network floor."""
    with conn.cursor() as cur:
        samples = []
        for _ in range(iters):
            t = time.perf_counter_ns()
            cur.execute("SELECT 1")
            cur.fetchone()
            samples.append((time.perf_counter_ns() - t) / 1e6)
    return summarize(samples)


def measure_connect(connect_kwargs, iters):
    """Cost to establish a brand-new connection (TCP + TLS handshake + auth)."""
    samples = []
    for _ in range(iters):
        t = time.perf_counter_ns()
        c = psycopg.connect(**connect_kwargs)
        samples.append((time.perf_counter_ns() - t) / 1e6)
        c.close()
    return summarize(samples)


def measure_query_total(conn, sql, iters, fetch_mode="all"):
    """Client wall-clock: execute() through the last row landing in the client.

    With a default (client-side) cursor, execute() already streams the whole result
    into the client, so this is RTT + server + transfer + deserialize — the number that
    corresponds to the query editor's 'total'.
    """
    samples = []
    rows = None
    for _ in range(iters):
        with conn.cursor() as cur:
            t = time.perf_counter_ns()
            cur.execute(sql)
            if cur.description is not None:
                if fetch_mode == "all":
                    data = cur.fetchall()
                    rows = len(data)
                else:
                    rows = 0
                    while True:
                        batch = cur.fetchmany(1000)
                        if not batch:
                            break
                        rows += len(batch)
            else:
                rows = cur.rowcount
            samples.append((time.perf_counter_ns() - t) / 1e6)
    return summarize(samples), rows


def _sum_buffers(node, acc):
    for k in ("Shared Hit Blocks", "Shared Read Blocks", "Shared Written Blocks",
              "Temp Read Blocks", "Temp Written Blocks", "I/O Read Time", "I/O Write Time"):
        if k in node:
            acc[k] = acc.get(k, 0) + node[k]
    for child in node.get("Plans", []):
        _sum_buffers(child, acc)
    return acc


def explain_analyze(conn, sql):
    """Server-side plan + execution with buffer accounting. TIMING adds per-row overhead,
    so treat Execution Time here as an upper bound; prefer pg_stat_statements when available."""
    with conn.cursor() as cur:
        try:
            cur.execute("SET track_io_timing = on")
        except Exception:
            pass
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, TIMING, VERBOSE, FORMAT JSON) " + sql)
        plan = cur.fetchone()[0][0]
    buffers = _sum_buffers(plan["Plan"], {})
    return {
        "planning_ms": round(plan.get("Planning Time", 0.0), 3),
        "execution_ms": round(plan.get("Execution Time", 0.0), 3),
        "shared_hit_blocks": buffers.get("Shared Hit Blocks", 0),
        "shared_read_blocks": buffers.get("Shared Read Blocks", 0),
        "io_read_ms": round(buffers.get("I/O Read Time", 0.0), 3),
        "plan_json": plan,
    }


def pgss_server_time(conn, match_substr, run_fn):
    """Clean server execution time with NO instrumentation overhead, via pg_stat_statements.
    Snapshots (calls, total_exec_time) for rows matching the query, runs the workload, diffs.
    Returns mean server ms/call, or None if the extension is unavailable."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        if cur.fetchone() is None:
            return None, "pg_stat_statements not installed"

        def snapshot():
            cur.execute(
                "SELECT COALESCE(sum(calls),0), COALESCE(sum(total_exec_time),0) "
                "FROM pg_stat_statements WHERE query LIKE %s",
                (f"%{match_substr}%",),
            )
            return cur.fetchone()

        before_calls, before_time = snapshot()
        run_fn()
        after_calls, after_time = snapshot()

    # Postgres sum(calls) is numeric -> psycopg returns Decimal; sum(total_exec_time) is float.
    # Coerce both to float so the division never hits a Decimal/float type mismatch.
    d_calls = float(after_calls) - float(before_calls)
    d_time = float(after_time) - float(before_time)
    if d_calls <= 0:
        return None, f"no pg_stat_statements rows matched LIKE %{match_substr}% (check match substring)"
    return round(d_time / d_calls, 3), f"{int(d_calls)} calls observed"

# COMMAND ----------

# MAGIC %md ## Run the probe

# COMMAND ----------

print("Warming up...")
for _ in range(WARMUP):
    with conn.cursor() as cur:
        cur.execute(TARGET_SQL)
        if cur.description is not None:
            cur.fetchall()

print(f"Connect ({CONNECT_ITERS}x)...")
connect_stats = measure_connect(CONNECT_KWARGS, CONNECT_ITERS)

print(f"Network RTT ({RTT_ITERS}x SELECT 1)...")
rtt_stats = measure_rtt(conn, RTT_ITERS)

print(f"Query total ({ITERS}x, fetch={FETCH_MODE})...")
total_stats, rowcount = measure_query_total(conn, TARGET_SQL, ITERS, FETCH_MODE)

print("EXPLAIN (ANALYZE, BUFFERS)...")
ea = explain_analyze(conn, TARGET_SQL)

print(f"Server time via pg_stat_statements ({ITERS}x)...")
def _run_workload():
    for _ in range(ITERS):
        with conn.cursor() as cur:
            cur.execute(TARGET_SQL)
            if cur.description is not None:
                cur.fetchall()
pgss_ms, pgss_note = pgss_server_time(conn, PGSS_MATCH, _run_workload)

print("Done.")

# COMMAND ----------

# MAGIC %md ## Latency ledger

# COMMAND ----------

import pandas as pd

# Prefer the clean pg_stat_statements server time; fall back to EXPLAIN exec (upper bound).
server_exec_ms = pgss_ms if pgss_ms is not None else ea["execution_ms"]
server_total_ms = round((pgss_ms if pgss_ms is not None else ea["execution_ms"]) + ea["planning_ms"], 3)
net_ms = rtt_stats["p50_ms"]
total_ms = total_stats["p50_ms"]
residual_ms = round(max(total_ms - net_ms - server_total_ms, 0.0), 3)

ledger = pd.DataFrame(
    [
        {"line_item": "Connect (TCP+TLS+auth)", "min_ms": connect_stats["min_ms"], "p50_ms": connect_stats["p50_ms"], "p95_ms": connect_stats["p95_ms"], "note": "amortized away by connection pooling"},
        {"line_item": "Network RTT (empty)", "min_ms": rtt_stats["min_ms"], "p50_ms": rtt_stats["p50_ms"], "p95_ms": rtt_stats["p95_ms"], "note": "SELECT 1 — network floor from this vantage point"},
        {"line_item": "Server planning", "min_ms": ea["planning_ms"], "p50_ms": ea["planning_ms"], "p95_ms": ea["planning_ms"], "note": "EXPLAIN Planning Time"},
        {"line_item": "Server execution", "min_ms": server_exec_ms, "p50_ms": server_exec_ms, "p95_ms": server_exec_ms, "note": pgss_note if pgss_ms is not None else "EXPLAIN exec (TIMING overhead = upper bound)"},
        {"line_item": "Transfer + client (residual)", "min_ms": residual_ms, "p50_ms": residual_ms, "p95_ms": residual_ms, "note": "total − RTT − server; serialize/stream/deserialize"},
        {"line_item": "CLIENT TOTAL (wall)", "min_ms": total_stats["min_ms"], "p50_ms": total_stats["p50_ms"], "p95_ms": total_stats["p95_ms"], "note": f"execute→last row; {rowcount} row(s)"},
    ]
)
print(f"Rows returned: {rowcount}")
print(f"Buffers — shared hit: {ea['shared_hit_blocks']} (cache) | shared read: {ea['shared_read_blocks']} (storage) | I/O read: {ea['io_read_ms']} ms")
print(f"Server time source: {'pg_stat_statements' if pgss_ms is not None else 'EXPLAIN ANALYZE'}")
display(ledger)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to read this
# MAGIC - **CLIENT TOTAL** is the honest "how long did the user wait" number from this vantage point.
# MAGIC - **Server planning + execution** is the engine's true work — compare this directly to Oracle's `V$SQL ELAPSED_TIME`/exec. This is the fairest apples-to-apples engine number.
# MAGIC - **Network RTT** depends entirely on where this notebook runs. Re-run from the real client's network to get a representative end-to-end total.
# MAGIC - **shared hit vs shared read** tells you warm vs cold: a cold first run reads from storage (shared read > 0) and will dominate; warm runs serve from cache. Report both and label them.
# MAGIC - **Residual** is transfer + driver deserialize. For a 1-row `count(*)` it should be tiny; for wide/large result sets it grows and is where fetch-size tuning pays off.
# MAGIC
# MAGIC ### Keep it fair (must match the Oracle run)
# MAGIC 1. Identical SQL text **and predicate values** → identical result cardinality. If the two engines' queries filter on different values (and return different row counts), you're not comparing the same work.
# MAGIC 2. Equivalent indexes present on both engines.
# MAGIC 3. Same data volume in both tables.
# MAGIC 4. Control warm vs cold cache explicitly and report both.
# MAGIC 5. Match fetch size / whether all rows are materialized.
# MAGIC 6. Same measurement vantage point (network topology) when comparing totals.

# COMMAND ----------

conn.close()
print("Connection closed.")

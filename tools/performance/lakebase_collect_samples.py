# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Performance Sampler — `lakebase_collect_samples`
# MAGIC
# MAGIC Captures a **raw-counter snapshot** from the target Lakebase instance and appends one row
# MAGIC per metric per tick to the `lakebase_perf_samples` Delta table.  Designed for three uses:
# MAGIC
# MAGIC - **Point-in-time check** — run once (ticks=1) interactively from a notebook.
# MAGIC - **Short window** — run with ticks>1 to cover a burst without setting up a job.
# MAGIC - **Scheduled collection** — called by the Databricks Job that
# MAGIC   `lakebase_schedule_sampler.py` creates; in that mode ticks=1 and the job's cron
# MAGIC   controls the cadence.
# MAGIC
# MAGIC Raw counters are stored as-is; deltas and rates are computed at analysis time from
# MAGIC consecutive ticks (see `lakebase_report_card.py`).
# MAGIC
# MAGIC **Sources per tick**
# MAGIC | Source | Metrics |
# MAGIC |---|---|
# MAGIC | `pg_stat_database` | xact_commit/rollback, blks_hit/read, deadlocks, tup_* (10 counters) |
# MAGIC | `pg_stat_activity` | session state counts: active / idle / idle_in_transaction / total |
# MAGIC | `pg_stat_statements` | top-N by total_exec_time (skipped if extension absent) |
# MAGIC
# MAGIC **No writes to the target database** — the only output is the UC `samples_table`.
# MAGIC
# MAGIC > Schema contract: `tools/performance/DIAGNOSTIC_SPEC.md` → `lakebase_perf_samples`

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "psycopg[binary]>=3.1" "databricks-sdk>=0.81" pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("project_id", "", "1. Lakebase project id (blank = auto-pick if only one)")
dbutils.widgets.text("branch", "production", "2. Branch (default production)")
dbutils.widgets.text("endpoint", "primary", "3. Endpoint/compute (default primary)")
dbutils.widgets.text("database", "databricks_postgres", "4. Postgres database name")
dbutils.widgets.text("samples_table", "lakebase_perf_samples", "5. UC table to append samples to")
dbutils.widgets.text("ticks", "1", "6. Number of sample ticks to collect")
dbutils.widgets.text("interval_seconds", "60", "7. Seconds between ticks (ignored when ticks=1)")

PROJECT_ID = dbutils.widgets.get("project_id").strip()
BRANCH = dbutils.widgets.get("branch").strip() or "production"
ENDPOINT = dbutils.widgets.get("endpoint").strip() or "primary"
DATABASE = dbutils.widgets.get("database").strip()
SAMPLES_TABLE = dbutils.widgets.get("samples_table").strip() or "lakebase_perf_samples"
TICKS = max(1, int(dbutils.widgets.get("ticks") or "1"))
INTERVAL_SECONDS = max(0.0, float(dbutils.widgets.get("interval_seconds") or "60"))

print(f"Target:  project={PROJECT_ID or '(auto-pick)'}  branch={BRANCH}  endpoint={ENDPOINT}  db={DATABASE}")
print(f"Output:  {SAMPLES_TABLE}")
print(f"Ticks:   {TICKS}  interval={INTERVAL_SECONDS}s")

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

# MAGIC %md ## Sampling helpers

# COMMAND ----------

import uuid
import time
from datetime import datetime, timezone

# Top-N pg_stat_statements statements captured per tick (by total_exec_time).
_PGSS_TOP_N = 10


def _sample_pg_stat_database(conn):
    """Return tall metric rows from pg_stat_database for the current database.

    Stores raw cumulative counters; deltas are computed at analysis time
    between consecutive ticks (that is why we never subtract here).
    """
    row = conn.execute(
        """
        SELECT
            xact_commit,
            xact_rollback,
            blks_hit,
            blks_read,
            deadlocks,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted
        FROM pg_stat_database
        WHERE datname = current_database()
        """
    ).fetchone()
    if row is None:
        return []
    names = [
        "xact_commit", "xact_rollback",
        "blks_hit", "blks_read",
        "deadlocks",
        "tup_returned", "tup_fetched",
        "tup_inserted", "tup_updated", "tup_deleted",
    ]
    return [
        {"metric_name": name, "metric_value": float(val), "source": "pg_stat_database"}
        for name, val in zip(names, row)
    ]


def _sample_pg_stat_activity(conn):
    """Return session state-count rows from pg_stat_activity.

    Excludes this sampler's own backend (pid != pg_backend_pid()) to avoid
    inflating the 'active' count.
    """
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE state = 'active')              AS active,
            COUNT(*) FILTER (WHERE state = 'idle')                AS idle,
            COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
            COUNT(*)                                               AS total
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid != pg_backend_pid()
        """
    ).fetchone()
    if row is None:
        return []
    names = ["active", "idle", "idle_in_transaction", "total"]
    return [
        {"metric_name": f"sessions_{name}", "metric_value": float(val), "source": "pg_stat_activity"}
        for name, val in zip(names, row)
    ]


def _sample_pg_stat_statements(conn, top_n=_PGSS_TOP_N):
    """Return top-N statement rows from pg_stat_statements, or [] if not installed.

    Metric names follow the convention `pgss.<queryid>.<field>` so that a
    grouping pass at analysis time can reconstruct per-statement history.

    Captures `total_exec_time` and `calls` for each queryid, which together
    allow computing mean latency and per-interval rate.
    """
    # Confirm extension is installed before touching pg_stat_statements.
    ext_present = conn.execute(
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
    ).fetchone()
    if ext_present is None:
        return []

    rows = conn.execute(
        """
        SELECT
            queryid,
            total_exec_time,
            calls
        FROM pg_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        ORDER BY total_exec_time DESC
        LIMIT %s
        """,
        (top_n,),
    ).fetchall()

    out = []
    for queryid, total_exec_time, calls in rows:
        qid = str(queryid)
        out.append({
            "metric_name": f"pgss.{qid}.total_exec_time",
            "metric_value": float(total_exec_time),
            "source": "pg_stat_statements",
        })
        out.append({
            "metric_name": f"pgss.{qid}.calls",
            "metric_value": float(calls),
            "source": "pg_stat_statements",
        })
    return out


def collect_tick(conn, sample_id, sample_ts, project_id, endpoint_path):
    """Execute one sample tick and return a list of row dicts for lakebase_perf_samples."""
    base = {
        "sample_id": sample_id,
        "sample_ts": sample_ts,
        "project_id": project_id,
        "endpoint": endpoint_path,
    }
    metrics = (
        _sample_pg_stat_database(conn)
        + _sample_pg_stat_activity(conn)
        + _sample_pg_stat_statements(conn)
    )
    return [{**base, **m} for m in metrics]

# COMMAND ----------

# MAGIC %md ## Run sample ticks

# COMMAND ----------

import pandas as pd

all_rows = []

with psycopg.connect(**CONNECT_KWARGS) as sample_conn:
    for i in range(TICKS):
        if i > 0 and INTERVAL_SECONDS > 0:
            print(f"  Sleeping {INTERVAL_SECONDS:.0f}s before tick {i + 1}/{TICKS}...")
            time.sleep(INTERVAL_SECONDS)

        sample_id = str(uuid.uuid4())
        sample_ts = datetime.now(timezone.utc)
        print(f"Tick {i + 1}/{TICKS}  sample_id={sample_id[:8]}...  ts={sample_ts.isoformat()}")

        tick_rows = collect_tick(sample_conn, sample_id, sample_ts, proj, ENDPOINT_PATH)
        all_rows.extend(tick_rows)
        print(f"  Captured {len(tick_rows)} metric rows.")

print(f"\nTotal rows collected across {TICKS} tick(s): {len(all_rows)}")

# COMMAND ----------

# MAGIC %md ## Append to UC table and display

# COMMAND ----------

# Build the DataFrame with the exact column order the SPEC defines.
SCHEMA_COLS = ["sample_id", "sample_ts", "project_id", "endpoint", "metric_name", "metric_value", "source"]

df = pd.DataFrame(all_rows, columns=SCHEMA_COLS) if all_rows else pd.DataFrame(columns=SCHEMA_COLS)
# Ensure metric_value is float (DOUBLE in Delta).
if not df.empty:
    df["metric_value"] = df["metric_value"].astype(float)

if not df.empty:
    sdf = spark.createDataFrame(df)
    sdf.write.mode("append").saveAsTable(SAMPLES_TABLE)
    print(f"Appended {len(df)} row(s) to {SAMPLES_TABLE}.")
else:
    print("No rows collected — nothing written to the UC table.")

display(df)

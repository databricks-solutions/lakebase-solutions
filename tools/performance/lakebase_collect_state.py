# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase State Collector — point-in-time performance snapshot
# MAGIC
# MAGIC Captures a read-only snapshot of connection counts, cache hit rates, database
# MAGIC statistics, per-table working sets, and top-N slow queries into three Delta
# MAGIC output tables. Feeds runbook §2 (workload inputs) and §3 (baseline).
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Import this file into a Databricks workspace (notebook source), attach to any
# MAGIC    compute (serverless is fine), and **Run All**.
# MAGIC 2. Adjust widgets if needed. Defaults (`production` branch, `primary` endpoint,
# MAGIC    `databricks_postgres` database, top 20 queries) are correct for most sessions.
# MAGIC 3. Read the **summary** at the bottom — rows written per output table.
# MAGIC
# MAGIC No hostname, password, or token is supplied here. Host and OAuth token are
# MAGIC resolved from the workspace identity — the same file runs unchanged against any
# MAGIC project in any workspace.
# MAGIC
# MAGIC ## Output tables (appended per run)
# MAGIC | table | description |
# MAGIC |---|---|
# MAGIC | `lakebase_perf_metrics` | tall: one row per metric (connections, cache, counters, settings) |
# MAGIC | `lakebase_perf_tables` | per user table: size + heap/index IO |
# MAGIC | `lakebase_perf_queries` | top-N statements by total_exec_time (pg_stat_statements) |

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "psycopg[binary]>=3.1" "databricks-sdk>=0.81" pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("project_id",      "",                      "1. Lakebase project id (blank = auto-pick if only one)")
dbutils.widgets.text("branch",          "production",            "2. Branch (default production)")
dbutils.widgets.text("endpoint",        "primary",               "3. Endpoint/compute (default primary)")
dbutils.widgets.text("database",        "databricks_postgres",   "4. Postgres database name")
dbutils.widgets.text("top_n_queries",   "20",                    "5. Top-N queries by total_exec_time (pg_stat_statements)")
dbutils.widgets.text("metrics_table",   "lakebase_perf_metrics", "6. UC table: tall metrics output")
dbutils.widgets.text("tables_table",    "lakebase_perf_tables",  "7. UC table: per-table size/IO output")
dbutils.widgets.text("queries_table",   "lakebase_perf_queries", "8. UC table: top-N query output")

PROJECT_ID    = dbutils.widgets.get("project_id").strip()
BRANCH        = dbutils.widgets.get("branch").strip() or "production"
ENDPOINT      = dbutils.widgets.get("endpoint").strip() or "primary"
DATABASE      = dbutils.widgets.get("database").strip()
TOP_N         = int(dbutils.widgets.get("top_n_queries").strip() or "20")
METRICS_TABLE = dbutils.widgets.get("metrics_table").strip()
TABLES_TABLE  = dbutils.widgets.get("tables_table").strip()
QUERIES_TABLE = dbutils.widgets.get("queries_table").strip()

print(f"Branch: {BRANCH}  |  Endpoint: {ENDPOINT}  |  Database: {DATABASE}")
print(f"Top-N queries  : {TOP_N}")
print(f"metrics table  : {METRICS_TABLE}")
print(f"tables  table  : {TABLES_TABLE}")
print(f"queries table  : {QUERIES_TABLE}")

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

HOST  = endpoint.status.hosts.host
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

# MAGIC %md ## Run identity
# MAGIC One `run_id` + `run_ts` stamps every output row so runs append cleanly and are self-identifying.

# COMMAND ----------

import uuid
import datetime

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.datetime.utcnow()

print(f"run_id  : {RUN_ID}")
print(f"run_ts  : {RUN_TS.isoformat()}Z")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect metrics (tall)
# MAGIC Fills `lakebase_perf_metrics`. One row per metric:
# MAGIC - **§2.connection** — active / idle / total connection counts (`pg_stat_activity`)
# MAGIC - **§3.baseline** — cache hit %, xact counters, tuple counters, deadlocks (`pg_stat_database`)
# MAGIC - **§4.1.compute** — database size in bytes (`pg_database_size`)
# MAGIC - **§3.baseline** — key settings: `max_connections`, `shared_buffers`, server `version()`

# COMMAND ----------

def _metric_row(
    section,
    metric_name,
    metric_value=None,
    metric_text=None,
    unit=None,
    source=None,
):
    """Build one lakebase_perf_metrics row dict (all run-identity fields pre-populated)."""
    return {
        "run_id":       RUN_ID,
        "run_ts":       RUN_TS,
        "project_id":   proj,
        "endpoint":     ENDPOINT_PATH,
        "section":      section,
        "metric_name":  metric_name,
        "metric_value": float(metric_value) if metric_value is not None else None,
        "metric_text":  metric_text,
        "unit":         unit,
        "source":       source,
    }


metrics_rows = []

with psycopg.connect(**CONNECT_KWARGS) as conn:

    # ── §2.connection — active / idle / total from pg_stat_activity ──────────
    state_rows = conn.execute(
        "SELECT state, COUNT(*) AS cnt FROM pg_stat_activity GROUP BY state"
    ).fetchall()
    state_counts = {(r[0] if r[0] is not None else "__null__"): int(r[1]) for r in state_rows}
    active_count = state_counts.get("active", 0)
    idle_count   = state_counts.get("idle",   0)
    total_count  = sum(state_counts.values())

    metrics_rows.extend([
        _metric_row("2.connection", "connections_active", active_count, unit="conns", source="pg_stat_activity"),
        _metric_row("2.connection", "connections_idle",   idle_count,   unit="conns", source="pg_stat_activity"),
        _metric_row("2.connection", "connections_total",  total_count,  unit="conns", source="pg_stat_activity"),
    ])

    # ── §3.baseline — counters + cache hit % from pg_stat_database ───────────
    db_row = conn.execute(
        """
        SELECT
            blks_hit,
            blks_read,
            deadlocks,
            xact_commit,
            xact_rollback,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted
        FROM pg_stat_database
        WHERE datname = current_database()
        """
    ).fetchone()

    if db_row:
        (
            blks_hit, blks_read, deadlocks,
            xact_commit, xact_rollback,
            tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
        ) = [int(v) for v in db_row]

        total_blks   = blks_hit + blks_read
        cache_hit_pct = round(100.0 * blks_hit / total_blks, 4) if total_blks > 0 else None
        total_xact    = xact_commit + xact_rollback
        rollback_ratio_pct = round(100.0 * xact_rollback / total_xact, 4) if total_xact > 0 else None

        metrics_rows.extend([
            _metric_row("3.baseline", "cache_hit_pct",      cache_hit_pct,      unit="pct", source="pg_stat_database"),
            _metric_row("3.baseline", "rollback_ratio_pct", rollback_ratio_pct, unit="pct", source="pg_stat_database"),
            _metric_row("3.baseline", "deadlocks",       deadlocks,                   source="pg_stat_database"),
            _metric_row("3.baseline", "xact_commit",     xact_commit,                 source="pg_stat_database"),
            _metric_row("3.baseline", "xact_rollback",   xact_rollback,               source="pg_stat_database"),
            _metric_row("3.baseline", "tup_returned",    tup_returned,                source="pg_stat_database"),
            _metric_row("3.baseline", "tup_fetched",     tup_fetched,                 source="pg_stat_database"),
            _metric_row("3.baseline", "tup_inserted",    tup_inserted,                source="pg_stat_database"),
            _metric_row("3.baseline", "tup_updated",     tup_updated,                 source="pg_stat_database"),
            _metric_row("3.baseline", "tup_deleted",     tup_deleted,                 source="pg_stat_database"),
        ])

    # ── §4.1.compute — database size ─────────────────────────────────────────
    db_size = int(conn.execute(
        "SELECT pg_database_size(current_database())"
    ).fetchone()[0])
    metrics_rows.append(
        _metric_row("4.1.compute", "db_size_bytes", db_size, unit="bytes", source="pg_database_size")
    )

    # ── §3.baseline — key settings from pg_settings ──────────────────────────
    max_connections_val = None
    for setting_name in ("max_connections", "shared_buffers"):
        setting_row = conn.execute(
            "SELECT name, setting, unit FROM pg_settings WHERE name = %s",
            (setting_name,),
        ).fetchone()
        if setting_row:
            _, setting_val, setting_unit = setting_row
            try:
                numeric_val  = float(setting_val)
                text_val     = None
            except (ValueError, TypeError):
                numeric_val  = None
                text_val     = str(setting_val)
            if setting_name == "max_connections":
                max_connections_val = numeric_val
            metrics_rows.append(
                _metric_row(
                    "3.baseline",
                    f"setting_{setting_name}",
                    metric_value=numeric_val,
                    metric_text=text_val,
                    unit=(setting_unit.strip() if setting_unit else None),
                    source="pg_settings",
                )
            )

    # ── §2.connection — derived: connections as % of max_connections ─────────
    if max_connections_val and max_connections_val > 0:
        conn_pct = round(100.0 * total_count / max_connections_val, 4)
        metrics_rows.append(
            _metric_row("2.connection", "connections_pct_of_max", conn_pct, unit="pct",
                        source="pg_stat_activity+pg_settings")
        )

    # ── §3.baseline — server version string ──────────────────────────────────
    metrics_rows.append(
        _metric_row("3.baseline", "version", metric_text=ver, source="version")
    )

print(f"Collected {len(metrics_rows)} metric rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect per-table stats
# MAGIC Fills `lakebase_perf_tables` — total/heap/index size + working-set IO per user table
# MAGIC across all non-system schemas. Ordered by `total_bytes DESC`.

# COMMAND ----------

_TABLE_SQL = """
SELECT
    n.nspname                                    AS schema_name,
    c.relname                                    AS table_name,
    pg_total_relation_size(c.oid)                AS total_bytes,
    pg_relation_size(c.oid)                      AS heap_bytes,
    pg_indexes_size(c.oid)                       AS index_bytes,
    COALESCE(s.heap_blks_hit,  0)                AS heap_blks_hit,
    COALESCE(s.heap_blks_read, 0)                AS heap_blks_read
FROM pg_class c
JOIN pg_namespace n
  ON n.oid = c.relnamespace
LEFT JOIN pg_statio_user_tables s
  ON s.relid = c.oid
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY total_bytes DESC
"""

tables_rows = []

with psycopg.connect(**CONNECT_KWARGS) as conn:
    for row in conn.execute(_TABLE_SQL).fetchall():
        schema_name, table_name, total_bytes, heap_bytes, index_bytes, heap_blks_hit, heap_blks_read = row
        tables_rows.append({
            "run_id":         RUN_ID,
            "run_ts":         RUN_TS,
            "project_id":     proj,
            "endpoint":       ENDPOINT_PATH,
            "schema_name":    schema_name,
            "table_name":     table_name,
            "total_bytes":    int(total_bytes)      if total_bytes      is not None else 0,
            "heap_bytes":     int(heap_bytes)       if heap_bytes       is not None else 0,
            "index_bytes":    int(index_bytes)      if index_bytes      is not None else 0,
            "heap_blks_hit":  int(heap_blks_hit)    if heap_blks_hit    is not None else 0,
            "heap_blks_read": int(heap_blks_read)   if heap_blks_read   is not None else 0,
        })

print(f"Collected {len(tables_rows)} table rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect top-N queries
# MAGIC Fills `lakebase_perf_queries` from `pg_stat_statements` (top-N by `total_exec_time`).
# MAGIC Query text is truncated to 500 characters; literal redaction is left to the analyzer.
# MAGIC
# MAGIC If the extension is absent or inaccessible, one note row is appended to
# MAGIC `lakebase_perf_metrics` (`metric_name='pg_stat_statements'`, `metric_text='not installed'`)
# MAGIC and the queries table is skipped — the notebook never crashes on a missing extension.

# COMMAND ----------

# pg_stat_statements uses total_exec_time / mean_exec_time (Postgres 13+).
# Lakebase targets recent Postgres; if an older version surfaces total_time,
# the UndefinedColumn branch below falls back automatically.
_QUERY_SQL_PG13 = """
SELECT
    queryid,
    calls,
    mean_exec_time   AS mean_exec_ms,
    total_exec_time  AS total_exec_ms,
    rows,
    shared_blks_hit,
    shared_blks_read,
    LEFT(query, 500) AS query_sample
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT %s
"""

_QUERY_SQL_PG12 = """
SELECT
    queryid,
    calls,
    mean_time        AS mean_exec_ms,
    total_time       AS total_exec_ms,
    rows,
    shared_blks_hit,
    shared_blks_read,
    LEFT(query, 500) AS query_sample
FROM pg_stat_statements
ORDER BY total_time DESC
LIMIT %s
"""

queries_rows  = []
_pss_missing  = False

with psycopg.connect(**CONNECT_KWARGS) as conn:
    try:
        raw = conn.execute(_QUERY_SQL_PG13, (TOP_N,)).fetchall()
    except psycopg.errors.UndefinedColumn:
        # Older Postgres: fall back to total_time / mean_time column names.
        try:
            raw = conn.execute(_QUERY_SQL_PG12, (TOP_N,)).fetchall()
        except (
            psycopg.errors.UndefinedTable,
            psycopg.errors.UndefinedFunction,
            psycopg.errors.InsufficientPrivilege,
        ):
            raw          = []
            _pss_missing = True
        except Exception as _e:
            if "pg_stat_statements" in str(_e).lower() or "undefined" in str(_e).lower():
                raw          = []
                _pss_missing = True
            else:
                raise
    except (
        psycopg.errors.UndefinedTable,
        psycopg.errors.UndefinedFunction,
        psycopg.errors.InsufficientPrivilege,
    ):
        raw          = []
        _pss_missing = True
    except Exception as _e:
        if "pg_stat_statements" in str(_e).lower() or "undefined" in str(_e).lower():
            raw          = []
            _pss_missing = True
        else:
            raise

    for row in raw:
        queryid, calls, mean_exec_ms, total_exec_ms, rows, shared_blks_hit, shared_blks_read, query_sample = row
        queries_rows.append({
            "run_id":          RUN_ID,
            "run_ts":          RUN_TS,
            "project_id":      proj,
            "endpoint":        ENDPOINT_PATH,
            "queryid":         str(queryid)          if queryid          is not None else None,
            "calls":           int(calls),
            "mean_exec_ms":    float(mean_exec_ms),
            "total_exec_ms":   float(total_exec_ms),
            "rows":            int(rows),
            "shared_blks_hit": int(shared_blks_hit),
            "shared_blks_read":int(shared_blks_read),
            "query_sample":    query_sample,
        })

if _pss_missing:
    print("pg_stat_statements not installed or not accessible — recording note row; queries table skipped.")
    metrics_rows.append(
        _metric_row(
            "3.baseline",
            "pg_stat_statements",
            metric_text="not installed",
            source="pg_stat_statements",
        )
    )
else:
    print(f"Collected {len(queries_rows)} query rows (top-{TOP_N} by total_exec_time).")

# COMMAND ----------

# MAGIC %md ## Write output tables

# COMMAND ----------

import pandas as pd

# ── lakebase_perf_metrics ────────────────────────────────────────────────────
metrics_df = pd.DataFrame(metrics_rows)
spark.createDataFrame(metrics_df).write.mode("append").saveAsTable(METRICS_TABLE)
n_metrics = len(metrics_df)
print(f"Appended {n_metrics} row(s) to {METRICS_TABLE}")

# ── lakebase_perf_tables ─────────────────────────────────────────────────────
n_tables = 0
if tables_rows:
    tables_df = pd.DataFrame(tables_rows)
    spark.createDataFrame(tables_df).write.mode("append").saveAsTable(TABLES_TABLE)
    n_tables = len(tables_df)
    print(f"Appended {n_tables} row(s) to {TABLES_TABLE}")
else:
    print(f"No user tables found — {TABLES_TABLE} not written this run.")

# ── lakebase_perf_queries ────────────────────────────────────────────────────
n_queries = 0
if queries_rows:
    queries_df = pd.DataFrame(queries_rows)
    spark.createDataFrame(queries_df).write.mode("append").saveAsTable(QUERIES_TABLE)
    n_queries = len(queries_df)
    print(f"Appended {n_queries} row(s) to {QUERIES_TABLE}")
elif _pss_missing:
    print(f"pg_stat_statements absent — {QUERIES_TABLE} not written this run.")
else:
    print(f"pg_stat_statements returned no rows — {QUERIES_TABLE} not written this run.")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

summary = pd.DataFrame({
    "output_table": [METRICS_TABLE,    TABLES_TABLE, QUERIES_TABLE],
    "rows_written": [n_metrics,        n_tables,     n_queries],
    "description":  [
        "tall metrics (connections, cache, counters, settings)",
        "per-table size + IO",
        f"top-{TOP_N} statements by total_exec_time",
    ],
})

print(
    f"run_id  : {RUN_ID}\n"
    f"run_ts  : {RUN_TS.isoformat()}Z\n"
    f"project : {proj}\n"
    f"endpoint: {ENDPOINT_PATH}"
)
if _pss_missing:
    print("\nNOTE: pg_stat_statements not installed — query rows empty; note row written to metrics table.")

display(summary)

# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase — index remediation & before/after EXPLAIN
# MAGIC
# MAGIC Create (or verify) an index on a Lakebase / Postgres table **from inside the workspace** and
# MAGIC measure its impact with `EXPLAIN (ANALYZE, BUFFERS)` before and after. Running from a notebook
# MAGIC deliberately sidesteps two common blockers:
# MAGIC - **No external connectivity** — if private networking isn't set up yet, you can't reach the
# MAGIC   instance with `psql` / DBeaver from a laptop. A notebook reaches the endpoint over the
# MAGIC   workspace's internal network.
# MAGIC - **The SQL-editor statement timeout** that kills a long `CREATE INDEX`. Here we own the
# MAGIC   session and set `statement_timeout = 0`.
# MAGIC
# MAGIC ### Why an index can be "missing" even when a primary key exists
# MAGIC A composite B-tree index is only usable when the query filters on its **leading column**
# MAGIC (its left-to-right prefix). A predicate on a non-leading column *alone* can't use it, and
# MAGIC Postgres has no index skip-scan — so the planner falls back to a **sequential scan**. The fix
# MAGIC is an index whose **leading column is the column you filter on** (optionally covering the join
# MAGIC columns so the scan can stay index-only). This is a tuning gap, not an engine limitation.
# MAGIC
# MAGIC Companion to `lakebase_latency_probe.py` (which decomposes query latency into a ledger); this
# MAGIC notebook focuses on the **plan-shape change** an index produces.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use
# MAGIC
# MAGIC **Run from inside a Databricks workspace** (serverless compute is fine) — not from a laptop.
# MAGIC
# MAGIC 1. Run the `%pip` cell (installs the driver + SDK, restarts Python).
# MAGIC 2. Fill the widgets:
# MAGIC    - **Connection** — `project_id` (leave blank to auto-pick if the workspace has one
# MAGIC      project), `branch` (`production`), `endpoint` (`primary`), `database`
# MAGIC      (`databricks_postgres`). Host, user, and a short-lived OAuth token are resolved
# MAGIC      automatically — no hostname, password, or connection string to supply.
# MAGIC    - **Index** — `target_schema`, `target_table`, `index_name`, and `index_columns`
# MAGIC      (comma-separated; **put the filtered column first** — the leading column is what matters).
# MAGIC    - **benchmark_sql** — the query you want to speed up (defaults to an example join).
# MAGIC 3. Run top-to-bottom: baseline plan → build index + `ANALYZE` → after plan → confirm. Compare
# MAGIC    the two `EXPLAIN` outputs — a `Seq Scan` on the filtered table should become an
# MAGIC    `Index [Only] Scan`.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "psycopg[binary]>=3.1" "databricks-sdk>=0.81"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("project_id", "", "1. Lakebase project id (blank = auto-pick if only one)")
dbutils.widgets.text("branch", "production", "2. Branch (default production)")
dbutils.widgets.text("endpoint", "primary", "3. Endpoint/compute (default primary)")
dbutils.widgets.text("database", "databricks_postgres", "4. Postgres database name")
dbutils.widgets.text("target_schema", "public", "5. Schema of the table to index")
dbutils.widgets.text("target_table", "order_lines", "6. Table to index")
dbutils.widgets.text("index_name", "idx_order_lines_region", "7. Index name to create")
dbutils.widgets.text("index_columns", "region_id, order_id", "8. Index columns (filtered col FIRST; add join cols to cover)")
dbutils.widgets.dropdown("concurrently", "no", ["no", "yes"], "9. CREATE INDEX CONCURRENTLY? (yes if the table takes writes)")
dbutils.widgets.text(
    "benchmark_sql",
    "SELECT count(*)\nFROM public.orders o, public.order_lines l\nWHERE o.order_id = l.order_id\n  AND l.region_id = 'EXAMPLE_REGION'",
    "10. Query to EXPLAIN before/after",
)

PROJECT_ID = dbutils.widgets.get("project_id").strip()
BRANCH = dbutils.widgets.get("branch").strip() or "production"
ENDPOINT = dbutils.widgets.get("endpoint").strip() or "primary"
DATABASE = dbutils.widgets.get("database").strip()
TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip()
TARGET_TABLE = dbutils.widgets.get("target_table").strip()
INDEX_NAME = dbutils.widgets.get("index_name").strip()
INDEX_COLUMNS = dbutils.widgets.get("index_columns").strip()
CONCURRENTLY = dbutils.widgets.get("concurrently") == "yes"
BENCHMARK_SQL = dbutils.widgets.get("benchmark_sql").strip().rstrip(";")

# COMMAND ----------

# MAGIC %md ## Connect to Lakebase
# MAGIC Host from `get_endpoint`, OAuth token (valid ~1h) as the Postgres password, `sslmode=require`.

# COMMAND ----------

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

host = endpoint.status.hosts.host                                              # no hostname to supply
token = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH).token  # short-lived OAuth as PG password

conn = psycopg.connect(
    host=host,
    dbname=DATABASE,
    user=USERNAME,
    password=token,
    sslmode="require",
    autocommit=True,  # required for CREATE INDEX CONCURRENTLY
)
print(f"Connected as {USERNAME} to {host}/{DATABASE}")
print(conn.execute("select version()").fetchone()[0])

# COMMAND ----------

fq_table = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE)\n{BENCHMARK_SQL};"

# COMMAND ----------

# MAGIC %md ## 1. Baseline plan — before the index
# MAGIC Expect a `Seq Scan` (often `Parallel Seq Scan`) on the filtered table, with large actual time.

# COMMAND ----------

print("\n".join(r[0] for r in conn.execute(explain_sql).fetchall()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the index + refresh stats
# MAGIC `statement_timeout = 0` removes the editor-style timeout. Plain `CREATE INDEX` takes a brief
# MAGIC lock (fine for a static / POC table); set the `concurrently` widget to `yes` for a table that
# MAGIC takes writes (no table-level write lock, but slower and can leave an INVALID index on failure).

# COMMAND ----------

conn.execute("SET statement_timeout = 0")

ddl = (
    f"CREATE INDEX {'CONCURRENTLY ' if CONCURRENTLY else ''}IF NOT EXISTS {INDEX_NAME}\n"
    f"  ON {fq_table} ({INDEX_COLUMNS})"
)
print(ddl)
conn.execute(ddl)
conn.execute(f"ANALYZE {fq_table}")
print("index built + stats refreshed")

# COMMAND ----------

# MAGIC %md ## 3. After plan — expect an Index [Only] Scan instead of a Seq Scan

# COMMAND ----------

print("\n".join(r[0] for r in conn.execute(explain_sql).fetchall()))

# COMMAND ----------

# MAGIC %md ## 4. Confirm the index landed, then close

# COMMAND ----------

for r in conn.execute(f"""
  SELECT indexname, indexdef
  FROM pg_indexes
  WHERE schemaname = '{TARGET_SCHEMA}' AND tablename = '{TARGET_TABLE}'
  ORDER BY indexname
""").fetchall():
    print(r[0], "->", r[1])

conn.close()
print("done")

# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Admin Console ASH Collector — Active Session History
# MAGIC
# MAGIC Continuously samples PostgreSQL `pg_stat_activity` at a configurable interval and
# MAGIC writes aggregate session metrics to `<schema>.ash_history` plus per-query detail to
# MAGIC `<schema>.ash_query_log`. This is the **continuous** counterpart to the admin
# MAGIC console's inline sampler (`routes/admin.py` → `/live-dashboard/summary`): the console
# MAGIC only samples while a user is on the session-activity screen, so this job gives the
# MAGIC history chart real, gapless coverage even when nobody is looking. Old samples are
# MAGIC pruned after `retention_days` (default 7) so the chart shows now + up to 7 days.
# MAGIC
# MAGIC It writes the **exact same table shapes** the inline sampler creates, so the console
# MAGIC reads the history unchanged. Each deployment runs its OWN collector against its OWN
# MAGIC Lakebase instance (no central store).
# MAGIC
# MAGIC **Run this as a continuous Databricks job** (`continuous: {pause_status: UNPAUSED}`)
# MAGIC so it stays always-on and restarts if it stops — the loop below runs indefinitely.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Lakebase instance running with the console's `<schema>` (default `workshop`)
# MAGIC - Databricks Secrets scope (the deployment scope) with the console's OWN connection
# MAGIC   keys: `pghost`, `pgdatabase`, `admin_app-pguser`, `admin_app-pgpassword`
# MAGIC - The `admin_app` PG role has CREATE/DML on the schema + `pg_monitor`
# MAGIC
# MAGIC ### Parameters
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `secret_scope` | _(required)_ | Databricks Secrets scope holding the connection keys |
# MAGIC | `schema` | `workshop` | Schema the console administers (holds the ASH tables) |
# MAGIC | `interval_seconds` | `60` | Seconds between each sample |
# MAGIC | `retention_days` | `7` | Days of history to keep (older samples are pruned) |

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("secret_scope", "", "Secret Scope")
dbutils.widgets.text("schema", "workshop", "Schema")
dbutils.widgets.text("interval_seconds", "60", "Sample Interval (seconds)")
dbutils.widgets.text("retention_days", "7", "Retention (days)")

# COMMAND ----------

import re
import time
from datetime import datetime

import psycopg2

secret_scope = dbutils.widgets.get("secret_scope")
schema = dbutils.widgets.get("schema") or "workshop"
interval = int(dbutils.widgets.get("interval_seconds") or "60")
retention_days = int(dbutils.widgets.get("retention_days") or "7")

# Validate the schema identifier: it is interpolated into DDL/DML below, so guard
# against injection the same way the console does (see routes/admin.py).
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
    raise ValueError(f"invalid schema identifier: {schema!r}")

# Connection info + the console's OWN credentials all live in the deployment scope.
pg_host = dbutils.secrets.get(scope=secret_scope, key="pghost")
pg_database = dbutils.secrets.get(scope=secret_scope, key="pgdatabase")
pg_user = dbutils.secrets.get(scope=secret_scope, key="admin_app-pguser")
pg_password = dbutils.secrets.get(scope=secret_scope, key="admin_app-pgpassword")

print(f"Host:       {pg_host}")
print(f"Database:   {pg_database}")
print(f"User:       {pg_user[:20]}...")
print(f"Schema:     {schema}")
print(f"Interval:   {interval} seconds")
print(f"Retention:  {retention_days} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase

# COMMAND ----------

def get_connection():
    """Get a psycopg2 connection using native PG auth."""
    conn = psycopg2.connect(
        host=pg_host, port=5432,
        user=pg_user, password=pg_password,
        database=pg_database, sslmode="require",
    )
    conn.autocommit = True
    return conn

conn = get_connection()
print(f"Connected to {pg_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure ASH tables exist
# MAGIC
# MAGIC EXACT same shapes as the console's inline sampler (routes/admin.py) so the
# MAGIC `/live-dashboard/history` read works unchanged.

# COMMAND ----------

def ensure_tables(conn):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.ash_history (
            sample_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            active_sessions INTEGER DEFAULT 0,
            waiting_sessions INTEGER DEFAULT 0,
            blocked_sessions INTEGER DEFAULT 0,
            idle_in_txn INTEGER DEFAULT 0,
            total_sessions INTEGER DEFAULT 0,
            longest_sec INTEGER DEFAULT 0
        )
    """)
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_ash_history_time
        ON {schema}.ash_history (sample_time DESC)
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.ash_query_log (
            sample_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            pid INTEGER,
            usename TEXT,
            state TEXT,
            wait_event_type TEXT,
            wait_event TEXT,
            duration INTERVAL,
            query TEXT
        )
    """)
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_ash_query_log_time
        ON {schema}.ash_query_log (sample_time DESC)
    """)

try:
    ensure_tables(conn)
    print(f"{schema}.ash_history + {schema}.ash_query_log tables ready")
except Exception as e:
    # Table may already exist owned by admin — that's fine, we just need INSERT access.
    if "must be owner" in str(e) or "already exists" in str(e):
        print("ASH tables already exist (owned by admin) — OK")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sampling Loop (continuous)

# COMMAND ----------

# Aggregate counts — mirrors routes/admin.py's inline sampler SELECT.
SAMPLE_SQL = """
    SELECT
        COUNT(*) FILTER (WHERE state = 'active' AND pid != pg_backend_pid()) AS active_sessions,
        COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state = 'active' AND pid != pg_backend_pid()) AS waiting_sessions,
        COUNT(*) FILTER (WHERE wait_event_type = 'Lock' AND pid != pg_backend_pid()) AS blocked_queries,
        COALESCE(EXTRACT(EPOCH FROM MAX(clock_timestamp() - query_start)
            FILTER (WHERE state = 'active' AND pid != pg_backend_pid()))::int, 0) AS longest_sec,
        COUNT(*) FILTER (WHERE state = 'idle in transaction' AND pid != pg_backend_pid()) AS idle_in_txn
    FROM pg_stat_activity
    WHERE datname = current_database()
"""

INSERT_SQL = f"""
    INSERT INTO {schema}.ash_history
    (active_sessions, waiting_sessions, blocked_sessions, idle_in_txn, total_sessions, longest_sec)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# Per-session detail — mirrors routes/admin.py's inline ash_query_log INSERT SELECT.
QUERY_LOG_SQL = f"""
    INSERT INTO {schema}.ash_query_log (pid, usename, state, wait_event_type, wait_event, duration, query)
    SELECT pid, usename, state, wait_event_type, wait_event,
           clock_timestamp() - query_start, LEFT(query, 2000)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid != pg_backend_pid()
      AND state != 'idle'
      AND query IS NOT NULL
      AND query != ''
"""

PRUNE_SQL = f"""
    DELETE FROM {schema}.ash_history
    WHERE sample_time < NOW() - INTERVAL '{retention_days} days'
"""

PRUNE_QUERY_LOG_SQL = f"""
    DELETE FROM {schema}.ash_query_log
    WHERE sample_time < NOW() - INTERVAL '{retention_days} days'
"""

samples = 0
errors = 0

print(f"Starting continuous ASH sampling at {interval}s intervals (retention {retention_days}d)...")

# Runs indefinitely — the continuous Databricks job restarts it if it exits.
while True:
    try:
        cur = conn.cursor()
        cur.execute(SAMPLE_SQL)
        active, waiting, blocked, longest, idle_txn = cur.fetchone()

        cur.execute(INSERT_SQL,
                    (active, waiting, blocked, idle_txn,
                     active + waiting + blocked + idle_txn, longest))
        cur.execute(QUERY_LOG_SQL)

        samples += 1

        # Prune old data every 60 samples (~hourly at the 60s default).
        if samples % 60 == 0:
            cur.execute(PRUNE_SQL)
            pruned = cur.rowcount
            cur.execute(PRUNE_QUERY_LOG_SQL)
            if pruned > 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Pruned {pruned} old history samples")

        # Status every 30 samples.
        if samples % 30 == 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {samples} samples | "
                  f"Active:{active} Waiting:{waiting} Blocked:{blocked} Idle-Txn:{idle_txn}")

    except psycopg2.OperationalError as e:
        errors += 1
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connection error ({errors}): {e}")
        try:
            conn.close()
        except Exception:
            pass
        time.sleep(5)
        try:
            conn = get_connection()
            print("  Reconnected successfully")
        except Exception as e2:
            print(f"  Reconnect failed: {e2}")
            time.sleep(30)

    except Exception as e:
        # One bad pass must never kill the loop.
        errors += 1
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error ({errors}): {e}")

    time.sleep(interval)

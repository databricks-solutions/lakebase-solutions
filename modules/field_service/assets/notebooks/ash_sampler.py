# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakebase ASH Sampler — Active Session History
# MAGIC
# MAGIC Continuously samples PostgreSQL `pg_stat_activity` at a configurable interval and
# MAGIC writes aggregate session metrics to `field_service.ash_history` plus per-query
# MAGIC detail to `field_service.ash_query_log`. This data powers the "Live Query Dashboard"
# MAGIC in the admin UI, providing a time-series view of database activity, wait events,
# MAGIC and long-running queries. Old samples are automatically pruned after 24 hours.
# MAGIC
# MAGIC The sampler runs independently of the Databricks App so diagnostic data is always
# MAGIC collected, even during app restarts or redeployments.
# MAGIC
# MAGIC **Schedule this as a recurring Databricks job** (e.g., hourly with 55-min duration
# MAGIC for gapless coverage, or continuous with unlimited retries).
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Lakebase instance running with `field_service` schema
# MAGIC - Databricks Secrets scope (`lakebase-secrets`) with `pguser` and `pgpassword` keys
# MAGIC - PG role must have `SELECT` on `pg_stat_activity` and `INSERT` on `field_service.ash_history`
# MAGIC
# MAGIC ### Parameters
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `duration_minutes` | `55` | How long to sample before exiting |
# MAGIC | `sample_interval_sec` | `10` | Seconds between each sample |
# MAGIC | `secret_scope` | `lakebase-secrets` | Databricks Secrets scope name |
# MAGIC | `pg_host` | _(required)_ | Lakebase PostgreSQL hostname |
# MAGIC | `pg_database` | `databricks_postgres` | Database name |

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("duration_minutes", "55", "Duration (minutes)")
dbutils.widgets.text("sample_interval_sec", "10", "Sample Interval (seconds)")
dbutils.widgets.text("secret_scope", "lakebase-secrets", "Secret Scope")
dbutils.widgets.text("pg_host", "", "PG Host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG Database")

# COMMAND ----------

import time, traceback
from datetime import datetime, timedelta
import psycopg2

duration = int(dbutils.widgets.get("duration_minutes"))
interval = int(dbutils.widgets.get("sample_interval_sec"))
secret_scope = dbutils.widgets.get("secret_scope")
pg_host = dbutils.widgets.get("pg_host")
pg_database = dbutils.widgets.get("pg_database")

# Get credentials from Databricks secrets
pg_user = dbutils.secrets.get(scope=secret_scope, key="pguser")
pg_password = dbutils.secrets.get(scope=secret_scope, key="pgpassword")

print(f"Host:      {pg_host}")
print(f"Database:  {pg_database}")
print(f"User:      {pg_user[:20]}...")
print(f"Duration:  {duration} minutes")
print(f"Interval:  {interval} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase

# COMMAND ----------

def get_connection():
    """Get a psycopg2 connection using native PG auth."""
    conn = psycopg2.connect(
        host=pg_host, port=5432,
        user=pg_user, password=pg_password,
        database=pg_database, sslmode="require"
    )
    conn.autocommit = True
    return conn

conn = get_connection()
print(f"Connected to {pg_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure ash_history table exists

# COMMAND ----------

cur = conn.cursor()
try:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS field_service.ash_history (
            sample_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            active_sessions INTEGER DEFAULT 0,
            waiting_sessions INTEGER DEFAULT 0,
            blocked_sessions INTEGER DEFAULT 0,
            idle_in_txn INTEGER DEFAULT 0,
            total_sessions INTEGER DEFAULT 0,
            longest_sec INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ash_history_time
        ON field_service.ash_history (sample_time DESC)
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS field_service.ash_query_log (
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
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ash_query_log_time
        ON field_service.ash_query_log (sample_time DESC)
    """)
    print("ash_history + ash_query_log tables ready")
except Exception as e:
    # Table may already exist owned by admin — that's fine, we just need INSERT access
    if "must be owner" in str(e) or "already exists" in str(e):
        print(f"ash_history table already exists (owned by admin) — OK")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sampling Loop

# COMMAND ----------

SAMPLE_SQL = """
    SELECT
        COUNT(*) FILTER (WHERE state = 'active' AND pid != pg_backend_pid()) AS active_sessions,
        COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state = 'active' AND pid != pg_backend_pid()) AS waiting_sessions,
        COUNT(*) FILTER (WHERE wait_event_type = 'Lock' AND pid != pg_backend_pid()) AS blocked_queries,
        COALESCE(EXTRACT(EPOCH FROM MAX(clock_timestamp() - query_start)
            FILTER (WHERE state = 'active' AND pid != pg_backend_pid()))::int, 0) AS longest_sec,
        COUNT(*) FILTER (WHERE state = 'idle in transaction' AND pid != pg_backend_pid()) AS idle_in_txn,
        COUNT(*) AS total
    FROM pg_stat_activity
    WHERE datname = current_database()
"""

INSERT_SQL = """
    INSERT INTO field_service.ash_history
    (active_sessions, waiting_sessions, blocked_sessions, idle_in_txn, total_sessions, longest_sec)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

QUERY_LOG_SQL = """
    INSERT INTO field_service.ash_query_log (pid, usename, state, wait_event_type, wait_event, duration, query)
    SELECT pid, usename, state, wait_event_type, wait_event,
           clock_timestamp() - query_start, LEFT(query, 2000)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid != pg_backend_pid()
      AND state != 'idle'
      AND query IS NOT NULL
      AND query != ''
"""

PRUNE_SQL = """
    DELETE FROM field_service.ash_history
    WHERE sample_time < NOW() - INTERVAL '24 hours'
"""

PRUNE_QUERY_LOG_SQL = """
    DELETE FROM field_service.ash_query_log
    WHERE sample_time < NOW() - INTERVAL '24 hours'
"""

end_time = datetime.now() + timedelta(minutes=duration)
samples = 0
errors = 0

print(f"Starting ASH sampling at {interval}s intervals until {end_time.strftime('%H:%M:%S')}...")

while datetime.now() < end_time:
    try:
        cur = conn.cursor()
        cur.execute(SAMPLE_SQL)
        row = cur.fetchone()
        active, waiting, blocked, longest, idle_txn, total = row

        cur.execute(INSERT_SQL, (active, waiting, blocked, idle_txn, total, longest))
        cur.execute(QUERY_LOG_SQL)

        samples += 1

        # Prune old data every 100 samples
        if samples % 100 == 0:
            cur.execute(PRUNE_SQL)
            cur.execute(PRUNE_QUERY_LOG_SQL)
            pruned = cur.rowcount
            if pruned > 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Pruned {pruned} old samples")

        # Status every 30 samples
        if samples % 30 == 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {samples} samples | "
                  f"Active:{active} Waiting:{waiting} Blocked:{blocked} Idle-Txn:{idle_txn} Total:{total}")

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
            print(f"  Reconnected successfully")
        except Exception as e2:
            print(f"  Reconnect failed: {e2}")
            time.sleep(30)

    except Exception as e:
        errors += 1
        if errors > 50:
            print(f"Too many errors ({errors}), stopping.")
            break
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error ({errors}): {e}")
        time.sleep(interval)

    time.sleep(interval)

try:
    conn.close()
except Exception:
    pass

print(f"\nDone. {samples} samples collected, {errors} errors.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook samples `pg_stat_activity` every N seconds and writes to `field_service.ash_history`.
# MAGIC The Live Query Dashboard in the admin UI reads from this table to display the activity history chart.
# MAGIC
# MAGIC **To run continuously:** Schedule as a Databricks job with:
# MAGIC - Task type: Notebook
# MAGIC - Duration: 55 minutes (with 1-hour schedule = no gaps)
# MAGIC - Compute: Serverless
# MAGIC - Retry policy: Unlimited retries on failure

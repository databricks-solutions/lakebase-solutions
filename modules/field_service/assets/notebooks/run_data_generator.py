# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakebase Demo — Real-Time Data Generator
# MAGIC
# MAGIC Simulates live field service activity by writing realistic operational data directly
# MAGIC to Lakebase (PostgreSQL). This powers the real-time dashboards and demonstrates
# MAGIC Lakebase as a live transactional backend for field service operations.
# MAGIC
# MAGIC **Event distribution:**
# MAGIC - **Work orders** — new installs, repairs, maintenance (50% of events)
# MAGIC - **Dispatch** — assigning technicians and scheduling (25% of events)
# MAGIC - **Completions** — closing work orders with tech notes (15% of events)
# MAGIC - **Notes** — system and technician updates (10% of events)
# MAGIC
# MAGIC **Set the duration below and run all cells.** Credentials resolve automatically
# MAGIC from `deployment/config.yaml`.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Lakebase instance provisioned (run `deploy_all` first or `01_create_lakebase.py`)
# MAGIC - Tables created in `field_service` schema (run `02_create_tables.py`)
# MAGIC - `deployment/config.yaml` with valid `instance_name` and credentials
# MAGIC
# MAGIC ### Parameters
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `duration_minutes` | `5` | How long to generate data (minutes) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Dependencies

# COMMAND ----------

# MAGIC %pip install pyyaml "databricks-sdk>=0.87.0" psycopg2-binary --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Duration

# COMMAND ----------

# Duration widget — change this to run longer or shorter
dbutils.widgets.text("duration_minutes", "5", "Duration (minutes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Resolve Paths & Load Config

# COMMAND ----------

import os, sys
from pathlib import Path

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
# The generator module is a sibling of this notebook.
sys.path.insert(0, "/Workspace" + os.path.dirname(nb_path))

from realtime_data_generator import FieldServiceGenerator  # noqa: E402

# Widget-first (job base_params); no deployment/config.yaml. Creds from the scope.
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")
dbutils.widgets.text("schema", "field_service", "PG schema")

duration = int(dbutils.widgets.get("duration_minutes"))
host = dbutils.widgets.get("pg_host")
database = dbutils.widgets.get("pg_database") or "databricks_postgres"
schema = dbutils.widgets.get("schema") or "field_service"
_scope = dbutils.widgets.get("secret_scope")
user = dbutils.secrets.get(scope=_scope, key="pguser")
password = dbutils.secrets.get(scope=_scope, key="pgpassword")

print(f"Host:      {host}")
print(f"Database:  {database}")
print(f"Schema:    {schema}")
print(f"Duration:  {duration} minutes")

# COMMAND ----------

generator = FieldServiceGenerator(
    host=host, database=database, user=user, password=password,
    schema=schema, notebook_mode=True,
)

try:
    generator.run(duration_minutes=duration)
except KeyboardInterrupt:
    generator.stop()
    print("Stopped by user.")
except Exception as e:
    generator.stop()
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

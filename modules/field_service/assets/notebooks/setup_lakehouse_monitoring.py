# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakehouse Monitoring — Table Quality & Data Freshness
# MAGIC
# MAGIC Enables Databricks Lakehouse Monitoring on the pipeline's Iceberg tables
# MAGIC (quality, freshness, statistical drift). Best-effort: monitors are created
# MAGIC only for tables that exist and have data; a per-table failure is logged and
# MAGIC skipped so the step never blocks the deploy.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.87.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorSnapshot,
    MonitorTimeSeries,
    MonitorInferenceLog,
)

# Widget-first (job base_params); no deployment/config.yaml.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"

w = WorkspaceClient()
ASSETS_DIR = f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}"
OUTPUT_SCHEMA = f"{CATALOG}.{SCHEMA}"
print(f"Lakehouse Monitoring for {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Classification problem-type enum (string is rejected by the SDK serializer).
# Resolve defensively across SDK versions; None disables the inference monitor.
try:
    from databricks.sdk.service.catalog import MonitorInferenceLogProblemType as _PT
    _CLASSIFICATION = _PT.PROBLEM_TYPE_CLASSIFICATION
except Exception:
    _CLASSIFICATION = None


def _spec(kind: str):
    if kind == "timeseries":
        return {"time_series": MonitorTimeSeries(timestamp_col="timestamp", granularities=["1 day"])}
    if kind == "snapshot":
        return {"snapshot": MonitorSnapshot()}
    if kind == "inference":
        if _CLASSIFICATION is None:
            return None
        return {"inference_log": MonitorInferenceLog(
            problem_type=_CLASSIFICATION, prediction_col="prediction",
            label_col="needs_maintenance", timestamp_col="scored_at",
            granularities=["1 day"])}
    return {"snapshot": MonitorSnapshot()}


# (table, spec-kind) — only tables that exist + have rows are monitored.
MONITORS = [
    ("silver_iot_telemetry", "timeseries"),
    ("silver_vehicle_telemetry", "timeseries"),
    ("gold_iot_device_health", "snapshot"),
    ("gold_vehicle_health", "snapshot"),
    ("gold_node_maintenance_risk", "snapshot"),
    ("ml_predictions_maintenance", "inference"),
]

created, skipped, failed = [], [], []
for tbl, kind in MONITORS:
    fqn = f"{CATALOG}.{SCHEMA}.{tbl}"
    # Skip tables that don't exist or are empty (e.g. IoT when the simulator
    # hasn't run) — monitoring an empty table errors and adds no value.
    try:
        if spark.table(f"`{CATALOG}`.`{SCHEMA}`.`{tbl}`").limit(1).count() == 0:
            skipped.append(f"{tbl} (empty)")
            continue
    except Exception:
        skipped.append(f"{tbl} (missing)")
        continue
    # Idempotent: reuse an existing monitor.
    try:
        w.quality_monitors.get(fqn)
        created.append(f"{tbl} (exists)")
        continue
    except Exception:
        pass
    spec = _spec(kind)
    if spec is None:
        skipped.append(f"{tbl} (spec unavailable)")
        continue
    try:
        w.quality_monitors.create(
            table_name=fqn, assets_dir=ASSETS_DIR, output_schema_name=OUTPUT_SCHEMA, **spec)
        created.append(tbl)
        print(f"  created monitor: {tbl}")
    except Exception as e:  # best-effort — log + continue
        failed.append(f"{tbl}: {str(e)[:120]}")
        print(f"  WARN monitor {tbl} failed: {str(e)[:160]}")

summary = {"created": created, "skipped": skipped, "failed": failed}
print(json.dumps(summary, indent=1))
# Best-effort: monitoring failures never fail the step.
dbutils.notebook.exit(json.dumps(summary))  # noqa: F821

# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakehouse Monitoring — Table Quality & Data Freshness
# MAGIC
# MAGIC Enables Databricks Lakehouse Monitoring on key Iceberg tables to track:
# MAGIC - **Data quality** — null rates, schema drift, value distributions
# MAGIC - **Data freshness** — ingestion lag, update frequency
# MAGIC - **Statistical drift** — feature distribution changes over time
# MAGIC
# MAGIC **Databricks capabilities showcased:**
# MAGIC - Unity Catalog Lakehouse Monitoring
# MAGIC - Automated data quality rules
# MAGIC - Metric tables for dashboarding and alerting

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.87.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorInfo,
    MonitorSnapshot,
    MonitorTimeSeries,
    MonitorInferenceLog,
)

# Resolve catalog from deployment config
import os, yaml
# Widget-first (job base_params); no deployment/config.yaml.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"

w = WorkspaceClient()

print(f"Setting up Lakehouse Monitoring for {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor 1: Silver IoT Telemetry (Time Series)
# MAGIC
# MAGIC Tracks signal strength, latency, temperature, and other sensor readings
# MAGIC for distribution drift and anomalies over time.

# COMMAND ----------

TABLE_IOT = f"{CATALOG}.{SCHEMA}.silver_iot_telemetry"

try:
    monitor = w.quality_monitors.get(TABLE_IOT)
    print(f"Monitor already exists for {TABLE_IOT}")
    print(f"  Status: {monitor.status}")
    print(f"  Dashboard: {monitor.dashboard_id}")
except Exception as e:
    if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "ResourceDoesNotExist" in type(e).__name__:
        print(f"Creating time-series monitor for {TABLE_IOT}...")
        monitor = w.quality_monitors.create(
            table_name=TABLE_IOT,
            assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}",
            output_schema_name=f"{CATALOG}.{SCHEMA}",
            time_series=MonitorTimeSeries(
                timestamp_col="timestamp",
                granularities=["1 day"],
            ),
            slicing_exprs=["device_type", "infrastructure_id"],
        )
        print(f"Monitor created for {TABLE_IOT}")
        print(f"  Profile table: {monitor.profile_metrics_table_name}")
        print(f"  Drift table: {monitor.drift_metrics_table_name}")
    else:
        print(f"Error checking monitor: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor 2: Gold IoT Device Health (Snapshot)
# MAGIC
# MAGIC Monitors the aggregated health scores. Detects if health score distribution
# MAGIC shifts (e.g., more assets dropping below threshold).

# COMMAND ----------

TABLE_HEALTH = f"{CATALOG}.{SCHEMA}.gold_iot_device_health"

try:
    monitor = w.quality_monitors.get(TABLE_HEALTH)
    print(f"Monitor already exists for {TABLE_HEALTH}")
    print(f"  Status: {monitor.status}")
except Exception as e:
    if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "ResourceDoesNotExist" in type(e).__name__:
        print(f"Creating snapshot monitor for {TABLE_HEALTH}...")
        monitor = w.quality_monitors.create(
            table_name=TABLE_HEALTH,
            assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}",
            output_schema_name=f"{CATALOG}.{SCHEMA}",
            snapshot=MonitorSnapshot(),
            slicing_exprs=["infrastructure_id"],
        )
        print(f"Monitor created for {TABLE_HEALTH}")
        print(f"  Profile table: {monitor.profile_metrics_table_name}")
        print(f"  Drift table: {monitor.drift_metrics_table_name}")
    else:
        print(f"Error checking monitor: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor 3: Gold Node Maintenance Risk (Snapshot)
# MAGIC
# MAGIC Tracks the distribution of maintenance risk scores. Alerts if risk
# MAGIC concentration shifts unexpectedly (e.g., sudden spike in HIGH risk nodes).

# COMMAND ----------

TABLE_RISK = f"{CATALOG}.{SCHEMA}.gold_node_maintenance_risk"

try:
    monitor = w.quality_monitors.get(TABLE_RISK)
    print(f"Monitor already exists for {TABLE_RISK}")
    print(f"  Status: {monitor.status}")
except Exception as e:
    if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "ResourceDoesNotExist" in type(e).__name__:
        print(f"Creating snapshot monitor for {TABLE_RISK}...")
        monitor = w.quality_monitors.create(
            table_name=TABLE_RISK,
            assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}",
            output_schema_name=f"{CATALOG}.{SCHEMA}",
            snapshot=MonitorSnapshot(),
            slicing_exprs=["risk_category", "node_type"],
        )
        print(f"Monitor created for {TABLE_RISK}")
        print(f"  Profile table: {monitor.profile_metrics_table_name}")
        print(f"  Drift table: {monitor.drift_metrics_table_name}")
    else:
        print(f"Error checking monitor: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor 4: ML Predictions (Inference Log)
# MAGIC
# MAGIC If the predictive maintenance model has been deployed, monitor prediction
# MAGIC quality and drift in model inputs/outputs.

# COMMAND ----------

TABLE_PREDS = f"{CATALOG}.{SCHEMA}.ml_predictions_maintenance"

# Only create if the predictions table exists (model must have run first)
try:
    spark.table(f"`{CATALOG}`.`{SCHEMA}`.ml_predictions_maintenance")
    table_exists = True
except Exception:
    table_exists = False
    print(f"Predictions table {TABLE_PREDS} not found — run predictive_maintenance notebook first")

if table_exists:
    try:
        monitor = w.quality_monitors.get(TABLE_PREDS)
        print(f"Monitor already exists for {TABLE_PREDS}")
    except Exception as e:
        if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "ResourceDoesNotExist" in type(e).__name__:
            print(f"Creating inference monitor for {TABLE_PREDS}...")
            monitor = w.quality_monitors.create(
                table_name=TABLE_PREDS,
                assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}",
                output_schema_name=f"{CATALOG}.{SCHEMA}",
                inference_log=MonitorInferenceLog(
                    problem_type="classification",
                    prediction_col="prediction",
                    label_col="needs_maintenance",
                    timestamp_col="scored_at",
                    granularities=["1 day"],
                    model_id_col=None,
                ),
            )
            print(f"Monitor created for {TABLE_PREDS}")
        else:
            print(f"Error: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor 5: Silver Vehicle Telemetry (Time Series)
# MAGIC
# MAGIC Tracks engine temperature, oil life, and battery voltage for distribution drift —
# MAGIC the "we don't trust our telematics" answer. A drifting sensor distribution is the
# MAGIC early warning that raw data is degrading before it causes false-alarm dispatches.

# COMMAND ----------

TABLE_VEH_TEL = f"{CATALOG}.{SCHEMA}.silver_vehicle_telemetry"

try:
    monitor = w.quality_monitors.get(TABLE_VEH_TEL)
    print(f"Monitor already exists for {TABLE_VEH_TEL}")
    print(f"  Status: {monitor.status}")
except Exception as e:
    if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "ResourceDoesNotExist" in type(e).__name__:
        print(f"Creating time-series monitor for {TABLE_VEH_TEL}...")
        monitor = w.quality_monitors.create(
            table_name=TABLE_VEH_TEL,
            assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}",
            output_schema_name=f"{CATALOG}.{SCHEMA}",
            time_series=MonitorTimeSeries(
                timestamp_col="reading_ts",
                granularities=["1 day"],
            ),
            slicing_exprs=["health_profile", "region_code"],
        )
        print(f"Monitor created for {TABLE_VEH_TEL}")
        print(f"  Profile table: {monitor.profile_metrics_table_name}")
        print(f"  Drift table: {monitor.drift_metrics_table_name}")
    else:
        print(f"Error checking monitor: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor 6: Gold Vehicle Health (Snapshot)
# MAGIC
# MAGIC Monitors the per-vehicle maintenance-risk distribution. Alerts if the fleet's
# MAGIC risk concentration shifts (e.g., a spike in CRITICAL vehicles in one region).

# COMMAND ----------

TABLE_VEH_HEALTH = f"{CATALOG}.{SCHEMA}.gold_vehicle_health"

try:
    monitor = w.quality_monitors.get(TABLE_VEH_HEALTH)
    print(f"Monitor already exists for {TABLE_VEH_HEALTH}")
    print(f"  Status: {monitor.status}")
except Exception as e:
    if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "ResourceDoesNotExist" in type(e).__name__:
        print(f"Creating snapshot monitor for {TABLE_VEH_HEALTH}...")
        monitor = w.quality_monitors.create(
            table_name=TABLE_VEH_HEALTH,
            assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}",
            output_schema_name=f"{CATALOG}.{SCHEMA}",
            snapshot=MonitorSnapshot(),
            slicing_exprs=["risk_category", "region_code"],
        )
        print(f"Monitor created for {TABLE_VEH_HEALTH}")
        print(f"  Profile table: {monitor.profile_metrics_table_name}")
        print(f"  Drift table: {monitor.drift_metrics_table_name}")
    else:
        print(f"Error checking monitor: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor 7: Fleet ML Predictions (Inference Log)

# COMMAND ----------

TABLE_FLEET_PREDS = f"{CATALOG}.{SCHEMA}.ml_predictions_fleet"
try:
    spark.table(f"`{CATALOG}`.`{SCHEMA}`.ml_predictions_fleet")
    fleet_preds_exists = True
except Exception:
    fleet_preds_exists = False
    print(f"Predictions table {TABLE_FLEET_PREDS} not found — run score_fleet_work_orders first")

if fleet_preds_exists:
    try:
        monitor = w.quality_monitors.get(TABLE_FLEET_PREDS)
        print(f"Monitor already exists for {TABLE_FLEET_PREDS}")
    except Exception as e:
        if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "ResourceDoesNotExist" in type(e).__name__:
            print(f"Creating inference monitor for {TABLE_FLEET_PREDS}...")
            monitor = w.quality_monitors.create(
                table_name=TABLE_FLEET_PREDS,
                assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitoring/{SCHEMA}",
                output_schema_name=f"{CATALOG}.{SCHEMA}",
                inference_log=MonitorInferenceLog(
                    problem_type="classification",
                    prediction_col="prediction",
                    label_col="needs_maintenance",
                    timestamp_col="scored_at",
                    granularities=["1 day"],
                    model_id_col=None,
                ),
            )
            print(f"Monitor created for {TABLE_FLEET_PREDS}")
        else:
            print(f"Error: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh All Monitors
# MAGIC
# MAGIC Trigger an initial metrics refresh for all monitors.

# COMMAND ----------

tables = [TABLE_IOT, TABLE_HEALTH, TABLE_RISK, TABLE_VEH_TEL, TABLE_VEH_HEALTH]
if table_exists:
    tables.append(TABLE_PREDS)
if fleet_preds_exists:
    tables.append(TABLE_FLEET_PREDS)

for tbl in tables:
    try:
        w.quality_monitors.run_refresh(table_name=tbl)
        print(f"Refresh triggered: {tbl}")
    except Exception as e:
        print(f"Could not refresh {tbl}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Monitor Type | Slicing | Purpose |
# MAGIC |-------|-------------|---------|---------|
# MAGIC | `silver_iot_telemetry` | Time Series | device_type, infrastructure_id | Sensor drift detection |
# MAGIC | `gold_iot_device_health` | Snapshot | infrastructure_id | Health score distribution |
# MAGIC | `gold_node_maintenance_risk` | Snapshot | risk_category, node_type | Risk concentration shifts |
# MAGIC | `ml_predictions_maintenance` | Inference Log | — | Model quality monitoring |
# MAGIC | `silver_vehicle_telemetry` | Time Series | health_profile, region_code | Telematics sensor drift / data trust |
# MAGIC | `gold_vehicle_health` | Snapshot | risk_category, region_code | Fleet risk concentration shifts |
# MAGIC | `ml_predictions_fleet` | Inference Log | — | Fleet model quality monitoring |
# MAGIC
# MAGIC **Metric tables** are created in `dba-lakebase-network.network_data` with suffixes
# MAGIC `_profile_metrics` and `_drift_metrics`. Use these in Lakeview dashboards for
# MAGIC continuous data quality visibility.

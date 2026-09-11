# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Fleet Predictive Maintenance — Batch Scoring & Work Order Creation
# MAGIC
# MAGIC Loads the `@production` fleet maintenance model from Unity Catalog, scores every
# MAGIC vehicle against current telematics features in `gold_vehicle_health`, persists
# MAGIC predictions to Managed Iceberg, refreshes the operational `fleet_vehicles` snapshot
# MAGIC (health_score / risk_category / predicted_failure_date) in Lakebase, and writes
# MAGIC predictive **fleet_maintenance** work orders for at-risk vehicles. Duplicate work
# MAGIC orders are prevented by checking `work_order_number`.
# MAGIC
# MAGIC **Schedule daily** (alongside `score_and_create_work_orders`) to continuously turn
# MAGIC vehicle-failure predictions into the field-service workflow — and to keep techs off
# MAGIC vans that are about to break down (the dispatch tie-in).
# MAGIC
# MAGIC **Databricks capabilities showcased:**
# MAGIC - UC Model Registry (`@production`), batch inference on Managed Iceberg
# MAGIC - Lakebase write-back for operational work orders + health snapshot
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `fleet_predictive_maintenance` has run (fleet_maintenance_model @production)
# MAGIC - `gold_vehicle_health` populated; Lakebase fleet tables seeded

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.87.0" "mlflow[databricks]" psycopg2-binary lightgbm scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow, os, json, base64, yaml
from pathlib import Path
from datetime import datetime, timezone, timedelta
from databricks.sdk import WorkspaceClient

# Widget-first (job base_params); no deployment/config.yaml. PG creds from scope.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")

CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.fleet_maintenance_model"
PG_DB = dbutils.widgets.get("pg_database") or "databricks_postgres"
PG_HOST = dbutils.widgets.get("pg_host")
_secret_scope = dbutils.widgets.get("secret_scope")
PG_USER = dbutils.secrets.get(scope=_secret_scope, key="pguser")
PG_TOKEN = dbutils.secrets.get(scope=_secret_scope, key="pgpassword")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"Model: {MODEL_NAME}\nLakebase: {PG_HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Score all vehicles with the production model

# COMMAND ----------

feature_cols = [
    "odometer_km",
    "latest_engine_temp_c", "engine_temp_7d_max", "engine_temp_trend",
    "latest_oil_life_pct", "oil_life_7d_min",
    "latest_battery_voltage", "battery_7d_min",
    "latest_tire_pressure_psi", "harsh_events_7d", "avg_dtc_active",
]

scoring_df = spark.table("gold_vehicle_health").select(
    "vehicle_id", "make", "model", "region_code", "health_profile",
    "avg_health_score", "maintenance_risk_score", "risk_category", "odometer_km",
    *[c for c in feature_cols if c != "odometer_km"],
).fillna(0)
scoring_pd = scoring_df.toPandas()

mlflow.set_registry_uri("databricks-uc")
model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@production")
predictions = model.predict(scoring_pd[feature_cols])
try:
    proba = model._model_impl.predict_proba(scoring_pd[feature_cols])
    scoring_pd["confidence"] = [max(p) for p in proba]
except Exception:
    scoring_pd["confidence"] = 0.85
scoring_pd["prediction"] = predictions

at_risk = scoring_pd[scoring_pd["prediction"] == 1].copy()
print(f"Vehicles scored: {len(scoring_pd)} | at-risk: {len(at_risk)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Save predictions to Iceberg

# COMMAND ----------

from pyspark.sql import functions as F
spark.createDataFrame(scoring_pd).withColumn("scored_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.ml_predictions_fleet")
print(f"Saved {len(scoring_pd)} predictions to {CATALOG}.{SCHEMA}.ml_predictions_fleet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Refresh fleet_vehicles snapshot + create predictive work orders

# COMMAND ----------

import psycopg2

REGION_MAP = {"PNW": 1, "SW": 2, "SC": 3, "SE": 4, "MW": 5, "NE": 6}

def risk_to_priority(cat):
    return {"CRITICAL": "critical", "HIGH": "high", "MEDIUM": "medium"}.get(cat, "low")

def days_to_failure(cat):
    return {"CRITICAL": 7, "HIGH": 21, "MEDIUM": 60}.get(cat, None)

conn = psycopg2.connect(host=PG_HOST, port=5432, dbname=PG_DB, user=PG_USER, password=PG_TOKEN, sslmode="require")
conn.autocommit = True
cur = conn.cursor()
now = datetime.now(timezone.utc)

# Refresh the operational snapshot for ALL scored vehicles
for _, r in scoring_pd.iterrows():
    cat = r["risk_category"]
    d2f = days_to_failure(cat)
    pred_date = (now + timedelta(days=d2f)).date() if d2f else None
    cur.execute("""
        UPDATE field_service.fleet_vehicles
        SET health_score = %s, risk_category = %s, predicted_failure_date = %s, updated_at = %s
        WHERE vehicle_id = %s
    """, (round(float(r["avg_health_score"]), 1), cat, pred_date, now, r["vehicle_id"]))

# Create predictive work orders for at-risk vehicles
created = 0
for _, r in at_risk.iterrows():
    vehicle_id = r["vehicle_id"]
    cat = r["risk_category"]
    priority = risk_to_priority(cat)
    confidence = round(float(r["confidence"]), 4)
    region_id = REGION_MAP.get(r["region_code"], 1)
    sla_hours = {"critical": 8, "high": 24, "medium": 72, "low": 120}[priority]
    wo_number = f"FM-{now.strftime('%m%d')}-{vehicle_id}"

    cur.execute("SELECT 1 FROM field_service.work_orders WHERE work_order_number = %s", (wo_number,))
    if cur.fetchone():
        continue

    title = f"Predictive Fleet Maintenance: {vehicle_id} ({r['make']} {r['model']})"
    description = (
        f"Fleet PdM model predicts this vehicle needs maintenance.\n\n"
        f"Vehicle: {vehicle_id} ({r['make']} {r['model']})\n"
        f"Region: {r['region_code']}\n"
        f"Maintenance Risk: {r['maintenance_risk_score']}/100 ({cat})\n"
        f"Model Confidence: {confidence:.1%}\n"
        f"Odometer: {r['odometer_km']:.0f} km\n"
        f"Engine temp (7d max): {r['engine_temp_7d_max']} C\n"
        f"Oil life (7d min): {r['oil_life_7d_min']}%\n"
        f"Battery (7d min): {r['battery_7d_min']} V\n"
        f"Active fault codes (avg): {r['avg_dtc_active']}\n\n"
        f"Auto-created by the fleet predictive maintenance pipeline."
    )
    cur.execute("""
        INSERT INTO field_service.work_orders (
            work_order_number, category, subcategory, priority, status,
            title, description, reported_issue,
            predicted_category, predicted_priority, confidence_score,
            region_id, vehicle_id, sla_due_at, created_at, updated_at
        ) VALUES (
            %s, 'maintenance', 'fleet_maintenance', %s, 'open',
            %s, %s, %s,
            'maintenance', %s, %s,
            %s, %s, %s, %s, %s
        )
    """, (
        wo_number, priority, title, description,
        f"Fleet PdM prediction: risk {r['maintenance_risk_score']}/100",
        priority, confidence, region_id, vehicle_id,
        now + timedelta(hours=sla_hours), now, now,
    ))
    created += 1

cur.close()
conn.close()
print(f"Refreshed {len(scoring_pd)} fleet_vehicles snapshots; created {created} predictive fleet work orders.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | Component | Detail |
# MAGIC |------|-----------|--------|
# MAGIC | 1 | Batch scoring | `fleet_maintenance_model@production` over `gold_vehicle_health` |
# MAGIC | 2 | Predictions | `ml_predictions_fleet` (Iceberg) |
# MAGIC | 3 | Write-back | fleet_vehicles snapshot refresh + `fleet_maintenance` work orders |
# MAGIC
# MAGIC **Schedule daily** to keep predictions and the Fleet dashboard current.

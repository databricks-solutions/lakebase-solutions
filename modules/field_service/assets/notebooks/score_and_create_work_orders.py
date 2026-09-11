# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Predictive Maintenance — Batch Scoring & Work Order Creation
# MAGIC
# MAGIC Loads the `@production` model from Unity Catalog Model Registry, scores every
# MAGIC active network infrastructure asset against current IoT health and 7-day trend
# MAGIC features, persists predictions to a Managed Iceberg table, and writes predictive
# MAGIC maintenance work orders directly to Lakebase (PostgreSQL) for at-risk assets.
# MAGIC Duplicate work orders are prevented by checking `work_order_number` before insert.
# MAGIC
# MAGIC **Schedule this notebook daily** (via `deploy_all` Step 15 or manually) to
# MAGIC continuously operationalize predictions into the field service workflow.
# MAGIC
# MAGIC **Databricks capabilities showcased:**
# MAGIC - Unity Catalog Model Registry (`@production` alias)
# MAGIC - Batch inference on Managed Iceberg tables
# MAGIC - Lakebase PostgreSQL write-back for operational work orders
# MAGIC - End-to-end ML operationalization (model to action)
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `predictive_maintenance` notebook has been run (model registered with `@production` alias)
# MAGIC - DLT pipeline gold tables populated (`gold_node_maintenance_risk`, `gold_daily_node_health`)
# MAGIC - Lakebase instance running with `field_service.work_orders` table
# MAGIC - `deployment/config.yaml` with PG connection details, or widget parameters set
# MAGIC
# MAGIC ### Parameters
# MAGIC | Widget | Fallback | Description |
# MAGIC |--------|----------|-------------|
# MAGIC | `pg_host` | `config.yaml` | Lakebase PostgreSQL hostname |
# MAGIC | `pg_database` | `databricks_postgres` | Database name |
# MAGIC | `pg_user` | `app_sp_id` from config | PG login role |
# MAGIC | `pg_password` | `app_password` from config | PG password |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install Dependencies
# MAGIC
# MAGIC `lightgbm` is required here because MLflow deserializes the trained model,
# MAGIC which needs the original library to reconstruct the estimator.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.87.0" "mlflow[databricks]" psycopg2-binary lightgbm scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0b: Resolve Configuration

# COMMAND ----------

import mlflow
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone

# Configuration — widget-first (job base_params); no deployment/config.yaml.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")

CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.predictive_maintenance_model"

# Lakebase creds come from the secret scope (never plaintext job params).
PG_HOST = dbutils.widgets.get("pg_host")
PG_DB = dbutils.widgets.get("pg_database") or "databricks_postgres"
_secret_scope = dbutils.widgets.get("secret_scope")
PG_USER = dbutils.secrets.get(scope=_secret_scope, key="pguser")
PG_PASS = dbutils.secrets.get(scope=_secret_scope, key="pgpassword")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print(f"Model: {MODEL_NAME}")
print(f"Lakebase: {PG_HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Scoring Features
# MAGIC Same feature engineering as training, but for all active nodes.

# COMMAND ----------

# Node risk baseline
node_risk = spark.table("gold_node_maintenance_risk").select(
    "node_id", "node_name", "node_type", "region_code",
    "age_days", "days_since_maintenance",
    F.col("has_backup_power").cast("int").alias("has_backup_power"),
    "recent_avg_health", "recent_outage_count", "maintenance_risk_score",
)

# Daily trend features (7-day window)
daily_health = spark.table("gold_daily_node_health")
w7 = Window.partitionBy("node_id").orderBy("measurement_date").rowsBetween(-6, 0)

daily_features = daily_health.withColumn(
    "health_score_7d_avg", F.round(F.avg("health_score").over(w7), 1)
).withColumn(
    "health_score_7d_min", F.min("health_score").over(w7)
).withColumn(
    "latency_7d_max", F.round(F.max("avg_latency_ms").over(w7), 1)
).withColumn(
    "errors_7d_sum", F.sum("total_errors").over(w7)
).withColumn(
    "health_score_trend",
    F.round(F.col("health_score") - F.avg("health_score").over(w7), 1)
)

latest_daily = daily_features.withColumn(
    "rn", F.row_number().over(Window.partitionBy("node_id").orderBy(F.desc("measurement_date")))
).filter("rn = 1").drop("rn", "measurement_date")

# Assemble scoring dataset
scoring_df = node_risk.join(
    latest_daily.select(
        "node_id", "health_score_7d_avg", "health_score_7d_min",
        "latency_7d_max", "errors_7d_sum", "health_score_trend",
    ),
    on="node_id", how="left",
).withColumn(
    "node_type_idx",
    F.when(F.col("node_type") == "router", 0)
     .when(F.col("node_type") == "switch", 1)
     .when(F.col("node_type") == "access_point", 2)
     .when(F.col("node_type") == "fiber_terminal", 3)
     .when(F.col("node_type") == "repeater", 4)
     .otherwise(5)
).fillna(0)

print(f"Scoring {scoring_df.count()} assets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Score with Production Model

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@production")

feature_cols = [
    "node_type_idx", "age_days", "days_since_maintenance", "has_backup_power",
    "recent_avg_health", "recent_outage_count", "maintenance_risk_score",
    "health_score_7d_avg", "health_score_7d_min", "latency_7d_max",
    "errors_7d_sum", "health_score_trend",
]

# Score
scoring_pd = scoring_df.toPandas()
predictions = model.predict(scoring_pd[feature_cols])

# Try to get class probabilities for a confidence score.
# Not all model types support predict_proba (e.g. some sklearn pipelines),
# so we fall back to a fixed 0.85 confidence if unavailable.
try:
    probabilities = model._model_impl.predict_proba(scoring_pd[feature_cols])
    scoring_pd["confidence"] = [max(p) for p in probabilities]
except Exception:
    scoring_pd["confidence"] = 0.85  # fallback

scoring_pd["prediction"] = predictions

# Filter to at-risk assets (prediction = 1 = needs maintenance)
at_risk = scoring_pd[scoring_pd["prediction"] == 1].copy()

print(f"Total assets scored: {len(scoring_pd)}")
print(f"At-risk assets: {len(at_risk)}")
display(spark.createDataFrame(at_risk[["node_id", "node_name", "node_type", "region_code",
                                        "maintenance_risk_score", "prediction", "confidence"]]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save Predictions to Iceberg

# COMMAND ----------

predictions_spark = spark.createDataFrame(scoring_pd)
predictions_spark.withColumn(
    "scored_at", F.current_timestamp()
).write.mode("overwrite").saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.ml_predictions_maintenance")

print(f"Saved {len(scoring_pd)} predictions to {CATALOG}.{SCHEMA}.ml_predictions_maintenance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Predictive Maintenance Work Orders in Lakebase

# COMMAND ----------

import psycopg2
from datetime import timedelta

if len(at_risk) == 0:
    print("No at-risk assets — no work orders to create.")
    dbutils.notebook.exit("No at-risk assets")

# Map region_code strings to integer region_id foreign keys in the work_orders table.
# Both abbreviated and full region names are supported for compatibility.
REGION_MAP = {"PNW": 1, "SW": 2, "SC": 3, "SE": 4, "MW": 5, "NE": 6,
              "MTN_WEST": 1, "BAY_AREA": 2, "SOUTH_CENTRAL": 3,
              "SOUTHEAST": 4, "MIDWEST": 5, "NORTHEAST": 6}

# Priority based on risk score
def risk_to_priority(score):
    if score >= 75: return "critical"
    if score >= 50: return "high"
    if score >= 35: return "medium"
    return "low"

# Connect to Lakebase
conn = psycopg2.connect(
    host=PG_HOST,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASS,
    sslmode="require",
)
conn.autocommit = True
cursor = conn.cursor()

created_count = 0
now = datetime.now(timezone.utc)

for _, row in at_risk.iterrows():
    node_id = row["node_id"]
    node_name = row["node_name"]
    node_type = row["node_type"]
    region = row["region_code"]
    risk_score = row["maintenance_risk_score"]
    confidence = round(row["confidence"], 4)
    priority = risk_to_priority(risk_score)

    # Work order number encodes date + node ID for idempotent daily runs.
    # Format: PM-MMDD-<first12chars_of_node_id> ensures one WO per node per day.
    wo_number = f"PM-{now.strftime('%m%d')}-{node_id.replace('-', '')[:12]}"

    # Idempotency check: skip if this exact WO already exists (safe for re-runs)
    cursor.execute("""
        SELECT 1 FROM field_service.work_orders
        WHERE work_order_number = %s
    """, (wo_number,))

    if cursor.fetchone():
        continue  # Skip duplicate

    region_id = REGION_MAP.get(region, 1)
    sla_hours = {"critical": 4, "high": 8, "medium": 24, "low": 48}[priority]

    title = f"Predictive Maintenance: {node_name} ({node_type})"
    description = (
        f"AutoML model predicts this asset needs maintenance within 48 hours.\n\n"
        f"Asset: {node_name} ({node_id})\n"
        f"Type: {node_type}\n"
        f"Region: {region}\n"
        f"Maintenance Risk Score: {risk_score}/100\n"
        f"Model Confidence: {confidence:.1%}\n"
        f"Health Score (7d avg): {row.get('health_score_7d_avg', 'N/A')}\n"
        f"Days Since Last Maintenance: {row.get('days_since_maintenance', 'N/A')}\n"
        f"Age (days): {row.get('age_days', 'N/A')}\n\n"
        f"This work order was automatically created by the predictive maintenance pipeline."
    )

    cursor.execute("""
        INSERT INTO field_service.work_orders (
            work_order_number, category, subcategory, priority, status,
            title, description, reported_issue,
            predicted_category, predicted_priority, confidence_score,
            region_id, sla_due_at, created_at, updated_at
        ) VALUES (
            %s, 'maintenance', 'predictive_maintenance', %s, 'open',
            %s, %s, %s,
            'maintenance', %s, %s,
            %s, %s, %s, %s
        )
    """, (
        wo_number, priority,
        title, description, f"ML model prediction: risk score {risk_score}/100",
        priority, confidence,
        region_id, now + timedelta(hours=sla_hours), now, now,
    ))
    created_count += 1

cursor.close()
conn.close()

print(f"\nCreated {created_count} predictive maintenance work orders in Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | Component | Detail |
# MAGIC |------|-----------|--------|
# MAGIC | 1 | Feature Engineering | IoT health + node risk + 7-day trends |
# MAGIC | 2 | Batch Scoring | Production model from UC Model Registry |
# MAGIC | 3 | Predictions Table | `ml_predictions_maintenance` (Iceberg) |
# MAGIC | 4 | Work Orders | Predictive WOs written to Lakebase |
# MAGIC
# MAGIC **Schedule this notebook as a daily job** to continuously create predictive work orders.

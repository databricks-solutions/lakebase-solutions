# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Predictive Maintenance — Model Training & Registration
# MAGIC
# MAGIC Trains a binary classification model to predict which network infrastructure assets
# MAGIC will need maintenance within the next 48 hours. Features are engineered from
# MAGIC Managed Iceberg gold tables produced by the DLT streaming pipeline: IoT device
# MAGIC health, node maintenance risk scores, and 7-day rolling trends. Three model
# MAGIC families (LightGBM, Random Forest, Logistic Regression) are evaluated with
# MAGIC cross-validated hyperparameter search, and the best model by F1 score is
# MAGIC registered in Unity Catalog with a `@production` alias.
# MAGIC
# MAGIC **Databricks capabilities showcased:**
# MAGIC - Unity Catalog Managed Iceberg tables as training data source
# MAGIC - MLflow Experiment Tracking with hyperparameter search
# MAGIC - Model Registry in Unity Catalog (3-level namespace)
# MAGIC - Feature engineering from streaming IoT data (7-day windows)
# MAGIC - Production alias for model serving
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - DLT pipeline has run at least once (gold tables must have data)
# MAGIC - Pipeline catalog exists (e.g. `dba-lakebase-network`)
# MAGIC - `deployment/config.yaml` with `pipeline_catalog` set
# MAGIC
# MAGIC ### Parameters
# MAGIC This notebook reads configuration from `deployment/config.yaml` (no widgets).
# MAGIC It can be called standalone or from `deploy_all.py` via `dbutils.notebook.run()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install Dependencies
# MAGIC
# MAGIC `lightgbm` must be installed in the scoring environment too (MLflow deserializes the model).

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.87.0" "mlflow[databricks]" lightgbm scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0b: Resolve Configuration

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration — resolve catalog from deployment config
# Catalog/schema come from the job base_params (widgets) FIRST; a deployment
# config.yaml is an optional fallback; the hardcoded default is a last resort.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
_w_cat = dbutils.widgets.get("catalog")
_w_schema = dbutils.widgets.get("schema")

import os, yaml
from pathlib import Path as _Path
_cfg = {}
try:
    _repo_root = _Path(os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    ).replace("/notebooks", ""))
    _config_path = _Path("/Workspace") / str(_repo_root).lstrip("/") / "deployment" / "config.yaml"
    if _config_path.exists():
        with open(_config_path) as _f:
            _cfg = yaml.safe_load(_f) or {}
except Exception:
    _cfg = {}

CATALOG = _w_cat or _cfg.get("pipeline_catalog", "dba-lakebase-network")
SCHEMA = _w_schema or "network_data"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.predictive_maintenance_model"
EXPERIMENT_NAME = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/predictive_maintenance"

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Model: {MODEL_NAME}")
print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Training Features
# MAGIC
# MAGIC Combine IoT health scores, network node risk factors, and historical patterns
# MAGIC into a feature table suitable for binary classification.

# COMMAND ----------

# Read gold IoT device health (aggregated per infrastructure asset)
iot_health = spark.table("gold_iot_device_health").select(
    "infrastructure_id",
    "device_count",
    "avg_signal_dbm",
    "avg_throughput_mbps",
    "avg_latency_ms",
    "avg_packet_loss_pct",
    "avg_temperature_c",
    "avg_battery_pct",
    "total_connected_clients",
    "total_errors",
    "iot_health_score",
)

print(f"IoT health records: {iot_health.count()}")
display(iot_health)

# COMMAND ----------

# Read network node maintenance risk scores
node_risk = spark.table("gold_node_maintenance_risk").select(
    "node_id",
    "node_type",
    "region_code",
    "age_days",
    "days_since_maintenance",
    F.col("has_backup_power").cast("int").alias("has_backup_power"),
    "recent_avg_health",
    "recent_outage_count",
    "maintenance_risk_score",
    "risk_category",
)

print(f"Node risk records: {node_risk.count()}")
display(node_risk)

# COMMAND ----------

# Read daily node health for trend features (rolling averages, slope)
daily_health = spark.table("gold_daily_node_health").select(
    "node_id",
    "measurement_date",
    "health_score",
    "avg_latency_ms",
    "avg_packet_loss_pct",
    "total_errors",
    "avg_connected_users",
)

# Compute 7-day rolling features per node
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

# Take the most recent row per node
latest_daily = daily_features.withColumn(
    "rn", F.row_number().over(Window.partitionBy("node_id").orderBy(F.desc("measurement_date")))
).filter("rn = 1").drop("rn", "measurement_date")

print(f"Daily feature records: {latest_daily.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Labels
# MAGIC
# MAGIC For each node, determine if it experienced a maintenance event (outage or dispatch)
# MAGIC in the observation window. This becomes our binary target: **needs_maintenance**.

# COMMAND ----------

# Check recent outages — nodes with recent outages that required dispatch
outages = spark.table("silver_network_outages").filter(
    "outage_date >= DATE_SUB(current_date(), 30) AND dispatch_required = TRUE"
).groupBy("node_id").agg(
    F.count("*").alias("recent_dispatches"),
    F.max("outage_date").alias("last_dispatch_date"),
)

print(f"Nodes with recent dispatches: {outages.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Assemble Training Dataset

# COMMAND ----------

# Join node risk + daily features + outage labels
training_df = node_risk.join(
    latest_daily.select(
        "node_id",
        "health_score_7d_avg",
        "health_score_7d_min",
        "latency_7d_max",
        "errors_7d_sum",
        "health_score_trend",
    ),
    on="node_id",
    how="left",
).join(
    outages,
    on="node_id",
    how="left",
).fillna({"recent_dispatches": 0})

# Create binary label: needs_maintenance if high risk OR recent dispatch
training_df = training_df.withColumn(
    "needs_maintenance",
    F.when(
        (F.col("maintenance_risk_score") >= 50) |
        (F.col("recent_dispatches") > 0) |
        (F.col("recent_avg_health") < 60),
        1
    ).otherwise(0).cast("int")
)

# Convert categoricals for AutoML
training_df = training_df.withColumn(
    "node_type_idx",
    F.when(F.col("node_type") == "router", 0)
     .when(F.col("node_type") == "switch", 1)
     .when(F.col("node_type") == "access_point", 2)
     .when(F.col("node_type") == "fiber_terminal", 3)
     .when(F.col("node_type") == "repeater", 4)
     .otherwise(5)
)

# Select features for training (drop identifiers and raw labels)
feature_cols = [
    "node_type_idx", "age_days", "days_since_maintenance", "has_backup_power",
    "recent_avg_health", "recent_outage_count", "maintenance_risk_score",
    "health_score_7d_avg", "health_score_7d_min", "latency_7d_max",
    "errors_7d_sum", "health_score_trend",
    "needs_maintenance",
]

training_final = training_df.select(*feature_cols).dropna()

# Save as a managed table for reproducibility
training_final.write.mode("overwrite").saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.ml_training_maintenance")

print(f"Training dataset: {training_final.count()} rows, {len(feature_cols)} columns")
print(f"Label distribution:")
display(training_final.groupBy("needs_maintenance").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train Models with Hyperparameter Search
# MAGIC
# MAGIC Trains LightGBM, Random Forest, and Logistic Regression with hyperparameter
# MAGIC search, all logged to MLflow. Selects the best model by F1 score.

# COMMAND ----------

import lightgbm as lgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
import numpy as np
import pandas as pd

# Point MLflow at our experiment; disable autolog so we control what gets logged
# (autolog can create excessive runs with nested cross-validation)
mlflow.set_experiment(EXPERIMENT_NAME)
# MLflow's sklearn serializer refuses to save a LightGBM estimator unless the
# LightGBM types are declared trusted, failing with
#   "The saved sklearn model references untrusted types ... set the
#    'skops_trusted_types' parameter"
# The notebook caught that and carried on, so the job reported SUCCESS while
# predictive_maintenance_model was never registered.
SKOPS_TRUSTED_TYPES = [
    "collections.OrderedDict",
    "lightgbm.basic.Booster",
    "lightgbm.sklearn.LGBMClassifier",
]

mlflow.sklearn.autolog(disable=True)

training_pd = training_final.toPandas()
X = training_pd.drop(columns=["needs_maintenance"])
y = training_pd["needs_maintenance"]

# Stratified K-Fold preserves class balance across folds (important for
# imbalanced maintenance labels where positive class may be ~20-30%)
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
best_f1 = 0
best_run_id = None

# Trial 1: LightGBM — gradient boosting, generally best for tabular data
lgb_configs = [
    {"n_estimators": 100, "max_depth": 4, "learning_rate": 0.1, "num_leaves": 15},
    {"n_estimators": 200, "max_depth": 6, "learning_rate": 0.05, "num_leaves": 31},
    {"n_estimators": 300, "max_depth": 8, "learning_rate": 0.03, "num_leaves": 63},
    {"n_estimators": 150, "max_depth": 5, "learning_rate": 0.08, "num_leaves": 20},
]

for i, params in enumerate(lgb_configs):
    with mlflow.start_run(run_name=f"LightGBM-{i+1}"):
        model = lgb.LGBMClassifier(**params, random_state=42, verbose=-1)
        scores = cross_val_score(model, X, y, cv=cv, scoring="f1")
        model.fit(X, y)
        y_pred = model.predict(X)
        f1 = np.mean(scores)
        mlflow.log_params(params)
        mlflow.log_metric("cv_f1_mean", f1)
        mlflow.log_metric("cv_f1_std", np.std(scores))
        mlflow.log_metric("train_accuracy", accuracy_score(y, y_pred))
        mlflow.log_metric("train_precision", precision_score(y, y_pred, zero_division=0))
        mlflow.log_metric("train_recall", recall_score(y, y_pred, zero_division=0))
        mlflow.sklearn.log_model(model, "model", input_example=X.head(1),
                                 skops_trusted_types=SKOPS_TRUSTED_TYPES)
        run_id = mlflow.active_run().info.run_id
        print(f"  LightGBM-{i+1}: F1={f1:.4f} (params={params})")
        if f1 > best_f1:
            best_f1 = f1
            best_run_id = run_id

# Trial 2: Random Forest — ensemble baseline, less prone to overfitting
rf_configs = [
    {"n_estimators": 100, "max_depth": 6},
    {"n_estimators": 200, "max_depth": 10},
]

for i, params in enumerate(rf_configs):
    with mlflow.start_run(run_name=f"RandomForest-{i+1}"):
        model = RandomForestClassifier(**params, random_state=42)
        scores = cross_val_score(model, X, y, cv=cv, scoring="f1")
        model.fit(X, y)
        y_pred = model.predict(X)
        f1 = np.mean(scores)
        mlflow.log_params(params)
        mlflow.log_metric("cv_f1_mean", f1)
        mlflow.log_metric("cv_f1_std", np.std(scores))
        mlflow.log_metric("train_accuracy", accuracy_score(y, y_pred))
        mlflow.sklearn.log_model(model, "model", input_example=X.head(1),
                                 skops_trusted_types=SKOPS_TRUSTED_TYPES)
        run_id = mlflow.active_run().info.run_id
        print(f"  RandomForest-{i+1}: F1={f1:.4f}")
        if f1 > best_f1:
            best_f1 = f1
            best_run_id = run_id

# Trial 3: Logistic Regression — linear baseline to sanity-check tree models
with mlflow.start_run(run_name="LogisticRegression"):
    model = LogisticRegression(max_iter=1000, random_state=42)
    scores = cross_val_score(model, X, y, cv=cv, scoring="f1")
    model.fit(X, y)
    f1 = np.mean(scores)
    mlflow.log_metric("cv_f1_mean", f1)
    mlflow.sklearn.log_model(model, "model", input_example=X.head(1),
                                 skops_trusted_types=SKOPS_TRUSTED_TYPES)
    run_id = mlflow.active_run().info.run_id
    print(f"  LogisticRegression: F1={f1:.4f}")
    if f1 > best_f1:
        best_f1 = f1
        best_run_id = run_id

print(f"\nBest model F1: {best_f1:.4f}")
print(f"Best run ID: {best_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Register Best Model in Unity Catalog

# COMMAND ----------

# Use Unity Catalog as the model registry (3-level namespace: catalog.schema.model)
mlflow.set_registry_uri("databricks-uc")

model_uri = f"runs:/{best_run_id}/model"
registered = mlflow.register_model(
    model_uri=model_uri,
    name=MODEL_NAME,
)

print(f"Model registered: {MODEL_NAME}")
print(f"Version: {registered.version}")
print(f"Run ID: {best_run_id}")
print(f"Best F1: {best_f1:.4f}")

# Set alias for production use
from mlflow import MlflowClient
client = MlflowClient()
client.set_registered_model_alias(MODEL_NAME, "production", registered.version)
print(f"Alias 'production' set to version {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Validate Model
# MAGIC
# MAGIC Quick sanity check — load the production model and score a sample.

# COMMAND ----------

import mlflow
import pandas as pd

model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@production")

# Score sample data
sample = spark.table(f"`{CATALOG}`.`{SCHEMA}`.ml_training_maintenance").limit(10).toPandas()
features = sample.drop(columns=["needs_maintenance"])
predictions = model.predict(features)

sample["prediction"] = predictions
print("Sample predictions:")
display(spark.createDataFrame(sample))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Value |
# MAGIC |-----------|-------|
# MAGIC | Training table | `dba-lakebase-network.network_data.ml_training_maintenance` |
# MAGIC | Model | `dba-lakebase-network.network_data.predictive_maintenance_model` |
# MAGIC | Alias | `@production` |
# MAGIC | Primary metric | F1 Score |
# MAGIC | Features | IoT health, node age, maintenance history, 7-day trends |
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Run `score_and_create_work_orders` notebook for batch inference
# MAGIC - Schedule daily scoring job to auto-create predictive maintenance work orders

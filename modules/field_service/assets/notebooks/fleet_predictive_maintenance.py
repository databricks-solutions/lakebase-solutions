# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Fleet Predictive Maintenance — Model Training & Registration
# MAGIC
# MAGIC Trains a binary classifier that predicts which fleet vehicles will need maintenance,
# MAGIC learning from **telematics trends** (engine coolant temperature, oil life, battery
# MAGIC voltage, harsh-driving events, active fault codes, mileage) engineered in the
# MAGIC Managed Iceberg gold table `gold_vehicle_health`. Three model families (LightGBM,
# MAGIC Random Forest, Logistic Regression) are evaluated with cross-validated search and
# MAGIC the best by F1 is registered in Unity Catalog with a `@production` alias.
# MAGIC
# MAGIC This is the vehicle-fleet analog of `predictive_maintenance.py` (which scores network
# MAGIC infrastructure). Kept as a separate model because the feature space is different.
# MAGIC
# MAGIC **Databricks capabilities showcased:**
# MAGIC - Managed Iceberg gold tables (`gold_vehicle_health`) as the training source
# MAGIC - MLflow experiment tracking + hyperparameter search
# MAGIC - Unity Catalog Model Registry (3-level namespace) with `@production` alias
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `iceberg_streaming_pipeline` has run (gold_vehicle_health populated)
# MAGIC - `deployment/config.yaml` with `pipeline_catalog`

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.87.0" "mlflow[databricks]" lightgbm scikit-learn psycopg2-binary --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F

# Widget-first (job base_params); no deployment/config.yaml.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")

CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.fleet_maintenance_model"
EXPERIMENT_NAME = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/fleet_predictive_maintenance"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print(f"Catalog: {CATALOG}")
print(f"Model: {MODEL_NAME}")
print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Training Dataset (telematics features → real maintenance outcomes)
# MAGIC
# MAGIC **Features** are 7-day telematics trends from the Iceberg gold table. The **label is a
# MAGIC real outcome**: whether the vehicle has actually suffered an `unplanned_repair`
# MAGIC (recorded in Lakebase `vehicle_maintenance_history`). This is the authentic
# MAGIC predictive-maintenance framing — learn *telematics → breakdown risk* from observed
# MAGIC failures, not reproduce a threshold rule. (Demo data is synthetic but the labeling,
# MAGIC training, registry, and scoring are the exact production pattern.)

# COMMAND ----------

import json, base64
import pandas as pd
from databricks.sdk import WorkspaceClient
import psycopg2

feature_cols = [
    "odometer_km",
    "latest_engine_temp_c", "engine_temp_7d_max", "engine_temp_trend",
    "latest_oil_life_pct", "oil_life_7d_min",
    "latest_battery_voltage", "battery_7d_min",
    "latest_tire_pressure_psi", "harsh_events_7d", "avg_dtc_active",
]

# Telematics features from the Iceberg gold table
gold_pd = spark.table("gold_vehicle_health").select("vehicle_id", *feature_cols).fillna(0).toPandas()

# Real outcome label from Lakebase: did this vehicle have an unplanned repair?
_host = dbutils.widgets.get("pg_host")
_scope = dbutils.widgets.get("secret_scope")
_user = dbutils.secrets.get(scope=_scope, key="pguser")
_token = dbutils.secrets.get(scope=_scope, key="pgpassword")
_conn = psycopg2.connect(host=_host, port=5432, user=_user, password=_token,
                         database=dbutils.widgets.get("pg_database") or "databricks_postgres", sslmode="require")
_cur = _conn.cursor()
_cur.execute("""
    SELECT vehicle_id, count(*) FILTER (WHERE service_type = 'unplanned_repair') AS unplanned
    FROM field_service.vehicle_maintenance_history
    GROUP BY vehicle_id
""")
outcomes = pd.DataFrame(_cur.fetchall(), columns=["vehicle_id", "unplanned"])
_cur.close(); _conn.close()

df = gold_pd.merge(outcomes, on="vehicle_id", how="left").fillna({"unplanned": 0})
# Label: vehicle experienced at least one unplanned (reactive) repair
df["needs_maintenance"] = (df["unplanned"].astype(float) > 0).astype(int)

training_final = spark.createDataFrame(df[feature_cols + ["needs_maintenance"]])
# overwriteSchema: the label dtype can change between runs (rule-flag int -> outcome bigint)
training_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"`{CATALOG}`.`{SCHEMA}`.ml_training_fleet_maintenance")

print(f"Training dataset: {len(df)} vehicles, {len(feature_cols)} telematics features")
print(f"Positives (had an unplanned repair): {int(df['needs_maintenance'].sum())} / {len(df)}")
display(training_final.groupBy("needs_maintenance").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Train Models with Hyperparameter Search

# COMMAND ----------

import lightgbm as lgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
import numpy as np
import pandas as pd

mlflow.set_experiment(EXPERIMENT_NAME)
mlflow.sklearn.autolog(disable=True)

# LightGBM's sklearn wrapper serializes via skops, which flags these types as
# "untrusted" on load/register unless declared here (same as the network model).
SKOPS_TRUSTED_TYPES = [
    "collections.OrderedDict",
    "lightgbm.basic.Booster",
    "lightgbm.sklearn.LGBMClassifier",
]

training_pd = training_final.toPandas()
X = training_pd.drop(columns=["needs_maintenance"])
y = training_pd["needs_maintenance"]

cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
best_f1 = 0
best_run_id = None

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

with mlflow.start_run(run_name="LogisticRegression"):
    model = LogisticRegression(max_iter=1000, random_state=42)
    scores = cross_val_score(model, X, y, cv=cv, scoring="f1")
    model.fit(X, y)
    f1 = np.mean(scores)
    mlflow.log_metric("cv_f1_mean", f1)
    mlflow.sklearn.log_model(model, "model", input_example=X.head(1))
    run_id = mlflow.active_run().info.run_id
    print(f"  LogisticRegression: F1={f1:.4f}")
    if f1 > best_f1:
        best_f1 = f1
        best_run_id = run_id

print(f"\nBest model F1: {best_f1:.4f}")
print(f"Best run ID: {best_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Register Best Model in Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
registered = mlflow.register_model(model_uri=f"runs:/{best_run_id}/model", name=MODEL_NAME)
from mlflow import MlflowClient
client = MlflowClient()
client.set_registered_model_alias(MODEL_NAME, "production", registered.version)
print(f"Model registered: {MODEL_NAME} v{registered.version} (F1={best_f1:.4f}), alias 'production' set.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate

# COMMAND ----------

model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@production")
sample = spark.table(f"`{CATALOG}`.`{SCHEMA}`.ml_training_fleet_maintenance").limit(10).toPandas()
features = sample.drop(columns=["needs_maintenance"])
sample["prediction"] = model.predict(features)
print("Sample predictions:")
display(spark.createDataFrame(sample))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Value |
# MAGIC |-----------|-------|
# MAGIC | Training table | `{CATALOG}.network_data.ml_training_fleet_maintenance` |
# MAGIC | Model | `{CATALOG}.network_data.fleet_maintenance_model` |
# MAGIC | Alias | `@production` |
# MAGIC | Primary metric | F1 Score |
# MAGIC | Features | Engine temp, oil life, battery voltage, harsh events, DTC count, mileage, 7-day trends |
# MAGIC
# MAGIC **Next:** run `score_fleet_work_orders` to score vehicles and create predictive fleet-maintenance work orders.

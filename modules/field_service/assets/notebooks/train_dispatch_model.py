# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install lightgbm scikit-learn mlflow psycopg2-binary pyyaml databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Train Dispatch Scoring Model
# MAGIC
# MAGIC Trains a LightGBM model on historical work order outcomes to predict
# MAGIC assignment quality. The model scores (technician, work_order) pairs
# MAGIC and is used by the smart-assign endpoint to optimize dispatch.
# MAGIC
# MAGIC **Target variable:** `assignment_quality` (0-1 composite score)
# MAGIC - 40% SLA met
# MAGIC - 30% First-fix indicator
# MAGIC - 20% Travel efficiency
# MAGIC - 10% Customer satisfaction
# MAGIC
# MAGIC **Features:** skill proficiency, distance, capacity, SLA urgency, tech rating,
# MAGIC first-fix rate, certification level, WO priority/category

# COMMAND ----------

import os, sys, json, base64, time
import numpy as np
import pandas as pd
from pathlib import Path

# Config — widget-first (job base_params); no deployment/config.py.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")

PIPELINE_CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
cfg = {
    "secret_scope": dbutils.widgets.get("secret_scope") or "lakebase-secrets",
    "pg_host": dbutils.widgets.get("pg_host"),
    "database": dbutils.widgets.get("pg_database") or "databricks_postgres",
}
print(f"Catalog: {PIPELINE_CATALOG}")

# COMMAND ----------

import psycopg2

# Connect to Lakebase using native PG auth from Databricks Secrets
def get_pg_connection():
    """Connect to Lakebase using secrets scope or env vars."""
    pg_host = os.environ.get("PGHOST") or cfg.get("pg_host", "")
    pg_user = os.environ.get("PGUSER") or cfg.get("pg_user", "")
    pg_pass = os.environ.get("PGPASSWORD", "")
    pg_db = os.environ.get("PGDATABASE") or cfg.get("database", "databricks_postgres")

    # If env vars aren't set, read from Databricks Secrets (works on serverless)
    scope = cfg.get("secret_scope", "lakebase-secrets")
    if not pg_user:
        try:
            pg_user = dbutils.secrets.get(scope=scope, key="pguser")
            print(f"  Got pguser from secrets scope '{scope}'")
        except Exception:
            pass
    if not pg_pass:
        try:
            pg_pass = dbutils.secrets.get(scope=scope, key="pgpassword")
            print(f"  Got pgpassword from secrets scope '{scope}'")
        except Exception:
            pass
    if not pg_host:
        try:
            pg_host = dbutils.secrets.get(scope=scope, key="pghost")
            print(f"  Got pghost from secrets scope '{scope}'")
        except Exception:
            # Try to discover from config
            pg_host = cfg.get("pg_host", "")

    if not (pg_host and pg_user and pg_pass):
        raise Exception(f"Missing PG credentials. host={bool(pg_host)}, user={bool(pg_user)}, pass={bool(pg_pass)}. "
                        f"Set PGHOST/PGUSER/PGPASSWORD env vars or store in '{scope}' secrets scope.")

    print(f"  Connecting to: {pg_host} as {pg_user}")
    return psycopg2.connect(host=pg_host, port=5432, user=pg_user, password=pg_pass,
                            database=pg_db or "databricks_postgres", sslmode="require")

conn = get_pg_connection()
print(f"Connected to Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Training Dataset
# MAGIC
# MAGIC Extract features from completed work orders with known outcomes.

# COMMAND ----------

# Build training data from completed WOs with assigned technicians
query = """
WITH tech_stats AS (
    SELECT
        assigned_technician_id,
        COUNT(*) as completed_count,
        AVG(CASE WHEN sla_met THEN 1.0 ELSE 0.0 END) as sla_rate,
        AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600) as avg_hours
    FROM field_service.work_orders
    WHERE status = 'completed'
      AND assigned_technician_id IS NOT NULL
      AND resolved_at IS NOT NULL
    GROUP BY assigned_technician_id
)
SELECT
    -- Features
    CASE t.certification_level
        WHEN 'lead' THEN 3 WHEN 'senior' THEN 2
        WHEN 'standard' THEN 1 ELSE 0
    END as cert_level_encoded,
    COALESCE(t.avg_rating, 3.0) as avg_rating,
    COALESCE(t.first_fix_rate, 70.0) as first_fix_rate,
    COALESCE(ts_stats.completed_count, 0) as tech_completed_count,
    COALESCE(ts_stats.sla_rate, 0.5) as tech_sla_rate,
    CASE COALESCE(ts.proficiency_level, 'none')
        WHEN 'expert' THEN 3 WHEN 'intermediate' THEN 2
        WHEN 'basic' THEN 1 ELSE 0
    END as proficiency_encoded,
    CASE wo.priority
        WHEN 'critical' THEN 3 WHEN 'high' THEN 2
        WHEN 'medium' THEN 1 ELSE 0
    END as priority_encoded,
    CASE wo.category
        WHEN 'repair' THEN 0 WHEN 'install' THEN 1
        WHEN 'maintenance' THEN 2 WHEN 'upgrade' THEN 3 ELSE 4
    END as category_encoded,
    COALESCE(
        field_service.haversine_km(t.current_latitude, t.current_longitude,
                                    wo.latitude, wo.longitude),
        25.0
    ) as distance_km,
    CASE WHEN t.region_id = wo.region_id THEN 1 ELSE 0 END as region_match,
    EXTRACT(HOUR FROM wo.created_at) as hour_of_day,
    EXTRACT(DOW FROM wo.created_at) as day_of_week,

    -- Target components
    CASE WHEN wo.sla_met THEN 1.0 ELSE 0.0 END as sla_met_val,
    LEAST(COALESCE(EXTRACT(EPOCH FROM (wo.resolved_at - wo.created_at)) / 3600, 48), 120) as resolution_hours,
    COALESCE(a.customer_rating, 3.5) as customer_rating

FROM field_service.work_orders wo
JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
LEFT JOIN field_service.technician_skills ts
    ON ts.technician_id = t.technician_id AND ts.skill_id = wo.required_skill_id
LEFT JOIN tech_stats ts_stats ON ts_stats.assigned_technician_id = t.technician_id
LEFT JOIN field_service.appointments a ON a.work_order_id = wo.work_order_id
WHERE wo.status = 'completed'
  AND wo.assigned_technician_id IS NOT NULL
  AND wo.resolved_at IS NOT NULL
ORDER BY RANDOM()
LIMIT 200000
"""

cur = conn.cursor()
cur.execute(query)
cols = [desc[0] for desc in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=cols)
# Convert Decimal columns to float (psycopg2 returns Decimal for NUMERIC)
for col in df.columns:
    if df[col].dtype == object:
        try:
            df[col] = pd.to_numeric(df[col], errors='ignore')
        except Exception:
            pass
    if hasattr(df[col].iloc[0] if len(df) > 0 else None, 'is_finite'):
        df[col] = df[col].astype(float)
# Force all numeric
numeric_cols = ['avg_rating', 'first_fix_rate', 'tech_sla_rate', 'distance_km',
                'sla_met_val', 'resolution_hours', 'customer_rating']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
print(f"Training data: {len(df)} rows, {len(df.columns)} columns")
print(f"Dtypes: {dict(df.dtypes)}")
df.head()

# COMMAND ----------

# Compute composite target variable
max_hours = df['resolution_hours'].quantile(0.95)
df['travel_efficiency'] = 1.0 - np.clip(df['resolution_hours'] / max_hours, 0, 1)
df['rating_norm'] = (df['customer_rating'] - 1.0) / 4.0  # 1-5 → 0-1

df['assignment_quality'] = (
    0.4 * df['sla_met_val']
    + 0.3 * (df['proficiency_encoded'] / 3.0)  # proxy for first-fix
    + 0.2 * df['travel_efficiency']
    + 0.1 * df['rating_norm']
)

# Feature columns
feature_cols = [
    'cert_level_encoded', 'avg_rating', 'first_fix_rate',
    'tech_completed_count', 'tech_sla_rate', 'proficiency_encoded',
    'priority_encoded', 'category_encoded', 'distance_km',
    'region_match', 'hour_of_day', 'day_of_week'
]

X = df[feature_cols].fillna(0)
y = df['assignment_quality']

print(f"Features: {feature_cols}")
print(f"Target stats: mean={y.mean():.3f}, std={y.std():.3f}, min={y.min():.3f}, max={y.max():.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Models with MLflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import lightgbm as lgb
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge

# Set experiment
experiment_path = "/Shared/dispatch_optimization/dispatch_scoring_model"
try:
    mlflow.set_experiment(experiment_path)
except Exception:
    # Create directory if needed
    try:
        w = get_workspace_client(cfg)
        w.workspace.mkdirs("/Shared/dispatch_optimization")
    except Exception:
        pass
    mlflow.set_experiment(experiment_path)

print(f"MLflow experiment: {experiment_path}")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Train: {len(X_train)}, Test: {len(X_test)}")

# COMMAND ----------

# Train LightGBM
best_model = None
best_rmse = float('inf')
best_name = ""

models = {
    "LightGBM": lgb.LGBMRegressor(
        n_estimators=200, max_depth=6, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1
    ),
    "RandomForest": RandomForestRegressor(
        n_estimators=100, max_depth=8, random_state=42, n_jobs=-1
    ),
    "Ridge": Ridge(alpha=1.0),
}

for name, model in models.items():
    with mlflow.start_run(run_name=name):
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        rmse = mean_squared_error(y_test, preds, squared=False)
        r2 = r2_score(y_test, preds)

        # Log metrics
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_param("model_type", name)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("train_size", len(X_train))

        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='neg_root_mean_squared_error')
        mlflow.log_metric("cv_rmse_mean", -cv_scores.mean())
        mlflow.log_metric("cv_rmse_std", cv_scores.std())

        # Log feature importances for tree models
        if hasattr(model, 'feature_importances_'):
            importances = dict(zip(feature_cols, model.feature_importances_))
            for feat, imp in sorted(importances.items(), key=lambda x: -x[1])[:5]:
                mlflow.log_metric(f"fi_{feat}", float(imp))

        print(f"{name}: RMSE={rmse:.4f}, R2={r2:.4f}, CV_RMSE={-cv_scores.mean():.4f}")

        if rmse < best_rmse:
            best_rmse = rmse
            best_model = model
            best_name = name

print(f"\nBest model: {name} (RMSE={best_rmse:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Best Model in Unity Catalog

# COMMAND ----------

MODEL_NAME = f"{PIPELINE_CATALOG}.agents.dispatch_scoring_model"
print(f"Registering model: {MODEL_NAME}")

# Ensure the schema exists
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{PIPELINE_CATALOG}`.agents")
except Exception as e:
    print(f"Schema creation note: {e}")

with mlflow.start_run(run_name=f"best_{best_name}"):
    # Log the winning model with input example
    input_example = X_test.head(1)
    mlflow.sklearn.log_model(
        best_model,
        artifact_path="model",
        input_example=input_example,
        registered_model_name=MODEL_NAME,
    )

    mlflow.log_metric("rmse", best_rmse)
    mlflow.log_param("model_type", best_name)
    mlflow.log_param("features", json.dumps(feature_cols))

    # Log feature importances as artifact
    if hasattr(best_model, 'feature_importances_'):
        fi_df = pd.DataFrame({
            'feature': feature_cols,
            'importance': best_model.feature_importances_
        }).sort_values('importance', ascending=False)
        fi_path = "/tmp/feature_importances.csv"
        fi_df.to_csv(fi_path, index=False)
        mlflow.log_artifact(fi_path)
        print("\nFeature importances:")
        for _, row in fi_df.iterrows():
            print(f"  {row['feature']:30s} {row['importance']:.4f}")

print(f"\nModel registered: {MODEL_NAME}")

# COMMAND ----------

# Set @production alias on latest version
client = mlflow.MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if versions:
    latest = max(versions, key=lambda v: int(v.version))
    try:
        client.set_registered_model_alias(MODEL_NAME, "production", latest.version)
        print(f"Set @production alias on v{latest.version}")
    except Exception as e:
        print(f"Alias note: {e}")
else:
    print("No model versions found")

# COMMAND ----------

conn.close()
print("Training complete!")
dbutils.notebook.exit(json.dumps({  # noqa: F821
    "model": MODEL_NAME,
    "best_model": best_name,
    "rmse": round(best_rmse, 4),
    "features": feature_cols,
}))

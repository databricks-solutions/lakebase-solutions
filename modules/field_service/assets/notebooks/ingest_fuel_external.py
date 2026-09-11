# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Fuel + External-Provider Data → Lakehouse (Auto Loader → Iceberg)
# MAGIC
# MAGIC Consolidates the two fragmented data sources a real fleet team lives with —
# MAGIC today scattered across **Google Sheets / AppSheet** (fuel-card transactions) and
# MAGIC **external provider systems** (third-party shop invoices) — onto the lakehouse.
# MAGIC
# MAGIC ### The "get off AppSheet" story
# MAGIC 1. Fuel + external-maintenance rows live in Lakebase (system of action), seeded by
# MAGIC    `data/fleet_fuel_and_costs.sql`.
# MAGIC 2. This notebook **exports them to CSV in a UC Volume landing zone** — exactly what
# MAGIC    the team's Sheets/AppSheet automation drops today, just pointed at the lake.
# MAGIC 3. The landing zone is **ingested into Bronze** Managed Iceberg tables.
# MAGIC 4. **Silver** range-validates (drops impossible fills — the data-trust layer).
# MAGIC 5. **Gold** computes per-vehicle fuel economy + cost — the governed analytical copy
# MAGIC    that powers cost-per-km / running-cost TCO and the fuel-anomaly PdM signal.
# MAGIC
# MAGIC Idempotent: each run rebuilds the landing zone and INSERT OVERWRITEs Bronze, so
# MAGIC re-running `deploy_all` produces a clean medallion (no duplicate-row accumulation).
# MAGIC
# MAGIC **Databricks capabilities showcased:** UC Volume landing-zone ingestion into a
# MAGIC Managed Iceberg medallion, Silver data-quality validation, Lakebase as the shared
# MAGIC OLTP source of truth.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `data/fleet_fuel_and_costs.sql` applied (fuel_transactions + external_maintenance seeded)
# MAGIC - Pipeline catalog + `network_data` schema/volume exist (created by `02d_create_pipeline.py`)

# COMMAND ----------

# MAGIC %pip install psycopg2-binary pyyaml "databricks-sdk>=0.87.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, json, base64, yaml
from pathlib import Path
from databricks.sdk import WorkspaceClient

repo_root = Path(os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
).replace("/notebooks", ""))
# Widget-first (job base_params); no deployment/config.yaml. PG creds from scope.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")

CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"
VOLUME = "raw_files"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
LANDING = f"{VOLUME_PATH}/fuel_landing"          # the "AppSheet/Sheets export drops here"
CHK = f"{VOLUME_PATH}/_chk"                       # Auto Loader checkpoints
FQN = f"`{CATALOG}`.{SCHEMA}"
PG_DB = dbutils.widgets.get("pg_database") or "databricks_postgres"
PG_HOST = dbutils.widgets.get("pg_host")
_secret_scope = dbutils.widgets.get("secret_scope")
PG_USER = dbutils.secrets.get(scope=_secret_scope, key="pguser")
PG_TOKEN = dbutils.secrets.get(scope=_secret_scope, key="pgpassword")

print(f"Catalog/Schema/Volume: {VOLUME_PATH}")
print(f"Lakebase: {PG_HOST}")

# COMMAND ----------

# Ensure catalog/schema/volume + landing zone exist (idempotent)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
dbutils.fs.mkdirs(LANDING)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Export Lakebase fuel + external-maintenance rows to the Volume landing zone
# MAGIC This is the automation that today writes to Google Sheets — re-pointed at the lake.

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(host=PG_HOST, port=5432, dbname=PG_DB, user=PG_USER, password=PG_TOKEN, sslmode="require")
cur = conn.cursor()

cur.execute("""
    SELECT ft.vehicle_id, ft.txn_date, ft.odometer_km, ft.distance_km, ft.liters,
           ft.price_per_liter, ft.total_cost, ft.merchant, ft.fuel_card_last4,
           r.region_code, ft.source
    FROM field_service.fuel_transactions ft
    LEFT JOIN field_service.fleet_vehicles v ON v.vehicle_id = ft.vehicle_id
    LEFT JOIN field_service.service_regions r ON r.region_id = ft.region_id
    ORDER BY ft.txn_date
""")
fuel_rows = cur.fetchall()

cur.execute("""
    SELECT em.vehicle_id, em.service_date, em.vendor, em.service_category, em.odometer_km,
           em.parts_cost, em.labor_cost, em.total_cost, em.invoice_ref, em.source
    FROM field_service.external_maintenance em
    ORDER BY em.service_date
""")
ext_rows = cur.fetchall()
cur.close(); conn.close()
print(f"Fetched {len(fuel_rows):,} fuel transactions, {len(ext_rows):,} external-maintenance invoices")

# COMMAND ----------

# Reset landing zone + checkpoints so each deploy rebuilds cleanly (idempotent)
for p in (LANDING, f"{CHK}/fuel_bronze", f"{CHK}/ext_bronze"):
    try:
        dbutils.fs.rm(p, recurse=True)  # noqa: F821
    except Exception as _e:
        print(f"  (reset {p}: {_e})")
dbutils.fs.mkdirs(LANDING)  # noqa: F821

FUEL_HEADER = ("vehicle_id|txn_date|odometer_km|distance_km|liters|price_per_liter|"
               "total_cost|merchant|fuel_card_last4|region_code|source")
fuel_lines = [FUEL_HEADER]
for r in fuel_rows:
    fuel_lines.append("|".join("" if x is None else str(x) for x in r))
with open(f"{LANDING}/fuel_transactions_export.csv", "w") as f:
    f.write("\n".join(fuel_lines))

EXT_HEADER = ("vehicle_id|service_date|vendor|service_category|odometer_km|"
              "parts_cost|labor_cost|total_cost|invoice_ref|source")
ext_lines = [EXT_HEADER]
for r in ext_rows:
    ext_lines.append("|".join("" if x is None else str(x) for x in r))
with open(f"{LANDING}/external_maintenance_export.csv", "w") as f:
    f.write("\n".join(ext_lines))

print(f"Wrote landing files to {LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze — ingest the Volume landing zone into Managed Iceberg
# MAGIC Batch file ingestion from the UC Volume landing zone + `INSERT OVERWRITE` into
# MAGIC pre-created Managed Iceberg tables (the idempotent pattern the telemetry medallion
# MAGIC uses — Managed Iceberg does not support creating a table via a streaming writer).

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# Pre-create Managed Iceberg bronze tables (same DDL style as the telemetry pipeline)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_fuel_transactions (
  vehicle_id STRING, txn_date DATE, odometer_km DOUBLE, distance_km DOUBLE,
  liters DOUBLE, price_per_liter DOUBLE, total_cost DOUBLE, merchant STRING,
  fuel_card_last4 STRING, region_code STRING, source STRING,
  _source_file STRING, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (vehicle_id)
COMMENT 'Raw fuel-card transactions ingested from the Sheets/AppSheet CSV export in the UC Volume landing zone.'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_external_maintenance (
  vehicle_id STRING, service_date DATE, vendor STRING, service_category STRING,
  odometer_km DOUBLE, parts_cost DOUBLE, labor_cost DOUBLE, total_cost DOUBLE,
  invoice_ref STRING, source STRING,
  _source_file STRING, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (vehicle_id)
COMMENT 'Raw third-party maintenance invoices ingested from the external-provider CSV export in the UC Volume.'
""")

def ingest_csv(glob, table, select_cols):
    df = (spark.read.format("csv")
          .option("header", "true").option("delimiter", "|").option("inferSchema", "false")
          .option("pathGlobFilter", glob)
          .load(LANDING)
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_ingested_at", current_timestamp()))
    df.createOrReplaceTempView("_tmp_ingest")
    spark.sql(f"INSERT OVERWRITE {table} SELECT {select_cols}, _source_file, _ingested_at FROM _tmp_ingest")

ingest_csv("fuel_transactions_*.csv", f"{FQN}.bronze_fuel_transactions",
           "vehicle_id, CAST(txn_date AS DATE), CAST(odometer_km AS DOUBLE), CAST(distance_km AS DOUBLE), "
           "CAST(liters AS DOUBLE), CAST(price_per_liter AS DOUBLE), CAST(total_cost AS DOUBLE), "
           "merchant, fuel_card_last4, region_code, source")
ingest_csv("external_maintenance_*.csv", f"{FQN}.bronze_external_maintenance",
           "vehicle_id, CAST(service_date AS DATE), vendor, service_category, CAST(odometer_km AS DOUBLE), "
           "CAST(parts_cost AS DOUBLE), CAST(labor_cost AS DOUBLE), CAST(total_cost AS DOUBLE), invoice_ref, source")

print("Bronze fuel:", spark.table(f"{FQN}.bronze_fuel_transactions").count(),
      "| Bronze external:", spark.table(f"{FQN}.bronze_external_maintenance").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver — range-validate (drop impossible fills: the data-trust layer)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.silver_fuel_transactions
CLUSTER BY (vehicle_id)
COMMENT 'Validated fuel transactions: positive volume/distance and a plausible km/L.'
AS SELECT
    vehicle_id, txn_date, odometer_km, distance_km, liters, price_per_liter,
    total_cost, merchant, fuel_card_last4, region_code,
    ROUND(distance_km / liters, 2) AS km_per_liter
FROM {FQN}.bronze_fuel_transactions
WHERE vehicle_id IS NOT NULL
  AND liters > 0 AND distance_km > 0
  AND price_per_liter BETWEEN 0.30 AND 3.00
  AND (distance_km / liters) BETWEEN 2 AND 25      -- implausible economy dropped
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.silver_external_maintenance
CLUSTER BY (vehicle_id)
COMMENT 'Validated third-party maintenance invoices.'
AS SELECT vehicle_id, service_date, vendor, service_category, odometer_km,
          parts_cost, labor_cost, total_cost, invoice_ref
FROM {FQN}.bronze_external_maintenance
WHERE vehicle_id IS NOT NULL AND total_cost >= 0
""")

_b = spark.table(f"{FQN}.bronze_fuel_transactions").count()
_s = spark.table(f"{FQN}.silver_fuel_transactions").count()
print(f"Silver fuel: {_s} kept / {_b} bronze ({_b - _s} dropped by validation)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold — per-vehicle fuel economy + cost (governed analytical copy)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.gold_vehicle_cost
CLUSTER BY (vehicle_id)
COMMENT 'Per-vehicle fuel economy, fuel + external-maintenance spend, and cost-per-km. Powers cost-per-km / running-cost analytics and the fuel-anomaly PdM signal.'
AS
WITH fuel AS (
    SELECT vehicle_id, region_code,
           COUNT(*) AS fills,
           SUM(liters) AS liters,
           SUM(distance_km) AS distance_km,
           SUM(total_cost) AS fuel_cost,
           SUM(distance_km) / NULLIF(SUM(liters), 0) AS efficiency_kmpl,
           SUM(CASE WHEN txn_date >= current_date() - INTERVAL 30 DAYS THEN distance_km END)
             / NULLIF(SUM(CASE WHEN txn_date >= current_date() - INTERVAL 30 DAYS THEN liters END), 0) AS recent_kmpl,
           SUM(CASE WHEN txn_date <  current_date() - INTERVAL 30 DAYS THEN distance_km END)
             / NULLIF(SUM(CASE WHEN txn_date <  current_date() - INTERVAL 30 DAYS THEN liters END), 0) AS older_kmpl
    FROM {FQN}.silver_fuel_transactions
    GROUP BY vehicle_id, region_code
),
ext AS (
    SELECT vehicle_id, COUNT(*) AS ext_invoices, SUM(total_cost) AS ext_maint_cost
    FROM {FQN}.silver_external_maintenance
    GROUP BY vehicle_id
)
SELECT
    f.vehicle_id, f.region_code, f.fills, ROUND(f.liters, 1) AS liters,
    ROUND(f.distance_km, 1) AS distance_km, ROUND(f.fuel_cost, 2) AS fuel_cost,
    ROUND(f.efficiency_kmpl, 2) AS efficiency_kmpl,
    ROUND(f.recent_kmpl, 2) AS recent_kmpl, ROUND(f.older_kmpl, 2) AS older_kmpl,
    ROUND((f.recent_kmpl - f.older_kmpl) / NULLIF(f.older_kmpl, 0) * 100, 1) AS eff_trend_pct,
    (f.older_kmpl > 0 AND f.recent_kmpl < f.older_kmpl * 0.92) AS fuel_anomaly,
    ROUND(f.fuel_cost / NULLIF(f.distance_km, 0), 3) AS fuel_cost_per_km,
    COALESCE(e.ext_invoices, 0) AS external_invoices,
    ROUND(COALESCE(e.ext_maint_cost, 0), 2) AS external_maint_cost
FROM fuel f
LEFT JOIN ext e ON e.vehicle_id = f.vehicle_id
""")

n = spark.table(f"{FQN}.gold_vehicle_cost").count()
anom = spark.sql(f"SELECT COUNT(*) c FROM {FQN}.gold_vehicle_cost WHERE fuel_anomaly").collect()[0]["c"]
print(f"Gold vehicle cost: {n} vehicles, {anom} with a fuel-efficiency anomaly")
display(spark.sql(f"SELECT * FROM {FQN}.gold_vehicle_cost ORDER BY eff_trend_pct ASC NULLS LAST LIMIT 10"))

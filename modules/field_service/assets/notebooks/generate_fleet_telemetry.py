# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Fleet Telemetry Backfill — Lakebase OLTP → UC Volume (CSV)
# MAGIC
# MAGIC Exports the seeded `field_service.vehicle_telemetry` rows from Lakebase (the
# MAGIC operational source of truth) into pipe-delimited CSV files in the pipeline's UC
# MAGIC Volume. The Structured Streaming pipeline (`iceberg_streaming_pipeline`) then
# MAGIC ingests them via Auto Loader (glob `vehicle_telemetry_*.csv`) into the
# MAGIC Bronze → Silver → Gold vehicle medallion.
# MAGIC
# MAGIC A small fraction of rows are intentionally corrupted (implausible coolant temp,
# MAGIC negative odometer) so the Silver-layer range validation visibly drops them —
# MAGIC the "we don't trust raw telematics" data-quality story.
# MAGIC
# MAGIC **Databricks capabilities showcased:**
# MAGIC - Lakebase (PostgreSQL OLTP) as a telematics landing zone
# MAGIC - UC Volumes as the lakehouse ingestion surface
# MAGIC - Single source of truth shared between OLTP app and lakehouse analytics
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `data/fleet_management.sql` applied (vehicle_telemetry + fleet_vehicles seeded)
# MAGIC - Pipeline catalog + `network_data` schema exist (created by `02d_create_pipeline.py`)
# MAGIC
# MAGIC No parameters — resolves the Lakebase connection from `deployment/config.yaml`
# MAGIC via the workspace credentials API (the same pattern the deploy Tier blocks use).

# COMMAND ----------

# MAGIC %pip install psycopg2-binary pyyaml "databricks-sdk>=0.87.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timezone

# Widget-first (job base_params); no deployment/config.yaml. PG creds from the scope.
dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")

CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"
VOLUME = "raw_files"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
PG_DB = dbutils.widgets.get("pg_database") or "databricks_postgres"
PG_HOST = dbutils.widgets.get("pg_host")
_secret_scope = dbutils.widgets.get("secret_scope")
PG_USER = dbutils.secrets.get(scope=_secret_scope, key="pguser")
PG_TOKEN = dbutils.secrets.get(scope=_secret_scope, key="pgpassword")

print(f"Catalog/Schema/Volume: {VOLUME_PATH}")
print(f"Lakebase: {PG_HOST}")

# COMMAND ----------

# Ensure the catalog/schema/volume exist (idempotent — pipeline step also creates them)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(host=PG_HOST, port=5432, dbname=PG_DB, user=PG_USER, password=PG_TOKEN, sslmode="require")
cur = conn.cursor()
cur.execute("""
    SELECT v.vehicle_id, v.vin, v.make, v.model, r.region_code, v.health_profile,
           t.recorded_at, t.odometer_km, t.speed_avg_kph, t.engine_temp_c, t.oil_life_pct,
           t.battery_voltage, t.fuel_level_pct, t.tire_pressure_psi, t.engine_rpm_avg,
           t.harsh_brake_count, t.harsh_accel_count, t.idle_minutes, t.dtc_active_count,
           t.latitude, t.longitude
    FROM field_service.vehicle_telemetry t
    JOIN field_service.fleet_vehicles v ON v.vehicle_id = t.vehicle_id
    JOIN field_service.service_regions r ON r.region_id = v.region_id
    ORDER BY t.recorded_at
""")
rows = cur.fetchall()
cur.close()
conn.close()
print(f"Fetched {len(rows):,} telemetry rows from Lakebase")

# COMMAND ----------

HEADER = ("vehicle_id|vin|make|model|region_code|health_profile|timestamp|odometer_km|"
          "speed_avg_kph|engine_temp_c|oil_life_pct|battery_voltage|fuel_level_pct|"
          "tire_pressure_psi|engine_rpm_avg|harsh_brake_count|harsh_accel_count|"
          "idle_minutes|dtc_active_count|lat|lng")

# Deterministic data-quality noise (no Math.random in scripts here, but a notebook is fine):
# corrupt ~0.3% of rows so the Silver range-validation visibly drops them.
def _corrupt(idx, field_vals):
    # field_vals is a mutable list aligned to HEADER columns
    mode = idx % 1000
    if mode == 7:
        field_vals[9] = "999.9"        # engine_temp_c absurd -> dropped by silver (>150)
    elif mode == 311:
        field_vals[7] = "-5.0"          # odometer_km negative -> dropped (>=0 check)
    elif mode == 613:
        field_vals[11] = "27.4"         # battery_voltage absurd -> dropped (6..16)
    elif mode == 877:
        field_vals[6] = ""              # missing timestamp -> dropped
    return field_vals

lines = [HEADER]
for i, row in enumerate(rows):
    (vehicle_id, vin, make, model, region_code, health_profile, recorded_at, odometer_km,
     speed_avg_kph, engine_temp_c, oil_life_pct, battery_voltage, fuel_level_pct,
     tire_pressure_psi, engine_rpm_avg, harsh_brake_count, harsh_accel_count,
     idle_minutes, dtc_active_count, latitude, longitude) = row
    vals = [
        str(vehicle_id), str(vin or ""), str(make or ""), str(model or ""),
        str(region_code or ""), str(health_profile or ""),
        recorded_at.isoformat() if recorded_at else "",
        str(odometer_km), str(speed_avg_kph), str(engine_temp_c), str(oil_life_pct),
        str(battery_voltage), str(fuel_level_pct), str(tire_pressure_psi),
        str(engine_rpm_avg), str(harsh_brake_count), str(harsh_accel_count),
        str(idle_minutes), str(dtc_active_count), str(latitude), str(longitude),
    ]
    vals = _corrupt(i, vals)
    lines.append("|".join(vals))

import glob

# Idempotent: remove any prior vehicle telemetry files, then write ONE fixed file.
# The pipeline batch-ingests vehicle_telemetry_*.csv with INSERT OVERWRITE, so a single
# file keeps re-runs of deploy_all clean (no duplicate-row accumulation in bronze).
for _old in glob.glob(f"{VOLUME_PATH}/vehicle_telemetry_*.csv"):
    try:
        os.remove(_old)
    except Exception as _e:
        print(f"  (could not remove {_old}: {_e})")

out_file = f"{VOLUME_PATH}/vehicle_telemetry_backfill.csv"
# Volumes are FUSE-mounted — plain file IO works on Databricks
with open(out_file, "w") as f:
    f.write("\n".join(lines))

print(f"Wrote {len(lines)-1:,} rows to {out_file}")
print("Run iceberg_streaming_pipeline to ingest into the vehicle medallion.")

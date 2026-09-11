# Databricks notebook source
# MAGIC %md
# MAGIC # Network Infrastructure Pipeline — Spark Structured Streaming → Managed Iceberg
# MAGIC
# MAGIC Ingests raw network monitoring data from UC Volumes into **Managed Iceberg** tables
# MAGIC using the Bronze → Silver → Gold medallion architecture.
# MAGIC
# MAGIC **Why Spark + Iceberg instead of SDP/DLT?**
# MAGIC
# MAGIC This pipeline writes directly to Managed Iceberg tables (`USING ICEBERG`) so that
# MAGIC open-source Iceberg clients (PyIceberg, Spark-Iceberg, Trino, etc.) can read and
# MAGIC write via the Unity Catalog Iceberg REST endpoint — demonstrating true format
# MAGIC interoperability. DLT/SDP Streaming Tables and Materialized Views use Delta
# MAGIC internally and are not accessible via the Iceberg REST API.
# MAGIC
# MAGIC **Alternative: Spark Declarative Pipelines (SDP/DLT)**
# MAGIC
# MAGIC If Delta Lake format is preferred, SDP provides significant value-add:
# MAGIC - **Declarative SQL** — define tables as SQL queries; the engine handles orchestration
# MAGIC - **Automatic dependency resolution** — DAG is inferred from `LIVE.` references
# MAGIC - **Data quality expectations** — `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW|FAIL UPDATE`
# MAGIC - **Auto-scaling clusters** — Enhanced Autoscaling with cost-optimized spot instances
# MAGIC - **Built-in monitoring** — event log, data quality metrics, lineage in Unity Catalog
# MAGIC - **Schema evolution** — automatic inference and evolution for streaming sources
# MAGIC - **Incremental processing** — STREAMING TABLE + `STREAM read_files()` for CDC-like ingest
# MAGIC - **Change Data Capture** — `APPLY CHANGES INTO` for SCD Type 1/2 from CDC sources
# MAGIC - See `archive/pipelines/network_infrastructure_pipeline.sql` for the SDP version (reference only, not deployed)
# MAGIC
# MAGIC **Source files (in UC Volume):**
# MAGIC - `network_nodes.csv` (comma-delimited, static)
# MAGIC - `network_performance.csv` (pipe-delimited, static, ~500K rows)
# MAGIC - `network_outages.json` (JSON Lines, static)
# MAGIC - `iot_telemetry_*.csv` (pipe-delimited, streaming from simulator)
# MAGIC
# MAGIC **Key Managed Iceberg limitations (vs Delta):**
# MAGIC - Streaming writes: supported via `.toTable()` on a pre-created table (cannot create via DataStreamWriter)
# MAGIC - No streaming reads — cannot use a Managed Iceberg table as a streaming source (no CDC)
# MAGIC - No deletion vectors or row tracking — these are Delta-only features
# MAGIC - Liquid clustering requires disabling DVs and row tracking (done via TBLPROPERTIES)
# MAGIC - Predictive optimization is automatic and mandatory (compaction, snapshot expiry)
# MAGIC
# MAGIC **Requires:** DBR 16.4 LTS+ (Managed Iceberg support)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "dba-lakebase-network", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
dbutils.widgets.text("volume_path", "/Volumes/dba-lakebase-network/network_data/raw_files", "Volume Path")

CATALOG = dbutils.widgets.get("catalog").replace("-", "_")
CATALOG_ORIG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_PATH = dbutils.widgets.get("volume_path")

FQN = f"`{CATALOG_ORIG}`.{SCHEMA}"
CHECKPOINT_BASE = f"{VOLUME_PATH}/_checkpoints"
SCHEMA_BASE = f"{VOLUME_PATH}/_autoloader_schema"

# Self-provision the standard catalog/schema/volume (the module passes a
# namespaced catalog that may not exist yet). Idempotent; safe to re-run.
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG_ORIG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG_ORIG}`.{SCHEMA}")
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG_ORIG}`.{SCHEMA}.raw_files")
except Exception as _vol_exc:
    print(f"Volume create skipped: {_vol_exc}")

spark.sql(f"USE CATALOG `{CATALOG_ORIG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Catalog: {CATALOG_ORIG}, Schema: {SCHEMA}, Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Managed Iceberg Tables

# COMMAND ----------

# ── BRONZE TABLES ────────────────────────────────────────────────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_network_nodes (
  node_id STRING, node_name STRING, node_type STRING, region_code STRING,
  state STRING, latitude STRING, longitude STRING, elevation_ft STRING,
  install_date STRING, vendor STRING, model STRING, firmware_version STRING,
  max_capacity_mbps STRING, power_consumption_kw STRING, backhaul_type STRING,
  has_backup_power STRING, status STRING, last_maintenance_date STRING,
  _source_file STRING, _file_modified_at TIMESTAMP, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (node_id, region_code)
COMMENT 'Raw network infrastructure nodes ingested from CSV export.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_network_performance (
  measurement_id STRING, node_id STRING, timestamp STRING,
  signal_strength_dbm STRING, throughput_mbps STRING, latency_ms STRING,
  packet_loss_pct STRING, jitter_ms STRING, connected_users STRING,
  cpu_utilization_pct STRING, memory_utilization_pct STRING,
  temperature_celsius STRING, uptime_hours STRING, error_count STRING,
  bandwidth_utilization_pct STRING,
  _source_file STRING, _file_modified_at TIMESTAMP, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (node_id)
COMMENT 'Raw hourly performance metrics from network monitoring systems.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_network_outages (
  outage_id STRING, node_id STRING, node_type STRING, region_code STRING,
  severity STRING, root_cause STRING, start_time STRING, end_time STRING,
  duration_minutes STRING, affected_customers STRING,
  impact STRUCT<estimated_revenue_impact: STRING, service_degraded: STRING, service_down: STRING>,
  resolution STRUCT<dispatch_required: STRING, resolved_by: STRING, work_order_created: STRING>,
  reported_at STRING, detected_by STRING,
  _source_file STRING, _file_modified_at TIMESTAMP, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (node_id, region_code)
COMMENT 'Raw network outage events from monitoring API exports.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_iot_telemetry (
  device_id STRING, infrastructure_id STRING, device_type STRING,
  timestamp STRING, signal_strength_dbm STRING, throughput_mbps STRING,
  latency_ms STRING, packet_loss_pct STRING, temperature_celsius STRING,
  battery_pct STRING, connected_clients STRING, error_count STRING,
  firmware_version STRING, lat STRING, lng STRING,
  _source_file STRING, _file_modified_at TIMESTAMP, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (infrastructure_id, device_id)
COMMENT 'Raw IoT device telemetry from field sensors, streamed via Auto Loader.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_vehicle_telemetry (
  vehicle_id STRING, vin STRING, make STRING, model STRING,
  region_code STRING, health_profile STRING, timestamp STRING,
  odometer_km STRING, speed_avg_kph STRING, engine_temp_c STRING,
  oil_life_pct STRING, battery_voltage STRING, fuel_level_pct STRING,
  tire_pressure_psi STRING, engine_rpm_avg STRING, harsh_brake_count STRING,
  harsh_accel_count STRING, idle_minutes STRING, dtc_active_count STRING,
  lat STRING, lng STRING,
  _source_file STRING, _file_modified_at TIMESTAMP, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (vehicle_id)
COMMENT 'Raw fleet vehicle telematics (Geotab-style), streamed via Auto Loader from the OLTP export.'
""")

print("Bronze Iceberg tables created/verified")

# COMMAND ----------

# ── SILVER TABLES ────────────────────────────────────────────────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.silver_network_nodes (
  node_id STRING, node_name STRING, node_type STRING, region_code STRING,
  state STRING, latitude DOUBLE, longitude DOUBLE, elevation_ft DOUBLE,
  install_date DATE, vendor STRING, model STRING, firmware_version STRING,
  max_capacity_mbps INT, power_consumption_kw DOUBLE, backhaul_type STRING,
  has_backup_power BOOLEAN, status STRING, last_maintenance_date DATE,
  age_days INT, days_since_maintenance INT,
  _source_file STRING, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (node_id, region_code)
COMMENT 'Cleaned network nodes with validated coordinates, capacity, and status.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.silver_network_performance (
  measurement_id BIGINT, node_id STRING,
  measurement_timestamp TIMESTAMP, signal_strength_dbm DOUBLE,
  throughput_mbps DOUBLE, latency_ms DOUBLE, packet_loss_pct DOUBLE,
  jitter_ms DOUBLE, connected_users INT, cpu_utilization_pct DOUBLE,
  memory_utilization_pct DOUBLE, temperature_celsius DOUBLE,
  uptime_hours DOUBLE, error_count INT, bandwidth_utilization_pct DOUBLE,
  measurement_date DATE, measurement_hour INT,
  _source_file STRING, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (node_id, measurement_date)
COMMENT 'Cleaned and typed hourly network performance telemetry.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.silver_network_outages (
  outage_id STRING, node_id STRING, node_type STRING, region_code STRING,
  severity STRING, root_cause STRING,
  start_time TIMESTAMP, end_time TIMESTAMP, duration_minutes INT,
  affected_customers INT, service_degraded BOOLEAN, service_down BOOLEAN,
  estimated_revenue_impact DOUBLE, resolved_by STRING,
  dispatch_required BOOLEAN, work_order_created BOOLEAN,
  reported_at TIMESTAMP, detected_by STRING, is_ongoing BOOLEAN,
  outage_date DATE,
  _source_file STRING, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (node_id, outage_date)
COMMENT 'Cleaned outage events with flattened nested fields.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.silver_iot_telemetry (
  device_id STRING, infrastructure_id STRING, device_type STRING,
  timestamp TIMESTAMP, signal_strength_dbm DOUBLE, throughput_mbps DOUBLE,
  latency_ms DOUBLE, packet_loss_pct DOUBLE, temperature_celsius DOUBLE,
  battery_pct DOUBLE, connected_clients INT, error_count INT,
  firmware_version STRING, lat DOUBLE, lng DOUBLE,
  reading_date DATE, reading_hour INT,
  _source_file STRING, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (infrastructure_id, reading_date)
COMMENT 'Cleaned IoT device telemetry with validated ranges.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.silver_vehicle_telemetry (
  vehicle_id STRING, vin STRING, make STRING, model STRING,
  region_code STRING, health_profile STRING, reading_ts TIMESTAMP,
  odometer_km DOUBLE, speed_avg_kph DOUBLE, engine_temp_c DOUBLE,
  oil_life_pct DOUBLE, battery_voltage DOUBLE, fuel_level_pct DOUBLE,
  tire_pressure_psi DOUBLE, engine_rpm_avg INT, harsh_brake_count INT,
  harsh_accel_count INT, idle_minutes DOUBLE, dtc_active_count INT,
  health_score DOUBLE, lat DOUBLE, lng DOUBLE,
  reading_date DATE,
  _source_file STRING, _ingested_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (vehicle_id, reading_date)
COMMENT 'Cleaned, typed, range-validated vehicle telemetry. Implausible sensor values (e.g. coolant temp outside -40..150C, oil life outside 0..100, battery outside 6..16V) are dropped here — the data-quality layer that prevents false-alarm dispatches.'
""")

print("Silver Iceberg tables created/verified")

# COMMAND ----------

# ── GOLD TABLES ──────────────────────────────────────────────────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_daily_node_health (
  node_id STRING, node_name STRING, node_type STRING, region_code STRING,
  vendor STRING, measurement_date DATE, measurement_count BIGINT,
  avg_throughput_mbps DOUBLE, peak_throughput_mbps DOUBLE,
  avg_latency_ms DOUBLE, max_latency_ms DOUBLE, avg_packet_loss_pct DOUBLE,
  avg_connected_users DOUBLE, peak_connected_users DOUBLE,
  avg_cpu_pct DOUBLE, peak_cpu_pct DOUBLE, avg_temperature_c DOUBLE,
  total_errors BIGINT, avg_bandwidth_util_pct DOUBLE, health_score DOUBLE
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (node_id, measurement_date)
COMMENT 'Daily health score per network node (0-100).'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_regional_network_summary (
  region_code STRING, measurement_date DATE, active_nodes BIGINT,
  avg_health_score DOUBLE, unhealthy_node_count BIGINT, healthy_node_count BIGINT,
  region_avg_throughput_mbps DOUBLE, region_avg_latency_ms DOUBLE,
  region_avg_packet_loss_pct DOUBLE, region_total_connected_users DOUBLE,
  region_total_errors BIGINT
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (region_code, measurement_date)
COMMENT 'Daily network health aggregated by region.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_daily_outage_summary (
  region_code STRING, outage_date DATE, severity STRING,
  outage_count BIGINT, total_affected_customers BIGINT,
  avg_duration_minutes DOUBLE, total_outage_minutes BIGINT,
  total_revenue_impact DOUBLE, dispatches_required BIGINT,
  work_orders_created BIGINT, ongoing_outages BIGINT
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (region_code, outage_date)
COMMENT 'Daily outage statistics by region and severity.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_node_maintenance_risk (
  node_id STRING, node_name STRING, node_type STRING, region_code STRING,
  vendor STRING, install_date DATE, age_days INT, days_since_maintenance INT,
  has_backup_power BOOLEAN, status STRING,
  recent_avg_health DOUBLE, recent_outage_count BIGINT,
  maintenance_risk_score DOUBLE, risk_category STRING
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (region_code, risk_category)
COMMENT 'Nodes at risk based on age, maintenance history, and health.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_outage_root_cause_analysis (
  root_cause STRING, severity STRING, total_outages BIGINT,
  total_affected_customers BIGINT, avg_duration_minutes DOUBLE,
  median_duration_minutes DOUBLE, p95_duration_minutes DOUBLE,
  total_dispatches BIGINT, dispatch_rate_pct DOUBLE, total_revenue_impact DOUBLE
) USING ICEBERG
COMMENT 'Root cause distribution for outages with MTTR.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_iot_device_health (
  infrastructure_id STRING, device_count BIGINT,
  avg_signal_dbm DOUBLE, avg_throughput_mbps DOUBLE, avg_latency_ms DOUBLE,
  avg_packet_loss_pct DOUBLE, avg_temperature_c DOUBLE, avg_battery_pct DOUBLE,
  total_connected_clients BIGINT, total_errors BIGINT,
  last_reading_time TIMESTAMP, iot_health_score DOUBLE
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (infrastructure_id)
COMMENT 'Aggregated IoT device health per infrastructure asset (0-100 score).'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_vehicle_health (
  vehicle_id STRING, vin STRING, make STRING, model STRING, region_code STRING,
  health_profile STRING, last_reading_ts TIMESTAMP, reading_count BIGINT,
  odometer_km DOUBLE,
  latest_engine_temp_c DOUBLE, engine_temp_7d_max DOUBLE, engine_temp_trend DOUBLE,
  latest_oil_life_pct DOUBLE, oil_life_7d_min DOUBLE,
  latest_battery_voltage DOUBLE, battery_7d_min DOUBLE,
  latest_tire_pressure_psi DOUBLE,
  harsh_events_7d BIGINT, avg_dtc_active DOUBLE,
  avg_health_score DOUBLE, latest_health_score DOUBLE,
  maintenance_risk_score DOUBLE, risk_category STRING, needs_maintenance INT
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (region_code, risk_category)
COMMENT 'Per-vehicle maintenance risk from telematics trends (0-100). Training/scoring source for the fleet predictive maintenance model.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.gold_fleet_summary (
  region_code STRING, total_vehicles BIGINT,
  avg_health_score DOUBLE, critical_count BIGINT, high_count BIGINT,
  medium_count BIGINT, low_count BIGINT, needs_maintenance_count BIGINT,
  avg_odometer_km DOUBLE, avg_engine_temp_c DOUBLE
) USING ICEBERG
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
CLUSTER BY (region_code)
COMMENT 'Per-region fleet health rollup for the Fleet dashboard.'
""")

# Write target for PyIceberg OSS demo
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.oss_iceberg_analytics (
  analysis_id STRING, source_table STRING, analysis_type STRING,
  metric_value DOUBLE, computed_at STRING, engine STRING
) USING ICEBERG
COMMENT 'Demo table written by PyIceberg OSS client to prove open-source Iceberg interop.'
""")

print("Gold + OSS Iceberg tables created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer — Raw Ingestion

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import TimestampType

# ── Static Bronze: Network Nodes ──
# Overwrite each run (static file, idempotent)
nodes_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{VOLUME_PATH}/network_nodes.csv")
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/network_nodes.csv"))
    .withColumn("_file_modified_at", current_timestamp())
    .withColumn("_ingested_at", current_timestamp())
)
nodes_df.createOrReplaceTempView("_tmp_bronze_nodes")
spark.sql(f"INSERT OVERWRITE {FQN}.bronze_network_nodes SELECT * FROM _tmp_bronze_nodes")
print(f"Bronze network_nodes: {spark.table(f'{FQN}.bronze_network_nodes').count()} rows loaded")

# COMMAND ----------

# ── Static Bronze: Network Performance ──
perf_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "false")
    .load(f"{VOLUME_PATH}/network_performance.csv")
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/network_performance.csv"))
    .withColumn("_file_modified_at", current_timestamp())
    .withColumn("_ingested_at", current_timestamp())
)
perf_df.createOrReplaceTempView("_tmp_bronze_perf")
spark.sql(f"INSERT OVERWRITE {FQN}.bronze_network_performance SELECT * FROM _tmp_bronze_perf")
print(f"Bronze network_performance: {spark.table(f'{FQN}.bronze_network_performance').count()} rows loaded")

# COMMAND ----------

# ── Static Bronze: Network Outages ──
# JSON has struct fields as strings ("true"/"false"), so we read with inference
# and explicitly cast struct fields to match the DDL
outages_df = (
    spark.read.format("json")
    .load(f"{VOLUME_PATH}/network_outages.json")
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/network_outages.json"))
    .withColumn("_file_modified_at", current_timestamp())
    .withColumn("_ingested_at", current_timestamp())
)
outages_df.createOrReplaceTempView("_tmp_bronze_outages")
spark.sql(f"""INSERT OVERWRITE {FQN}.bronze_network_outages
SELECT
  outage_id, node_id, node_type, region_code, severity, root_cause,
  start_time, end_time,
  CAST(duration_minutes AS STRING) AS duration_minutes,
  CAST(affected_customers AS STRING) AS affected_customers,
  NAMED_STRUCT(
    'estimated_revenue_impact', CAST(impact.estimated_revenue_impact AS STRING),
    'service_degraded', CAST(impact.service_degraded AS STRING),
    'service_down', CAST(impact.service_down AS STRING)
  ) AS impact,
  NAMED_STRUCT(
    'dispatch_required', CAST(resolution.dispatch_required AS STRING),
    'resolved_by', CAST(resolution.resolved_by AS STRING),
    'work_order_created', CAST(resolution.work_order_created AS STRING)
  ) AS resolution,
  reported_at, detected_by,
  _source_file, _file_modified_at, _ingested_at
FROM _tmp_bronze_outages""")
print(f"Bronze network_outages: {spark.table(f'{FQN}.bronze_network_outages').count()} rows loaded")

# COMMAND ----------

# ── Streaming Bronze: IoT Telemetry (Structured Streaming → Iceberg) ──
# Per internal docs: you CAN stream write to Managed Iceberg, but you must
# pre-create the table first (done above). Then use .toTable() with the
# pre-created table. Auto Loader (cloudFiles) uses a Delta-specific write
# path, so we use standard file streaming with trigger(availableNow=True).
try:
    from pyspark.sql.types import StructType, StructField, StringType as SparkStringType

    iot_schema = StructType([
        StructField("device_id", SparkStringType()),
        StructField("infrastructure_id", SparkStringType()),
        StructField("device_type", SparkStringType()),
        StructField("timestamp", SparkStringType()),
        StructField("signal_strength_dbm", SparkStringType()),
        StructField("throughput_mbps", SparkStringType()),
        StructField("latency_ms", SparkStringType()),
        StructField("packet_loss_pct", SparkStringType()),
        StructField("temperature_celsius", SparkStringType()),
        StructField("battery_pct", SparkStringType()),
        StructField("connected_clients", SparkStringType()),
        StructField("error_count", SparkStringType()),
        StructField("firmware_version", SparkStringType()),
        StructField("lat", SparkStringType()),
        StructField("lng", SparkStringType()),
    ])

    iot_stream = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("delimiter", "|")
        .option("pathGlobFilter", "iot_telemetry_*.csv")
        .schema(iot_schema)
        .load(VOLUME_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modified_at", current_timestamp())
        .withColumn("_ingested_at", current_timestamp())
    )

    iot_query = (
        iot_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/iot_bronze")
        .trigger(availableNow=True)
        .toTable(f"{FQN}.bronze_iot_telemetry")
    )
    iot_query.awaitTermination()
    print(f"Bronze IoT telemetry: streaming ingest completed ({spark.table(f'{FQN}.bronze_iot_telemetry').count()} rows)")
except Exception as e:
    if "Path does not exist" in str(e) or "is not a valid" in str(e):
        print("Bronze IoT telemetry: No IoT files found yet (run the simulator to generate data)")
    else:
        # Fall back to batch if streaming still fails
        print(f"Streaming write failed ({e}), falling back to batch...")
        iot_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", "|")
            .option("inferSchema", "false")
            .load(f"{VOLUME_PATH}/iot_telemetry_*.csv")
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_file_modified_at", current_timestamp())
            .withColumn("_ingested_at", current_timestamp())
        )
        iot_df.createOrReplaceTempView("_tmp_bronze_iot")
        spark.sql(f"INSERT OVERWRITE {FQN}.bronze_iot_telemetry SELECT * FROM _tmp_bronze_iot")
        print(f"Bronze IoT telemetry (batch fallback): {spark.table(f'{FQN}.bronze_iot_telemetry').count()} rows")

# COMMAND ----------

# ── Batch Bronze: Vehicle Telemetry (static export → Iceberg, idempotent) ──
# Vehicle telematics is exported from the Lakebase OLTP vehicle_telemetry table by
# notebooks/generate_fleet_telemetry to a single CSV in the Volume. We INSERT OVERWRITE
# (same pattern as the static network files above) so re-running deploy_all rebuilds
# bronze cleanly with no duplicate accumulation.
try:
    veh_df = (
        spark.read.format("csv")
        .option("header", "true").option("delimiter", "|").option("inferSchema", "false")
        .option("pathGlobFilter", "vehicle_telemetry_*.csv")
        .load(VOLUME_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modified_at", current_timestamp())
        .withColumn("_ingested_at", current_timestamp())
    )
    veh_df.createOrReplaceTempView("_tmp_bronze_veh")
    spark.sql(f"INSERT OVERWRITE {FQN}.bronze_vehicle_telemetry SELECT * FROM _tmp_bronze_veh")
    print(f"Bronze vehicle telemetry: {spark.table(f'{FQN}.bronze_vehicle_telemetry').count()} rows loaded")
except Exception as e:
    if any(s in str(e) for s in ("Path does not exist", "is not a valid", "UNABLE_TO_INFER_SCHEMA", "Unable to infer schema")):
        print("Bronze vehicle telemetry: No vehicle files found yet (run generate_fleet_telemetry)")
    else:
        print(f"Bronze vehicle telemetry load failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer — Cleaned, Typed, Validated

# COMMAND ----------

# ── Silver: Network Nodes ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.silver_network_nodes
SELECT
  node_id, node_name, node_type, region_code, state,
  TRY_CAST(latitude AS DOUBLE) AS latitude,
  TRY_CAST(longitude AS DOUBLE) AS longitude,
  TRY_CAST(NULLIF(elevation_ft, '') AS DOUBLE) AS elevation_ft,
  TRY_CAST(install_date AS DATE) AS install_date,
  vendor, model, firmware_version,
  TRY_CAST(max_capacity_mbps AS INT) AS max_capacity_mbps,
  TRY_CAST(power_consumption_kw AS DOUBLE) AS power_consumption_kw,
  NULLIF(backhaul_type, '') AS backhaul_type,
  CASE WHEN has_backup_power = 'Y' THEN TRUE
       WHEN has_backup_power = 'N' THEN FALSE ELSE NULL END AS has_backup_power,
  LOWER(TRIM(status)) AS status,
  TRY_CAST(last_maintenance_date AS DATE) AS last_maintenance_date,
  DATEDIFF(current_date(), TRY_CAST(install_date AS DATE)) AS age_days,
  DATEDIFF(current_date(), TRY_CAST(last_maintenance_date AS DATE)) AS days_since_maintenance,
  _source_file, _ingested_at
FROM {FQN}.bronze_network_nodes
WHERE node_id IS NOT NULL
  AND TRY_CAST(latitude AS DOUBLE) BETWEEN -90 AND 90
  AND TRY_CAST(longitude AS DOUBLE) BETWEEN -180 AND 180
  AND TRY_CAST(max_capacity_mbps AS INT) > 0
  AND status IS NOT NULL AND status != ''
  AND node_type IN ('cell_tower_5g', 'cell_tower_4g', 'fiber_cabinet',
                    'central_office', 'remote_terminal', 'small_cell', 'microwave_relay')
""")
print(f"Silver network_nodes: {spark.table(f'{FQN}.silver_network_nodes').count()} rows")

# COMMAND ----------

# ── Silver: Network Performance ──
# TRY_CAST tolerates malformed values (e.g. 'INVALID') by returning NULL
spark.sql(f"""
INSERT OVERWRITE {FQN}.silver_network_performance
SELECT
  TRY_CAST(measurement_id AS BIGINT) AS measurement_id,
  node_id,
  TRY_CAST(timestamp AS TIMESTAMP) AS measurement_timestamp,
  TRY_CAST(signal_strength_dbm AS DOUBLE) AS signal_strength_dbm,
  TRY_CAST(throughput_mbps AS DOUBLE) AS throughput_mbps,
  TRY_CAST(latency_ms AS DOUBLE) AS latency_ms,
  TRY_CAST(packet_loss_pct AS DOUBLE) AS packet_loss_pct,
  TRY_CAST(jitter_ms AS DOUBLE) AS jitter_ms,
  TRY_CAST(connected_users AS INT) AS connected_users,
  TRY_CAST(cpu_utilization_pct AS DOUBLE) AS cpu_utilization_pct,
  TRY_CAST(memory_utilization_pct AS DOUBLE) AS memory_utilization_pct,
  TRY_CAST(temperature_celsius AS DOUBLE) AS temperature_celsius,
  TRY_CAST(uptime_hours AS DOUBLE) AS uptime_hours,
  TRY_CAST(error_count AS INT) AS error_count,
  TRY_CAST(bandwidth_utilization_pct AS DOUBLE) AS bandwidth_utilization_pct,
  DATE(TRY_CAST(timestamp AS TIMESTAMP)) AS measurement_date,
  HOUR(TRY_CAST(timestamp AS TIMESTAMP)) AS measurement_hour,
  _source_file, _ingested_at
FROM {FQN}.bronze_network_performance
WHERE node_id IS NOT NULL
  AND timestamp IS NOT NULL
  AND TRY_CAST(throughput_mbps AS DOUBLE) >= 0
  AND TRY_CAST(cpu_utilization_pct AS DOUBLE) BETWEEN 0 AND 100
  AND TRY_CAST(latency_ms AS DOUBLE) >= 0
""")
print(f"Silver network_performance: {spark.table(f'{FQN}.silver_network_performance').count()} rows")

# COMMAND ----------

# ── Silver: Network Outages ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.silver_network_outages
SELECT
  outage_id, node_id, node_type, region_code,
  LOWER(TRIM(severity)) AS severity,
  root_cause,
  TRY_CAST(start_time AS TIMESTAMP) AS start_time,
  TRY_CAST(end_time AS TIMESTAMP) AS end_time,
  TRY_CAST(duration_minutes AS INT) AS duration_minutes,
  TRY_CAST(affected_customers AS INT) AS affected_customers,
  TRY_CAST(impact.service_degraded AS BOOLEAN) AS service_degraded,
  TRY_CAST(impact.service_down AS BOOLEAN) AS service_down,
  TRY_CAST(impact.estimated_revenue_impact AS DOUBLE) AS estimated_revenue_impact,
  resolution.resolved_by AS resolved_by,
  TRY_CAST(resolution.dispatch_required AS BOOLEAN) AS dispatch_required,
  TRY_CAST(resolution.work_order_created AS BOOLEAN) AS work_order_created,
  TRY_CAST(reported_at AS TIMESTAMP) AS reported_at,
  detected_by,
  CASE WHEN end_time IS NULL THEN TRUE ELSE FALSE END AS is_ongoing,
  DATE(TRY_CAST(start_time AS TIMESTAMP)) AS outage_date,
  _source_file, _ingested_at
FROM {FQN}.bronze_network_outages
WHERE outage_id IS NOT NULL
  AND node_id IS NOT NULL
  AND LOWER(TRIM(severity)) IN ('critical', 'major', 'minor', 'warning')
  AND start_time IS NOT NULL
""")
print(f"Silver network_outages: {spark.table(f'{FQN}.silver_network_outages').count()} rows")

# COMMAND ----------

# ── Silver: IoT Telemetry ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.silver_iot_telemetry
SELECT
  device_id,
  infrastructure_id,
  device_type,
  TRY_CAST(timestamp AS TIMESTAMP) AS timestamp,
  TRY_CAST(signal_strength_dbm AS DOUBLE) AS signal_strength_dbm,
  TRY_CAST(throughput_mbps AS DOUBLE) AS throughput_mbps,
  TRY_CAST(latency_ms AS DOUBLE) AS latency_ms,
  TRY_CAST(packet_loss_pct AS DOUBLE) AS packet_loss_pct,
  TRY_CAST(temperature_celsius AS DOUBLE) AS temperature_celsius,
  TRY_CAST(battery_pct AS DOUBLE) AS battery_pct,
  TRY_CAST(connected_clients AS INT) AS connected_clients,
  TRY_CAST(error_count AS INT) AS error_count,
  firmware_version,
  TRY_CAST(lat AS DOUBLE) AS lat,
  TRY_CAST(lng AS DOUBLE) AS lng,
  DATE(TRY_CAST(timestamp AS TIMESTAMP)) AS reading_date,
  HOUR(TRY_CAST(timestamp AS TIMESTAMP)) AS reading_hour,
  _source_file, _ingested_at
FROM {FQN}.bronze_iot_telemetry
WHERE device_id IS NOT NULL
  AND infrastructure_id IS NOT NULL
  AND TRY_CAST(signal_strength_dbm AS DOUBLE) BETWEEN -120 AND 0
  AND TRY_CAST(throughput_mbps AS DOUBLE) >= 0
  AND TRY_CAST(latency_ms AS DOUBLE) >= 0
  AND TRY_CAST(temperature_celsius AS DOUBLE) BETWEEN -40 AND 150
""")
print(f"Silver iot_telemetry: {spark.table(f'{FQN}.silver_iot_telemetry').count()} rows")

# COMMAND ----------

# ── Silver: Vehicle Telemetry ──
# Range validation is the "data trust" layer: implausible sensor readings (a coolant
# temp of 400C, a negative odometer, a 25V battery) are dropped so they never trigger
# a false-alarm maintenance dispatch. TRY_CAST turns malformed strings into NULL.
spark.sql(f"""
INSERT OVERWRITE {FQN}.silver_vehicle_telemetry
SELECT
  vehicle_id, vin, make, model, region_code, health_profile,
  TRY_CAST(timestamp AS TIMESTAMP) AS reading_ts,
  TRY_CAST(odometer_km AS DOUBLE) AS odometer_km,
  TRY_CAST(speed_avg_kph AS DOUBLE) AS speed_avg_kph,
  TRY_CAST(engine_temp_c AS DOUBLE) AS engine_temp_c,
  TRY_CAST(oil_life_pct AS DOUBLE) AS oil_life_pct,
  TRY_CAST(battery_voltage AS DOUBLE) AS battery_voltage,
  TRY_CAST(fuel_level_pct AS DOUBLE) AS fuel_level_pct,
  TRY_CAST(tire_pressure_psi AS DOUBLE) AS tire_pressure_psi,
  TRY_CAST(engine_rpm_avg AS INT) AS engine_rpm_avg,
  TRY_CAST(harsh_brake_count AS INT) AS harsh_brake_count,
  TRY_CAST(harsh_accel_count AS INT) AS harsh_accel_count,
  TRY_CAST(idle_minutes AS DOUBLE) AS idle_minutes,
  TRY_CAST(dtc_active_count AS INT) AS dtc_active_count,
  ROUND(GREATEST(0, LEAST(100,
    100
    - (CASE WHEN TRY_CAST(engine_temp_c AS DOUBLE) > 110 THEN 35
            WHEN TRY_CAST(engine_temp_c AS DOUBLE) > 100 THEN 15 ELSE 0 END)
    - (CASE WHEN TRY_CAST(oil_life_pct AS DOUBLE) < 15 THEN 25
            WHEN TRY_CAST(oil_life_pct AS DOUBLE) < 30 THEN 10 ELSE 0 END)
    - (CASE WHEN TRY_CAST(battery_voltage AS DOUBLE) < 11.8 THEN 20
            WHEN TRY_CAST(battery_voltage AS DOUBLE) < 12.1 THEN 8 ELSE 0 END)
  )), 1) AS health_score,
  TRY_CAST(lat AS DOUBLE) AS lat,
  TRY_CAST(lng AS DOUBLE) AS lng,
  DATE(TRY_CAST(timestamp AS TIMESTAMP)) AS reading_date,
  _source_file, _ingested_at
FROM {FQN}.bronze_vehicle_telemetry
WHERE vehicle_id IS NOT NULL
  AND TRY_CAST(timestamp AS TIMESTAMP) IS NOT NULL
  AND TRY_CAST(engine_temp_c AS DOUBLE) BETWEEN -40 AND 150
  AND TRY_CAST(oil_life_pct AS DOUBLE) BETWEEN 0 AND 100
  AND TRY_CAST(battery_voltage AS DOUBLE) BETWEEN 6 AND 16
  AND TRY_CAST(odometer_km AS DOUBLE) >= 0
""")
print(f"Silver vehicle_telemetry: {spark.table(f'{FQN}.silver_vehicle_telemetry').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer — Aggregated Analytics

# COMMAND ----------

# ── Gold: Daily Node Health ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_daily_node_health
SELECT
  p.node_id, n.node_name, n.node_type, n.region_code, n.vendor,
  p.measurement_date,
  COUNT(*) AS measurement_count,
  ROUND(AVG(p.throughput_mbps), 2) AS avg_throughput_mbps,
  ROUND(MAX(p.throughput_mbps), 2) AS peak_throughput_mbps,
  ROUND(AVG(p.latency_ms), 2) AS avg_latency_ms,
  ROUND(MAX(p.latency_ms), 2) AS max_latency_ms,
  ROUND(AVG(p.packet_loss_pct), 4) AS avg_packet_loss_pct,
  ROUND(AVG(p.connected_users), 0) AS avg_connected_users,
  ROUND(MAX(p.connected_users), 0) AS peak_connected_users,
  ROUND(AVG(p.cpu_utilization_pct), 1) AS avg_cpu_pct,
  ROUND(MAX(p.cpu_utilization_pct), 1) AS peak_cpu_pct,
  ROUND(AVG(p.temperature_celsius), 1) AS avg_temperature_c,
  SUM(p.error_count) AS total_errors,
  ROUND(AVG(p.bandwidth_utilization_pct), 1) AS avg_bandwidth_util_pct,
  ROUND(GREATEST(0, LEAST(100,
    100
    - (CASE WHEN AVG(p.latency_ms) > 50 THEN 20 WHEN AVG(p.latency_ms) > 25 THEN 10 ELSE 0 END)
    - (CASE WHEN AVG(p.packet_loss_pct) > 1.0 THEN 25 WHEN AVG(p.packet_loss_pct) > 0.5 THEN 10 ELSE 0 END)
    - (CASE WHEN SUM(p.error_count) > 50 THEN 20 WHEN SUM(p.error_count) > 10 THEN 10 ELSE 0 END)
    - (CASE WHEN AVG(p.bandwidth_utilization_pct) > 90 THEN 15 WHEN AVG(p.bandwidth_utilization_pct) > 75 THEN 5 ELSE 0 END)
    - (CASE WHEN AVG(p.cpu_utilization_pct) > 90 THEN 10 WHEN AVG(p.cpu_utilization_pct) > 80 THEN 5 ELSE 0 END)
  )), 0) AS health_score
FROM {FQN}.silver_network_performance p
JOIN {FQN}.silver_network_nodes n ON p.node_id = n.node_id
GROUP BY p.node_id, n.node_name, n.node_type, n.region_code, n.vendor, p.measurement_date
""")
print(f"Gold daily_node_health: {spark.table(f'{FQN}.gold_daily_node_health').count()} rows")

# COMMAND ----------

# ── Gold: Regional Network Summary ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_regional_network_summary
SELECT
  region_code, measurement_date,
  COUNT(DISTINCT node_id) AS active_nodes,
  ROUND(AVG(health_score), 1) AS avg_health_score,
  SUM(CASE WHEN health_score < 50 THEN 1 ELSE 0 END) AS unhealthy_node_count,
  SUM(CASE WHEN health_score >= 80 THEN 1 ELSE 0 END) AS healthy_node_count,
  ROUND(AVG(avg_throughput_mbps), 2) AS region_avg_throughput_mbps,
  ROUND(AVG(avg_latency_ms), 2) AS region_avg_latency_ms,
  ROUND(AVG(avg_packet_loss_pct), 4) AS region_avg_packet_loss_pct,
  ROUND(SUM(avg_connected_users), 0) AS region_total_connected_users,
  SUM(total_errors) AS region_total_errors
FROM {FQN}.gold_daily_node_health
GROUP BY region_code, measurement_date
""")
print(f"Gold regional_network_summary: {spark.table(f'{FQN}.gold_regional_network_summary').count()} rows")

# COMMAND ----------

# ── Gold: Daily Outage Summary ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_daily_outage_summary
SELECT
  region_code, outage_date, severity,
  COUNT(*) AS outage_count,
  SUM(affected_customers) AS total_affected_customers,
  ROUND(AVG(duration_minutes), 1) AS avg_duration_minutes,
  SUM(duration_minutes) AS total_outage_minutes,
  ROUND(SUM(estimated_revenue_impact), 2) AS total_revenue_impact,
  SUM(CASE WHEN dispatch_required THEN 1 ELSE 0 END) AS dispatches_required,
  SUM(CASE WHEN work_order_created THEN 1 ELSE 0 END) AS work_orders_created,
  SUM(CASE WHEN is_ongoing THEN 1 ELSE 0 END) AS ongoing_outages
FROM {FQN}.silver_network_outages
GROUP BY region_code, outage_date, severity
""")
print(f"Gold daily_outage_summary: {spark.table(f'{FQN}.gold_daily_outage_summary').count()} rows")

# COMMAND ----------

# ── Gold: Node Maintenance Risk ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_node_maintenance_risk
SELECT
  n.node_id, n.node_name, n.node_type, n.region_code, n.vendor,
  n.install_date, n.age_days, n.days_since_maintenance, n.has_backup_power, n.status,
  h.avg_health_score AS recent_avg_health,
  h.total_outages AS recent_outage_count,
  ROUND(LEAST(100,
    (CASE WHEN n.age_days > 2555 THEN 25 WHEN n.age_days > 1825 THEN 15 WHEN n.age_days > 1095 THEN 5 ELSE 0 END)
    + (CASE WHEN n.days_since_maintenance > 365 THEN 25 WHEN n.days_since_maintenance > 180 THEN 15 WHEN n.days_since_maintenance > 90 THEN 5 ELSE 0 END)
    + (CASE WHEN h.avg_health_score < 50 THEN 25 WHEN h.avg_health_score < 70 THEN 15 WHEN h.avg_health_score < 85 THEN 5 ELSE 0 END)
    + (CASE WHEN h.total_outages > 5 THEN 15 WHEN h.total_outages > 2 THEN 8 WHEN h.total_outages > 0 THEN 3 ELSE 0 END)
    + (CASE WHEN n.has_backup_power = FALSE THEN 10 ELSE 0 END)
  ), 0) AS maintenance_risk_score,
  CASE
    WHEN LEAST(100,
      (CASE WHEN n.age_days > 2555 THEN 25 WHEN n.age_days > 1825 THEN 15 WHEN n.age_days > 1095 THEN 5 ELSE 0 END)
      + (CASE WHEN n.days_since_maintenance > 365 THEN 25 WHEN n.days_since_maintenance > 180 THEN 15 WHEN n.days_since_maintenance > 90 THEN 5 ELSE 0 END)
      + (CASE WHEN h.avg_health_score < 50 THEN 25 WHEN h.avg_health_score < 70 THEN 15 WHEN h.avg_health_score < 85 THEN 5 ELSE 0 END)
      + (CASE WHEN h.total_outages > 5 THEN 15 WHEN h.total_outages > 2 THEN 8 WHEN h.total_outages > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN n.has_backup_power = FALSE THEN 10 ELSE 0 END)
    ) >= 75 THEN 'CRITICAL'
    WHEN LEAST(100,
      (CASE WHEN n.age_days > 2555 THEN 25 WHEN n.age_days > 1825 THEN 15 WHEN n.age_days > 1095 THEN 5 ELSE 0 END)
      + (CASE WHEN n.days_since_maintenance > 365 THEN 25 WHEN n.days_since_maintenance > 180 THEN 15 WHEN n.days_since_maintenance > 90 THEN 5 ELSE 0 END)
      + (CASE WHEN h.avg_health_score < 50 THEN 25 WHEN h.avg_health_score < 70 THEN 15 WHEN h.avg_health_score < 85 THEN 5 ELSE 0 END)
      + (CASE WHEN h.total_outages > 5 THEN 15 WHEN h.total_outages > 2 THEN 8 WHEN h.total_outages > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN n.has_backup_power = FALSE THEN 10 ELSE 0 END)
    ) >= 55 THEN 'HIGH'
    WHEN LEAST(100,
      (CASE WHEN n.age_days > 2555 THEN 25 WHEN n.age_days > 1825 THEN 15 WHEN n.age_days > 1095 THEN 5 ELSE 0 END)
      + (CASE WHEN n.days_since_maintenance > 365 THEN 25 WHEN n.days_since_maintenance > 180 THEN 15 WHEN n.days_since_maintenance > 90 THEN 5 ELSE 0 END)
      + (CASE WHEN h.avg_health_score < 50 THEN 25 WHEN h.avg_health_score < 70 THEN 15 WHEN h.avg_health_score < 85 THEN 5 ELSE 0 END)
      + (CASE WHEN h.total_outages > 5 THEN 15 WHEN h.total_outages > 2 THEN 8 WHEN h.total_outages > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN n.has_backup_power = FALSE THEN 10 ELSE 0 END)
    ) >= 30 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_category
FROM {FQN}.silver_network_nodes n
LEFT JOIN (
  SELECT
    node_id,
    ROUND(AVG(health_score), 1) AS avg_health_score,
    COUNT(DISTINCT outage_date) AS total_outages
  FROM (
    SELECT node_id, health_score, NULL AS outage_date
    FROM {FQN}.gold_daily_node_health
    WHERE measurement_date >= DATE_SUB(current_date(), 30)
    UNION ALL
    SELECT node_id, NULL, outage_date
    FROM {FQN}.silver_network_outages
    WHERE outage_date >= DATE_SUB(current_date(), 90)
  )
  GROUP BY node_id
) h ON n.node_id = h.node_id
WHERE n.status IN ('active', 'degraded')
""")
print(f"Gold node_maintenance_risk: {spark.table(f'{FQN}.gold_node_maintenance_risk').count()} rows")

# COMMAND ----------

# ── Gold: Outage Root Cause Analysis ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_outage_root_cause_analysis
SELECT
  root_cause, severity,
  COUNT(*) AS total_outages,
  SUM(affected_customers) AS total_affected_customers,
  ROUND(AVG(duration_minutes), 1) AS avg_duration_minutes,
  ROUND(PERCENTILE_APPROX(duration_minutes, 0.5), 1) AS median_duration_minutes,
  ROUND(PERCENTILE_APPROX(duration_minutes, 0.95), 1) AS p95_duration_minutes,
  SUM(CASE WHEN dispatch_required THEN 1 ELSE 0 END) AS total_dispatches,
  ROUND(SUM(CASE WHEN dispatch_required THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS dispatch_rate_pct,
  ROUND(SUM(estimated_revenue_impact), 2) AS total_revenue_impact
FROM {FQN}.silver_network_outages
WHERE NOT is_ongoing
GROUP BY root_cause, severity
""")
print(f"Gold outage_root_cause_analysis: {spark.table(f'{FQN}.gold_outage_root_cause_analysis').count()} rows")

# COMMAND ----------

# ── Gold: IoT Device Health ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_iot_device_health
SELECT
  infrastructure_id,
  COUNT(DISTINCT device_id) AS device_count,
  ROUND(AVG(signal_strength_dbm), 1) AS avg_signal_dbm,
  ROUND(AVG(throughput_mbps), 2) AS avg_throughput_mbps,
  ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
  ROUND(AVG(packet_loss_pct), 3) AS avg_packet_loss_pct,
  ROUND(AVG(temperature_celsius), 1) AS avg_temperature_c,
  ROUND(AVG(battery_pct), 1) AS avg_battery_pct,
  SUM(connected_clients) AS total_connected_clients,
  SUM(error_count) AS total_errors,
  MAX(timestamp) AS last_reading_time,
  ROUND(GREATEST(0, LEAST(100,
    100
    - (CASE WHEN AVG(latency_ms) > 100 THEN 25 WHEN AVG(latency_ms) > 40 THEN 15 WHEN AVG(latency_ms) > 20 THEN 5 ELSE 0 END)
    - (CASE WHEN AVG(packet_loss_pct) > 3.0 THEN 25 WHEN AVG(packet_loss_pct) > 1.0 THEN 10 WHEN AVG(packet_loss_pct) > 0.5 THEN 5 ELSE 0 END)
    - (CASE WHEN SUM(error_count) > 100 THEN 20 WHEN SUM(error_count) > 30 THEN 10 WHEN SUM(error_count) > 10 THEN 5 ELSE 0 END)
    - (CASE WHEN AVG(temperature_celsius) > 70 THEN 15 WHEN AVG(temperature_celsius) > 50 THEN 8 WHEN AVG(temperature_celsius) > 40 THEN 3 ELSE 0 END)
    - (CASE WHEN AVG(battery_pct) < 20 THEN 15 WHEN AVG(battery_pct) < 40 THEN 8 WHEN AVG(battery_pct) < 60 THEN 3 ELSE 0 END)
  )), 0) AS iot_health_score
FROM {FQN}.silver_iot_telemetry
WHERE reading_date >= DATE_SUB(current_date(), 1)
GROUP BY infrastructure_id
""")
print(f"Gold iot_device_health: {spark.table(f'{FQN}.gold_iot_device_health').count()} rows")

# COMMAND ----------

# ── Gold: Vehicle Health (per-vehicle maintenance risk from telematics trends) ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_vehicle_health
WITH latest AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY reading_ts DESC) AS rn
    FROM {FQN}.silver_vehicle_telemetry
  ) WHERE rn = 1
),
agg7 AS (
  SELECT
    vehicle_id,
    COUNT(*) AS reading_count,
    ROUND(MAX(engine_temp_c), 1) AS engine_temp_7d_max,
    ROUND(AVG(engine_temp_c), 1) AS engine_temp_7d_avg,
    ROUND(MIN(oil_life_pct), 1) AS oil_life_7d_min,
    ROUND(MIN(battery_voltage), 2) AS battery_7d_min,
    SUM(harsh_brake_count) + SUM(harsh_accel_count) AS harsh_events_7d,
    ROUND(AVG(dtc_active_count), 2) AS avg_dtc_active,
    ROUND(AVG(health_score), 1) AS avg_health_score
  FROM {FQN}.silver_vehicle_telemetry
  WHERE reading_date >= DATE_SUB(current_date(), 7)
  GROUP BY vehicle_id
),
scored AS (
  SELECT
    l.vehicle_id, l.vin, l.make, l.model, l.region_code, l.health_profile,
    l.reading_ts AS last_reading_ts, a.reading_count, l.odometer_km,
    l.engine_temp_c AS latest_engine_temp_c, a.engine_temp_7d_max,
    ROUND(l.engine_temp_c - a.engine_temp_7d_avg, 1) AS engine_temp_trend,
    l.oil_life_pct AS latest_oil_life_pct, a.oil_life_7d_min,
    l.battery_voltage AS latest_battery_voltage, a.battery_7d_min,
    l.tire_pressure_psi AS latest_tire_pressure_psi,
    a.harsh_events_7d, a.avg_dtc_active,
    a.avg_health_score, l.health_score AS latest_health_score,
    LEAST(100,
        (CASE WHEN a.engine_temp_7d_max > 115 THEN 30 WHEN a.engine_temp_7d_max > 105 THEN 18 WHEN a.engine_temp_7d_max > 98 THEN 8 ELSE 0 END)
      + (CASE WHEN a.oil_life_7d_min < 10 THEN 25 WHEN a.oil_life_7d_min < 25 THEN 14 WHEN a.oil_life_7d_min < 40 THEN 6 ELSE 0 END)
      + (CASE WHEN a.battery_7d_min < 11.5 THEN 20 WHEN a.battery_7d_min < 12.0 THEN 10 ELSE 0 END)
      + (CASE WHEN a.avg_dtc_active > 2 THEN 15 WHEN a.avg_dtc_active > 0.5 THEN 8 ELSE 0 END)
      + (CASE WHEN l.odometer_km > 240000 THEN 10 WHEN l.odometer_km > 160000 THEN 5 ELSE 0 END)
    ) AS maintenance_risk_score
  FROM latest l
  LEFT JOIN agg7 a ON l.vehicle_id = a.vehicle_id
)
SELECT
  vehicle_id, vin, make, model, region_code, health_profile,
  last_reading_ts, reading_count, odometer_km,
  latest_engine_temp_c, engine_temp_7d_max, engine_temp_trend,
  latest_oil_life_pct, oil_life_7d_min,
  latest_battery_voltage, battery_7d_min,
  latest_tire_pressure_psi, harsh_events_7d, avg_dtc_active,
  avg_health_score, latest_health_score,
  maintenance_risk_score,
  CASE WHEN maintenance_risk_score >= 60 THEN 'CRITICAL'
       WHEN maintenance_risk_score >= 40 THEN 'HIGH'
       WHEN maintenance_risk_score >= 20 THEN 'MEDIUM'
       ELSE 'LOW' END AS risk_category,
  CASE WHEN maintenance_risk_score >= 40 THEN 1 ELSE 0 END AS needs_maintenance
FROM scored
""")
print(f"Gold vehicle_health: {spark.table(f'{FQN}.gold_vehicle_health').count()} rows")

# COMMAND ----------

# ── Gold: Fleet Summary (per-region rollup) ──
spark.sql(f"""
INSERT OVERWRITE {FQN}.gold_fleet_summary
SELECT
  region_code,
  COUNT(*) AS total_vehicles,
  ROUND(AVG(avg_health_score), 1) AS avg_health_score,
  SUM(CASE WHEN risk_category = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
  SUM(CASE WHEN risk_category = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
  SUM(CASE WHEN risk_category = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_count,
  SUM(CASE WHEN risk_category = 'LOW' THEN 1 ELSE 0 END) AS low_count,
  SUM(needs_maintenance) AS needs_maintenance_count,
  ROUND(AVG(odometer_km), 0) AS avg_odometer_km,
  ROUND(AVG(latest_engine_temp_c), 1) AS avg_engine_temp_c
FROM {FQN}.gold_vehicle_health
GROUP BY region_code
""")
print(f"Gold fleet_summary: {spark.table(f'{FQN}.gold_fleet_summary').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Pipeline complete — all Managed Iceberg tables refreshed")
print("=" * 60)
for t in ["bronze_network_nodes", "bronze_network_performance", "bronze_network_outages",
          "bronze_iot_telemetry", "bronze_vehicle_telemetry",
          "silver_network_nodes", "silver_network_performance",
          "silver_network_outages", "silver_iot_telemetry", "silver_vehicle_telemetry",
          "gold_daily_node_health",
          "gold_regional_network_summary", "gold_daily_outage_summary",
          "gold_node_maintenance_risk", "gold_outage_root_cause_analysis",
          "gold_iot_device_health", "gold_vehicle_health", "gold_fleet_summary",
          "oss_iceberg_analytics"]:
    try:
        cnt = spark.table(f"{FQN}.{t}").count()
        fmt = spark.sql(f"DESCRIBE DETAIL {FQN}.{t}").select("format").first()[0]
        print(f"  {t:45s} {cnt:>8,} rows  [{fmt}]")
    except Exception as e:
        print(f"  {t:45s} ERROR: {e}")
print("=" * 60)

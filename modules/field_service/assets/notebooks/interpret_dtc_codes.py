# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # DTC Interpretation with AI Functions (`ai_query`)
# MAGIC
# MAGIC Reads active OBD-II diagnostic trouble codes from Lakebase, enriches each with the
# MAGIC vehicle's recent telematics context, and calls the Databricks Foundation Model API
# MAGIC via SQL `ai_query()` to determine the **true** severity and whether the code is a
# MAGIC **false positive** (e.g. a P0455/P0457 EVAP code caused by a loose fuel cap rather
# MAGIC than an engine failure). The interpretation is written back to
# MAGIC `field_service.vehicle_dtc_codes` (ai_severity / ai_explanation / is_false_positive).
# MAGIC
# MAGIC This directly answers the customer's data-trust concern: telematics "sometimes
# MAGIC misidentifies a loose fuel cap as an engine failure." The AI layer denoises raw
# MAGIC codes so dispatchers act on real faults, and the **false-alarm rate** becomes a
# MAGIC first-class, measurable KPI.
# MAGIC
# MAGIC **Databricks capabilities showcased:**
# MAGIC - AI Functions: `ai_query()` against a Foundation Model endpoint, batched over SQL
# MAGIC - Structured JSON output parsed and operationalized back into Lakebase
# MAGIC - LLM-assisted data quality on top of the medallion
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `data/fleet_management.sql` applied (vehicle_dtc_codes seeded)
# MAGIC - A Foundation Model endpoint (default `databricks-claude-sonnet-4-5`)
# MAGIC
# MAGIC ### Parameters
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `llm_endpoint` | `databricks-claude-sonnet-4-5` | Foundation Model serving endpoint |
# MAGIC | `reinterpret_all` | `false` | Re-interpret codes already interpreted |

# COMMAND ----------

# MAGIC %pip install psycopg2-binary pyyaml "databricks-sdk>=0.87.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, json, base64, yaml
from pathlib import Path
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

repo_root = Path(os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
).replace("/notebooks", ""))
config_path = Path("/Workspace") / str(repo_root).lstrip("/") / "deployment" / "config.yaml"
cfg = yaml.safe_load(open(config_path)) if config_path.exists() else {}

try:
    LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
except Exception:
    LLM_ENDPOINT = cfg.get("llm_endpoint", "databricks-claude-sonnet-4-5")
try:
    REINTERPRET_ALL = dbutils.widgets.get("reinterpret_all").strip().lower() == "true"
except Exception:
    REINTERPRET_ALL = False

# Lakebase creds from the secret scope (job base_params); no config.yaml.
dbutils.widgets.text("secret_scope", "", "Secret scope for PG creds")
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "databricks_postgres", "PG database")
PG_DB = dbutils.widgets.get("pg_database") or "databricks_postgres"
PG_HOST = dbutils.widgets.get("pg_host")
_secret_scope = dbutils.widgets.get("secret_scope")
PG_USER = dbutils.secrets.get(scope=_secret_scope, key="pguser")
PG_TOKEN = dbutils.secrets.get(scope=_secret_scope, key="pgpassword")

print(f"LLM endpoint: {LLM_ENDPOINT}")
print(f"Lakebase: {PG_HOST}  (reinterpret_all={REINTERPRET_ALL})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read active DTCs + telematics context from Lakebase

# COMMAND ----------

import psycopg2

where_ai = "" if REINTERPRET_ALL else "AND d.ai_interpreted_at IS NULL"
conn = psycopg2.connect(host=PG_HOST, port=5432, dbname=PG_DB, user=PG_USER, password=PG_TOKEN, sslmode="require")
cur = conn.cursor()
cur.execute(f"""
    WITH recent_tel AS (
        SELECT vehicle_id,
               ROUND(AVG(engine_temp_c)::numeric, 1)   AS avg_engine_temp_c,
               ROUND(AVG(oil_life_pct)::numeric, 1)     AS avg_oil_life_pct,
               ROUND(AVG(battery_voltage)::numeric, 2)  AS avg_battery_voltage,
               ROUND(AVG(dtc_active_count)::numeric, 1) AS avg_dtc_active
        FROM field_service.vehicle_telemetry
        WHERE recorded_at >= now() - INTERVAL '7 days'
        GROUP BY vehicle_id
    )
    SELECT d.dtc_id, d.vehicle_id, d.code, d.raw_description, d.raw_severity,
           v.health_profile,
           COALESCE(t.avg_engine_temp_c, 0), COALESCE(t.avg_oil_life_pct, 0),
           COALESCE(t.avg_battery_voltage, 0), COALESCE(t.avg_dtc_active, 0)
    FROM field_service.vehicle_dtc_codes d
    JOIN field_service.fleet_vehicles v ON v.vehicle_id = d.vehicle_id
    LEFT JOIN recent_tel t ON t.vehicle_id = d.vehicle_id
    WHERE d.status = 'active' {where_ai}
""")
rows = cur.fetchall()
cur.close()
conn.close()
print(f"DTC codes to interpret: {len(rows):,}")

if not rows:
    dbutils.notebook.exit("No DTC codes to interpret")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build prompts and run `ai_query` (batched over SQL)

# COMMAND ----------

# Build the prompt in plain Python per row. (Do NOT use Spark format_string here:
# the prompt contains a literal '%' for "oil life", which printf-style format_string
# treats as a malformed format specifier and crashes with java.util.Formatter errors.)
def _build_prompt(code, raw_description, temp, oil, batt, dtc):
    return (
        "You are an automotive fleet diagnostics expert. A service van reported OBD-II code "
        f"{code} ({raw_description}). Recent 7-day telematics: engine coolant temp avg {temp}C, "
        f"oil life {oil} percent, battery {batt}V, {dtc} active codes on average. "
        "EVAP codes such as P0455 and P0457 are very often caused by a loose or missing fuel cap "
        "rather than an engine failure; if the corroborating telematics (engine temp, oil, battery) "
        "look normal, treat the code as a false positive with low severity. Conversely, codes like "
        "P0300, P0217, P0562 with abnormal telematics are genuine. "
        "Respond with ONLY a JSON object, no prose: "
        '{"severity":"low|medium|high|critical","is_false_positive":true or false,'
        '"explanation":"one concise sentence"}.'
    )

# rows columns: dtc_id, vehicle_id, code, raw_description, raw_severity, health_profile,
#               avg_engine_temp_c, avg_oil_life_pct, avg_battery_voltage, avg_dtc_active
prompt_rows = [
    (r[0], r[2], r[4], _build_prompt(r[2], r[3], r[6], r[7], r[8], r[9]))
    for r in rows
]
sdf = spark.createDataFrame(prompt_rows, ["dtc_id", "code", "raw_severity", "prompt"])
sdf.createOrReplaceTempView("_dtc_to_interpret")

# ai_query returns the model's text; we ask for JSON and parse it below.
interpreted = spark.sql(f"""
    SELECT dtc_id, code, raw_severity,
           ai_query('{LLM_ENDPOINT}', prompt) AS ai_response
    FROM _dtc_to_interpret
""")
interpreted.createOrReplaceTempView("_dtc_raw_ai")

# Parse the JSON response defensively (model may wrap in fences)
parsed = spark.sql("""
    SELECT
        dtc_id, code, raw_severity,
        ai_response,
        from_json(
            -- (?s) = DOTALL: ai_query returns markdown-fenced, multi-line JSON, so the
            -- '.' must span newlines to capture the whole object.
            regexp_extract(ai_response, '(?s)\\\\{.*\\\\}', 0),
            'severity STRING, is_false_positive BOOLEAN, explanation STRING'
        ) AS j
    FROM _dtc_raw_ai
""").select(
    "dtc_id", "code", "raw_severity",
    F.coalesce(F.col("j.severity"), F.col("raw_severity")).alias("ai_severity"),
    F.coalesce(F.col("j.is_false_positive"), F.lit(False)).alias("is_false_positive"),
    F.coalesce(F.col("j.explanation"), F.lit("AI interpretation unavailable; using raw severity.")).alias("ai_explanation"),
)

results = parsed.collect()
print(f"Interpreted {len(results)} codes")
fp = sum(1 for r in results if r["is_false_positive"])
print(f"False positives flagged: {fp} ({(fp/len(results)*100):.1f}% false-alarm rate)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write interpretations back to Lakebase

# COMMAND ----------

conn = psycopg2.connect(host=PG_HOST, port=5432, dbname=PG_DB, user=PG_USER, password=PG_TOKEN, sslmode="require")
conn.autocommit = True
cur = conn.cursor()
now = datetime.now(timezone.utc)
updated = 0
for r in results:
    sev = (r["ai_severity"] or "medium").lower()
    if sev not in ("low", "medium", "high", "critical"):
        sev = (r["raw_severity"] or "medium").lower()
    cur.execute("""
        UPDATE field_service.vehicle_dtc_codes
        SET ai_severity = %s, ai_explanation = %s, is_false_positive = %s, ai_interpreted_at = %s
        WHERE dtc_id = %s
    """, (sev, (r["ai_explanation"] or "")[:1000], bool(r["is_false_positive"]), now, int(r["dtc_id"])))
    updated += 1
cur.close()
conn.close()
print(f"Updated {updated} DTC rows in Lakebase with AI interpretation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | Component | Detail |
# MAGIC |------|-----------|--------|
# MAGIC | 1 | Read | Active DTCs + 7-day telematics context from Lakebase |
# MAGIC | 2 | `ai_query` | Foundation Model classifies true severity + false positive |
# MAGIC | 3 | Write-back | ai_severity / ai_explanation / is_false_positive in Lakebase |
# MAGIC
# MAGIC The Fleet page surfaces the **false-alarm rate** and AI explanations so dispatchers
# MAGIC trust the alerts. Re-run with `reinterpret_all=true` after tuning the prompt.

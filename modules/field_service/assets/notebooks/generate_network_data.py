# Databricks notebook source

# MAGIC %md
# MAGIC # Generate Network Raw Data → UC Volume
# MAGIC
# MAGIC Writes the telco network monitoring source files
# MAGIC (`network_nodes.csv`, `network_performance.csv`, `network_outages.json`)
# MAGIC into the pipeline's UC Volume so the Iceberg streaming pipeline has
# MAGIC something to ingest. Runs BEFORE the pipeline step.
# MAGIC
# MAGIC Widget-driven (base_params): `catalog`, `schema`, `volume_path`.

# COMMAND ----------

import os
import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
sys.path.insert(0, "/Workspace" + os.path.dirname(nb_path))

dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "network_data", "UC Schema")
dbutils.widgets.text("volume_path", "", "Volume Path")

CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
SCHEMA = dbutils.widgets.get("schema") or "network_data"
VOLUME_PATH = dbutils.widgets.get("volume_path") or f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

# Self-provision the catalog/schema/volume (idempotent; the pipeline step also does this).
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.raw_files")

os.makedirs(VOLUME_PATH, exist_ok=True)
print(f"Writing network raw data to {VOLUME_PATH}")

# COMMAND ----------

# The generator functions are vendored alongside this notebook.
import generate_raw_data as gen  # noqa: E402

nodes = gen.generate_network_nodes(VOLUME_PATH)
gen.generate_performance_metrics(VOLUME_PATH, nodes)
gen.generate_outage_events(VOLUME_PATH, nodes)

import json  # noqa: E402
print(f"Done. {len(nodes)} nodes + performance + outages written to {VOLUME_PATH}")
dbutils.notebook.exit(json.dumps({"volume_path": VOLUME_PATH, "nodes": len(nodes)}))  # noqa: F821

# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install databricks-agents databricks-sdk mlflow databricks-langchain langgraph langgraph-supervisor --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy Agent Endpoint
# MAGIC
# MAGIC Logs a NEW model version from this workspace (not reusing echostar's version),
# MAGIC then deploys it to a Model Serving endpoint.

# COMMAND ----------

import os, sys, json
from pathlib import Path

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
REPO_ROOT = Path(os.path.dirname(nb_path).replace("/notebooks", ""))
WORKSPACE_ROOT = Path("/Workspace") / str(REPO_ROOT).lstrip("/")
DEPLOYMENT_DIR = WORKSPACE_ROOT / "deployment"
os.environ["DEPLOYMENT_CONFIG_PATH"] = str(DEPLOYMENT_DIR / "config.yaml")
sys.path.insert(0, str(DEPLOYMENT_DIR))

from config import load_config, save_config, get_workspace_client  # noqa: E402

cfg = load_config()
w = get_workspace_client(cfg)

# COMMAND ----------

import mlflow
import mlflow.langchain
from mlflow.models.resources import DatabricksGenieSpace

PIPELINE_CATALOG = cfg.get("pipeline_catalog", "dba-lakebase-network")
MODEL_NAME = f"{PIPELINE_CATALOG}.agents.multi_genie_supervisor"
AGENT_SCRIPT = DEPLOYMENT_DIR / "agent_supervisor.py"

# Set experiment to this workspace
experiment_path = f"/Shared/agent_traces/multi_genie_supervisor"
try:
    mlflow.set_experiment(experiment_path)
except Exception:
    w.workspace.mkdirs("/Shared/agent_traces")
    mlflow.set_experiment(experiment_path)

print(f"Model: {MODEL_NAME}")
print(f"Script: {AGENT_SCRIPT}")
print(f"Experiment: {experiment_path}")

# COMMAND ----------

# Build resources list from Genie space IDs
genie_ids = cfg.get("genie_space_ids", {})
resources = [DatabricksGenieSpace(genie_space_id=sid) for sid in genie_ids.values() if sid]
print(f"Resources: {len(resources)} Genie spaces")

input_example = {"messages": [{"role": "user", "content": "What is the SLA compliance rate?"}]}

# Set env vars for the agent script
for env_key, cfg_key in [("FIELD_OPS_SPACE_ID", "field_ops"), ("POSTGRES_SPACE_ID", "postgres"),
                          ("NETWORK_HEALTH_SPACE_ID", "network_health"), ("SLA_WORKFORCE_SPACE_ID", "sla_workforce")]:
    os.environ[env_key] = genie_ids.get(cfg_key, "")

# COMMAND ----------

# Model already registered (versions 2-5 exist from this workspace).
# Just deploy the latest version to a serving endpoint.
from databricks import agents

# Get latest version
client = mlflow.MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest = max(versions, key=lambda v: int(v.version))
print(f"Using {MODEL_NAME} v{latest.version} (run_id={latest.run_id[:20]}...)")

# COMMAND ----------

# Deploy via REST API (agents.deploy has schema compatibility issues with langchain-logged models)
ENDPOINT_NAME = f"agents_{PIPELINE_CATALOG.replace('-','_')}_agents_multi_genie_supervisor"
print(f"Creating serving endpoint: {ENDPOINT_NAME}")

endpoint_config = {
    "name": ENDPOINT_NAME,
    "config": {
        "served_entities": [{
            "entity_name": MODEL_NAME,
            "entity_version": str(latest.version),
            "workload_size": "Small",
            "scale_to_zero_enabled": True,
            "environment_vars": {
                "ENABLE_LANGCHAIN_STREAMING": "true",
                "ENABLE_MLFLOW_TRACING": "true",
                "FIELD_OPS_SPACE_ID": genie_ids.get("field_ops", ""),
                "POSTGRES_SPACE_ID": genie_ids.get("postgres", ""),
                "NETWORK_HEALTH_SPACE_ID": genie_ids.get("network_health", ""),
                "SLA_WORKFORCE_SPACE_ID": genie_ids.get("sla_workforce", ""),
                "LLM_ENDPOINT": "databricks-claude-sonnet-4-5",
            },
        }],
    },
    "tags": [{"key": "env", "value": "production"}],
}

try:
    w.api_client.do("POST", "/api/2.0/serving-endpoints", body=endpoint_config)
    print(f"Created endpoint: {ENDPOINT_NAME}")
except Exception as e:
    if "already exists" in str(e).lower():
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config", body=endpoint_config["config"])
        print(f"Updated existing endpoint: {ENDPOINT_NAME}")
    else:
        raise

cfg["agent_model_name"] = MODEL_NAME
cfg["agent_endpoint_name"] = ENDPOINT_NAME
save_config(cfg)
print(f"Saved to config.yaml")

# COMMAND ----------

print("Agent deployment complete!")
dbutils.notebook.exit(json.dumps({"model": MODEL_NAME, "version": str(latest.version), "endpoint": ENDPOINT_NAME}))  # noqa: F821

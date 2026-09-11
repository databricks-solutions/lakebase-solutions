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
# Self-contained config from the job base_params (widgets) — no deployment/config.py.
dbutils.widgets.text("endpoint_name", "", "Serving endpoint name")
dbutils.widgets.text("catalog", "", "UC catalog for the agent model")
dbutils.widgets.text("agent_schema", "agents", "UC schema for the agent model")
dbutils.widgets.text("genie_space_ids", "{}", "Genie space ids (JSON: key->space_id)")

from databricks.sdk import WorkspaceClient  # noqa: E402
w = WorkspaceClient()

# COMMAND ----------

import mlflow
import mlflow.langchain
from mlflow.models.resources import DatabricksGenieSpace

PIPELINE_CATALOG = dbutils.widgets.get("catalog") or "dba-lakebase-network"
AGENT_SCHEMA = dbutils.widgets.get("agent_schema") or "agents"
MODEL_NAME = f"{PIPELINE_CATALOG}.{AGENT_SCHEMA}.multi_genie_supervisor"
# agent_supervisor.py is a sibling of this notebook; log_model reads it via the
# /Workspace FUSE mount (model-from-code).
_nb_dir = os.path.dirname(nb_path)
AGENT_SCRIPT = f"/Workspace{_nb_dir}/agent_supervisor.py"

# Ensure the model's catalog + schema exist (self-provisioning).
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{PIPELINE_CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{PIPELINE_CATALOG}`.`{AGENT_SCHEMA}`")

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

# Genie space ids come from the job base_params (JSON), not a config file.
genie_ids = json.loads(dbutils.widgets.get("genie_space_ids") or "{}")
resources = [DatabricksGenieSpace(genie_space_id=sid) for sid in genie_ids.values() if sid]
print(f"Resources: {len(resources)} Genie spaces")

input_example = {"messages": [{"role": "user", "content": "What is the SLA compliance rate?"}]}

# Set env vars for the agent script
for env_key, cfg_key in [("FIELD_OPS_SPACE_ID", "field_ops"), ("POSTGRES_SPACE_ID", "postgres"),
                          ("NETWORK_HEALTH_SPACE_ID", "network_health"), ("SLA_WORKFORCE_SPACE_ID", "sla_workforce")]:
    os.environ[env_key] = genie_ids.get(cfg_key, "")

# COMMAND ----------

# Log the LangGraph supervisor (model-from-code) and register it in UC.
# (The FSM original assumed a pre-registered model; a fresh module must log it.)
mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run(run_name="multi_genie_supervisor"):
    logged = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=AGENT_SCRIPT,
        resources=resources,
        input_example=input_example,
        # Pin the langgraph family into the model env (MLflow's inferred reqs are
        # not specific enough — the serving container otherwise rebuilds with an
        # older langgraph core and every request fails). extra_pip_requirements
        # augments the inferred set rather than replacing it.
        extra_pip_requirements=[
            "langgraph>=1.0.13",
            "langgraph-prebuilt>=1.0.13",
        ],
    )
registered = mlflow.register_model(logged.model_uri, MODEL_NAME)
latest_version = registered.version
print(f"Registered {MODEL_NAME} v{latest_version}")

# COMMAND ----------

# Deploy via REST API (agents.deploy has schema compatibility issues with langchain-logged models).
# Honor the endpoint name the module passes so it matches what the harness + app expect.
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name") or f"agents_{PIPELINE_CATALOG.replace('-','_')}_agents_multi_genie_supervisor"
print(f"Creating serving endpoint: {ENDPOINT_NAME}")

endpoint_config = {
    "name": ENDPOINT_NAME,
    "config": {
        "served_entities": [{
            "entity_name": MODEL_NAME,
            "entity_version": str(latest_version),
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

print("Agent deployment complete!")
dbutils.notebook.exit(json.dumps({"model": MODEL_NAME, "version": str(latest_version), "endpoint": ENDPOINT_NAME}))  # noqa: F821

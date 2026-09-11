# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Password Rotation
# MAGIC
# MAGIC Rotates PG credentials using a dual-role pattern:
# MAGIC - Two login roles (`lakebase_app_a`, `lakebase_app_b`) alternate
# MAGIC - Both inherit from `lakebase_app_perms` (shared permissions)
# MAGIC - Rotation: generate new password -> activate standby -> update secrets -> disable old
# MAGIC
# MAGIC Schedule: Run weekly via Databricks Jobs.

# COMMAND ----------

import json
import base64
import time
import secrets as py_secrets

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Read defaults from deployment config when available
import os, yaml
from pathlib import Path as _Path
_cfg = {}
try:
    _repo_root = _Path(os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    ).replace("/notebooks", ""))
    _config_path = _Path("/Workspace") / str(_repo_root).lstrip("/") / "deployment" / "config.yaml"
    with open(_config_path) as _f:
        _cfg = yaml.safe_load(_f) or {}
except Exception:
    _cfg = {}  # Config is optional — job params override everything

SCOPE = _cfg.get("secret_scope", "lakebase-secrets")

# Parameterized via notebook widgets — pass from job or use defaults from config
dbutils.widgets.text("instance_name", _cfg.get("instance_name", ""))
dbutils.widgets.text("project_id", _cfg.get("autoscaling_project_id", ""))
dbutils.widgets.text("app_name", _cfg.get("app_name", ""))
dbutils.widgets.text("lakebase_type", _cfg.get("lakebase_type", "autoscaling"))

INSTANCE_NAME = dbutils.widgets.get("instance_name")
DATABASE = "databricks_postgres"
PROJECT_ID = dbutils.widgets.get("project_id")
APP_NAME = dbutils.widgets.get("app_name")
LAKEBASE_TYPE = dbutils.widgets.get("lakebase_type")

print("Starting password rotation...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Determine active and standby roles

# COMMAND ----------

# Read current active role from secrets
try:
    # Get active role - we stored this in the secret scope
    active_role_secret = w.api_client.do(
        "GET", "/api/2.0/secrets/get",
        body={"scope": SCOPE, "key": "active-role"}
    )
    # Secrets API returns base64-encoded value
    active_role = base64.b64decode(active_role_secret.get("value", "YQ==")).decode()
except Exception:
    active_role = "a"  # Default to 'a' if not set

standby_role = "b" if active_role == "a" else "a"
print(f"Active role: lakebase_app_{active_role}")
print(f"Standby role: lakebase_app_{standby_role} (will become new active)")

# Generate new password
new_password = py_secrets.token_urlsafe(32)
print(f"New password generated ({len(new_password)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Update standby role on Provisioned instance

# COMMAND ----------

import psycopg2

def get_admin_conn_provisioned(instance_name):
    """Get admin connection to a Provisioned Lakebase instance."""
    instance = w.api_client.do("GET", f"/api/2.0/database/instances/{instance_name}")
    host = instance["read_write_dns"]
    cred = w.api_client.do(
        "POST", "/api/2.0/database/credentials",
        body={"instance_names": [instance_name], "request_id": f"rotate-{int(time.time())}"}
    )
    token = cred["token"]
    parts = token.split(".")
    payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
    user = json.loads(base64.urlsafe_b64decode(payload))["sub"]
    return psycopg2.connect(host=host, port=5432, user=user, password=token,
                            database=DATABASE, sslmode="require"), host

def get_admin_conn_autoscaling(project_id):
    """Get admin connection to an Autoscaling Lakebase instance."""
    endpoints = w.api_client.do(
        "GET", f"/api/2.0/postgres/projects/{project_id}/branches/production/endpoints"
    )
    ep = endpoints["endpoints"][0]
    host = ep["status"]["hosts"]["host"]
    endpoint_name = ep["name"]
    cred = w.api_client.do(
        "POST", "/api/2.0/postgres/credentials",
        body={"endpoint": endpoint_name}
    )
    token = cred.get("token") or cred.get("password", "")
    parts = token.split(".")
    payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
    user = json.loads(base64.urlsafe_b64decode(payload))["sub"]
    return psycopg2.connect(host=host, port=5432, user=user, password=token,
                            database=DATABASE, sslmode="require"), host

# Connect based on Lakebase type
if LAKEBASE_TYPE == "autoscaling":
    pid = PROJECT_ID or INSTANCE_NAME
    print(f"Connecting to Autoscaling project: {pid}")
    conn, host = get_admin_conn_autoscaling(pid)
else:
    print(f"Connecting to Provisioned instance: {INSTANCE_NAME}")
    conn, host = get_admin_conn_provisioned(INSTANCE_NAME)

cur = conn.cursor()

# Activate standby with new password
cur.execute(f"ALTER ROLE lakebase_app_{standby_role} WITH LOGIN PASSWORD %s", (new_password,))
conn.commit()
print(f"[{LAKEBASE_TYPE}] lakebase_app_{standby_role} activated with new password on {host}")
cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: (Handled above — both Provisioned and Autoscaling are now unified in Step 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Update Databricks secrets

# COMMAND ----------

secrets_to_update = {
    "pguser": f"lakebase_app_{standby_role}",
    "pgpassword": new_password,
    "autoscaling-pguser": f"lakebase_app_{standby_role}",
    "autoscaling-pgpassword": new_password,
    "active-role": standby_role,
}
for key, value in secrets_to_update.items():
    w.api_client.do("POST", "/api/2.0/secrets/put", body={
        "scope": SCOPE, "key": key, "string_value": value
    })
    print(f"  Updated secret: {key}")

print(f"\nSecrets now point to lakebase_app_{standby_role}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Redeploy app to pick up new secrets

# COMMAND ----------

print(f"Redeploying {APP_NAME}...")

# Get current app to find source code path
app_info = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
source_path = None
active_deploy = app_info.get("active_deployment", {})
if active_deploy:
    source_path = active_deploy.get("source_code_path")
if not source_path:
    source_path = _cfg.get("source_code_path", f"/Workspace/Shared/apps/{APP_NAME}")

# Deploy via raw API (avoids SDK version mismatch on serverless)
deploy_resp = w.api_client.do("POST", f"/api/2.0/apps/{APP_NAME}/deployments", body={
    "source_code_path": source_path
})
deploy_id = deploy_resp.get("deployment_id", "unknown")
print(f"Deployment started: {deploy_id}")

# Poll for completion
import time as _time
for _i in range(30):
    _time.sleep(10)
    status_resp = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}/deployments/{deploy_id}")
    state = status_resp.get("status", {}).get("state", "UNKNOWN")
    print(f"  Deploy status: {state}")
    if state in ("SUCCEEDED", "FAILED"):
        break

if state == "SUCCEEDED":
    print("App redeployed successfully with new credentials!")
else:
    msg = status_resp.get("status", {}).get("message", "")
    print(f"WARNING: Deployment may have issues: {msg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Disable old role

# COMMAND ----------

# Wait a bit for the app to fully start with new creds
time.sleep(10)

# Disable the old active role
prov_conn2, _ = get_admin_conn(INSTANCE_NAME)
cur2 = prov_conn2.cursor()
cur2.execute(f"ALTER ROLE lakebase_app_{active_role} WITH NOLOGIN")
prov_conn2.commit()
print(f"[Provisioned] lakebase_app_{active_role} disabled (NOLOGIN)")
cur2.close()
prov_conn2.close()

try:
    endpoints = w.api_client.do(
        "GET", f"/api/2.0/postgres/projects/{PROJECT_ID}/branches/production/endpoints"
    )
    ep = endpoints["endpoints"][0]
    host = ep["status"]["hosts"]["host"]
    endpoint_name = ep["name"]
    cred = w.api_client.do(
        "POST", "/api/2.0/postgres/credentials",
        body={"endpoint": endpoint_name}
    )
    token = cred["token"]
    parts = token.split(".")
    payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
    user = json.loads(base64.urlsafe_b64decode(payload))["sub"]
    auto_conn2 = psycopg2.connect(host=host, port=5432, user=user, password=token,
                                   database=DATABASE, sslmode="require")
    auto_cur2 = auto_conn2.cursor()
    auto_cur2.execute(f"ALTER ROLE lakebase_app_{active_role} WITH NOLOGIN")
    auto_conn2.commit()
    print(f"[Autoscaling] lakebase_app_{active_role} disabled")
    auto_cur2.close()
    auto_conn2.close()
except Exception as e:
    print(f"[Autoscaling] Could not disable old role: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("PASSWORD ROTATION COMPLETE")
print("=" * 60)
print(f"  Old active: lakebase_app_{active_role} -> NOLOGIN")
print(f"  New active: lakebase_app_{standby_role} -> LOGIN")
print(f"  App redeployed: {APP_NAME}")
print(f"  Secrets updated in: {SCOPE}")
print("=" * 60)

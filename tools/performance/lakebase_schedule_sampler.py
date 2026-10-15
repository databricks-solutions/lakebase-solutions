# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Performance Sampler — `lakebase_schedule_sampler`
# MAGIC
# MAGIC Creates or updates a **Databricks Job** that calls `lakebase_collect_samples.py` on a
# MAGIC cron schedule so the `lakebase_perf_samples` Delta table accumulates an ongoing
# MAGIC time-series without manual intervention.
# MAGIC
# MAGIC ## How to use
# MAGIC 1. Import both this notebook and `lakebase_collect_samples.py` into your workspace.
# MAGIC 2. Set the widgets below (especially `notebook_path`).
# MAGIC 3. **Run all** — the default `create_job=no` prints the resolved job spec as JSON so you
# MAGIC    can inspect it.  Nothing is created until you flip `create_job=yes` and run again.
# MAGIC 4. The resulting job runs `lakebase_collect_samples` once per cron tick (`ticks=1`);
# MAGIC    the job schedule drives the sampling cadence.
# MAGIC
# MAGIC ## Widgets
# MAGIC | Widget | Default | Notes |
# MAGIC |---|---|---|
# MAGIC | `job_name` | `lakebase-perf-sampler` | Display name; used to find an existing job for update |
# MAGIC | `notebook_path` | _(required)_ | Workspace path to `lakebase_collect_samples.py` |
# MAGIC | `cron` | `0 */5 * * * ?` | Quartz cron — default every 5 minutes |
# MAGIC | `timezone` | `UTC` | IANA timezone for the schedule |
# MAGIC | `project_id` | _(blank)_ | Passed through to `lakebase_collect_samples` |
# MAGIC | `branch` | `production` | Passed through |
# MAGIC | `endpoint` | `primary` | Passed through |
# MAGIC | `database` | `databricks_postgres` | Passed through |
# MAGIC | `samples_table` | `lakebase_perf_samples` | UC table the collector writes to |
# MAGIC | `create_job` | `no` | **Must be `yes` to provision — plain Run All is always a dry run** |
# MAGIC
# MAGIC ## Compute
# MAGIC The task is a `notebook_task` with no explicit cluster reference, which targets
# MAGIC serverless compute on workspaces where serverless is the workspace default.  To run on a
# MAGIC specific cluster, add `existing_cluster_id` to the `Task` before calling `w.jobs.create`.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "databricks-sdk>=0.81"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("job_name", "lakebase-perf-sampler", "1. Job display name")
dbutils.widgets.text("notebook_path", "", "2. Workspace path to lakebase_collect_samples notebook (required)")
dbutils.widgets.text("cron", "0 */5 * * * ?", "3. Quartz cron expression (default: every 5 min)")
dbutils.widgets.text("timezone", "UTC", "4. Schedule timezone (IANA, e.g. America/New_York)")
# --- passthrough params for lakebase_collect_samples ---
dbutils.widgets.text("project_id", "", "5. project_id passthrough (blank = auto-pick in collector)")
dbutils.widgets.text("branch", "production", "6. branch passthrough")
dbutils.widgets.text("endpoint", "primary", "7. endpoint passthrough")
dbutils.widgets.text("database", "databricks_postgres", "8. database passthrough")
dbutils.widgets.text("samples_table", "lakebase_perf_samples", "9. samples_table passthrough")
# --- gate ---
dbutils.widgets.dropdown("create_job", "no", ["no", "yes"],
                         "10. create_job — flip to yes to provision (default no = dry run)")

JOB_NAME      = dbutils.widgets.get("job_name").strip() or "lakebase-perf-sampler"
NOTEBOOK_PATH = dbutils.widgets.get("notebook_path").strip()
CRON          = dbutils.widgets.get("cron").strip() or "0 */5 * * * ?"
TIMEZONE      = dbutils.widgets.get("timezone").strip() or "UTC"
PROJECT_ID    = dbutils.widgets.get("project_id").strip()
BRANCH        = dbutils.widgets.get("branch").strip() or "production"
ENDPOINT      = dbutils.widgets.get("endpoint").strip() or "primary"
DATABASE      = dbutils.widgets.get("database").strip()
SAMPLES_TABLE = dbutils.widgets.get("samples_table").strip() or "lakebase_perf_samples"
CREATE_JOB    = dbutils.widgets.get("create_job").strip().lower()

if not NOTEBOOK_PATH:
    raise ValueError(
        "Widget 'notebook_path' is required — set it to the workspace path of "
        "lakebase_collect_samples (e.g. /Repos/you@example.com/lakebase-solutions/"
        "tools/performance/lakebase_collect_samples)."
    )

print(f"Job:           {JOB_NAME}")
print(f"Notebook:      {NOTEBOOK_PATH}")
print(f"Schedule:      {CRON}  tz={TIMEZONE}")
print(f"Passthrough:   project_id={PROJECT_ID or '(auto)'}  branch={BRANCH}  endpoint={ENDPOINT}")
print(f"               database={DATABASE}  samples_table={SAMPLES_TABLE}")
print(f"create_job:    {CREATE_JOB!r}  {'← DRY RUN' if CREATE_JOB != 'yes' else '← WILL PROVISION'}")

# COMMAND ----------

# MAGIC %md ## Build job spec

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    JobSettings,
    NotebookTask,
    PauseStatus,
    Task,
)

w = WorkspaceClient()

# Base parameters forwarded to lakebase_collect_samples on every scheduled run.
# ticks=1 because the job's cron drives the sampling cadence; the collector
# collects one snapshot per invocation and the schedule provides the repetition.
base_parameters = {
    "project_id":    PROJECT_ID,
    "branch":        BRANCH,
    "endpoint":      ENDPOINT,
    "database":      DATABASE,
    "samples_table": SAMPLES_TABLE,
    "ticks":         "1",
    "interval_seconds": "60",  # unused when ticks=1; forwarded for completeness
}

job_task = Task(
    task_key="collect_samples",
    description="Collect one Lakebase performance sample tick into lakebase_perf_samples.",
    notebook_task=NotebookTask(
        notebook_path=NOTEBOOK_PATH,
        base_parameters=base_parameters,
    ),
    # No cluster reference → serverless compute (workspace default).
    # To pin to a specific cluster, add:
    #   existing_cluster_id="<cluster-id>",
    # or set a job_cluster_key pointing to a new_cluster spec in the JobSettings.
)

schedule = CronSchedule(
    quartz_cron_expression=CRON,
    timezone_id=TIMEZONE,
    pause_status=PauseStatus.UNPAUSED,
)

new_settings = JobSettings(
    name=JOB_NAME,
    tasks=[job_task],
    schedule=schedule,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dry-run preview
# MAGIC The resolved job spec is printed here on every run regardless of `create_job`.
# MAGIC Inspect it before setting `create_job=yes`.

# COMMAND ----------

def _settings_to_dict(settings: JobSettings) -> dict:
    """Convert a JobSettings dataclass to a plain dict for display."""
    try:
        # SDK >= 0.81 dataclasses expose as_dict().
        return settings.as_dict()
    except AttributeError:
        # Fallback: use the SDK's built-in serialization helper.
        from databricks.sdk.service import jobs as _jobs_mod
        return _jobs_mod._from_dict(settings, dict)


spec_dict = {
    "name": JOB_NAME,
    "tasks": [
        {
            "task_key": "collect_samples",
            "description": "Collect one Lakebase performance sample tick into lakebase_perf_samples.",
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATH,
                "base_parameters": base_parameters,
            },
            # NOTE: no cluster config → serverless (workspace default).
        }
    ],
    "schedule": {
        "quartz_cron_expression": CRON,
        "timezone_id": TIMEZONE,
        "pause_status": "UNPAUSED",
    },
}

print("=" * 72)
print("Resolved job spec (JSON):")
print("=" * 72)
print(json.dumps(spec_dict, indent=2))
print("=" * 72)
print()
if CREATE_JOB != "yes":
    print("DRY RUN — set create_job=yes and re-run to provision the job.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update job
# MAGIC
# MAGIC **Gated by `create_job=yes`** — a plain Run All (default `create_job=no`) never reaches
# MAGIC this cell's provisioning path.  Set the widget to `yes` only after reviewing the spec
# MAGIC above.
# MAGIC
# MAGIC Idempotent: if a job named `job_name` already exists in this workspace it is updated
# MAGIC in-place (via `jobs.reset`); otherwise a new job is created.

# COMMAND ----------

if CREATE_JOB != "yes":
    print(f"Skipping provisioning (create_job={CREATE_JOB!r}). Flip to 'yes' to create/update the job.")
else:
    # Look up any existing job with this exact name.
    existing = [
        j for j in w.jobs.list(name=JOB_NAME)
        if j.settings and j.settings.name == JOB_NAME
    ]

    if existing:
        job_id = existing[0].job_id
        w.jobs.reset(job_id=job_id, new_settings=new_settings)
        action = "Updated"
    else:
        result = w.jobs.create(
            name=JOB_NAME,
            tasks=[job_task],
            schedule=schedule,
        )
        job_id = result.job_id
        action = "Created"

    workspace_url = w.config.host.rstrip("/")
    job_url = f"{workspace_url}/#job/{job_id}"

    print(f"{action} job '{JOB_NAME}'")
    print(f"  Job id:  {job_id}")
    print(f"  Job URL: {job_url}")
    print()
    print(f"The job will run lakebase_collect_samples on schedule: {CRON} ({TIMEZONE}).")
    print(f"Samples accumulate in: {SAMPLES_TABLE}")
    print()
    print("To pause the job, update the schedule pause_status or disable it in the Jobs UI.")
    print("To change the cadence, re-run this notebook with a new 'cron' value (create_job=yes).")

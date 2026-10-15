# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Diagnostic — one-run orchestrator
# MAGIC
# MAGIC The front door to the performance toolkit. It conducts the individual tools in order and
# MAGIC leaves you with a **report card**:
# MAGIC
# MAGIC ```
# MAGIC collect state  →  [schedule ongoing sampler]  →  [load benchmark]  →  report card
# MAGIC ```
# MAGIC
# MAGIC Each step is an existing notebook in this folder, run via `dbutils.notebook.run` with the
# MAGIC parameters you set here — so the collectors/benchmark/report card stay independently usable and
# MAGIC this notebook just sequences them. Every step is toggleable; a plain **Run all** does the safe
# MAGIC default: collect current state, then produce the report card (no load generated, no job created).
# MAGIC
# MAGIC Immutable + parameterized like every tool here: nothing hardcoded, OAuth and endpoint resolved
# MAGIC from the workspace identity inside each sub-notebook. The same file runs against any project.
# MAGIC
# MAGIC > **Ordering note.** The report card defaults to the **latest** collector run (max `run_ts` in
# MAGIC > the metrics table), so running collect → report in one pass analyzes the state this run just
# MAGIC > captured. Point `run_id` at a specific run in the report-card step if you need to pin it.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

# Shared connection parameters (passed through to every step).
dbutils.widgets.text("project_id", "", "Lakebase project id (blank = auto-pick)")
dbutils.widgets.text("branch", "production", "Branch")
dbutils.widgets.text("endpoint", "primary", "Endpoint/compute")
dbutils.widgets.text("database", "databricks_postgres", "Postgres database")

# Which steps to run.
dbutils.widgets.dropdown("run_collect",  "yes", ["yes", "no"], "Step 1: collect current state")
dbutils.widgets.dropdown("run_schedule", "no",  ["yes", "no"], "Step 2: schedule ongoing sampler (Job)")
dbutils.widgets.dropdown("run_benchmark", "no", ["yes", "no"], "Step 3: run load benchmark")
dbutils.widgets.dropdown("run_report",   "yes", ["yes", "no"], "Step 4: produce report card")

# Sub-notebook paths (default: siblings in this folder). Override if your layout differs.
dbutils.widgets.text("nb_collect",   "./lakebase_collect_state",    "Notebook: state collector")
dbutils.widgets.text("nb_schedule",  "./lakebase_schedule_sampler", "Notebook: sampler scheduler")
dbutils.widgets.text("nb_benchmark", "./lakebase_load_harness",     "Notebook: load harness")
dbutils.widgets.text("nb_report",    "./lakebase_report_card",      "Notebook: report card")

# Step-specific passthroughs.
dbutils.widgets.text("workload_spec", "", "Benchmark: workload_spec JSON (blank = harness default)")
dbutils.widgets.text("concurrency_levels", "1,2,4,8", "Benchmark: concurrency sweep")
dbutils.widgets.text("step_timeout_seconds", "1800", "Per-step timeout (s)")

P = {
    "project_id": dbutils.widgets.get("project_id").strip(),
    "branch":     dbutils.widgets.get("branch").strip() or "production",
    "endpoint":   dbutils.widgets.get("endpoint").strip() or "primary",
    "database":   dbutils.widgets.get("database").strip(),
}
RUN_COLLECT   = dbutils.widgets.get("run_collect") == "yes"
RUN_SCHEDULE  = dbutils.widgets.get("run_schedule") == "yes"
RUN_BENCHMARK = dbutils.widgets.get("run_benchmark") == "yes"
RUN_REPORT    = dbutils.widgets.get("run_report") == "yes"
NB = {
    "collect":   dbutils.widgets.get("nb_collect").strip(),
    "schedule":  dbutils.widgets.get("nb_schedule").strip(),
    "benchmark": dbutils.widgets.get("nb_benchmark").strip(),
    "report":    dbutils.widgets.get("nb_report").strip(),
}
WORKLOAD_SPEC = dbutils.widgets.get("workload_spec").strip()
CONCURRENCY = dbutils.widgets.get("concurrency_levels").strip()
TIMEOUT = int(dbutils.widgets.get("step_timeout_seconds"))

# COMMAND ----------

# MAGIC %md ## Run the pipeline

# COMMAND ----------

import json

results = []

def _run(step_name, notebook, args):
    """Run one sub-notebook; record success/failure without aborting the whole pipeline."""
    print(f"▶ {step_name}: {notebook}")
    try:
        out = dbutils.notebook.run(notebook, TIMEOUT, {k: str(v) for k, v in args.items()})
        results.append({"step": step_name, "notebook": notebook, "status": "ok", "detail": (out or "")[:200]})
        print(f"  ✓ {step_name} complete")
    except Exception as e:  # noqa: BLE001 — surface the failing step, keep going
        results.append({"step": step_name, "notebook": notebook, "status": "FAILED", "detail": str(e)[:300]})
        print(f"  ✗ {step_name} FAILED: {e}")

# Step 1 — collect current state (fills the perf tables the report card reads).
if RUN_COLLECT:
    _run("collect_state", NB["collect"], dict(P))

# Step 2 — schedule the ongoing sampler. The scheduler itself defaults to a dry run
# (prints the Job spec, creates nothing) unless its own create_job widget is set to yes.
if RUN_SCHEDULE:
    _run("schedule_sampler", NB["schedule"], dict(P, notebook_path=NB["collect"].replace("collect_state", "collect_samples")))

# Step 3 — load benchmark (generates traffic; off by default). Passes the workload spec through.
if RUN_BENCHMARK:
    bench_args = dict(P, concurrency_levels=CONCURRENCY)
    if WORKLOAD_SPEC:
        bench_args["workload_spec"] = WORKLOAD_SPEC
    _run("load_benchmark", NB["benchmark"], bench_args)

# Step 4 — report card (analyzes the latest collected state against the rule pack).
if RUN_REPORT:
    _run("report_card", NB["report"], {})

# COMMAND ----------

# MAGIC %md ## Pipeline summary

# COMMAND ----------

import pandas as pd

summary = pd.DataFrame(results) if results else pd.DataFrame([{"step": "(none selected)", "status": "-", "detail": ""}])
print("Diagnostic pipeline complete.")
if RUN_REPORT and any(r["step"] == "report_card" and r["status"] == "ok" for r in results):
    print("→ Review the report card output in the report_card step above, and the lakebase_perf_findings table.")
display(summary)

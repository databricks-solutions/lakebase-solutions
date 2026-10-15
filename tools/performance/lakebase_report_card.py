# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Report Card — turn collected metrics into a prioritized, runbook-linked diagnosis
# MAGIC
# MAGIC The collector (`lakebase_collect_state.py`) and the sampler (`lakebase_collect_samples.py`)
# MAGIC gather raw `pg_stat_*` / sizing / statement data into UC Delta tables. The load harness
# MAGIC (`lakebase_load_harness.py`) measures behavior under concurrency. This notebook is the last
# MAGIC stage: it reads those **already-collected** tables, applies a pack of declarative **rules**,
# MAGIC and emits a **report card** — a prioritized list of findings where each one links to a section
# MAGIC of the performance configuration runbook and names a concrete fix (sp_Blitz-style).
# MAGIC
# MAGIC There is **no Postgres connection here** — the engine reads UC Delta via Spark, not the live
# MAGIC database. It is read-only except for one append to the findings table.
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Import this file (it's notebook source), attach to compute, **Run all**. The first cell
# MAGIC    `%pip`-installs PyYAML and restarts Python.
# MAGIC 2. Set the widgets. Usually you change nothing: it reads the default table names and diagnoses
# MAGIC    the **latest** run found in `lakebase_perf_metrics`. Set `run_id` to pin a specific run.
# MAGIC 3. Read the **findings** table and the printed **Markdown report card** at the bottom.
# MAGIC
# MAGIC ## Extending it (the whole point)
# MAGIC Adding a check = adding a rule to a `*.yaml` under `rules_dir`. You never edit this engine.
# MAGIC Each rule declares which collected table to read, a safe boolean `detect` expression over that
# MAGIC table's columns, a `threshold`, and the `finding`/`fix`/`runbook_section` text. See
# MAGIC `rules/core.yaml` and the "Rule format" section of `DIAGNOSTIC_SPEC.md`.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("rules_dir", "rules", "1. Directory of rule packs (*.yaml)")
dbutils.widgets.text("metrics_table", "lakebase_perf_metrics", "2. Point-in-time metrics table")
dbutils.widgets.text("tables_table", "lakebase_perf_tables", "3. Per-table size / working-set table")
dbutils.widgets.text("queries_table", "lakebase_perf_queries", "4. Top-N statements table")
dbutils.widgets.text("samples_table", "lakebase_perf_samples", "5. Ongoing time-series samples table")
dbutils.widgets.text("findings_table", "lakebase_perf_findings", "6. UC table to append findings to")
dbutils.widgets.text("run_id", "", "7. run_id to diagnose (blank = latest in metrics by run_ts)")

RULES_DIR = dbutils.widgets.get("rules_dir").strip() or "rules"
METRICS_TABLE = dbutils.widgets.get("metrics_table").strip()
TABLES_TABLE = dbutils.widgets.get("tables_table").strip()
QUERIES_TABLE = dbutils.widgets.get("queries_table").strip()
SAMPLES_TABLE = dbutils.widgets.get("samples_table").strip()
FINDINGS_TABLE = dbutils.widgets.get("findings_table").strip()
RUN_ID = dbutils.widgets.get("run_id").strip()

# Logical source_table aliases a rule may use, mapped to the widget-provided names.
# A rule can write either the short alias (metrics|tables|queries|samples) or the
# full default name; an unrecognized value is treated as a literal table name.
TABLE_MAP = {
    "metrics": METRICS_TABLE, "lakebase_perf_metrics": METRICS_TABLE,
    "tables": TABLES_TABLE, "lakebase_perf_tables": TABLES_TABLE,
    "queries": QUERIES_TABLE, "lakebase_perf_queries": QUERIES_TABLE,
    "samples": SAMPLES_TABLE, "lakebase_perf_samples": SAMPLES_TABLE,
}

PRIORITY_LABELS = {1: "Critical", 2: "High", 3: "Medium", 4: "Low", 5: "Informational"}

print(f"Rules dir:       {RULES_DIR}")
print(f"Metrics table:   {METRICS_TABLE}")
print(f"Findings table:  {FINDINGS_TABLE}")
print(f"run_id:          {RUN_ID or '(latest)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the rule packs
# MAGIC Every `*.yaml` in `rules_dir` is a list of rule objects. A file that is missing, unparseable,
# MAGIC or not a list is skipped with a warning — a bad pack never stops the run.

# COMMAND ----------

import glob
import os

import yaml

# Required keys every rule must carry; anything missing → the rule is skipped.
REQUIRED_RULE_KEYS = ("id", "priority", "category", "source_table", "detect", "runbook_section", "finding", "fix")

skipped_notes = []  # (rule_id_or_file, reason) — surfaced at the end, never raised


def load_rule_packs(rules_dir):
    """Load and shallow-validate every *.yaml rule in `rules_dir`.

    Returns a list of rule dicts. Malformed files or rules are recorded in
    `skipped_notes` and omitted rather than raising, so one bad pack cannot
    abort the diagnosis.
    """
    paths = sorted(glob.glob(os.path.join(rules_dir, "*.yaml")) + glob.glob(os.path.join(rules_dir, "*.yml")))
    if not paths:
        skipped_notes.append((rules_dir, "no *.yaml rule files found in rules_dir"))
        return []
    loaded = []
    seen_ids = set()
    for path in paths:
        try:
            with open(path, "r") as fh:
                doc = yaml.safe_load(fh)
        except Exception as exc:  # unparseable YAML
            skipped_notes.append((os.path.basename(path), f"YAML parse error: {exc}"))
            continue
        if not isinstance(doc, list):
            skipped_notes.append((os.path.basename(path), "rule pack is not a YAML list"))
            continue
        for rule in doc:
            if not isinstance(rule, dict):
                skipped_notes.append((os.path.basename(path), f"rule entry is not a mapping: {rule!r}"))
                continue
            missing = [k for k in REQUIRED_RULE_KEYS if k not in rule]
            if missing:
                skipped_notes.append((rule.get("id", os.path.basename(path)), f"missing required keys: {missing}"))
                continue
            rid = rule["id"]
            if rid in seen_ids:
                skipped_notes.append((rid, "duplicate rule id — later definition ignored"))
                continue
            seen_ids.add(rid)
            rule["_source_file"] = os.path.basename(path)
            loaded.append(rule)
    return loaded


RULES = load_rule_packs(RULES_DIR)
print(f"Loaded {len(RULES)} rule(s): {[r['id'] for r in RULES]}")
if skipped_notes:
    print("Load-time warnings:")
    for who, why in skipped_notes:
        print(f"  - [{who}] {why}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve the run to diagnose
# MAGIC Blank `run_id` → the latest run in the metrics table (max `run_ts`). We also pull that run's
# MAGIC `project_id` / `endpoint` so every emitted finding is self-identifying.

# COMMAND ----------

import datetime as _dt

RUN_TS = None
PROJECT_ID = None
ENDPOINT = None


def resolve_run():
    """Determine run_id/run_ts/project_id/endpoint from the metrics table.

    Returns True when a run was resolved. If the metrics table is unreadable or
    empty, records a warning and returns False (the engine then produces an
    empty report card rather than crashing).
    """
    global RUN_ID, RUN_TS, PROJECT_ID, ENDPOINT
    try:
        mdf = spark.table(METRICS_TABLE)
    except Exception as exc:
        skipped_notes.append((METRICS_TABLE, f"cannot read metrics table to resolve run: {exc}"))
        return False
    if RUN_ID:
        mdf = mdf.where(mdf.run_id == RUN_ID)
    rows = mdf.orderBy(mdf.run_ts.desc()).limit(1).collect()
    if not rows:
        skipped_notes.append((METRICS_TABLE, f"no rows for run_id={RUN_ID or '(latest)'}"))
        return False
    r = rows[0].asDict()
    RUN_ID = r.get("run_id")
    RUN_TS = r.get("run_ts") or _dt.datetime.utcnow()
    PROJECT_ID = r.get("project_id")
    ENDPOINT = r.get("endpoint")
    return True


HAVE_RUN = resolve_run()
print(f"Diagnosing run_id={RUN_ID} run_ts={RUN_TS} project_id={PROJECT_ID} endpoint={ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The rule engine
# MAGIC For each rule: read its `source_table` filtered to this run, then evaluate `detect` **safely**
# MAGIC on every row — a restricted `eval` with **no builtins** whose namespace is only the row's
# MAGIC columns plus `threshold`. The expression can therefore read values and compare them but can
# MAGIC neither import, call builtins, nor cause side effects. On a `True` match we format the
# MAGIC `finding`/`fix` from the matched row and emit a finding. A rule referencing an unknown column
# MAGIC or table, or a malformed expression, is skipped with a warning.

# COMMAND ----------

import json

# No builtins are exposed to detect expressions — attacker-free, side-effect-free.
_SAFE_GLOBALS = {"__builtins__": {}}


class _SafeDict(dict):
    """format_map helper: leave unknown placeholders literal instead of raising."""

    def __missing__(self, key):
        return "{" + str(key) + "}"


def _safe_eval(expr, namespace):
    """Evaluate a detect expression with no builtins over the given namespace."""
    return bool(eval(expr, _SAFE_GLOBALS, namespace))  # noqa: S307 — intentionally sandboxed (no builtins)


def _fmt(template, record, threshold):
    """str.format a finding/fix/details template from a row + threshold, tolerant of missing keys."""
    ns = dict(record)
    ns["threshold"] = threshold
    try:
        return str(template).format_map(_SafeDict(ns))
    except Exception:
        return str(template)


def _load_source_rows(rule):
    """Read a rule's source_table, filtered to the current run_id when that column exists.

    Returns a list of row dicts. Raises on an unreadable table so the caller can
    skip the rule.
    """
    src = str(rule["source_table"])
    table = TABLE_MAP.get(src, src)
    sdf = spark.table(table)  # raises if the table is missing → caller skips the rule
    if RUN_ID and "run_id" in sdf.columns:
        sdf = sdf.where(sdf.run_id == RUN_ID)
    return [row.asDict() for row in sdf.collect()]


def evaluate_rule(rule):
    """Evaluate one rule against its source rows, returning a list of finding dicts.

    Any failure (missing table, unknown column referenced by detect, malformed
    expression) records a warning in `skipped_notes` and returns [] — the rule is
    skipped, the notebook keeps going.
    """
    rid = rule["id"]
    try:
        rows = _load_source_rows(rule)
    except Exception as exc:
        skipped_notes.append((rid, f"cannot read source_table '{rule['source_table']}': {exc}"))
        return []

    threshold = rule.get("threshold")
    detect = rule["detect"]
    findings = []
    for rec in rows:
        ns = dict(rec)
        ns["threshold"] = threshold
        try:
            hit = _safe_eval(detect, ns)
        except NameError as exc:
            # detect references a column this table does not have → misconfigured rule, skip it whole.
            skipped_notes.append((rid, f"detect references unknown field for this table: {exc}"))
            return []
        except Exception as exc:
            # malformed expression / bad comparison → skip the rule, do not crash.
            skipped_notes.append((rid, f"detect evaluation error: {exc}"))
            return []
        if not hit:
            continue
        details = rule.get("details")
        details_text = _fmt(details, rec, threshold) if details else json.dumps(rec, default=str)[:900]
        findings.append({
            "run_id": RUN_ID,
            "run_ts": RUN_TS,
            "project_id": PROJECT_ID,
            "endpoint": ENDPOINT,
            "priority": int(rule.get("priority", 5)),
            "category": str(rule.get("category", "")),
            "finding": _fmt(rule["finding"], rec, threshold),
            "details": details_text,
            "runbook_section": str(rule.get("runbook_section", "")),
            "fix": _fmt(rule["fix"], rec, threshold),
            "rule_id": rid,
        })
    return findings


all_findings = []
if HAVE_RUN:
    for _rule in RULES:
        all_findings.extend(evaluate_rule(_rule))
all_findings.sort(key=lambda f: (f["priority"], f["rule_id"]))
print(f"Produced {len(all_findings)} finding(s) from {len(RULES)} rule(s).")

# COMMAND ----------

# MAGIC %md ## Findings table

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

FINDINGS_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("run_ts", TimestampType()),
    StructField("project_id", StringType()),
    StructField("endpoint", StringType()),
    StructField("priority", IntegerType()),
    StructField("category", StringType()),
    StructField("finding", StringType()),
    StructField("details", StringType()),
    StructField("runbook_section", StringType()),
    StructField("fix", StringType()),
    StructField("rule_id", StringType()),
])
_FINDINGS_COLS = [f.name for f in FINDINGS_SCHEMA.fields]

findings_sdf = spark.createDataFrame(
    [tuple(f[c] for c in _FINDINGS_COLS) for f in all_findings],
    schema=FINDINGS_SCHEMA,
)
display(findings_sdf)

# COMMAND ----------

# MAGIC %md ## Append to the findings table

# COMMAND ----------

if FINDINGS_TABLE:
    findings_sdf.write.mode("append").saveAsTable(FINDINGS_TABLE)
    print(f"Appended {findings_sdf.count()} finding(s) to {FINDINGS_TABLE}")
else:
    print("findings_table blank — not persisting.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report card (Markdown)
# MAGIC A grouped, exportable rendering: one `##` section per priority tier, each finding with its
# MAGIC runbook link and fix. Copy the printed string into an issue, a doc, or a Slack message.

# COMMAND ----------


def build_report_card(findings):
    """Render findings as a grouped Markdown report card string (## per priority tier)."""
    lines = [
        "# Lakebase performance report card",
        "",
        f"_run_id: `{RUN_ID}` · project: `{PROJECT_ID}` · endpoint: `{ENDPOINT}` · "
        f"run_ts: `{RUN_TS}` · {len(findings)} finding(s)_",
        "",
    ]
    if not findings:
        lines.append("**No findings.** Every rule passed for this run (or no run was found).")
        return "\n".join(lines)
    for tier in sorted({f["priority"] for f in findings}):
        label = PRIORITY_LABELS.get(tier, "Unknown")
        tier_findings = [f for f in findings if f["priority"] == tier]
        lines.append(f"## P{tier} — {label} ({len(tier_findings)})")
        lines.append("")
        for f in tier_findings:
            lines.append(f"- **[{f['category']}]** {f['finding']}  _(rule `{f['rule_id']}`)_")
            lines.append(f"  - Runbook: {f['runbook_section']}")
            lines.append(f"  - Fix: {f['fix']}")
        lines.append("")
    return "\n".join(lines)


report_card_md = build_report_card(all_findings)
print(report_card_md)

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

_counts = {p: 0 for p in sorted(PRIORITY_LABELS)}
for f in all_findings:
    _counts[f["priority"]] = _counts.get(f["priority"], 0) + 1
summary = "Report card: " + " · ".join(f"{n} P{p}" for p, n in sorted(_counts.items())) + \
    f" ({len(all_findings)} finding(s), {len(RULES)} rule(s) evaluated)"
print(summary)
if skipped_notes:
    print(f"Skipped/warned {len(skipped_notes)} item(s):")
    for who, why in skipped_notes:
        print(f"  - [{who}] {why}")

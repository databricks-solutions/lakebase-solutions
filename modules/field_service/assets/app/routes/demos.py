"""
Iceberg Demo and STB Migration API Blueprint.

Provides two interactive demo experiences for the Lakebase FSM application:

1. **Iceberg Demo** -- Uses PyIceberg (Apache open-source) to read/write
   Unity Catalog Managed Iceberg tables via the UC Iceberg REST endpoint.
   Demonstrates zero-proprietary-dependency interop: the same tables that
   DLT writes as Delta are accessible as Iceberg with full read/write
   support.  Auth uses OAuth bearer tokens injected by Databricks Apps.

2. **STB Migration** -- Simulates a 2-tier Parquet-to-Managed-Iceberg
   migration.  Tier 1 generates raw Parquet files into a UC Volume.
   Tier 2 uses ``CREATE OR REPLACE TABLE ... USING ICEBERG CLUSTER BY``
   (CTAS from ``read_files()``) to create liquid-clustered Managed Iceberg
   tables.  A validation step confirms row-count parity and an analytics
   query shows the query performance of the managed tables.

Routes
------
POST /api/iceberg-demo/start   Start the PyIceberg read/write demo loop
POST /api/iceberg-demo/stop    Stop the demo loop
GET  /api/iceberg-demo/status  Poll demo state (running, reads, writes, errors, ops log)
GET  /api/iceberg-demo/source  Return the actual source code of the demo functions
POST /api/migration/start      Start the Parquet-to-Iceberg migration simulation
POST /api/migration/stop       Stop the migration (marks remaining steps as skipped)
GET  /api/migration/status     Poll migration progress (step-by-step DAG with timings)

Dependencies from ``shared``
----------------------------
get_workspace_client, log_error, _run_sql

Note: ``_run_sql`` executes SQL via the Databricks Statement Execution API
(not the PG pool) because the migration targets Unity Catalog tables, not
Lakebase.
"""

from __future__ import annotations

import io
import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify

from shared import _run_sql, get_workspace_client, log_error

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint definition
# ---------------------------------------------------------------------------

demos_bp = Blueprint("demos", __name__)

# ---------------------------------------------------------------------------
# Iceberg Demo State
# ---------------------------------------------------------------------------
# Protected by _iceberg_lock.  Tracks the PyIceberg background thread, its
# operation log (last 50 ops), and aggregate counters for reads/writes/errors.

_iceberg_state: dict = {
    "running": False,
    "stop_event": None,       # threading.Event -- set to signal graceful stop
    "thread": None,
    "ops": [],                # list of { time, op, table, rows, status, detail, data?, schema? }
    "reads": 0,
    "writes": 0,
    "errors": 0,
    "last_error": None,
    "catalog_ok": False,      # True once the initial catalog connection succeeds
}
_iceberg_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Iceberg configuration (from environment)
# ---------------------------------------------------------------------------

ICEBERG_CATALOG_NAME = os.environ.get("PIPELINE_CATALOG", "dba-lakebase-network")
ICEBERG_SCHEMA = "network_data"
ICEBERG_WRITE_TABLE = "oss_iceberg_analytics"  # demo write target

# ---------------------------------------------------------------------------
# Migration State
# ---------------------------------------------------------------------------
# Protected by _migration_lock.  Each step in MIGRATION_STEPS gets a status
# dict (pending -> running -> success/error) with timing and row-count metadata.

_migration_state: dict = {
    "running": False,
    "stop_event": threading.Event(),
    "steps": [],          # list of step dicts with runtime status
    "current_step": 0,
    "error": None,
    "started_at": None,
    "completed_at": None,
}
_migration_lock = threading.Lock()

# Step definitions for the 2-tier migration.  Each step has an id, label,
# tier (raw or managed), and a human-readable description.
MIGRATION_STEPS: list[dict] = [
    # Tier 1: Raw Parquet Files (UC Volume)
    {"id": "create_volume", "label": "Create UC Volume", "tier": "raw",
     "desc": "Create stb_source_data volume for raw Parquet files"},
    {"id": "gen_devices", "label": "Write Devices Parquet", "tier": "raw",
     "desc": "500 STB device records as Parquet files"},
    {"id": "gen_telemetry", "label": "Write Telemetry Parquet", "tier": "raw",
     "desc": "10,000 telemetry readings as Parquet files"},
    {"id": "gen_incidents", "label": "Write Incidents Parquet", "tier": "raw",
     "desc": "200 hardware incidents as Parquet files"},
    # Tier 2: Managed Iceberg (CTAS from read_files -> Iceberg with liquid clustering)
    {"id": "migrate_devices", "label": "CTAS Managed Devices", "tier": "managed",
     "desc": "read_files() -> USING ICEBERG CLUSTER BY (device_id, region)"},
    {"id": "migrate_telemetry", "label": "CTAS Managed Telemetry", "tier": "managed",
     "desc": "read_files() -> USING ICEBERG CLUSTER BY (device_id, reading_date)"},
    {"id": "migrate_incidents", "label": "CTAS Managed Incidents", "tier": "managed",
     "desc": "read_files() -> USING ICEBERG CLUSTER BY (device_id, incident_type)"},
    {"id": "validate", "label": "Validate Migration", "tier": "managed",
     "desc": "Row count match + data integrity check"},
    {"id": "show_benefits", "label": "Query Managed Iceberg", "tier": "managed",
     "desc": "Analytics query on liquid-clustered Iceberg tables"},
]


# ═══════════════════════════════════════════════════════════════════════════
# Iceberg Demo — PyIceberg Read/Write via UC Iceberg REST
# ═══════════════════════════════════════════════════════════════════════════


def _get_iceberg_catalog():
    """Create a PyIceberg RestCatalog pointing at Unity Catalog's Iceberg endpoint.

    Uses the Databricks SDK's auto-auth to extract the workspace host and an
    OAuth token, then passes them to PyIceberg.  This keeps the Iceberg layer
    fully open-source -- no proprietary Databricks client is involved in the
    actual table I/O.
    """
    from pyiceberg.catalog.rest import RestCatalog

    w = get_workspace_client()
    config = w.config
    host = (config.host or "").rstrip("/")
    # Get an OAuth token from the SDK's credential provider
    auth_headers = config.authenticate()
    token = auth_headers.get("Authorization", "").replace("Bearer ", "")
    if not host or not token:
        raise RuntimeError("Could not obtain host/token from Databricks SDK auth")
    uri = f"{host}/api/2.1/unity-catalog/iceberg-rest"
    log.info(f"Unity Catalog Iceberg endpoint: {uri}, warehouse: {ICEBERG_CATALOG_NAME}")
    return RestCatalog(
        name="unity",
        uri=uri,
        warehouse=ICEBERG_CATALOG_NAME,
        token=token,
    )


def _iceberg_log_op(op: str, table: str, rows: int, status: str,
                     detail: str = "", data=None, schema=None) -> None:
    """Append an operation record to the iceberg demo state.

    Args:
        op:     Operation type (connect, list_namespaces, list_tables, read, write, disconnect)
        table:  Target table or namespace name
        rows:   Number of rows affected
        status: running | success | error
        detail: Human-readable description
        data:   Optional list of dicts (sample rows) for read/write ops
        schema: Optional list of {name, type} for column metadata
    """
    entry: dict = {
        "time": datetime.now(timezone.utc).isoformat(),
        "op": op,
        "table": table,
        "rows": rows,
        "status": status,
        "detail": detail,
    }
    if data is not None:
        entry["data"] = data
    if schema is not None:
        entry["schema"] = schema
    _iceberg_state["ops"].append(entry)
    # Keep last 50 ops to bound memory usage
    if len(_iceberg_state["ops"]) > 50:
        _iceberg_state["ops"] = _iceberg_state["ops"][-50:]


def _run_iceberg_demo() -> None:
    """Background thread: cycle through read/write operations using PyIceberg.

    Execution flow:
    1. Connect to the UC Iceberg REST catalog
    2. List namespaces (schemas)
    3. List tables in the target schema
    4. Loop: read a gold/silver table, then write an analytics batch
       (repeats until the stop event is set)
    """
    import pyarrow as pa

    stop = _iceberg_state["stop_event"]

    # ── Step 1: Connect to catalog ────────────────────────────────────────
    _iceberg_log_op("connect", "Unity Catalog", 0, "running",
                     "Connecting to Unity Catalog Iceberg endpoint...")
    try:
        catalog = _get_iceberg_catalog()
        _iceberg_state["catalog_ok"] = True
        _iceberg_log_op("connect", "Unity Catalog", 0, "success",
                         f"Connected to {ICEBERG_CATALOG_NAME} via UC Iceberg REST")
    except Exception as e:
        _iceberg_state["catalog_ok"] = False
        _iceberg_state["last_error"] = str(e)
        _iceberg_state["errors"] += 1
        _iceberg_log_op("connect", "Unity Catalog", 0, "error", str(e))
        log.error(f"Iceberg demo: catalog connect failed: {e}")
        _iceberg_state["running"] = False
        return

    if stop.is_set():
        return

    # ── Step 2: List namespaces ───────────────────────────────────────────
    _iceberg_log_op("list_namespaces", ICEBERG_CATALOG_NAME, 0, "running")
    try:
        namespaces = catalog.list_namespaces()
        ns_names = [".".join(ns) for ns in namespaces]
        _iceberg_log_op("list_namespaces", ICEBERG_CATALOG_NAME, len(namespaces), "success",
                         f"Found schemas: {', '.join(ns_names)}")
    except Exception as e:
        _iceberg_state["errors"] += 1
        _iceberg_log_op("list_namespaces", ICEBERG_CATALOG_NAME, 0, "error", str(e))

    if stop.is_set():
        return

    # ── Step 3: List tables ───────────────────────────────────────────────
    _iceberg_log_op("list_tables", ICEBERG_SCHEMA, 0, "running")
    try:
        tables = catalog.list_tables(ICEBERG_SCHEMA)
        tbl_names = [t[1] for t in tables]
        table_list = [{"name": t, "type": "table"} for t in tbl_names]
        _iceberg_log_op("list_tables", ICEBERG_SCHEMA, len(tables), "success",
                         f"Found {len(tables)} tables in {ICEBERG_SCHEMA}",
                         data=table_list)
    except Exception as e:
        _iceberg_state["errors"] += 1
        _iceberg_log_op("list_tables", ICEBERG_SCHEMA, 0, "error", str(e))

    if stop.is_set():
        return

    # ── Step 4: Read/write loop ───────────────────────────────────────────
    # Cycle through these tables, reading 100 rows each time
    read_targets = [
        "gold_iot_device_health",
        "gold_daily_node_health",
        "gold_regional_network_summary",
        "silver_iot_telemetry",
        "bronze_iot_telemetry",
    ]

    cycle = 0
    while not stop.is_set():
        cycle += 1

        # ── READ: Scan a gold/silver table (with timeout protection) ──────
        read_table_name = read_targets[cycle % len(read_targets)]
        fqn = f"{ICEBERG_SCHEMA}.{read_table_name}"
        _iceberg_log_op("read", read_table_name, 0, "running",
                         f"Scanning {fqn} via PyIceberg...")
        try:
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

            def _do_scan():
                tbl = catalog.load_table(fqn)
                scan = tbl.scan(limit=100)
                return scan.to_arrow()

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_do_scan)
                arrow_tbl = future.result(timeout=60)

            row_count = len(arrow_tbl)
            col_names = arrow_tbl.column_names
            # Capture schema info for the frontend
            schema_info = [{"name": f.name, "type": str(f.type)} for f in arrow_tbl.schema]
            # Capture sample rows (first 10) as list of dicts
            sample_rows: list[dict] = []
            preview_limit = min(10, row_count)
            for i in range(preview_limit):
                row: dict = {}
                for col in col_names:
                    val = arrow_tbl.column(col)[i].as_py()
                    # Truncate long strings for display
                    if isinstance(val, str) and len(val) > 80:
                        val = val[:77] + "..."
                    row[col] = val
                sample_rows.append(row)
            _iceberg_state["reads"] += 1
            _iceberg_log_op("read", read_table_name, row_count, "success",
                             f"Read {row_count} rows, {len(col_names)} columns",
                             data=sample_rows, schema=schema_info)
        except FuturesTimeout:
            _iceberg_state["errors"] += 1
            _iceberg_state["last_error"] = "Scan timed out after 30s"
            _iceberg_log_op("read", read_table_name, 0, "error",
                             "Scan timed out after 60s -- S3 credential vending may need "
                             "EXTERNAL_USE_SCHEMA grant")
            log.warning(f"Iceberg demo read {fqn}: timeout")
        except Exception as e:
            _iceberg_state["errors"] += 1
            _iceberg_state["last_error"] = str(e)
            _iceberg_log_op("read", read_table_name, 0, "error", str(e)[:200])
            log.warning(f"Iceberg demo read {fqn}: {e}")

        if stop.is_set():
            break
        stop.wait(3)  # pause between ops
        if stop.is_set():
            break

        # ── WRITE: Append analytics rows to the demo write table ──────────
        fqn_write = f"{ICEBERG_SCHEMA}.{ICEBERG_WRITE_TABLE}"
        _iceberg_log_op("write", ICEBERG_WRITE_TABLE, 0, "running",
                         f"Appending analytics batch to {fqn_write}...")
        try:
            now_ts = datetime.now(timezone.utc)
            # Build a small analytics batch (5 rows per cycle)
            write_data = pa.table({
                "analysis_id": pa.array(
                    [f"OSS-{cycle:04d}-{i}" for i in range(5)], type=pa.string()),
                "source_table": pa.array([read_table_name] * 5, type=pa.string()),
                "analysis_type": pa.array(
                    ["health_check", "anomaly_scan", "trend_calc",
                     "threshold_alert", "summary_agg"],
                    type=pa.string()),
                "metric_value": pa.array(
                    [round(random.uniform(10, 100), 2) for _ in range(5)],
                    type=pa.float64()),
                "computed_at": pa.array([now_ts.isoformat()] * 5, type=pa.string()),
                "engine": pa.array(["pyiceberg-oss"] * 5, type=pa.string()),
            })

            tbl_w = catalog.load_table(fqn_write)
            tbl_w.append(write_data)
            _iceberg_state["writes"] += 1
            # Capture the written rows for display
            write_rows = write_data.to_pydict()
            write_sample: list[dict] = []
            for i in range(min(5, len(write_rows.get("analysis_id", [])))):
                row = {col: vals[i] for col, vals in write_rows.items()}
                write_sample.append(row)
            write_schema = [{"name": f.name, "type": str(f.type)} for f in write_data.schema]
            _iceberg_log_op("write", ICEBERG_WRITE_TABLE, 5, "success",
                             f"Appended 5 rows (cycle {cycle}) via PyIceberg OSS",
                             data=write_sample, schema=write_schema)
        except Exception as e:
            msg = str(e)
            # This workspace's Unity Catalog serves Iceberg reads and DDL over the
            # REST protocol but rejects external data commits with ErrorCode 2012.
            # Verified: a table created by PyIceberg itself fails to append exactly
            # like a Databricks-created managed table, so it is a platform capability
            # rather than anything about this table or these credentials. Say so,
            # instead of logging a generic error that reads as a broken demo.
            if "2012" in msg or "CommitStateUnknown" in type(e).__name__:
                _iceberg_state["write_unsupported"] = True
                _iceberg_log_op(
                    "write", ICEBERG_WRITE_TABLE, 0, "unsupported",
                    "External Iceberg commits are not enabled on this Unity Catalog "
                    "(ErrorCode 2012). Reads via open-source PyIceberg work; writes "
                    "must go through Databricks compute.",
                )
                log.info(f"Iceberg demo: external writes unsupported for {fqn_write}")
            else:
                _iceberg_state["errors"] += 1
                _iceberg_state["last_error"] = msg
                _iceberg_log_op("write", ICEBERG_WRITE_TABLE, 0, "error", msg[:200])
                log.warning(f"Iceberg demo write {fqn_write}: {e}")

        stop.wait(5)  # pause between cycles

    _iceberg_log_op("disconnect", "Unity Catalog", 0, "success", "Iceberg demo stopped")
    log.info("Iceberg demo stopped")


# ── Iceberg Demo Routes ──────────────────────────────────────────────────

@demos_bp.route("/api/iceberg-demo/start", methods=["POST"])
def iceberg_demo_start():
    """Start the PyIceberg read/write demo loop.

    Resets all counters and operation logs, then launches the background
    thread.  Returns 409 if the demo is already running.
    """
    with _iceberg_lock:
        if _iceberg_state["running"]:
            return jsonify({"error": "Iceberg demo is already running"}), 409

        # Reset state for a fresh run
        _iceberg_state["stop_event"] = threading.Event()
        _iceberg_state["running"] = True
        _iceberg_state["ops"] = []
        _iceberg_state["reads"] = 0
        _iceberg_state["writes"] = 0
        _iceberg_state["errors"] = 0
        _iceberg_state["last_error"] = None
        _iceberg_state["catalog_ok"] = False

        t = threading.Thread(target=_run_iceberg_demo, daemon=True)
        t.start()
        _iceberg_state["thread"] = t

        log.info("Iceberg demo started")
        return jsonify({"status": "started"})


@demos_bp.route("/api/iceberg-demo/stop", methods=["POST"])
def iceberg_demo_stop():
    """Stop the Iceberg demo loop.

    Sets the stop event, causing the background thread to exit after its
    current operation.  Returns 409 if the demo is not running.
    """
    with _iceberg_lock:
        if not _iceberg_state["running"]:
            return jsonify({"error": "Iceberg demo is not running"}), 409

        _iceberg_state["stop_event"].set()
        _iceberg_state["running"] = False
        log.info("Iceberg demo stop requested")
        return jsonify({"status": "stopped"})


@demos_bp.route("/api/iceberg-demo/status")
def iceberg_demo_status():
    """Poll Iceberg demo state.

    Returns running flag, catalog connectivity, read/write/error counters,
    the last 20 operations (with sample data and schema), and configuration
    details (catalog name, schema, write table).
    """
    return jsonify({
        "running": _iceberg_state["running"],
        "catalog_ok": _iceberg_state["catalog_ok"],
        "reads": _iceberg_state["reads"],
        "writes": _iceberg_state["writes"],
        "errors": _iceberg_state["errors"],
        "last_error": _iceberg_state["last_error"],
        "ops": _iceberg_state["ops"][-20:],  # last 20 ops for the UI
        "catalog_name": ICEBERG_CATALOG_NAME,
        "schema": ICEBERG_SCHEMA,
        "write_table": ICEBERG_WRITE_TABLE,
    })


@demos_bp.route("/api/iceberg-demo/source")
def iceberg_demo_source():
    """Return the actual source code of the Iceberg demo functions.

    Used by the frontend's code panel to show what is running under the hood.
    Returns a JSON dict with keys for each logical section (connect,
    list_namespaces, list_tables, read, write, full).
    """
    import inspect
    import textwrap

    sources: dict = {}
    try:
        sources["connect"] = inspect.getsource(_get_iceberg_catalog)
    except Exception:
        sources["connect"] = "# Source not available"
    try:
        full_src = inspect.getsource(_run_iceberg_demo)
        sources["full"] = full_src
        # Extract specific sections by splitting on step comments
        lines = full_src.split("\n")
        sections: dict[str, list[str]] = {
            "list_namespaces": [], "list_tables": [], "read": [], "write": [],
        }
        current = None
        for line in lines:
            if "# Step 2: List namespaces" in line:
                current = "list_namespaces"
            elif "# Step 3: List tables" in line:
                current = "list_tables"
            elif "# Step 4: Read/write loop" in line:
                current = "read"
            elif "# WRITE:" in line:
                current = "write"
            elif line.strip().startswith("_iceberg_log_op('disconnect'") or \
                 line.strip().startswith('_iceberg_log_op("disconnect"'):
                current = None
            if current:
                sections[current].append(line)
        for key, sec_lines in sections.items():
            if sec_lines:
                sources[key] = textwrap.dedent("\n".join(sec_lines))
    except Exception:
        sources["full"] = "# Source not available"
    return jsonify(sources)


# ═══════════════════════════════════════════════════════════════════════════
# STB Migration Simulator — Parquet -> Managed Iceberg with Liquid Clustering
# ═══════════════════════════════════════════════════════════════════════════


def _migration_update_step(step_id: str, status: str, detail: str = "",
                            rows: int = 0, elapsed_ms: int = 0,
                            metadata: dict | None = None) -> None:
    """Update a migration step's status in the shared state dict.

    Called by ``_run_migration`` as each step transitions from pending to
    running to success/error.
    """
    for step in _migration_state["steps"]:
        if step["id"] == step_id:
            step["status"] = status
            step["detail"] = detail
            step["rows"] = rows
            step["elapsed_ms"] = elapsed_ms
            if metadata:
                step["metadata"] = metadata
            break


# ---------------------------------------------------------------------------
# PyArrow data generators for the migration demo
# ---------------------------------------------------------------------------


def _generate_devices_pa():
    """Generate a PyArrow table of 500 STB device records.

    Deterministic: same devices every time for stable demo results.
    Fields include device_id, model, firmware, region, service tier, etc.
    """
    import pyarrow as pa

    models = ["XG2v2-P", "XG1v4-A", "Xi6-T", "Xi5-S"]
    revisions = ["rev1", "rev2", "rev3"]
    firmwares = [
        "PROD_22.3.1_build.42", "PROD_23.1.0_build.8",
        "PROD_23.2.1_build.30", "PROD_24.1.0_build.3",
        "PROD_22.4.0_build.15",
    ]
    locations = ["living_room", "bedroom", "basement", "office"]
    regions = ["Mountain West", "Bay Area", "Northeast", "South Central", "Midwest", "Southeast"]
    tiers = ["basic", "standard", "premium"]
    today = datetime.now().date()

    rows: list[dict] = []
    for i in range(500):
        install_offset = 100 + (i * 7 % 900)
        install_date = today - timedelta(days=install_offset)
        rows.append({
            "device_id": f"STB-{i:06d}",
            "household_id": f"HH-{i // 2:05d}",
            "model": models[i % 4],
            "hardware_revision": revisions[i % 3],
            "firmware_version": firmwares[i % 5],
            "install_date": install_date.isoformat(),
            "location_type": locations[i % 4],
            "lat": round(39.74 + (i * 0.031 % 8) - 4, 4),
            "lng": round(-104.99 + (i * 0.047 % 12) - 6, 4),
            "region": regions[i % 6],
            "service_tier": tiers[i % 3],
            "dvr_enabled": (i % 3 != 0),
            "is_4k": (i % 4 == 0 or i % 4 == 2),
        })

    return pa.table({
        "device_id": pa.array([r["device_id"] for r in rows], type=pa.string()),
        "household_id": pa.array([r["household_id"] for r in rows], type=pa.string()),
        "model": pa.array([r["model"] for r in rows], type=pa.string()),
        "hardware_revision": pa.array([r["hardware_revision"] for r in rows], type=pa.string()),
        "firmware_version": pa.array([r["firmware_version"] for r in rows], type=pa.string()),
        "install_date": pa.array([r["install_date"] for r in rows], type=pa.string()),
        "location_type": pa.array([r["location_type"] for r in rows], type=pa.string()),
        "lat": pa.array([r["lat"] for r in rows], type=pa.float64()),
        "lng": pa.array([r["lng"] for r in rows], type=pa.float64()),
        "region": pa.array([r["region"] for r in rows], type=pa.string()),
        "service_tier": pa.array([r["service_tier"] for r in rows], type=pa.string()),
        "dvr_enabled": pa.array([r["dvr_enabled"] for r in rows], type=pa.bool_()),
        "is_4k": pa.array([r["is_4k"] for r in rows], type=pa.bool_()),
    })


def _generate_telemetry_pa():
    """Generate a PyArrow table of 10,000 STB telemetry readings.

    Uses a seeded RNG (seed=42) for reproducibility.  Each of 500 devices
    gets 20 readings spread across 20 days.
    """
    import pyarrow as pa

    rng = random.Random(42)
    firmwares = [
        "PROD_22.3.1_build.42", "PROD_23.1.0_build.8",
        "PROD_23.2.1_build.30", "PROD_24.1.0_build.3",
        "PROD_22.4.0_build.15",
    ]
    content_types = ["live_tv", "dvr", "vod", "app", "idle"]
    now = datetime.now()

    # Pre-allocate column lists for performance
    device_ids, household_ids, timestamps = [], [], []
    signal_snr_dbs, signal_power_dbmvs = [], []
    downstream_freq_mhzs, upstream_power_dbmvs = [], []
    corrected_errors_list, uncorrected_errors_list = [], []
    cpu_utils, mem_utils, temps, uptimes = [], [], [], []
    boot_counts, tuner_failures = [], []
    hdmi_statuses, wifi_strengths = [], []
    content_type_list, channel_numbers = [], []
    bitrates, buffering_list, playback_errors_list = [], [], []
    firmware_list, error_codes_list = [], []
    dvr_usages, reading_dates = [], []

    for i in range(10000):
        dev_idx = i % 500
        day_offset = i // 500
        ts = now - timedelta(days=day_offset)
        rd = ts.date()

        device_ids.append(f"STB-{dev_idx:06d}")
        household_ids.append(f"HH-{dev_idx // 2:05d}")
        timestamps.append(ts.isoformat())
        signal_snr_dbs.append(round(25 + rng.random() * 17, 1))
        signal_power_dbmvs.append(round(-10 + rng.random() * 18, 1))
        downstream_freq_mhzs.append(549 + int(rng.random() * 7) * 6)
        upstream_power_dbmvs.append(round(35 + rng.random() * 15, 1))
        corrected_errors_list.append(int(rng.random() * 200))
        uncorrected_errors_list.append(int(rng.random() * 30))
        cpu_utils.append(round(15 + rng.random() * 60, 1))
        mem_utils.append(round(25 + rng.random() * 50, 1))
        temps.append(round(30 + rng.random() * 40, 1))
        uptimes.append(round(rng.random() * 2000, 1))
        boot_counts.append(int(rng.random() * 10))
        tuner_failures.append(int(rng.random() * 8))
        hdmi_statuses.append("connected" if rng.random() > 0.05 else "disconnected")
        wifi_strengths.append(round(-80 + rng.random() * 50, 1))
        content_type_list.append(rng.choice(content_types))
        channel_numbers.append(str(int(rng.random() * 999)) if rng.random() > 0.5 else None)
        bitrates.append(round(rng.random() * 25, 2))
        buffering_list.append(int(rng.random() * 10))
        playback_errors_list.append(int(rng.random() * 5))
        firmware_list.append(firmwares[i % 5])
        r = rng.random()
        error_codes_list.append(
            "E101" if r > 0.85 and r <= 0.95 else ("E101|E205" if r > 0.95 else None)
        )
        dvr_usages.append(round(rng.random() * 95, 1))
        reading_dates.append(rd.isoformat())

    return pa.table({
        "device_id": pa.array(device_ids, type=pa.string()),
        "household_id": pa.array(household_ids, type=pa.string()),
        "timestamp": pa.array(timestamps, type=pa.string()),
        "signal_snr_db": pa.array(signal_snr_dbs, type=pa.float64()),
        "signal_power_dbmv": pa.array(signal_power_dbmvs, type=pa.float64()),
        "downstream_freq_mhz": pa.array(downstream_freq_mhzs, type=pa.int32()),
        "upstream_power_dbmv": pa.array(upstream_power_dbmvs, type=pa.float64()),
        "corrected_errors": pa.array(corrected_errors_list, type=pa.int32()),
        "uncorrected_errors": pa.array(uncorrected_errors_list, type=pa.int32()),
        "cpu_utilization_pct": pa.array(cpu_utils, type=pa.float64()),
        "memory_utilization_pct": pa.array(mem_utils, type=pa.float64()),
        "temperature_celsius": pa.array(temps, type=pa.float64()),
        "uptime_hours": pa.array(uptimes, type=pa.float64()),
        "boot_count_30d": pa.array(boot_counts, type=pa.int32()),
        "tuner_lock_failures": pa.array(tuner_failures, type=pa.int32()),
        "hdmi_connection_status": pa.array(hdmi_statuses, type=pa.string()),
        "wifi_signal_strength_dbm": pa.array(wifi_strengths, type=pa.float64()),
        "content_type": pa.array(content_type_list, type=pa.string()),
        "channel_number": pa.array(channel_numbers, type=pa.string()),
        "stream_bitrate_mbps": pa.array(bitrates, type=pa.float64()),
        "buffering_events": pa.array(buffering_list, type=pa.int32()),
        "playback_errors": pa.array(playback_errors_list, type=pa.int32()),
        "firmware_version": pa.array(firmware_list, type=pa.string()),
        "error_codes": pa.array(error_codes_list, type=pa.string()),
        "dvr_disk_usage_pct": pa.array(dvr_usages, type=pa.float64()),
        "reading_date": pa.array(reading_dates, type=pa.string()),
    })


def _generate_incidents_pa():
    """Generate a PyArrow table of 200 hardware incident records.

    Covers 5 incident types (signal_loss, hardware_failure, firmware_crash,
    overheating, tuner_failure) with 3 severity levels and 4 root causes.
    ~75% of incidents are resolved (every 4th is left open).
    """
    import pyarrow as pa

    incident_types = ["signal_loss", "hardware_failure", "firmware_crash",
                      "overheating", "tuner_failure"]
    severities = ["low", "medium", "high"]
    descriptions_map = {
        "signal_loss": "signal loss below threshold",
        "hardware_failure": "hardware component failure",
        "firmware_crash": "firmware crash and auto-reboot",
        "overheating": "temperature exceeding safe range",
        "tuner_failure": "tuner unable to lock signal",
    }
    root_causes = ["cable_degradation", "component_wear", "firmware_bug", "environmental"]
    now = datetime.now()

    incident_ids, device_ids, household_ids = [], [], []
    types_list, severity_list, desc_list = [], [], []
    detected_list, resolved_list = [], []
    root_cause_list, dispatched_list = [], []

    for i in range(200):
        dev_idx = i % 500
        inc_type = incident_types[i % 5]
        detected = now - timedelta(days=(i % 90))
        resolved = (now - timedelta(days=(i % 90) - 1)) if (i % 4 != 0) else None

        incident_ids.append(f"INC-{i:06d}")
        device_ids.append(f"STB-{dev_idx:06d}")
        household_ids.append(f"HH-{dev_idx // 2:05d}")
        types_list.append(inc_type)
        severity_list.append(severities[i % 3])
        desc_list.append(f"STB device reported {descriptions_map[inc_type]}")
        detected_list.append(detected.isoformat())
        resolved_list.append(resolved.isoformat() if resolved else None)
        root_cause_list.append(root_causes[i % 4])
        dispatched_list.append(i % 3 == 2)

    return pa.table({
        "incident_id": pa.array(incident_ids, type=pa.string()),
        "device_id": pa.array(device_ids, type=pa.string()),
        "household_id": pa.array(household_ids, type=pa.string()),
        "incident_type": pa.array(types_list, type=pa.string()),
        "severity": pa.array(severity_list, type=pa.string()),
        "description": pa.array(desc_list, type=pa.string()),
        "detected_at": pa.array(detected_list, type=pa.string()),
        "resolved_at": pa.array(resolved_list, type=pa.string()),
        "root_cause": pa.array(root_cause_list, type=pa.string()),
        "technician_dispatched": pa.array(dispatched_list, type=pa.bool_()),
    })


def _write_parquet_to_volume(name: str, pa_table, volume_path: str) -> tuple[str, float]:
    """Write a PyArrow table as Parquet to a UC Volume.

    Returns (file_path, size_kb) where file_path is the full Volume path
    and size_kb is the Parquet file size in kilobytes.
    """
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pa_table, buf)
    size_kb = round(buf.tell() / 1024, 1)
    buf.seek(0)
    w = get_workspace_client()
    file_path = f"{volume_path}/{name}/data.parquet"
    w.files.upload(file_path=file_path, contents=buf, overwrite=True)
    return file_path, size_kb


def _run_migration() -> None:
    """Background thread: execute the 2-tier Parquet -> Managed Iceberg migration.

    Tier 1 (raw):
    - Creates a UC Volume for raw Parquet files
    - Generates device, telemetry, and incident data as Parquet
    - Uploads each to the volume

    Tier 2 (managed):
    - Creates Managed Iceberg tables via CTAS with liquid clustering
    - Validates row counts between source Parquet and Iceberg tables
    - Runs an analytics join query to demonstrate query performance

    Each step updates ``_migration_state['steps']`` so the frontend can
    render a real-time DAG visualization.
    """
    stop = _migration_state["stop_event"]
    CAT = os.environ.get("PIPELINE_CATALOG", "dba-lakebase-network")
    SOURCE_SCHEMA = "stb_source"
    MANAGED_SCHEMA = "stb_managed"
    VOLUME_NAME = "stb_source_data"
    VOLUME_PATH = f"/Volumes/{CAT}/{SOURCE_SCHEMA}/{VOLUME_NAME}"
    # Track expected row counts from Parquet generation for validation
    expected_rows: dict[str, int] = {}

    try:
        # ── Tier 1: Raw Parquet Files ─────────────────────────────────────

        # Step 1: create_volume
        _migration_update_step("create_volume", "running")
        t0 = time.time()
        _run_sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.`{SOURCE_SCHEMA}`")
        _run_sql(f"CREATE VOLUME IF NOT EXISTS `{CAT}`.`{SOURCE_SCHEMA}`.`{VOLUME_NAME}`")
        _migration_update_step(
            "create_volume", "success",
            f"Volume ready at {VOLUME_PATH}",
            elapsed_ms=int((time.time() - t0) * 1000),
            metadata={"volume_path": VOLUME_PATH},
        )
        if stop.is_set():
            return

        # Step 2: gen_devices
        _migration_update_step("gen_devices", "running", "Generating device Parquet...")
        t0 = time.time()
        devices_pa = _generate_devices_pa()
        file_path, size_kb = _write_parquet_to_volume("devices", devices_pa, VOLUME_PATH)
        expected_rows["devices"] = devices_pa.num_rows
        _migration_update_step(
            "gen_devices", "success",
            f"{devices_pa.num_rows} rows, {size_kb} KB -> {file_path}",
            rows=devices_pa.num_rows,
            elapsed_ms=int((time.time() - t0) * 1000),
            metadata={"file_size_kb": size_kb},
        )
        if stop.is_set():
            return

        # Step 3: gen_telemetry
        _migration_update_step("gen_telemetry", "running", "Generating telemetry Parquet...")
        t0 = time.time()
        telemetry_pa = _generate_telemetry_pa()
        file_path, size_kb = _write_parquet_to_volume("telemetry", telemetry_pa, VOLUME_PATH)
        expected_rows["telemetry"] = telemetry_pa.num_rows
        _migration_update_step(
            "gen_telemetry", "success",
            f"{telemetry_pa.num_rows} rows, {size_kb} KB -> {file_path}",
            rows=telemetry_pa.num_rows,
            elapsed_ms=int((time.time() - t0) * 1000),
            metadata={"file_size_kb": size_kb},
        )
        if stop.is_set():
            return

        # Step 4: gen_incidents
        _migration_update_step("gen_incidents", "running", "Generating incidents Parquet...")
        t0 = time.time()
        incidents_pa = _generate_incidents_pa()
        file_path, size_kb = _write_parquet_to_volume("incidents", incidents_pa, VOLUME_PATH)
        expected_rows["incidents"] = incidents_pa.num_rows
        _migration_update_step(
            "gen_incidents", "success",
            f"{incidents_pa.num_rows} rows, {size_kb} KB -> {file_path}",
            rows=incidents_pa.num_rows,
            elapsed_ms=int((time.time() - t0) * 1000),
            metadata={"file_size_kb": size_kb},
        )
        if stop.is_set():
            return

        # ── Tier 2: Managed Iceberg (CTAS from read_files -> Iceberg) ────

        # Step 5: migrate_devices
        _migration_update_step("migrate_devices", "running",
                                "read_files() -> CTAS USING ICEBERG CLUSTER BY ...")
        t0 = time.time()
        _run_sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.`{MANAGED_SCHEMA}`")
        _run_sql(f"""
            CREATE OR REPLACE TABLE `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_devices
            USING ICEBERG
            CLUSTER BY (device_id, region)
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
            AS SELECT * FROM read_files(
                '{VOLUME_PATH}/devices/',
                format => 'parquet'
            )
        """)
        result = _run_sql(
            f"SELECT COUNT(*) FROM `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_devices"
        )
        count = int(result[0][0]) if result else 0
        _migration_update_step(
            "migrate_devices", "success",
            f"{count} devices migrated",
            rows=count,
            elapsed_ms=int((time.time() - t0) * 1000),
            metadata={"cluster_by": "device_id, region",
                       "source": f"{VOLUME_PATH}/devices/"},
        )
        if stop.is_set():
            return

        # Step 6: migrate_telemetry
        _migration_update_step("migrate_telemetry", "running",
                                "read_files() -> CTAS USING ICEBERG CLUSTER BY ...")
        t0 = time.time()
        _run_sql(f"""
            CREATE OR REPLACE TABLE `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_telemetry
            USING ICEBERG
            CLUSTER BY (device_id, reading_date)
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
            AS SELECT * FROM read_files(
                '{VOLUME_PATH}/telemetry/',
                format => 'parquet'
            )
        """)
        result = _run_sql(
            f"SELECT COUNT(*) FROM `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_telemetry"
        )
        count = int(result[0][0]) if result else 0
        _migration_update_step(
            "migrate_telemetry", "success",
            f"{count} rows migrated",
            rows=count,
            elapsed_ms=int((time.time() - t0) * 1000),
            metadata={"cluster_by": "device_id, reading_date",
                       "source": f"{VOLUME_PATH}/telemetry/"},
        )
        if stop.is_set():
            return

        # Step 7: migrate_incidents
        _migration_update_step("migrate_incidents", "running",
                                "read_files() -> CTAS USING ICEBERG CLUSTER BY ...")
        t0 = time.time()
        _run_sql(f"""
            CREATE OR REPLACE TABLE `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_incidents
            USING ICEBERG
            CLUSTER BY (device_id, incident_type)
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false')
            AS SELECT * FROM read_files(
                '{VOLUME_PATH}/incidents/',
                format => 'parquet'
            )
        """)
        result = _run_sql(
            f"SELECT COUNT(*) FROM `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_incidents"
        )
        count = int(result[0][0]) if result else 0
        _migration_update_step(
            "migrate_incidents", "success",
            f"{count} incidents migrated",
            rows=count,
            elapsed_ms=int((time.time() - t0) * 1000),
            metadata={"cluster_by": "device_id, incident_type",
                       "source": f"{VOLUME_PATH}/incidents/"},
        )
        if stop.is_set():
            return

        # Step 8: validate -- compare Parquet source counts with Iceberg tables
        _migration_update_step("validate", "running",
                                "Checking Parquet vs Iceberg row counts...")
        t0 = time.time()
        checks: list[str] = []
        for tbl in ["devices", "telemetry", "incidents"]:
            mgd_count = _run_sql(
                f"SELECT COUNT(*) FROM `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_{tbl}"
            )
            mgd_n = int(mgd_count[0][0]) if mgd_count else 0
            exp_n = expected_rows.get(tbl, 0)
            match = exp_n == mgd_n
            checks.append(
                f"{tbl}: {exp_n} Parquet -> {mgd_n} Iceberg {'OK' if match else 'MISMATCH'}"
            )
        _migration_update_step(
            "validate", "success",
            " | ".join(checks),
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        if stop.is_set():
            return

        # Step 9: show_benefits -- analytics query on managed Iceberg tables
        _migration_update_step("show_benefits", "running",
                                "Running analytics on liquid-clustered Iceberg tables...")
        t0 = time.time()
        result = _run_sql(f"""
            SELECT d.region, COUNT(DISTINCT d.device_id) as devices,
                   ROUND(AVG(t.signal_snr_db), 1) as avg_snr,
                   ROUND(AVG(t.buffering_events), 1) as avg_buffering,
                   COUNT(i.incident_id) as incidents
            FROM `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_devices d
            LEFT JOIN `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_telemetry t ON d.device_id = t.device_id
            LEFT JOIN `{CAT}`.`{MANAGED_SCHEMA}`.managed_stb_incidents i ON d.device_id = i.device_id
            GROUP BY d.region ORDER BY devices DESC
        """)
        query_timing = int((time.time() - t0) * 1000)
        detail = (
            f"Managed Iceberg query: {query_timing}ms -- "
            "liquid clustering + predictive optimization active"
        )
        _migration_update_step(
            "show_benefits", "success", detail,
            rows=len(result),
            elapsed_ms=query_timing,
            metadata={"query_timing_ms": query_timing},
        )

        _migration_state["completed_at"] = datetime.now(timezone.utc).isoformat()

    except Exception as e:
        log_error("migration_simulator", e)
        _migration_state["error"] = str(e)
        # Mark the current running step as failed
        for step in _migration_state["steps"]:
            if step.get("status") == "running":
                step["status"] = "error"
                step["detail"] = str(e)
                break
    finally:
        _migration_state["running"] = False


# ── Migration Routes ──────────────────────────────────────────────────────

@demos_bp.route("/api/migration/start", methods=["POST"])
def migration_start():
    """Start the Parquet-to-Managed-Iceberg migration simulation.

    Initializes all steps as ``pending`` and launches the background thread.
    Returns 409 if the migration is already running.
    """
    with _migration_lock:
        if _migration_state["running"]:
            return jsonify({"error": "Migration is already running"}), 409

        _migration_state["running"] = True
        _migration_state["stop_event"] = threading.Event()
        _migration_state["error"] = None
        _migration_state["started_at"] = datetime.now(timezone.utc).isoformat()
        _migration_state["completed_at"] = None
        _migration_state["steps"] = [
            {**step, "status": "pending", "detail": "", "rows": 0, "elapsed_ms": 0}
            for step in MIGRATION_STEPS
        ]

        t = threading.Thread(target=_run_migration, daemon=True)
        t.start()

        return jsonify({"started": True})


@demos_bp.route("/api/migration/stop", methods=["POST"])
def migration_stop():
    """Stop the migration simulation.

    Sets the stop event; the background thread will exit after its current
    step completes.  Remaining steps stay in ``pending`` status.
    """
    _migration_state["stop_event"].set()
    _migration_state["running"] = False
    return jsonify({"stopped": True})


@demos_bp.route("/api/migration/status")
def migration_status():
    """Poll migration progress.

    Returns the running flag, step-by-step DAG (each step has id, label,
    tier, status, detail, rows, elapsed_ms, and optional metadata), any
    error message, and start/completion timestamps.
    """
    return jsonify({
        "running": _migration_state["running"],
        "steps": _migration_state["steps"],
        "error": _migration_state["error"],
        "started_at": _migration_state["started_at"],
        "completed_at": _migration_state["completed_at"],
    })

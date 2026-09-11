"""
Simulator and Environment Scale-Up API Blueprint.

Provides real-time data generation and bulk scale-up endpoints for the
Lakebase FSM demo.  The simulator creates work orders, dispatches
technicians, uploads IoT/STB telemetry to UC Volumes, and triggers the
DLT pipeline -- all running as background threads with start/stop/status
controls.  The upscale API additively bulk-inserts rows using
``generate_series`` to grow the environment to millions of records.

Routes
------
POST /api/simulator/start     Start the simulator (work orders + IoT + STB generators)
GET  /api/simulator/status    Poll simulator state (RUNNING / COMPLETED / FAILED / IDLE)
POST /api/simulator/stop      Signal the simulator to stop gracefully
GET  /api/simulator/live-stats  Real-time counts and recent events since a given timestamp
POST /api/upscale/start       Start bulk scale-up to a target customer count
GET  /api/upscale/status      Poll scale-up progress (phase, rows added, errors)
POST /api/upscale/stop        Signal the scale-up to stop after the current batch

Dependencies from ``shared``
----------------------------
get_pool, get_workspace_client, log_error

Dependencies from ``app`` (main module)
---------------------------------------
_run_generator         — main work-order generator thread function
_run_iot_generator     — IoT telemetry CSV upload thread
_run_stb_generator     — STB telemetry CSV upload thread
_ensure_position_thread — starts the always-on tech position update thread
_bootstrap_moving_techs — seeds movement routes for en_route technicians
_trigger_iot_pipeline   — submits the Iceberg streaming pipeline job
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request

from shared import get_pool, get_workspace_client, log_error

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint definition
# ---------------------------------------------------------------------------

simulator_bp = Blueprint("simulator", __name__)

# ---------------------------------------------------------------------------
# Simulator state — accessed from app.py via lazy import (single source of truth)
# ---------------------------------------------------------------------------
# The _get_sim_state() dict and _get_sim_lock() are defined in app.py because the
# generator thread functions (_run_generator, _run_iot_generator, etc.)
# also live there and write to the same state object.
#
# IMPORTANT: We cannot import from app at module level — it causes a circular
# import (app.py imports this blueprint, this file imports from app.py).
# Instead, we use a lazy accessor that imports on first use.

_sim_state_ref = None
_sim_lock_ref = None

def _get_sim_state():
    """Lazy import of _get_sim_state() from app.py to avoid circular import."""
    global _sim_state_ref
    if _sim_state_ref is None:
        from app import _sim_state
        _sim_state_ref = _sim_state
    return _sim_state_ref

def _get_sim_lock():
    """Lazy import of _get_sim_lock() from app.py to avoid circular import."""
    global _sim_lock_ref
    if _sim_lock_ref is None:
        from app import _sim_lock
        _sim_lock_ref = _sim_lock
    return _sim_lock_ref

# ---------------------------------------------------------------------------
# Upscale state
# ---------------------------------------------------------------------------
# Protected by _upscale_lock.  Tracks the progress of a bulk
# ``generate_series`` scale-up across customers, technicians, work orders,
# equipment, and notes.

_upscale_lock = threading.Lock()
_upscale_state: dict = {
    "running": False,
    "progress": "",
    "phase": "",       # customers | technicians | work_orders | equipment | notes | done | error
    "error": None,
    "rows_added": 0,
    "start_time": None,
    "stop_event": None,
}


# ── Simulator Routes ──────────────────────────────────────────────────────

@simulator_bp.route("/api/simulator/start", methods=["POST"])
def simulator_start():
    """Start the full simulator suite.

    Launches four background threads in parallel:
    1. Work-order generator -- creates, assigns, and completes WOs in Lakebase
    2. IoT telemetry generator -- uploads CSV files to the UC Volume for DLT
    3. STB telemetry generator -- uploads STB CSV files for the STB pipeline
    4. Network incident generator -- creates, escalates, and resolves incidents

    Also bootstraps technician movement routes and triggers the DLT pipeline.

    Request JSON (all optional):
        duration_minutes (int): how long the simulator runs (default 2)
        speed_factor (float):   multiplier for event frequency (default 1)

    Returns 409 if the simulator is already running.
    """
    # Lazy imports for functions defined in the main app module
    from app import (
        _bootstrap_moving_techs,
        _ensure_position_thread,
        _run_generator,
        _run_incident_generator,
        _run_iot_generator,
        _run_source_refresh_and_pipeline,
        _run_stb_generator,
    )

    with _get_sim_lock():
        if _get_sim_state()["running"]:
            return jsonify({"error": "Simulator is already running"}), 409

        data = request.json or {}
        duration_minutes = data.get("duration_minutes", 2)
        speed_factor = data.get("speed_factor", 1)
        start_time = datetime.now(timezone.utc).isoformat()

        # Reset state for a fresh run
        _get_sim_state()["stop_event"] = threading.Event()
        _get_sim_state()["running"] = True
        _get_sim_state()["start_time"] = start_time
        _get_sim_state()["duration_minutes"] = duration_minutes
        # Recorded so other surfaces can mirror the controls of a run they did not
        # start — without it the page kept showing its default while the popup
        # showed the real setting.
        _get_sim_state()["speed_factor"] = speed_factor
        _get_sim_state()["error"] = None
        _get_sim_state()["iot_files_generated"] = 0
        _get_sim_state()["stb_files_generated"] = 0
        _get_sim_state()["incidents_generated"] = 0
        _get_sim_state()["pipeline_phase"] = "generating"
        _get_sim_state()["pipeline_phase_detail"] = "Starting source file generation..."
        _get_sim_state()["pipeline_started_at"] = None
        _get_sim_state()["pipeline_completed_at"] = None

        # Launch work-order generator thread
        t = threading.Thread(
            target=_run_generator,
            args=(duration_minutes,),
            kwargs={"speed_factor": speed_factor},
            daemon=True,
        )
        t.start()
        _get_sim_state()["thread"] = t

        # Launch IoT telemetry generator thread
        iot_t = threading.Thread(target=_run_iot_generator, args=(speed_factor,), daemon=True)
        iot_t.start()
        _get_sim_state()["iot_thread"] = iot_t

        # Launch STB telemetry generator thread
        stb_t = threading.Thread(target=_run_stb_generator, args=(speed_factor,), daemon=True)
        stb_t.start()
        _get_sim_state()["stb_thread"] = stb_t

        # Launch network incident generator thread
        inc_t = threading.Thread(target=_run_incident_generator, args=(speed_factor,), daemon=True)
        inc_t.start()
        _get_sim_state()["incident_thread"] = inc_t

        # Ensure position update thread is running (idempotent, always-on)
        _ensure_position_thread()

        # Bootstrap movement for any techs already en_route from seed data
        try:
            _bootstrap_moving_techs(get_pool())
        except Exception as e:
            log.warning(f"Bootstrap en_route failed: {e}")

        # Launch source file refresh + pipeline monitor thread
        pipe_t = threading.Thread(target=_run_source_refresh_and_pipeline, args=(speed_factor,), daemon=True)
        pipe_t.start()
        _get_sim_state()["pipeline_thread"] = pipe_t

        log.info(f"Simulator started: duration={duration_minutes}m, speed={speed_factor}x")
        return jsonify({"start_time": start_time, "duration_minutes": duration_minutes})


@simulator_bp.route("/api/simulator/status")
def simulator_status():
    """Poll the simulator's current state.

    Returns one of four states:
    - RUNNING:   simulator threads are active
    - COMPLETED: last run finished normally
    - FAILED:    last run ended with an error (error message in ``result``)
    - IDLE:      simulator has never been started this session
    """
    sim = _get_sim_state()
    running = sim["running"]
    error = sim["error"]
    if running:
        state = "RUNNING"
        result = None
    elif error:
        state = "FAILED"
        result = error
    elif sim["start_time"] and not running:
        state = "COMPLETED"
        result = "SUCCESS"
    else:
        state = "IDLE"
        result = None

    # Timing is part of the state, not something each surface tracks locally.
    # The simulator page used to key its whole display off a start time recorded
    # when *it* pressed Start, so a run launched from the popup left the page
    # showing no counters and a STANDBY waveform. Anything that polls this
    # endpoint can now render the same run identically.
    start_time = sim.get("start_time")
    started_at = None
    elapsed_sec = None
    if start_time:
        # start_time is stored as an ISO-8601 *string* (see the start handler), not a
        # datetime — so parse it rather than poking at datetime attributes.
        started_at = start_time if isinstance(start_time, str) else start_time.isoformat()
        try:
            base = datetime.fromisoformat(started_at)
            now = datetime.now(base.tzinfo) if base.tzinfo else datetime.now()
            elapsed_sec = int((now - base).total_seconds())
        except Exception as e:
            log.warning(f"simulator_status: could not compute elapsed_sec from {started_at!r}: {e}")

    return jsonify({
        "state": state,
        "result": result,
        "started_at": started_at,
        "elapsed_sec": elapsed_sec,
        "duration_minutes": sim.get("duration_minutes", 0),
        "speed_factor": sim.get("speed_factor", 0),
    })


@simulator_bp.route("/api/simulator/stop", methods=["POST"])
def simulator_stop():
    """Signal the simulator to stop gracefully.

    Sets the stop event, which all background threads check periodically.
    Threads will finish their current iteration and exit.
    """
    _get_sim_state()["stop_event"].set()
    log.info("Simulator stop requested")
    return jsonify({"success": True})


@simulator_bp.route("/api/simulator/pipeline-status")
def simulator_pipeline_status():
    """Get the current state of the Iceberg pipeline refresh.

    Returns phase, detail text, elapsed time, and estimated total for progress bars.
    """
    import time as _time
    phase = _get_sim_state().get("pipeline_phase", "idle")
    detail = _get_sim_state().get("pipeline_phase_detail", "")
    started = _get_sim_state().get("pipeline_started_at")
    completed = _get_sim_state().get("pipeline_completed_at")

    elapsed = int(_time.time() - started) if started else 0
    # Rough estimate: generating ~5s, uploading ~5s, pipeline ~240s
    est_total = 300 if phase.startswith("pipeline") else 10

    return jsonify({
        "phase": phase,
        "detail": detail,
        "elapsed_seconds": elapsed,
        "estimated_total_seconds": est_total,
        "completed": completed is not None,
    })


@simulator_bp.route("/api/simulator/live-stats")
def simulator_live_stats():
    """Real-time simulator statistics since a given timestamp.

    Query parameter:
        since (str): ISO-8601 timestamp -- only count events after this time

    Returns counts of new work orders, completions, appointments, notes,
    IoT/STB files generated, and a list of the 10 most recent events.
    """
    try:
        since = request.args.get("since")
        pool = get_pool()
        stats: dict = {}
        recent_events: list[dict] = []

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Batch count queries -- work orders, appointments, notes
                cur.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM field_service.work_orders WHERE created_at >= %s::timestamptz),
                        (SELECT COUNT(*) FROM field_service.work_orders WHERE status = 'completed' AND resolved_at >= %s::timestamptz),
                        (SELECT COUNT(*) FROM field_service.appointments WHERE created_at >= %s::timestamptz),
                        (SELECT COUNT(*) FROM field_service.work_order_notes WHERE created_at >= %s::timestamptz)
                """, (since, since, since, since))
                row = cur.fetchone()
                stats["new_work_orders"] = row[0]
                stats["completed_orders"] = row[1]
                stats["appointments"] = row[2]
                stats["notes_added"] = row[3]

                # Recent events (union of creations, completions, notes)
                cur.execute("""
                    (
                        SELECT wo.created_at, 'work_order' AS type,
                               wo.category || ': ' || wo.reported_issue AS detail,
                               wo.priority AS extra
                        FROM field_service.work_orders wo
                        WHERE wo.created_at >= %s::timestamptz
                        ORDER BY wo.created_at DESC LIMIT 5
                    )
                    UNION ALL
                    (
                        SELECT wo.resolved_at, 'completed', 'WO-' || wo.work_order_id || ' completed', wo.category
                        FROM field_service.work_orders wo
                        WHERE wo.status = 'completed' AND wo.resolved_at >= %s::timestamptz
                        ORDER BY wo.resolved_at DESC LIMIT 3
                    )
                    UNION ALL
                    (
                        SELECT n.created_at, 'note', LEFT(n.content, 80), n.note_type
                        FROM field_service.work_order_notes n
                        WHERE n.created_at >= %s::timestamptz
                        ORDER BY n.created_at DESC LIMIT 2
                    )
                    ORDER BY created_at DESC LIMIT 10
                """, (since, since, since))

                event_icons = {
                    "work_order": "\U0001F4CB",   # clipboard
                    "completed": "\u2705",         # check mark
                    "note": "\U0001F4DD",          # memo
                }
                for row in cur.fetchall():
                    ts, etype, detail, extra = row
                    if etype == "work_order":
                        text = f"New: {detail[:60]}" if detail else "New work order"
                    elif etype == "completed":
                        text = detail or "Order completed"
                    else:
                        text = f"Note: {detail[:60]}" if detail else "Note added"
                    recent_events.append({
                        "time": ts.isoformat() if ts else "",
                        "type": etype,
                        "icon": event_icons.get(etype, "\u25CF"),
                        "text": text,
                        "priority": (extra or "low").lower() if etype == "work_order" else "low",
                    })

        # Append file generation counters from the in-memory simulator state
        stats["iot_files_generated"] = _get_sim_state().get("iot_files_generated", 0)
        stats["stb_files_generated"] = _get_sim_state().get("stb_files_generated", 0)
        return jsonify({"stats": stats, "recent_events": recent_events})
    except Exception as e:
        log_error("live_stats", e)
        return jsonify({"error": str(e)}), 500


# ── Environment Scale-Up Routes ───────────────────────────────────────────

@simulator_bp.route("/api/upscale/start", methods=["POST"])
def upscale_start():
    """Start bulk scale-up to a target customer count.

    Additively inserts customers, technicians, work orders, equipment, and
    notes using ``generate_series`` with proportional ratios.  Runs in a
    background thread with progress tracking.

    Request JSON (optional):
        target_customers (int): desired total customer count (default 2,000,000)

    Returns 409 if a scale-up is already running.
    """
    with _upscale_lock:
        if _upscale_state["running"]:
            return jsonify({"error": "Scale-up already running"}), 409

        data = request.json or {}
        target = data.get("target_customers", 2_000_000)

        _upscale_state["running"] = True
        _upscale_state["error"] = None
        _upscale_state["rows_added"] = 0
        _upscale_state["phase"] = "starting"
        _upscale_state["progress"] = "Initializing..."
        _upscale_state["start_time"] = datetime.now(timezone.utc).isoformat()
        _upscale_state["stop_event"] = threading.Event()

        t = threading.Thread(target=_run_upscale, args=(target,), daemon=True)
        t.start()

        return jsonify({"target_customers": target, "started": True})


@simulator_bp.route("/api/upscale/status")
def upscale_status():
    """Poll scale-up progress.

    Returns the current phase, progress message, total rows added, any
    error, and the start timestamp.
    """
    return jsonify({
        "running": _upscale_state["running"],
        "phase": _upscale_state["phase"],
        "progress": _upscale_state["progress"],
        "rows_added": _upscale_state["rows_added"],
        "error": _upscale_state["error"],
        "start_time": _upscale_state["start_time"],
    })


@simulator_bp.route("/api/upscale/stop", methods=["POST"])
def upscale_stop():
    """Signal the scale-up to stop after the current batch.

    The background thread checks the stop event between batches and will
    exit gracefully, leaving the data consistent (no partial batches).
    """
    if _upscale_state["stop_event"]:
        _upscale_state["stop_event"].set()
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Scale-Up Background Worker
# ---------------------------------------------------------------------------


def _run_upscale(target_customers: int) -> None:
    """Background thread: additively bulk-insert rows to scale up the environment.

    Uses psycopg2 (not psycopg3 pool) with ``autocommit=True`` and
    ``statement_timeout=0`` for maximum throughput.  Inserts in batches of
    500,000 rows using ``generate_series``.

    Phases:
    1. Customers   -- proportional to target
    2. Technicians -- ~1 per 800 customers (min 100)
    3. Work Orders -- 2.5 per customer
    4. Equipment   -- 1.5 per customer
    5. Notes       -- one per new work order
    """
    import psycopg2 as pg2

    try:
        _upscale_state["phase"] = "connecting"
        _upscale_state["progress"] = "Connecting to Lakebase..."

        host = os.environ.get("PGHOST", "")
        user = os.environ.get("PGUSER", "")
        pw = os.environ.get("PGPASSWORD", "")
        db = os.environ.get("PGDATABASE", "databricks_postgres")

        conn = pg2.connect(host=host, port=5432, user=user, password=pw, database=db, sslmode="require")
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 0")

        stop = _upscale_state["stop_event"]

        # ── Get current max IDs and counts ────────────────────────────────
        cur.execute("SELECT MAX(customer_id) FROM field_service.customers")
        current_max_cust = cur.fetchone()[0] or 0
        cur.execute("SELECT MAX(work_order_id) FROM field_service.work_orders")
        current_max_wo = cur.fetchone()[0] or 0
        cur.execute("SELECT MAX(technician_id) FROM field_service.technicians")
        current_max_tech = cur.fetchone()[0] or 0
        cur.execute("SELECT MAX(inventory_id) FROM field_service.equipment_inventory")
        current_max_equip = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM field_service.customers")
        current_customers = cur.fetchone()[0] or 0

        # Already at or above target -- nothing to do
        if current_customers >= target_customers:
            _upscale_state["progress"] = (
                f"Already at {current_customers:,} customers (target: {target_customers:,})"
            )
            _upscale_state["phase"] = "done"
            conn.close()
            return

        # Calculate how many rows to add (proportional scaling)
        new_customers = target_customers - current_customers
        wo_ratio = 2.5  # 2.5 WOs per customer
        new_work_orders = int(new_customers * wo_ratio)
        tech_ratio = new_customers / 800  # ~1 tech per 800 customers
        new_techs = max(int(tech_ratio), 100)
        new_equipment = int(new_customers * 1.5)

        _upscale_state["progress"] = (
            f"Scaling from {current_customers:,} to {target_customers:,} customers. "
            f"Adding {new_customers:,} customers, {new_work_orders:,} WOs, "
            f"{new_techs:,} techs, {new_equipment:,} equipment..."
        )

        BATCH = 500_000
        total_added = 0

        # ── Phase 1: Customers ────────────────────────────────────────────
        _upscale_state["phase"] = "customers"
        cust_start = current_max_cust + 1
        cust_end = current_max_cust + new_customers
        for batch_start in range(cust_start, cust_end + 1, BATCH):
            if stop and stop.is_set():
                break
            batch_end = min(batch_start + BATCH - 1, cust_end)
            batch_size = batch_end - batch_start + 1
            _upscale_state["progress"] = (
                f"Customers: inserting {batch_start:,}-{batch_end:,} of {new_customers:,}"
            )
            cur.execute(f"""
                INSERT INTO field_service.customers (
                    customer_id,
                    account_number, first_name, last_name, email, phone, address_line1, city, state_province,
                    postal_code, region_id, customer_tier, account_status, service_type,
                    contract_start, contract_end, monthly_revenue, lifetime_value
                )
                SELECT
                    i AS customer_id,
                    'ACCT-S-' || LPAD(i::TEXT, 8, '0') AS account_number,
                    'Customer' AS first_name,
                    'C' || i::TEXT AS last_name,
                    'cust' || i || '@example.com' AS email,
                    '+1' || LPAD(((i::BIGINT * 123456789) % 9000000000 + 1000000000)::TEXT, 10, '0') AS phone,
                    (100 + (i % 9900))::TEXT || ' Scale St' AS address_line1,
                    CASE ((i * 7) % 6) WHEN 0 THEN 'Seattle' WHEN 1 THEN 'Phoenix' WHEN 2 THEN 'Dallas'
                        WHEN 3 THEN 'Atlanta' WHEN 4 THEN 'Chicago' ELSE 'New York' END AS city,
                    CASE ((i * 7) % 6) WHEN 0 THEN 'WA' WHEN 1 THEN 'AZ' WHEN 2 THEN 'TX'
                        WHEN 3 THEN 'GA' WHEN 4 THEN 'IL' ELSE 'NY' END AS state_province,
                    LPAD((10000 + (i * 17) % 89999)::TEXT, 5, '0') AS postal_code,
                    ((i * 7) % 6) + 1 AS region_id,
                    CASE WHEN (i % 100) < 75 THEN 'residential' WHEN (i % 100) < 92 THEN 'small_business' ELSE 'enterprise' END,
                    CASE WHEN (i % 100) < 90 THEN 'active' WHEN (i % 100) < 95 THEN 'suspended' ELSE 'cancelled' END,
                    CASE ((i * 3) % 5) WHEN 0 THEN 'fiber' WHEN 1 THEN 'dsl' WHEN 2 THEN 'cable' WHEN 3 THEN 'fixed_wireless' ELSE 'satellite' END,
                    CURRENT_DATE - ((i * 17) % 1825)::INTEGER,
                    CURRENT_DATE - ((i * 17) % 1825)::INTEGER + 730,
                    49.99 + (i % 50) * 5,
                    500.00 + (i % 200) * 100
                FROM generate_series({batch_start}, {batch_end}) i
            """)
            total_added += batch_size
            _upscale_state["rows_added"] = total_added

        # customer_id is BIGSERIAL: we assign it explicitly above (dense, contiguous
        # with existing ids) so downstream FK references resolve. Bump the identity
        # sequence past our max so later nextval-based inserts don't collide.
        cur.execute(
            "SELECT setval(pg_get_serial_sequence('field_service.customers','customer_id'), "
            "(SELECT MAX(customer_id) FROM field_service.customers))"
        )

        # ── Phase 2: Technicians ──────────────────────────────────────────
        if not (stop and stop.is_set()):
            _upscale_state["phase"] = "technicians"
            _upscale_state["progress"] = f"Adding {new_techs:,} technicians..."
            tech_start = current_max_tech + 1
            tech_end = current_max_tech + new_techs
            cur.execute(f"""
                INSERT INTO field_service.technicians (
                    technician_id,
                    employee_id, first_name, last_name, email, phone, region_id,
                    status, shift, hire_date, certification_level,
                    current_latitude, current_longitude, avg_rating,
                    jobs_completed_mtd, jobs_completed_ytd, first_fix_rate
                )
                SELECT
                    i,
                    'TECH-S-' || LPAD(i::TEXT, 6, '0'),
                    'Tech' AS first_name, 'T' || i::TEXT AS last_name,
                    'tech.s' || i || '@fieldops.com', '+1555' || LPAD(i::TEXT, 7, '0'),
                    ((i - 1) % 6) + 1, 'available', 'day',
                    CURRENT_DATE - (90 + (i * 23) % 3650)::INTEGER,
                    CASE WHEN (i * 23) % 3650 < 365 THEN 'junior' WHEN (i * 23) % 3650 < 1460 THEN 'standard'
                         WHEN (i * 23) % 3650 < 2920 THEN 'senior' ELSE 'lead' END,
                    CASE ((i-1)%6) WHEN 0 THEN 47.6 WHEN 1 THEN 33.4 WHEN 2 THEN 32.7 WHEN 3 THEN 33.7 WHEN 4 THEN 41.8 ELSE 40.7 END + (RANDOM()*0.5-0.25),
                    CASE ((i-1)%6) WHEN 0 THEN -122.3 WHEN 1 THEN -112.0 WHEN 2 THEN -96.8 WHEN 3 THEN -84.3 WHEN 4 THEN -87.6 ELSE -74.0 END + (RANDOM()*0.5-0.25),
                    3.50 + (RANDOM() * 1.50), 5 + (RANDOM() * 30)::INTEGER,
                    50 + (RANDOM() * 300)::INTEGER, 70.0 + (RANDOM() * 25.0)
                FROM generate_series({tech_start}, {tech_end}) i
            """)
            total_added += new_techs
            _upscale_state["rows_added"] = total_added
            # Same as customers: technician_id assigned explicitly (dense), bump seq.
            cur.execute(
                "SELECT setval(pg_get_serial_sequence('field_service.technicians','technician_id'), "
                "(SELECT MAX(technician_id) FROM field_service.technicians))"
            )

        # ── Phase 3: Work Orders ──────────────────────────────────────────
        if not (stop and stop.is_set()):
            _upscale_state["phase"] = "work_orders"
            new_max_cust = current_max_cust + new_customers
            new_max_tech = current_max_tech + new_techs
            techs_per_region = new_max_tech // 6
            wo_start = current_max_wo + 1
            wo_end = current_max_wo + new_work_orders
            for batch_start in range(wo_start, wo_end + 1, BATCH):
                if stop and stop.is_set():
                    break
                batch_end = min(batch_start + BATCH - 1, wo_end)
                batch_size = batch_end - batch_start + 1
                _upscale_state["progress"] = (
                    f"Work orders: {batch_start - wo_start + 1:,}-"
                    f"{batch_end - wo_start + 1:,} of {new_work_orders:,}"
                )
                cur.execute(f"""
                    INSERT INTO field_service.work_orders (
                        work_order_number, customer_id, sla_id, category, priority, status,
                        title, description, address_line1, city, state_province, postal_code,
                        region_id, latitude, longitude, assigned_technician_id,
                        created_at, updated_at, sla_due_at
                    )
                    SELECT
                        'WO-S-' || LPAD(i::TEXT, 8, '0'),
                        (1 + (i * 7) % {new_max_cust})::BIGINT,
                        CASE (i % 4) WHEN 0 THEN 1 WHEN 1 THEN 2 WHEN 2 THEN 3 ELSE 4 END,
                        CASE WHEN (i*13)%100 < 25 THEN 'install' WHEN (i*13)%100 < 60 THEN 'repair'
                             WHEN (i*13)%100 < 80 THEN 'maintenance' WHEN (i*13)%100 < 95 THEN 'upgrade' ELSE 'disconnect' END,
                        CASE (i%4) WHEN 0 THEN 'low' WHEN 1 THEN 'medium' WHEN 2 THEN 'high' ELSE 'critical' END,
                        CASE WHEN (i*19)%100 < 85 THEN 'completed' WHEN (i*19)%100 < 90 THEN 'cancelled'
                             WHEN (i*19)%100 < 93 THEN 'open' WHEN (i*19)%100 < 96 THEN 'assigned'
                             WHEN (i*19)%100 < 98 THEN 'in_progress' ELSE 'en_route' END,
                        'Scale work order #' || i,
                        'Bulk-generated work order for scale testing',
                        (100 + (i%9900))::TEXT || ' Service Rd',
                        CASE ((i*7)%6) WHEN 0 THEN 'Seattle' WHEN 1 THEN 'Phoenix' WHEN 2 THEN 'Dallas'
                             WHEN 3 THEN 'Atlanta' WHEN 4 THEN 'Chicago' ELSE 'New York' END,
                        CASE ((i*7)%6) WHEN 0 THEN 'WA' WHEN 1 THEN 'AZ' WHEN 2 THEN 'TX'
                             WHEN 3 THEN 'GA' WHEN 4 THEN 'IL' ELSE 'NY' END,
                        LPAD((10000 + (i*23)%89999)::TEXT, 5, '0'),
                        ((i*7)%6) + 1,
                        CASE ((i*7)%6) WHEN 0 THEN 47.6 WHEN 1 THEN 33.4 WHEN 2 THEN 32.7 WHEN 3 THEN 33.7 WHEN 4 THEN 41.8 ELSE 40.7 END + ((i*11%100-50)::FLOAT/200),
                        CASE ((i*7)%6) WHEN 0 THEN -122.3 WHEN 1 THEN -112.0 WHEN 2 THEN -96.8 WHEN 3 THEN -84.3 WHEN 4 THEN -87.6 ELSE -74.0 END + ((i*13%100-50)::FLOAT/200),
                        CASE WHEN (i*19)%100 >= 93 THEN NULL
                             ELSE ((((i*7)%6) * {techs_per_region}) + 1 + (i % {techs_per_region}))::BIGINT END,
                        CURRENT_TIMESTAMP - ((i*3)%365 || ' days')::INTERVAL,
                        CURRENT_TIMESTAMP - ((i*3)%365 || ' days')::INTERVAL + ('2 hours')::INTERVAL,
                        CURRENT_TIMESTAMP - ((i*3)%365 || ' days')::INTERVAL + ('24 hours')::INTERVAL
                    FROM generate_series({batch_start}, {batch_end}) i
                """)
                total_added += batch_size
                _upscale_state["rows_added"] = total_added

        # ── Phase 4: Equipment ────────────────────────────────────────────
        if not (stop and stop.is_set()):
            _upscale_state["phase"] = "equipment"
            eq_start = current_max_equip + 1
            eq_end = current_max_equip + new_equipment
            for batch_start in range(eq_start, eq_end + 1, BATCH):
                if stop and stop.is_set():
                    break
                batch_end = min(batch_start + BATCH - 1, eq_end)
                batch_size = batch_end - batch_start + 1
                _upscale_state["progress"] = (
                    f"Equipment: {batch_start - eq_start + 1:,}-"
                    f"{batch_end - eq_start + 1:,} of {new_equipment:,}"
                )
                cur.execute(f"""
                    INSERT INTO field_service.equipment_inventory (
                        equipment_type_id, serial_number, status, warehouse_location,
                        region_id, purchased_at
                    )
                    SELECT
                        CASE WHEN (i%100) < 40 THEN 1 + (i%8) WHEN (i%100) < 55 THEN 9 + (i%7)
                             WHEN (i%100) < 70 THEN 16 + (i%5) ELSE 21 + (i%6) END,
                        'SN-S-' || LPAD(i::TEXT, 8, '0'),
                        CASE WHEN (i%100) < 30 THEN 'in_stock' WHEN (i%100) < 80 THEN 'installed'
                             WHEN (i%100) < 90 THEN 'defective' ELSE 'returned' END,
                        'Warehouse-' || CASE ((i-1)%6) WHEN 0 THEN 'PNW' WHEN 1 THEN 'SW' WHEN 2 THEN 'SC'
                             WHEN 3 THEN 'SE' WHEN 4 THEN 'MW' ELSE 'NE' END,
                        ((i-1)%6) + 1,
                        CURRENT_DATE - (30 + (i*13)%730)::INTEGER
                    FROM generate_series({batch_start}, {batch_end}) i
                """)
                total_added += batch_size
                _upscale_state["rows_added"] = total_added

        # ── Phase 5: Notes for new WOs ────────────────────────────────────
        if not (stop and stop.is_set()):
            _upscale_state["phase"] = "notes"
            _upscale_state["progress"] = "Adding work order notes..."
            cur.execute(f"""
                INSERT INTO field_service.work_order_notes (work_order_id, author, note_type, content, created_at)
                SELECT wo.work_order_id, 'System', 'status_change',
                    'Work order created with priority: ' || wo.priority, wo.created_at
                FROM field_service.work_orders wo
                WHERE wo.work_order_id > {current_max_wo}
            """)
            notes_added = cur.rowcount or 0
            total_added += notes_added
            _upscale_state["rows_added"] = total_added

        # ── Done ──────────────────────────────────────────────────────────
        conn.close()
        _upscale_state["phase"] = "done"
        _upscale_state["progress"] = f"Scale-up complete! Added {total_added:,} rows total."
        log.info(
            f"Upscale complete: {total_added:,} rows added "
            f"({new_customers:,} customers, {new_work_orders:,} WOs)"
        )

    except Exception as e:
        log.error(f"Upscale error: {e}")
        _upscale_state["error"] = str(e)
        _upscale_state["phase"] = "error"
        _upscale_state["progress"] = f"Error: {e}"
    finally:
        _upscale_state["running"] = False

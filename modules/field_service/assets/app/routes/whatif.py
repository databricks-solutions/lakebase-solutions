"""
What-If Analysis blueprint — Lakebase Autoscaling database branching.

Purpose
=======
Enables scenario planning by forking the production Lakebase database into
an ephemeral copy-on-write branch, running a "what-if" DML scenario on the
branch, comparing KPIs (before vs. after), and cleaning up.  Branches are
created via the Lakebase Autoscaling REST API and use native PG auth.

Routes (4)
==========
GET   /api/whatif/scenarios    List available what-if scenarios
POST  /api/whatif/create       Create branch, run scenario, return diff
POST  /api/whatif/cleanup      Delete the active branch
GET   /api/whatif/status       Check whether a branch is currently active

Data sources
============
- Lakebase Autoscaling API    Branch lifecycle (create / endpoint / delete)
- Lakebase PostgreSQL          Scenario DML + KPI queries on branch & production

Related files
=============
- app/templates/whatif.html    Front-end for scenario selection and results
- app/shared.py                get_workspace_client, get_pool, log_error
- deployment/config.yaml       autoscaling_project_id, lakebase_type
"""

import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone, timedelta

import psycopg
from flask import Blueprint, jsonify, request

from shared import get_pool, get_workspace_client, log_error

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint creation
# ---------------------------------------------------------------------------

whatif_bp = Blueprint("whatif", __name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

# Autoscaling project ID — set via LAKEBASE_PROJECT_ID env var in app.yaml
LAKEBASE_PROJECT_ID = os.environ.get("LAKEBASE_PROJECT_ID", "")

# All what-if branches share this prefix. Cleanup is discovery-based (list the
# project's branches and delete everything with this prefix) rather than relying
# on the in-memory _active_whatif dict, which only tracks one branch and is lost
# when the Databricks App process recycles — orphaning branches at 1 CU each.
WHATIF_BRANCH_PREFIX = "what-if-"

# Tracks the currently active what-if branch (only one at a time)
_active_whatif = {}
_active_whatif_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

WHATIF_SCENARIOS = {
    "reassign_breached": {
        "name": "Reassign SLA-Breached to Top Performers",
        "description": (
            "Reassign all SLA-breached high-priority orders to the top 3 "
            "technicians per region by historical completion rate."
        ),
        "icon": "arrow-right-left",
    },
    "escalate_repairs": {
        "name": "Escalate Medium Repairs to High Priority",
        "description": (
            "Promote all medium-priority repair orders to high priority to "
            "see the SLA impact."
        ),
        "icon": "arrow-up",
    },
    "redistribute_load": {
        "name": "Redistribute Regional Workload",
        "description": (
            "Balance work orders evenly across available technicians to "
            "reduce hotspots."
        ),
        "icon": "scale",
    },
    "storm_surge": {
        "name": "Storm Surge Simulation",
        "description": (
            "Inject 50 emergency repair WOs in the Midwest to stress-test "
            "dispatch capacity and SLA compliance. Answers: can we handle "
            "a Category 3 storm with current staffing?"
        ),
        "icon": "cloud-lightning",
    },
}

# ---------------------------------------------------------------------------
# Branch connection helper
# ---------------------------------------------------------------------------


def _whatif_get_branch_conn(branch_id):
    """Connect to a specific branch on the Autoscaling project using native PG auth.

    Looks up the branch endpoint host via the Lakebase API, then opens a
    psycopg connection with the same PGUSER/PGPASSWORD used for production.
    Returns (connection, host_string).
    """
    wc = get_workspace_client()
    endpoints = wc.api_client.do(
        "GET",
        f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}/endpoints",
    )
    ep = endpoints["endpoints"][0]
    host = ep["status"]["hosts"].get("host")
    log.info(f"Autoscaling branch conn: host={host} branch={branch_id}")

    # Native PG auth — the SP role is inherited by forked branches
    pg_user = os.environ.get("PGUSER", "")
    pg_password = os.environ.get("PGPASSWORD", "")

    conn = psycopg.connect(
        host=host,
        port=5432,
        user=pg_user,
        password=pg_password,
        dbname="databricks_postgres",
        sslmode="require",
    )
    return conn, host


# ---------------------------------------------------------------------------
# Branch cleanup helpers (discovery-based)
# ---------------------------------------------------------------------------


def _whatif_list_branch_ids(wc):
    """Return all what-if branch IDs currently on the project.

    Discovery-based so cleanup works even after the app process recycled and
    lost the in-memory _active_whatif state. Never returns 'production'.
    """
    resp = wc.api_client.do(
        "GET", f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches"
    )
    ids = []
    for b in resp.get("branches", []):
        bid = b.get("branch_id") or ""
        if bid.startswith(WHATIF_BRANCH_PREFIX):
            ids.append(bid)
    return ids


def _whatif_delete_branch(wc, branch_id):
    """Delete a single branch, ignoring 'not found' (already gone). Returns True on success."""
    try:
        wc.api_client.do(
            "DELETE",
            f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}",
        )
        return True
    except Exception as e:
        if "not found" in str(e).lower() or "does not exist" in str(e).lower():
            return True
        log_error("whatif_delete_branch", e)
        return False


def _whatif_delete_all_branches(wc):
    """Delete every what-if branch on the project. Returns (deleted_ids, failed_ids)."""
    deleted, failed = [], []
    for bid in _whatif_list_branch_ids(wc):
        if _whatif_delete_branch(wc, bid):
            deleted.append(bid)
        else:
            failed.append(bid)
    return deleted, failed


# ---------------------------------------------------------------------------
# KPI query (runs against both production and branch connections)
# ---------------------------------------------------------------------------


def _whatif_query_kpis(conn):
    """Query KPI snapshot from a connection (works for both production and branch).

    Uses two focused queries instead of scanning the full 5M-row table.
    """
    with conn.cursor() as cur:
        # Active WO stats (uses partial index idx_wo_active)
        cur.execute("""
            SELECT
                COUNT(*) as open_orders,
                COUNT(*) FILTER (WHERE sla_due_at IS NOT NULL AND sla_due_at < CURRENT_TIMESTAMP) as sla_breached,
                COUNT(*) FILTER (WHERE sla_due_at IS NOT NULL AND sla_due_at < CURRENT_TIMESTAMP
                    AND assigned_technician_id IS NOT NULL) as breached_assigned,
                COUNT(*) FILTER (WHERE priority IN ('high','critical')
                    AND assigned_technician_id IS NULL) as unassigned_high
            FROM field_service.work_orders
            WHERE status NOT IN ('completed', 'cancelled')
        """)
        active = cur.fetchone()

        # SLA compliance from completed WOs (sample for speed on branches)
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE sla_met = true) as sla_met,
                   COUNT(*) as sla_total
            FROM field_service.work_orders
            WHERE status = 'completed' AND sla_met IS NOT NULL
              AND resolved_at >= CURRENT_TIMESTAMP - interval '30 days'
        """)
        sla = cur.fetchone()
        sla_met = sla[0] or 0
        sla_total = sla[1] or 0
        return {
            "open_orders": active[0] or 0,
            "sla_breached": active[1] or 0,
            "breached_assigned": active[2] or 0,
            "unassigned_high": active[3] or 0,
            "sla_met": sla_met,
            "sla_total": sla_total,
            "compliance_pct": round(sla_met * 100.0 / sla_total, 1) if sla_total > 0 else 0,
        }


# ---------------------------------------------------------------------------
# Scenario runner functions
# ---------------------------------------------------------------------------


def _whatif_run_reassign_breached(conn):
    """Run the 'reassign SLA-breached to top performers' scenario.

    Steps:
    1. Identify all open high/emergency orders past their SLA deadline.
    2. Find the top 3 technicians per region by completed-order count.
    3. Round-robin assign breached orders to those top performers.

    Returns (changes_list, update_count).
    """
    changes = []

    with conn.cursor() as cur:
        # Step 1: Capture before state (orders that will be reassigned)
        cur.execute("""
            SELECT wo.work_order_id, wo.work_order_number,
                   COALESCE(old_t.first_name || ' ' || old_t.last_name, 'Unassigned') as old_tech_name,
                   wo.assigned_technician_id as old_tech_id,
                   sr.region_name,
                   wo.priority,
                   ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - wo.sla_due_at)) / 3600, 1) as hours_breached
            FROM field_service.work_orders wo
            LEFT JOIN field_service.technicians old_t ON wo.assigned_technician_id = old_t.technician_id
            LEFT JOIN field_service.service_regions sr ON wo.region_id = sr.region_id
            WHERE wo.status NOT IN ('completed', 'cancelled')
              AND wo.priority IN ('high', 'critical')
              AND wo.sla_due_at IS NOT NULL
              AND wo.sla_due_at < CURRENT_TIMESTAMP
            ORDER BY wo.sla_due_at
        """)
        breached_orders = cur.fetchall()

        if not breached_orders:
            return [], 0

        # Step 2: Find top 3 techs per region by completed orders
        cur.execute("""
            WITH ranked AS (
                SELECT t.technician_id, t.region_id,
                       t.first_name || ' ' || t.last_name as tech_name,
                       COUNT(wo.work_order_id) FILTER (WHERE wo.status = 'completed') as completions,
                       ROW_NUMBER() OVER (
                           PARTITION BY t.region_id
                           ORDER BY COUNT(wo.work_order_id) FILTER (WHERE wo.status = 'completed') DESC
                       ) as rank
                FROM field_service.technicians t
                LEFT JOIN field_service.work_orders wo ON wo.assigned_technician_id = t.technician_id
                WHERE t.status IN ('available', 'on_site', 'en_route')
                GROUP BY t.technician_id, t.region_id, t.first_name, t.last_name
            )
            SELECT technician_id, region_id, tech_name, completions, rank
            FROM ranked WHERE rank <= 3
            ORDER BY region_id, rank
        """)
        top_techs = cur.fetchall()

        # Build region -> [tech1, tech2, tech3] mapping
        region_techs = {}
        for tid, rid, tname, completions, rank in top_techs:
            if rid not in region_techs:
                region_techs[rid] = []
            region_techs[rid].append({"id": tid, "name": tname, "completions": completions})

        # Step 3: Round-robin assign breached orders to top techs per region
        cur.execute("""
            SELECT wo.work_order_id, wo.region_id
            FROM field_service.work_orders wo
            WHERE wo.status NOT IN ('completed', 'cancelled')
              AND wo.priority IN ('high', 'critical')
              AND wo.sla_due_at IS NOT NULL
              AND wo.sla_due_at < CURRENT_TIMESTAMP
            ORDER BY wo.sla_due_at
        """)
        breached_with_region = cur.fetchall()

        region_counters = {}  # round-robin counter per region
        updated = 0

        for wo_id, region_id in breached_with_region:
            techs = region_techs.get(region_id, [])
            if not techs:
                continue
            idx = region_counters.get(region_id, 0)
            new_tech = techs[idx % len(techs)]
            region_counters[region_id] = idx + 1

            cur.execute("""
                UPDATE field_service.work_orders
                SET assigned_technician_id = %s, status = 'assigned', updated_at = CURRENT_TIMESTAMP
                WHERE work_order_id = %s
            """, (new_tech["id"], wo_id))
            updated += 1

            # Find the matching before-state row for the change log
            for bo in breached_orders:
                if bo[0] == wo_id:
                    changes.append({
                        "work_order_number": bo[1],
                        "old_tech": bo[2],
                        "new_tech": new_tech["name"],
                        "region": bo[4],
                        "priority": bo[5],
                        "hours_breached": float(bo[6]) if bo[6] else 0,
                    })
                    break

        conn.commit()

    return changes, updated


def _whatif_run_escalate_repairs(conn):
    """Escalate medium-priority repair orders to high.

    Targets only active (non-completed) medium-priority repairs, capped at
    500 to stay within branch disk quota.  Returns (changes_list, update_count).
    """
    with conn.cursor() as cur:
        # Identify affected WOs first (avoids RETURNING + trigger disk bloat)
        cur.execute("""
            SELECT work_order_id, work_order_number
            FROM field_service.work_orders
            WHERE status NOT IN ('completed', 'cancelled')
              AND priority = 'medium'
              AND category = 'repair'
            ORDER BY sla_due_at ASC NULLS LAST
            LIMIT 500
        """)
        targets = cur.fetchall()
        if not targets:
            return [], 0

        target_ids = [r[0] for r in targets]
        # Batch update — disable SLA trigger to avoid disk bloat on branch
        # Batch update to stay within branch disk quota
        cur.execute("""
            UPDATE field_service.work_orders
            SET priority = 'high', updated_at = CURRENT_TIMESTAMP
            WHERE work_order_id = ANY(%s)
        """, (target_ids,))
        conn.commit()

        changes = [{"work_order_number": r[1], "change": "medium -> high priority"} for r in targets]
    return changes, len(changes)


def _whatif_run_redistribute(conn):
    """Redistribute unassigned orders evenly across available techs.

    Round-robin assigns every unassigned open order to available/on_site
    technicians in the same region.  Returns (changes_list, update_count).
    """
    with conn.cursor() as cur:
        # Get available techs per region
        cur.execute("""
            SELECT t.technician_id, t.region_id, t.first_name || ' ' || t.last_name as name
            FROM field_service.technicians t
            WHERE t.status IN ('available', 'on_site')
            ORDER BY t.region_id, t.technician_id
        """)
        techs = cur.fetchall()
        region_techs = {}
        for tid, rid, name in techs:
            region_techs.setdefault(rid, []).append({"id": tid, "name": name})

        # Get unassigned orders
        cur.execute("""
            SELECT work_order_id, work_order_number, region_id
            FROM field_service.work_orders
            WHERE status NOT IN ('completed', 'cancelled')
              AND assigned_technician_id IS NULL
            ORDER BY region_id, created_at
        """)
        unassigned = cur.fetchall()

        region_counters = {}
        changes = []
        for wo_id, wo_num, rid in unassigned:
            avail = region_techs.get(rid, [])
            if not avail:
                continue
            idx = region_counters.get(rid, 0)
            tech = avail[idx % len(avail)]
            region_counters[rid] = idx + 1
            cur.execute("""
                UPDATE field_service.work_orders
                SET assigned_technician_id = %s, status = 'assigned', updated_at = CURRENT_TIMESTAMP
                WHERE work_order_id = %s
            """, (tech["id"], wo_id))
            changes.append({"work_order_number": wo_num, "new_tech": tech["name"]})

        conn.commit()
    return changes, len(changes)


def _whatif_query_capacity(conn):
    """Query per-region capacity utilization using pre-aggregated subqueries."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sr.region_name,
                   COALESCE(tc.total, 0) as tech_count,
                   COALESCE(tc.busy, 0) as techs_busy,
                   COALESCE(wo_agg.active_wos, 0) as active_wos,
                   COALESCE(wo_agg.unassigned_urgent, 0) as unassigned_urgent
            FROM field_service.service_regions sr
            LEFT JOIN (
                SELECT region_id,
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE status NOT IN ('available', 'off_duty')) as busy
                FROM field_service.technicians WHERE is_active GROUP BY region_id
            ) tc ON tc.region_id = sr.region_id
            LEFT JOIN (
                SELECT region_id,
                       COUNT(*) as active_wos,
                       COUNT(*) FILTER (WHERE assigned_technician_id IS NULL AND priority IN ('high', 'critical')) as unassigned_urgent
                FROM field_service.work_orders
                WHERE status NOT IN ('completed', 'cancelled')
                GROUP BY region_id
            ) wo_agg ON wo_agg.region_id = sr.region_id
            ORDER BY sr.region_name
        """)
        return [{
            "region": r[0],
            "techs": r[1] or 0,
            "techs_busy": r[2] or 0,
            "active_wos": r[3] or 0,
            "utilization_pct": round((r[3] or 0) / max((r[1] or 1) * 8, 1) * 100, 1),
            "unassigned_urgent": r[4] or 0,
        } for r in cur.fetchall()]


def _whatif_run_storm_surge(conn):
    """Simulate a Category 3 storm in the Midwest.

    Injects 200 emergency/high-priority repair WOs, then runs a simplified
    Smart Assign to show capacity gaps.  Returns (changes_list, update_count).
    """
    TARGET_REGION_ID = 5  # Midwest
    SURGE_COUNT = 50  # Keep small to stay within branch disk quota

    STORM_TITLES = [
        "Storm damage: aerial fiber span severed",
        "Storm damage: cell tower antenna displacement",
        "Power outage: backup generator failure",
        "Flooding: underground cabinet water ingress",
        "Wind damage: microwave dish misalignment",
        "Lightning strike: ONT equipment failure",
        "Storm damage: utility pole down — fiber at risk",
        "Ice accumulation: tower structural stress alert",
        "Power surge: OLT line card failure post-outage",
        "Flooding: remote terminal site inaccessible",
        "Storm damage: customer drop cable severed",
        "Wind damage: small cell mount failure",
    ]
    SUBCATEGORIES = ['no_service', 'equipment_failure', 'signal_degradation']

    changes = []

    with conn.cursor() as cur:
        # Get customer IDs from the target region
        cur.execute("""
            SELECT customer_id FROM field_service.customers
            WHERE region_id = %s
            ORDER BY RANDOM() LIMIT %s
        """, (TARGET_REGION_ID, SURGE_COUNT))
        customer_ids = [r[0] for r in cur.fetchall()]
        if not customer_ids:
            return [], 0

        # Get SLA info for high priority
        cur.execute("""
            SELECT sla_id, resolution_hours FROM field_service.sla_policies
            WHERE priority = 'high' LIMIT 1
        """)
        sla_row = cur.fetchone()
        sla_id = sla_row[0] if sla_row else 1
        sla_hours = sla_row[1] if sla_row else 8

        # Region center for GPS (Midwest = Chicago area)
        center_lat = 41.88
        center_lng = -87.63

        # Inject 200 storm WOs
        injected_ids = []
        for i in range(SURGE_COUNT):
            cust_id = customer_ids[i % len(customer_ids)]
            pri_roll = random.random()
            # Valid priorities: low, medium, high, critical
            priority = 'critical' if pri_roll < 0.55 else ('high' if pri_roll < 0.90 else 'medium')
            title = random.choice(STORM_TITLES)
            subcat = random.choice(SUBCATEGORIES)
            sla_due = datetime.now(timezone.utc) + timedelta(hours=random.uniform(2, sla_hours))
            lat = center_lat + random.uniform(-0.3, 0.3)
            lng = center_lng + random.uniform(-0.3, 0.3)
            wo_num = f"WO-STORM-{i+1:04d}"

            # INSERT without sla_due_at — the SLA trigger does heavy work
            # when sla_due_at is set, causing disk quota issues on branches.
            # We'll set sla_due_at in a lightweight batch UPDATE after all inserts.
            cur.execute("""
                INSERT INTO field_service.work_orders (
                    work_order_number, customer_id, category, subcategory, priority,
                    status, title, reported_issue,
                    sla_id, region_id, latitude, longitude
                ) VALUES (%s, %s, 'repair', %s, %s, 'open', %s, %s, %s, %s, %s, %s)
                RETURNING work_order_id
            """, (wo_num, cust_id, subcat, priority, title,
                  f"Storm-related: {title}. Customer reports complete service loss.",
                  sla_id, TARGET_REGION_ID, lat, lng))
            wo_id = cur.fetchone()[0]
            injected_ids.append((wo_id, wo_num, title, priority, sla_due))

            if (i + 1) % 10 == 0:
                conn.commit()

        conn.commit()

        # Now set sla_due_at on the injected WOs (triggers SLA risk calc but one row at a time)
        for wo_id, wo_num, title, priority, sla_due in injected_ids:
            cur.execute("""
                UPDATE field_service.work_orders
                SET sla_due_at = %s WHERE work_order_id = %s
            """, (sla_due, wo_id))
        conn.commit()

        # Run simplified Smart Assign on the branch
        # Get available techs in target region
        cur.execute("""
            SELECT t.technician_id, t.first_name || ' ' || t.last_name as name,
                   COUNT(wo.work_order_id) as active_count
            FROM field_service.technicians t
            LEFT JOIN field_service.work_orders wo
                ON wo.assigned_technician_id = t.technician_id
                AND wo.status NOT IN ('completed', 'cancelled')
            WHERE t.is_active AND t.region_id = %s
            GROUP BY t.technician_id, t.first_name, t.last_name
            HAVING COUNT(wo.work_order_id) < 8
            ORDER BY COUNT(wo.work_order_id) ASC, t.avg_rating DESC NULLS LAST
        """, (TARGET_REGION_ID,))
        available_techs = cur.fetchall()

        # Build capacity tracker
        tech_capacity = {r[0]: {"name": r[1], "current": r[2]} for r in available_techs}
        assigned_count = 0
        unassignable = 0

        for wo_id, wo_num, title, priority, _sla in injected_ids:
            # Find tech with lowest load under cap
            best_tech = None
            for tid, info in sorted(tech_capacity.items(), key=lambda x: x[1]["current"]):
                if info["current"] < 8:
                    best_tech = tid
                    break

            if best_tech:
                cur.execute("""
                    UPDATE field_service.work_orders
                    SET assigned_technician_id = %s, status = 'assigned', updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s
                """, (best_tech, wo_id))
                tech_capacity[best_tech]["current"] += 1
                assigned_count += 1
                changes.append({
                    "work_order_number": wo_num,
                    "title": title,
                    "priority": priority,
                    "assigned_to": tech_capacity[best_tech]["name"],
                    "status": "assigned",
                })
            else:
                unassignable += 1
                changes.append({
                    "work_order_number": wo_num,
                    "title": title,
                    "priority": priority,
                    "assigned_to": None,
                    "status": "unassignable",
                })

        conn.commit()

    return changes, assigned_count


# Maps scenario keys to their runner functions
SCENARIO_RUNNERS = {
    "reassign_breached": _whatif_run_reassign_breached,
    "escalate_repairs": _whatif_run_escalate_repairs,
    "redistribute_load": _whatif_run_redistribute,
    "storm_surge": _whatif_run_storm_surge,
}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# ── List available scenarios ──────────────────────────────────────────────

@whatif_bp.route("/api/whatif/scenarios")
def whatif_scenarios():
    return jsonify(WHATIF_SCENARIOS)


# ── Create branch, run scenario (async) ─────────────────────────────────

def _whatif_run_async(scenario_key):
    """Background thread: create branch, run scenario, collect KPIs."""
    wc = get_workspace_client()
    branch_id = f"{WHATIF_BRANCH_PREFIX}{int(time.time())}"

    try:
        t0 = time.time()
        with _active_whatif_lock:
            _active_whatif["phase"] = "creating_branch"
            _active_whatif["branch_id"] = branch_id
            _active_whatif["scenario"] = scenario_key

        # Step 0: Sweep any leftover what-if branches from prior runs so they
        # don't accumulate (each holds a 1 CU endpoint). Best-effort — failures
        # here must not block the new scenario.
        try:
            swept, _ = _whatif_delete_all_branches(wc)
            if swept:
                log.info(f"Swept {len(swept)} stale what-if branch(es): {swept}")
        except Exception as sweep_err:
            log.warning(f"Pre-run branch sweep failed (continuing): {sweep_err}")

        # Step 1: Create branch
        wc.api_client.do(
            "POST",
            f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches?branch_id={branch_id}",
            body={"spec": {
                "source_branch": f"projects/{LAKEBASE_PROJECT_ID}/branches/production",
                "ttl": "3600s",
            }},
        )

        # Step 2: Wait for READY
        for _ in range(30):
            br = wc.api_client.do(
                "GET",
                f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}",
            )
            if br.get("status", {}).get("current_state") == "READY":
                break
            time.sleep(0.5)
        branch_time_ms = round((time.time() - t0) * 1000)

        # Step 3: Create endpoint and wait for ACTIVE
        with _active_whatif_lock:
            _active_whatif["phase"] = "provisioning_endpoint"

        try:
            wc.api_client.do(
                "POST",
                f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}/endpoints?endpoint_id=primary",
                body={"spec": {"endpoint_type": "ENDPOINT_TYPE_READ_WRITE"}},
            )
        except Exception as ep_err:
            if "already exists" not in str(ep_err).lower():
                raise

        ep_host = None
        for _ in range(90):
            eps = wc.api_client.do(
                "GET",
                f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}/endpoints",
            )
            ep_list = eps.get("endpoints", [])
            if ep_list:
                ep_state = ep_list[0].get("status", {}).get("current_state", "")
                if ep_state == "ACTIVE":
                    ep_host = (
                        ep_list[0]["status"]["hosts"].get("read_write_pooled_host")
                        or ep_list[0]["status"]["hosts"]["host"]
                    )
                    break
            time.sleep(1)

        if not ep_host:
            with _active_whatif_lock:
                _active_whatif["phase"] = "error"
                _active_whatif["error"] = "Branch endpoint did not become active"
            return

        endpoint_time_ms = round((time.time() - t0) * 1000)

        # Step 4: Run scenario
        with _active_whatif_lock:
            _active_whatif["phase"] = "running_scenario"
            _active_whatif["branch_host"] = ep_host

        branch_conn, branch_host = _whatif_get_branch_conn(branch_id)
        runner = SCENARIO_RUNNERS[scenario_key]
        changes, update_count = runner(branch_conn)

        # Step 5: Query KPIs
        with _active_whatif_lock:
            _active_whatif["phase"] = "comparing_results"

        after_kpis = _whatif_query_kpis(branch_conn)
        capacity_after = _whatif_query_capacity(branch_conn)
        branch_conn.close()

        prod_conn, _ = _whatif_get_branch_conn("production")
        before_kpis = _whatif_query_kpis(prod_conn)
        capacity_before = _whatif_query_capacity(prod_conn)
        prod_conn.close()

        total_time_ms = round((time.time() - t0) * 1000)

        # Build surge summary
        surge_summary = None
        if scenario_key == "storm_surge":
            assigned_storm = sum(1 for c in changes if c.get("status") == "assigned")
            unassignable_storm = sum(1 for c in changes if c.get("status") == "unassignable")
            mw_after = next((r for r in capacity_after if r["region"] == "Midwest"), {})
            mw_before = next((r for r in capacity_before if r["region"] == "Midwest"), {})
            needed_techs = max(0, (unassignable_storm + 7) // 8)
            surge_summary = {
                "injected": len(changes),
                "assigned": assigned_storm,
                "unassignable": unassignable_storm,
                "target_region": "Midwest",
                "capacity_gap_techs": needed_techs,
                "before_utilization": mw_before.get("utilization_pct", 0),
                "after_utilization": mw_after.get("utilization_pct", 0),
            }

        # Store results
        with _active_whatif_lock:
            _active_whatif["phase"] = "complete"
            _active_whatif["created_at"] = datetime.now(timezone.utc).isoformat()
            _active_whatif["result"] = {
                "branch_id": branch_id,
                "branch_host": branch_host,
                "branch_time_ms": branch_time_ms,
                "endpoint_time_ms": endpoint_time_ms,
                "total_time_ms": total_time_ms,
                "scenario": WHATIF_SCENARIOS[scenario_key],
                "before": before_kpis,
                "after": after_kpis,
                "capacity_before": capacity_before,
                "capacity_after": capacity_after,
                "surge_summary": surge_summary,
                "changes": changes[:100],
                "changes_total": update_count,
            }

    except Exception as e:
        log_error("whatif_async", e)
        with _active_whatif_lock:
            _active_whatif["phase"] = "error"
            _active_whatif["error"] = str(e)
        try:
            wc.api_client.do(
                "DELETE",
                f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}",
            )
        except Exception:
            pass


@whatif_bp.route("/api/whatif/create", methods=["POST"])
def whatif_create():
    """Start a what-if scenario in a background thread. Returns immediately.

    Poll /api/whatif/status for progress and results.
    """
    if not LAKEBASE_PROJECT_ID:
        return jsonify({"error": "LAKEBASE_PROJECT_ID not configured"}), 503

    data = request.get_json() or {}
    scenario_key = data.get("scenario", "reassign_breached")
    if scenario_key not in WHATIF_SCENARIOS:
        return jsonify({"error": f"Unknown scenario: {scenario_key}"}), 400

    with _active_whatif_lock:
        if _active_whatif.get("phase") in ("creating_branch", "provisioning_endpoint", "running_scenario", "comparing_results"):
            return jsonify({"error": "A scenario is already running"}), 409
        _active_whatif.clear()
        _active_whatif["phase"] = "starting"
        _active_whatif["result"] = None
        _active_whatif["error"] = None

    t = threading.Thread(target=_whatif_run_async, args=(scenario_key,), daemon=True)
    t.start()

    return jsonify({"status": "started", "scenario": scenario_key})


# ── Delete the active what-if branch ─────────────────────────────────────

@whatif_bp.route("/api/whatif/cleanup", methods=["POST"])
def whatif_cleanup():
    """Delete ALL what-if branches on the project (discovery-based).

    Does not rely on the in-memory _active_whatif dict — it lists the project's
    branches and deletes every one with the what-if prefix. This cleans up
    orphans left behind by earlier runs or app process restarts.
    """
    if not LAKEBASE_PROJECT_ID:
        return jsonify({"error": "Not configured"}), 503

    wc = get_workspace_client()
    try:
        deleted, failed = _whatif_delete_all_branches(wc)
    except Exception as e:
        log_error("whatif_cleanup", e)
        return jsonify({"error": str(e)}), 500

    with _active_whatif_lock:
        _active_whatif.clear()

    if failed:
        return jsonify({
            "message": f"Deleted {len(deleted)} branch(es), {len(failed)} failed",
            "deleted": deleted,
            "failed": failed,
        }), 500
    if not deleted:
        return jsonify({"message": "No active branches to clean up", "deleted": []})
    return jsonify({
        "message": f"Deleted {len(deleted)} branch(es)",
        "deleted": deleted,
    })


# ── Check active branch status ───────────────────────────────────────────

@whatif_bp.route("/api/whatif/status")
def whatif_status():
    with _active_whatif_lock:
        phase = _active_whatif.get("phase", "idle")
        resp = {
            "active": phase not in ("idle", "complete", "error", None),
            "phase": phase,
            "branch_id": _active_whatif.get("branch_id"),
            "branch_host": _active_whatif.get("branch_host"),
            "scenario": _active_whatif.get("scenario"),
            "created_at": _active_whatif.get("created_at"),
            "error": _active_whatif.get("error"),
        }
        if phase == "complete":
            resp["result"] = _active_whatif.get("result")
        return jsonify(resp)


# ═══════════════════════════════════════════════════════════════════════════
# INSTANT UNDO — point-in-time "time travel" demo (all on branches, never prod)
# ═══════════════════════════════════════════════════════════════════════════
#
# Flow: branch production (W) → delete a chunk on W → create a recovery branch (R)
# from W using spec.source_branch_time set to just BEFORE the delete → the rows
# are back. Proves Lakebase can rewind a database to any point in time. Production
# is never touched (all work happens on short-TTL branches).

_undo_state = {}
_undo_lock = threading.Lock()


def _provision_branch(wc, branch_id, source_branch, source_branch_time=None, ttl="3600s", no_expiry=False):
    """Create a branch (optionally at a point in time), wait READY, add a R/W endpoint,
    wait ACTIVE, and return the connectable host. Raises on failure.

    Pass no_expiry=True for a branch that will itself be a PARENT — Lakebase rejects
    child branches under a branch that has an expiration date.
    """
    spec = {"source_branch": source_branch}
    if no_expiry:
        spec["no_expiry"] = True
    else:
        spec["ttl"] = ttl
    if source_branch_time:
        spec["source_branch_time"] = source_branch_time
    wc.api_client.do(
        "POST",
        f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches?branch_id={branch_id}",
        body={"spec": spec},
    )
    for _ in range(30):
        br = wc.api_client.do(
            "GET", f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}"
        )
        if br.get("status", {}).get("current_state") == "READY":
            break
        time.sleep(0.5)
    try:
        wc.api_client.do(
            "POST",
            f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}/endpoints?endpoint_id=primary",
            body={"spec": {"endpoint_type": "ENDPOINT_TYPE_READ_WRITE"}},
        )
    except Exception as ep_err:
        if "already exists" not in str(ep_err).lower():
            raise
    for _ in range(90):
        eps = wc.api_client.do(
            "GET", f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}/endpoints"
        )
        ep_list = eps.get("endpoints", [])
        if ep_list and ep_list[0].get("status", {}).get("current_state") == "ACTIVE":
            hosts = ep_list[0]["status"]["hosts"]
            return hosts.get("read_write_pooled_host") or hosts["host"]
        time.sleep(1)
    raise RuntimeError(f"Endpoint for branch {branch_id} did not become active")


def _undo_count_active(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM field_service.work_orders "
            "WHERE status NOT IN ('completed', 'cancelled')"
        )
        return cur.fetchone()[0]


def _undo_run_async(chunk):
    """Background worker for the Instant Undo demo.

    Branches are created with a 60-min TTL and are NOT auto-deleted, so they remain
    visible in the Databricks UI (Lakebase auto-expires them at the TTL; the manual
    "Clean up" button removes them sooner). Every real API call and SQL statement is
    recorded into state['steps'] for the live "under the hood" console. Recovery uses
    a point-in-time branch from production (source_branch_time), which keeps both
    branches TTL'd (no no_expiry parent) and never touches production.
    """
    wc = get_workspace_client()
    ts = int(time.time())
    w_branch = f"{WHATIF_BRANCH_PREFIX}undo-{ts}"
    r_branch = f"{WHATIF_BRANCH_PREFIX}recovery-{ts}"
    src = f"projects/{LAKEBASE_PROJECT_ID}/branches/production"

    def phase(p, **extra):
        with _undo_lock:
            _undo_state["phase"] = p
            _undo_state.update(extra)

    def step(kind, label, code=None):
        with _undo_lock:
            _undo_state.setdefault("steps", []).append(
                {"kind": kind, "label": label, "code": code, "t": time.strftime("%H:%M:%S")}
            )

    try:
        phase("branching", w_branch=w_branch)
        step("api", "Fork production into a working branch (copy-on-write; no-expiry so it can parent the recovery branch)",
             f'POST /api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches?branch_id={w_branch}\n'
             '{"spec": {"source_branch": ".../branches/production", "no_expiry": true}}')
        _provision_branch(wc, w_branch, src, no_expiry=True)
        step("result", f"Working branch ready: {w_branch}")

        # The branch clone is instant (copy-on-write). Connecting requires the branch's
        # compute endpoint to activate first — that spin-up is the real visible wait, so
        # surface it as its own phase instead of leaving it under "Forking".
        phase("activating")
        step("api", "Activate the branch's compute endpoint, then connect (the clone is instant; endpoint spin-up is the wait)",
             f"GET /api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{w_branch}/endpoints")
        conn, _ = _whatif_get_branch_conn(w_branch)
        try:
            baseline = _undo_count_active(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM field_service.work_orders")
                total_rows = cur.fetchone()[0]
            step("sql", f"Count open work orders on the branch → {baseline}",
                 "SELECT COUNT(*) FROM field_service.work_orders\nWHERE status NOT IN ('completed','cancelled');")
            with conn.cursor() as cur:
                cur.execute("SELECT (CURRENT_TIMESTAMP)::timestamptz")
                t0_dt = cur.fetchone()[0]
            t0 = t0_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            step("result", f"Captured recovery point T0 = {t0}")
            time.sleep(2)  # WAL separation between T0 and the delete

            phase("deleting", baseline=baseline)
            n = min(int(chunk), baseline)
            # work_orders has FK children (appointments, work_order_parts, work_order_notes,
            # and tier2/3 tables). Deleting a referenced work order trips
            # appointments_work_order_id_fkey etc. On this throwaway branch we capture the
            # target set, auto-discover every table with an FK to work_orders, clear those
            # rows first, then delete the work_orders themselves. Auto-discovery keeps this
            # correct if child tables are added later.
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE _undo_targets AS "
                    "SELECT work_order_id FROM field_service.work_orders "
                    "WHERE status NOT IN ('completed','cancelled') ORDER BY work_order_id LIMIT %s",
                    (n,),
                )
                cur.execute(
                    "SELECT con.conrelid::regclass::text, att.attname "
                    "FROM pg_constraint con "
                    "JOIN pg_attribute att ON att.attrelid = con.conrelid "
                    "  AND att.attnum = ANY(con.conkey) "
                    "WHERE con.contype = 'f' "
                    "  AND con.confrelid = 'field_service.work_orders'::regclass"
                )
                children = cur.fetchall()
                child_lines = "\n".join(
                    f"DELETE FROM {c} WHERE {col} IN (SELECT work_order_id FROM _undo_targets);"
                    for c, col in children
                )
                del_sql = (
                    "CREATE TEMP TABLE _undo_targets AS\n"
                    "  SELECT work_order_id FROM field_service.work_orders\n"
                    "  WHERE status NOT IN ('completed','cancelled')\n"
                    f"  ORDER BY work_order_id LIMIT {n};\n"
                    f"{child_lines}\n"
                    "DELETE FROM field_service.work_orders\n"
                    "  WHERE work_order_id IN (SELECT work_order_id FROM _undo_targets);"
                )
                step("sql", "Delete every open work order on the branch (clears FK children first, destructive)", del_sql)
                for child_tbl, fk_col in children:
                    cur.execute(
                        f'DELETE FROM {child_tbl} WHERE "{fk_col}" IN '
                        "(SELECT work_order_id FROM _undo_targets)"
                    )
                cur.execute(
                    "DELETE FROM field_service.work_orders "
                    "WHERE work_order_id IN (SELECT work_order_id FROM _undo_targets)"
                )
                deleted = cur.rowcount
                cur.execute("DROP TABLE _undo_targets")
            conn.commit()
            after_delete = _undo_count_active(conn)
            step("result", f"Deleted {deleted} rows on the branch → {after_delete} open orders remain")
        finally:
            conn.close()

        # Recover: point-in-time branch from PRODUCTION as of T0 (before the delete).
        phase("recovering", t0=t0, deleted=deleted, after_delete=after_delete)
        step("api", "Create a point-in-time recovery branch from the working branch, as of T0 (before the delete)",
             f'POST /api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches?branch_id={r_branch}\n'
             f'{{"spec": {{"source_branch": ".../branches/{w_branch}", "source_branch_time": "{t0}", "ttl": "3600s"}}}}')
        _provision_branch(wc, r_branch, f"projects/{LAKEBASE_PROJECT_ID}/branches/{w_branch}", source_branch_time=t0)
        rconn, _ = _whatif_get_branch_conn(r_branch)
        try:
            recovered = _undo_count_active(rconn)
        finally:
            rconn.close()
        step("sql", f"Count open work orders on the recovery branch → {recovered}",
             "SELECT COUNT(*) FROM field_service.work_orders\nWHERE status NOT IN ('completed','cancelled');")
        step("result", f"Recovered {recovered} orders — the database exactly as it was at T0. Production never touched.")

        with _undo_lock:
            _undo_state["phase"] = "complete"
            _undo_state["result"] = {
                "baseline": baseline,
                "deleted": deleted,
                "after_delete": after_delete,
                "recovered": recovered,
                "restored_ok": recovered == baseline,
                "total_rows": total_rows,
                "t0": t0,
                "w_branch": w_branch,
                "r_branch": r_branch,
            }
    except Exception as e:
        log_error("undo_run", e)
        step("result", f"Error: {e}")
        with _undo_lock:
            _undo_state["phase"] = "error"
            _undo_state["error"] = str(e)
    # No auto-cleanup: branches persist (visible in Databricks) and Lakebase
    # auto-expires them at their 60-min TTL. Manual cleanup via the "Clean up" button.


@whatif_bp.route("/api/lakebase/undo/start", methods=["POST"])
def undo_start():
    if not LAKEBASE_PROJECT_ID:
        return jsonify({"error": "LAKEBASE_PROJECT_ID not configured"}), 503
    data = request.get_json(silent=True) or {}
    try:
        chunk = max(1, min(500000, int(data.get("chunk", 50000))))
    except (TypeError, ValueError):
        chunk = 50000
    with _undo_lock:
        if _undo_state.get("phase") in ("branching", "activating", "deleting", "recovering"):
            return jsonify({"error": "An undo demo is already running"}), 409
        _undo_state.clear()
        _undo_state["phase"] = "branching"
        _undo_state["steps"] = []
    threading.Thread(target=_undo_run_async, args=(chunk,), daemon=True).start()
    return jsonify({"status": "started", "chunk": chunk})


@whatif_bp.route("/api/lakebase/undo/status")
def undo_status():
    with _undo_lock:
        phase = _undo_state.get("phase", "idle")
        resp = {
            "active": phase in ("branching", "activating", "deleting", "recovering"),
            "phase": phase,
            "baseline": _undo_state.get("baseline"),
            "deleted": _undo_state.get("deleted"),
            "after_delete": _undo_state.get("after_delete"),
            "t0": _undo_state.get("t0"),
            "steps": _undo_state.get("steps", []),
            "error": _undo_state.get("error"),
        }
        if phase == "complete":
            resp["result"] = _undo_state.get("result")
        return jsonify(resp)


# ═══════════════════════════════════════════════════════════════════════════
# PARALLEL UNIVERSES — many branches at once, each a different scenario
# ═══════════════════════════════════════════════════════════════════════════
#
# Forks production into several isolated branches simultaneously, runs a
# different what-if scenario on each, and compares every outcome against the
# live production baseline. Every branch is torn down afterward (each holds a
# ~1 CU endpoint), so cleanup is deliberate and discovery-based.

_pu_state = {}
_pu_lock = threading.Lock()
_PU_SCENARIOS = ["reassign_breached", "escalate_repairs", "storm_surge"]


def _pu_step(kind, label, code=None):
    with _pu_lock:
        _pu_state.setdefault("steps", []).append(
            {"kind": kind, "label": label, "code": code, "t": time.strftime("%H:%M:%S")}
        )


def _pu_run_one(wc, scenario_key, idx, results):
    name = WHATIF_SCENARIOS[scenario_key]["name"]
    branch_id = f"{WHATIF_BRANCH_PREFIX}pu{idx}-{int(time.time())}"
    with _pu_lock:
        results[scenario_key] = {
            "scenario": scenario_key, "name": name, "branch_id": branch_id, "state": "provisioning",
        }
    _pu_step("api", f"[{name}] Fork production (copy-on-write) → {branch_id}",
             f'POST /api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches?branch_id={branch_id}\n'
             '{"spec": {"source_branch": ".../branches/production", "ttl": "3600s"}}')
    try:
        _provision_branch(wc, branch_id, f"projects/{LAKEBASE_PROJECT_ID}/branches/production")
        _pu_step("result", f"[{name}] Branch created instantly (copy-on-write)")
        # The clone is instant; connecting waits for the branch's compute endpoint to
        # activate (~tens of seconds). Surface it as its own step so the console isn't
        # silent — and so "Run scenario" isn't shown while we're actually still connecting
        # (matches the Instant Undo treatment).
        _pu_step("api", f"[{name}] Activating the branch compute endpoint, then connecting…",
                 f"GET /api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}/endpoints")
        conn, _ = _whatif_get_branch_conn(branch_id)
        with _pu_lock:
            results[scenario_key]["state"] = "running"
        _pu_step("sql", f"[{name}] Endpoint active — run scenario on the branch", WHATIF_SCENARIOS[scenario_key]["description"])
        try:
            changes, update_count = SCENARIO_RUNNERS[scenario_key](conn)
            kpis = _whatif_query_kpis(conn)
        finally:
            conn.close()
        with _pu_lock:
            results[scenario_key].update({"state": "done", "kpis": kpis, "changes_total": update_count})
        _pu_step("result", f"[{name}] {update_count} rows changed on the branch; KPIs computed")
    except Exception as e:  # noqa: BLE001
        log_error("pu_run_one", e)
        with _pu_lock:
            results[scenario_key].update({"state": "error", "error": str(e)})
        _pu_step("result", f"[{name}] Error: {e}")
    # No auto-delete: each branch persists (visible in Databricks), auto-expires at
    # its 60-min TTL, and can be removed sooner via the "Clean up" button.


def _pu_run_async():
    wc = get_workspace_client()
    results = {}
    try:
        # Production baseline for comparison (no pre-run sweep — prior branches persist).
        pconn, _ = _whatif_get_branch_conn("production")
        try:
            baseline = _whatif_query_kpis(pconn)
        finally:
            pconn.close()
        _pu_step("sql", "Read live production baseline KPIs",
                 "SELECT COUNT(*) FILTER (...) AS open_orders, ... FROM field_service.work_orders;")

        with _pu_lock:
            _pu_state["phase"] = "running"
            _pu_state["baseline"] = baseline
            _pu_state["branches"] = results

        threads = []
        for i, sk in enumerate(_PU_SCENARIOS):
            t = threading.Thread(target=_pu_run_one, args=(wc, sk, i, results), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        _pu_step("result", "All branches complete and left running (visible in Databricks; TTL 60 min)")
        with _pu_lock:
            _pu_state["phase"] = "complete"
    except Exception as e:  # noqa: BLE001
        log_error("pu_run", e)
        with _pu_lock:
            _pu_state["phase"] = "error"
            _pu_state["error"] = str(e)


@whatif_bp.route("/api/lakebase/universes/start", methods=["POST"])
def universes_start():
    if not LAKEBASE_PROJECT_ID:
        return jsonify({"error": "LAKEBASE_PROJECT_ID not configured"}), 503
    with _pu_lock:
        if _pu_state.get("phase") == "running":
            return jsonify({"error": "Parallel Universes demo already running"}), 409
        _pu_state.clear()
        _pu_state["phase"] = "running"
        _pu_state["branches"] = {}
        _pu_state["steps"] = []
    threading.Thread(target=_pu_run_async, daemon=True).start()
    return jsonify({"status": "started", "count": len(_PU_SCENARIOS)})


@whatif_bp.route("/api/lakebase/universes/status")
def universes_status():
    with _pu_lock:
        phase = _pu_state.get("phase", "idle")
        return jsonify({
            "active": phase == "running",
            "phase": phase,
            "baseline": _pu_state.get("baseline"),
            "branches": list(_pu_state.get("branches", {}).values()),
            "steps": _pu_state.get("steps", []),
            "error": _pu_state.get("error"),
        })

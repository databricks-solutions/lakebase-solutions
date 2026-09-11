"""
Fleet Health blueprint — Fleet Vehicle Predictive Maintenance.

Purpose
=======
Serves the Fleet Health dashboard, which surfaces predictive-maintenance
signals for the field-service vehicle fleet.  The OPERATIONAL views read the
Lakebase PostgreSQL pool (always populated).  ONE ANALYTICAL view —
reliability-by-model — reads the Managed Iceberg gold medallion via the SQL
warehouse (and degrades gracefully if the gold tables are not built yet).

The marquee demo visual is the "Diagnostic Trouble Codes — AI Interpreted"
panel: raw OBD-II DTC codes are interpreted by an AI step that grades severity
and flags false positives (e.g. a P0455 loose fuel cap that does not warrant a
truck roll).  Those AI columns may be NULL before the AI step runs, so every
endpoint degrades gracefully ("Pending AI review").

Routes (6)
==========
GET  /api/fleet/summary             KPI cards + per-region rollup + DTC false-alarm rate
GET  /api/fleet/vehicles            Filterable, paginated vehicle list (risk-ordered)
GET  /api/fleet/vehicle/<id>        Vehicle detail: telemetry trend, DTCs, history, WOs
GET  /api/fleet/dtc-codes           Active fleet DTC codes with AI interpretation
GET  /api/fleet/map                 Vehicles with lat/long + risk for a map view
GET  /api/fleet/reliability-by-model  Make/model reliability from Iceberg gold (SQL warehouse)

RBAC
====
Region filtering mirrors dispatch.py / assets.py: non-admins are restricted to
their assigned region IDs via ``get_user_role_and_regions()``; admins (None)
see all regions.

Data sources
============
- Lakebase PostgreSQL (interactive pool)    fleet_vehicles, vehicle_telemetry,
    vehicle_dtc_codes, vehicle_maintenance_history, technicians, service_regions,
    v_fleet_health_summary, work_orders
- SQL Warehouse over Managed Iceberg        gold_vehicle_health  (reliability-by-model only)

Related files
=============
- app/templates/fleet.html          Fleet health UI
- app/shared.py                      get_pool, get_user_role_and_regions, log_error
- data/fleet_management.sql          Table definitions and seed data
"""

import logging
import os
import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, jsonify, request

from shared import _run_sql, get_pool, get_user_role_and_regions, log_error

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint creation
# ---------------------------------------------------------------------------

fleet_bp = Blueprint("fleet", __name__)

# ---------------------------------------------------------------------------
# Parts / cost reference for the remediation drill-down
# ---------------------------------------------------------------------------
# Maps an OBD-II fault code to the vehicle parts a service would need, with
# illustrative real-world unit costs. Powers "what parts do I order, what do they
# cost, what's the chargeback to my team" in the remediation preview.
_DTC_PARTS = {
    "P0217": [("Coolant thermostat", 1, 45.0), ("Water pump", 1, 185.0), ("Coolant flush kit", 1, 35.0)],
    "P0128": [("Coolant thermostat", 1, 45.0)],
    "P0606": [("Engine control module (ECM)", 1, 620.0)],
    "P0562": [("Battery (AGM)", 1, 210.0), ("Alternator", 1, 340.0)],
    "B1318": [("Battery (AGM)", 1, 210.0)],
    "P0521": [("Oil pressure sensor", 1, 60.0), ("Oil & filter kit", 1, 55.0)],
    "P0300": [("Spark plug set", 1, 90.0), ("Ignition coil pack", 1, 240.0)],
    "P0420": [("Catalytic converter", 1, 720.0)],
    "C0035": [("Wheel speed sensor", 1, 85.0)],
    "P0455": [("Fuel cap", 1, 25.0)],
    "P0457": [("Fuel cap", 1, 25.0)],
}
_DEFAULT_SERVICE_PARTS = [("Oil & filter kit", 1, 55.0)]
_LABOR_RATE = 120.0  # USD/hr (shop rate)


def _recommend_parts(dtcs):
    """dtcs: list of (code, desc, severity, is_false_positive). -> (parts, parts_total).

    Genuine codes only (skip AI-flagged false positives). Dedupes by part name.
    """
    chosen = {}
    for code, _desc, _sev, is_fp in dtcs:
        if is_fp:
            continue
        for name, qty, cost in _DTC_PARTS.get(code, []):
            if name not in chosen or qty > chosen[name][0]:
                chosen[name] = (qty, cost)
    if not chosen:
        for name, qty, cost in _DEFAULT_SERVICE_PARTS:
            chosen[name] = (qty, cost)
    parts = [{"part": n, "qty": q, "unit_cost": c, "line_total": round(q * c, 2)}
             for n, (q, c) in chosen.items()]
    parts.sort(key=lambda p: -p["line_total"])
    return parts, round(sum(p["line_total"] for p in parts), 2)


def _labor_estimate(dtcs):
    """Rough labor hours/cost from the count of genuine (non-false-positive) codes."""
    genuine = sum(1 for d in dtcs if not d[3]) or 1
    hours = round(max(1.0, genuine * 1.5), 1)
    return hours, round(hours * _LABOR_RATE, 2)


@fleet_bp.route("/api/fleet/reliability-by-model")
def fleet_reliability_by_model():
    """Vehicle-model reliability analytics from the Managed Iceberg gold medallion.

    Aggregates ``gold_vehicle_health`` (the Bronze->Silver->Gold telemetry pipeline)
    by make/model via the SQL warehouse — the asset-reliability roll-up a fleet
    manager uses to drive replacement and purchasing decisions (which models break
    down most). This is the one Fleet view served from the lakehouse; the
    operational sections read Lakebase. Degrades gracefully if the gold tables
    have not been built yet (pipeline not run).
    """
    try:
        catalog = os.environ.get("PIPELINE_CATALOG", "dba-lakebase-network")
        rows = _run_sql(f"""
            SELECT make, model,
                   COUNT(*) AS vehicles,
                   ROUND(AVG(avg_health_score), 1) AS avg_health,
                   ROUND(100.0 * SUM(CASE WHEN risk_category IN ('HIGH','CRITICAL') THEN 1 ELSE 0 END)
                         / COUNT(*), 1) AS at_risk_pct,
                   ROUND(AVG(odometer_km), 0) AS avg_odometer_km,
                   ROUND(AVG(latest_engine_temp_c), 1) AS avg_engine_temp_c,
                   SUM(needs_maintenance) AS needs_maintenance
            FROM `{catalog}`.network_data.gold_vehicle_health
            GROUP BY make, model
            ORDER BY at_risk_pct DESC, vehicles DESC
        """, catalog=catalog)
        models = [{
            "make": r[0], "model": r[1],
            "vehicles": int(r[2]) if r[2] is not None else 0,
            "avg_health": float(r[3]) if r[3] is not None else None,
            "at_risk_pct": float(r[4]) if r[4] is not None else 0.0,
            "avg_odometer_km": float(r[5]) if r[5] is not None else None,
            "avg_engine_temp_c": float(r[6]) if r[6] is not None else None,
            "needs_maintenance": int(r[7]) if r[7] is not None else 0,
        } for r in rows]
        return jsonify({"models": models, "available": True, "source": "iceberg"})
    except Exception as e:
        # Gold tables may not exist yet (pipeline hasn't run) — degrade gracefully.
        log_error("fleet_reliability_by_model", e)
        return jsonify({"models": [], "available": False, "error": str(e)[:200]})


# ===========================================================================
# Fuel + external-provider cost data — consolidated off Sheets/AppSheet
# ===========================================================================
# These read v_vehicle_cost_summary (fuel-card spend + own/external maintenance
# joined per vehicle). The marquee signal is FUEL-EFFICIENCY DECLINE as a leading
# mechanical-failure indicator — surfaced before the telematics risk model flags it.

@fleet_bp.route("/api/fleet/cost-summary")
def fleet_cost_summary():
    """Fleet-wide fuel + maintenance economics from v_vehicle_cost_summary.

    Returns fleet totals (60-day fuel spend, trailing-year maintenance, annualized
    running cost), a per-region rollup, the count of vehicles with a fuel-efficiency
    anomaly, and the costliest vehicles to run. RBAC: non-admins see only their regions.
    """
    try:
        _, user_regions = get_user_role_and_regions()
        where = ["v.status = 'active'"]
        params: list = []
        if user_regions:
            where.append("cs.region_id = ANY(%s)")
            params.append(user_regions)
        wc = " AND ".join(where)
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT
                        COUNT(*),
                        ROUND(SUM(cs.fuel_cost_60d), 2),
                        ROUND(SUM(cs.maint_cost_365d), 2),
                        ROUND(SUM(cs.annual_running_cost), 0),
                        COUNT(*) FILTER (WHERE cs.fuel_anomaly),
                        ROUND(AVG(cs.efficiency_kmpl) FILTER (WHERE cs.efficiency_kmpl IS NOT NULL), 2),
                        ROUND(AVG(cs.fuel_cost_per_km) FILTER (WHERE cs.fuel_cost_per_km IS NOT NULL), 3)
                    FROM field_service.v_vehicle_cost_summary cs
                    JOIN field_service.fleet_vehicles v ON v.vehicle_id = cs.vehicle_id
                    WHERE {wc}
                """, params)
                t = cur.fetchone()
                # Per-region rollup
                cur.execute(f"""
                    SELECT sr.region_code,
                           ROUND(SUM(cs.fuel_cost_60d), 0)        AS fuel_60d,
                           ROUND(SUM(cs.annual_running_cost), 0)  AS annual_run,
                           COUNT(*) FILTER (WHERE cs.fuel_anomaly) AS anomalies,
                           ROUND(AVG(cs.efficiency_kmpl) FILTER (WHERE cs.efficiency_kmpl IS NOT NULL), 2) AS avg_kmpl
                    FROM field_service.v_vehicle_cost_summary cs
                    JOIN field_service.fleet_vehicles v ON v.vehicle_id = cs.vehicle_id
                    LEFT JOIN field_service.service_regions sr ON sr.region_id = cs.region_id
                    WHERE {wc}
                    GROUP BY sr.region_code
                    ORDER BY annual_run DESC NULLS LAST
                """, params)
                regions = [{"region_code": r[0] or "—", "fuel_60d": float(r[1] or 0),
                            "annual_running_cost": float(r[2] or 0), "anomalies": int(r[3] or 0),
                            "avg_kmpl": float(r[4]) if r[4] is not None else None}
                           for r in cur.fetchall()]
                # Costliest vehicles to run
                cur.execute(f"""
                    SELECT cs.vehicle_id, cs.make, cs.model, cs.model_year, sr.region_code,
                           cs.risk_category, cs.annual_running_cost, cs.fuel_cost_per_km,
                           cs.efficiency_kmpl, cs.eff_trend_pct, cs.fuel_anomaly
                    FROM field_service.v_vehicle_cost_summary cs
                    JOIN field_service.fleet_vehicles v ON v.vehicle_id = cs.vehicle_id
                    LEFT JOIN field_service.service_regions sr ON sr.region_id = cs.region_id
                    WHERE {wc}
                    ORDER BY cs.annual_running_cost DESC NULLS LAST
                    LIMIT 15
                """, params)
                top = [{"vehicle_id": r[0], "label": f"{r[3]} {r[1]} {r[2]}", "region_code": r[4],
                        "risk_category": r[5],
                        "annual_running_cost": float(r[6]) if r[6] is not None else None,
                        "fuel_cost_per_km": float(r[7]) if r[7] is not None else None,
                        "efficiency_kmpl": float(r[8]) if r[8] is not None else None,
                        "eff_trend_pct": float(r[9]) if r[9] is not None else None,
                        "fuel_anomaly": bool(r[10])} for r in cur.fetchall()]
        return jsonify({
            "totals": {
                "vehicles": int(t[0] or 0),
                "fuel_cost_60d": float(t[1] or 0),
                "maint_cost_365d": float(t[2] or 0),
                "annual_running_cost": float(t[3] or 0),
                "fuel_anomalies": int(t[4] or 0),
                "avg_efficiency_kmpl": float(t[5]) if t[5] is not None else None,
                "avg_fuel_cost_per_km": float(t[6]) if t[6] is not None else None,
            },
            "regions": regions,
            "top_cost_vehicles": top,
        })
    except Exception as e:
        log_error("fleet_cost_summary", e)
        return jsonify({"error": str(e)}), 500


@fleet_bp.route("/api/fleet/fuel-anomalies")
def fleet_fuel_anomalies():
    """Vehicles whose fuel economy is declining — a LEADING failure signal.

    A van burning more fuel per km than its own baseline is degrading mechanically.
    We surface these and flag the ones where the telematics risk model has NOT yet
    caught up (risk still LOW/MEDIUM) — fuel data as an EARLY warning. Ordered by
    the steepest efficiency drop.
    """
    try:
        _, user_regions = get_user_role_and_regions()
        where = ["cs.fuel_anomaly = TRUE", "v.status = 'active'"]
        params: list = []
        if user_regions:
            where.append("cs.region_id = ANY(%s)")
            params.append(user_regions)
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT cs.vehicle_id, cs.make, cs.model, cs.model_year, sr.region_code,
                           cs.risk_category, cs.health_score, cs.efficiency_kmpl,
                           cs.recent_kmpl, cs.older_kmpl, cs.eff_trend_pct,
                           cs.fuel_cost_60d, cs.fuel_cost_per_km,
                           t.first_name || ' ' || t.last_name AS tech_name
                    FROM field_service.v_vehicle_cost_summary cs
                    JOIN field_service.fleet_vehicles v ON v.vehicle_id = cs.vehicle_id
                    LEFT JOIN field_service.service_regions sr ON sr.region_id = cs.region_id
                    LEFT JOIN field_service.technicians t ON t.technician_id = v.assigned_technician_id
                    WHERE {' AND '.join(where)}
                    -- Surface EARLY warnings (risk model still rates LOW/MEDIUM) first,
                    -- then the steepest declines.
                    ORDER BY (cs.risk_category IN ('LOW','MEDIUM')) DESC, cs.eff_trend_pct ASC NULLS LAST
                    LIMIT 40
                """, params)
                items = []
                for r in cur.fetchall():
                    risk = r[5]
                    early = risk in ("LOW", "MEDIUM")  # fuel flags it before the risk model
                    items.append({
                        "vehicle_id": r[0], "label": f"{r[3]} {r[1]} {r[2]}", "region_code": r[4],
                        "risk_category": risk,
                        "health_score": float(r[6]) if r[6] is not None else None,
                        "efficiency_kmpl": float(r[7]) if r[7] is not None else None,
                        "recent_kmpl": float(r[8]) if r[8] is not None else None,
                        "older_kmpl": float(r[9]) if r[9] is not None else None,
                        "eff_trend_pct": float(r[10]) if r[10] is not None else None,
                        "fuel_cost_60d": float(r[11]) if r[11] is not None else None,
                        "fuel_cost_per_km": float(r[12]) if r[12] is not None else None,
                        "tech_name": r[13] or "Unassigned",
                        "early_warning": early,
                    })
                # Full-population counts (not just the returned 40) for honest KPIs
                cur.execute(f"""
                    SELECT COUNT(*),
                           COUNT(*) FILTER (WHERE cs.risk_category IN ('LOW','MEDIUM'))
                    FROM field_service.v_vehicle_cost_summary cs
                    JOIN field_service.fleet_vehicles v ON v.vehicle_id = cs.vehicle_id
                    WHERE {' AND '.join(where)}
                """, params)
                tot = cur.fetchone()
        return jsonify({"anomalies": items, "count": len(items),
                        "total_anomalies": int(tot[0] or 0),
                        "early_warning_count": int(tot[1] or 0)})
    except Exception as e:
        log_error("fleet_fuel_anomalies", e)
        return jsonify({"error": str(e)}), 500


# ===========================================================================
# Predictive Maintenance Action Center — operational, closed-loop remediation
# ===========================================================================
# These endpoints turn a prediction into real-world action: schedule service,
# ground the vehicle, reassign the grounded technician's open jobs to the best
# available techs, and reserve parts — all real Lakebase mutations.

@fleet_bp.route("/api/fleet/action-queue")
def fleet_action_queue():
    """Triage queue: active CRITICAL/HIGH vehicles that need a maintenance DECISION.

    Bounded (top 25 by risk then health) — the operator works a queue, not a 2,500-row
    scroll. Each item carries the assigned tech, their open-job load (SLA exposure if the
    van is grounded), and the top active fault code.
    """
    try:
        _, user_regions = get_user_role_and_regions()
        where = ["fv.status = 'active'", "fv.risk_category IN ('CRITICAL','HIGH')"]
        params: list = []
        if user_regions:
            where.append("fv.region_id = ANY(%s)")
            params.append(user_regions)
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    /* page:fleet/action_queue */
                    SELECT fv.vehicle_id, fv.make, fv.model, fv.model_year, fv.risk_category,
                           fv.health_score, fv.predicted_failure_date, fv.odometer_km,
                           sr.region_code, fv.assigned_technician_id,
                           t.first_name || ' ' || t.last_name AS tech_name,
                           (SELECT count(*) FROM field_service.work_orders w
                             WHERE w.assigned_technician_id = fv.assigned_technician_id
                               AND w.status IN ('open','assigned','en_route','in_progress')) AS open_jobs,
                           (SELECT d.code FROM field_service.vehicle_dtc_codes d
                             WHERE d.vehicle_id = fv.vehicle_id AND d.status = 'active'
                               AND COALESCE(d.is_false_positive, FALSE) = FALSE
                             ORDER BY CASE COALESCE(d.ai_severity, d.raw_severity)
                                      WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                                      WHEN 'medium' THEN 2 ELSE 3 END
                             LIMIT 1) AS top_dtc
                    FROM field_service.fleet_vehicles fv
                    LEFT JOIN field_service.service_regions sr ON sr.region_id = fv.region_id
                    LEFT JOIN field_service.technicians t ON t.technician_id = fv.assigned_technician_id
                    WHERE {' AND '.join(where)}
                    ORDER BY CASE fv.risk_category WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1 ELSE 2 END,
                             fv.health_score ASC
                    LIMIT 25
                """, params)
                queue = [{
                    "vehicle_id": r[0], "make": r[1], "model": r[2], "model_year": r[3],
                    "risk_category": r[4],
                    "health_score": float(r[5]) if r[5] is not None else None,
                    "predicted_failure_date": str(r[6]) if r[6] else None,
                    "odometer_km": float(r[7]) if r[7] is not None else None,
                    "region_code": r[8], "technician_id": r[9],
                    "tech_name": r[10] or "Unassigned", "open_jobs": r[11] or 0,
                    "top_dtc": r[12],
                } for r in cur.fetchall()]
        return jsonify({"queue": queue, "count": len(queue)})
    except Exception as e:
        log_error("fleet_action_queue", e)
        return jsonify({"error": str(e)}), 500


@fleet_bp.route("/api/fleet/remediation-plan/<vehicle_id>")
def fleet_remediation_plan(vehicle_id):
    """AI-recommended remediation plan (ai_query) + a preview of what Execute will do."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT fv.make, fv.model, fv.model_year, fv.risk_category, fv.health_score,
                           fv.predicted_failure_date, fv.assigned_technician_id, sr.region_code
                    FROM field_service.fleet_vehicles fv
                    LEFT JOIN field_service.service_regions sr ON sr.region_id = fv.region_id
                    WHERE fv.vehicle_id = %s
                """, (vehicle_id,))
                v = cur.fetchone()
                if not v:
                    return jsonify({"error": "vehicle not found"}), 404
                tech_id = v[6]
                # The actual open jobs that grounding would strand (reassign targets)
                cur.execute("""SELECT work_order_number, title, priority FROM field_service.work_orders
                               WHERE assigned_technician_id = %s
                                 AND status IN ('open','assigned','en_route')
                               ORDER BY CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                                        WHEN 'medium' THEN 2 ELSE 3 END
                               LIMIT 25""", (tech_id,))
                jobs = [{"work_order_number": r[0], "title": r[1], "priority": r[2]} for r in cur.fetchall()]
                open_jobs = len(jobs)
                cur.execute("""SELECT code, raw_description, COALESCE(ai_severity, raw_severity),
                                      COALESCE(is_false_positive, FALSE)
                               FROM field_service.vehicle_dtc_codes
                               WHERE vehicle_id = %s AND status = 'active'
                               ORDER BY reported_at DESC LIMIT 8""", (vehicle_id,))
                dtcs = cur.fetchall()

        dtc_txt = "; ".join(
            f"{d[0]} {d[1]} (severity {d[2]}{', FALSE POSITIVE' if d[3] else ''})" for d in dtcs
        ) or "no active fault codes"
        prompt = (
            f"You are a fleet maintenance dispatcher. Vehicle {v[2]} {v[0]} {v[1]} is {v[3]} risk "
            f"(health score {v[4]}). Active OBD-II fault codes: {dtc_txt}. The assigned technician "
            f"currently has {open_jobs} open work orders. In 2-3 concise sentences, give an operational "
            f"remediation plan: when to pull the vehicle for service, which fault codes are genuine vs "
            f"noise (loose-fuel-cap EVAP codes are usually noise), and note that the technician's "
            f"{open_jobs} open jobs must be reassigned to protect SLAs."
        )
        plan = None
        try:
            rows = _run_sql("SELECT ai_query('databricks-claude-sonnet-4-5', '%s')"
                            % prompt.replace("'", "''"))
            if rows and rows[0]:
                plan = rows[0][0]
        except Exception as ai_e:
            log_error("fleet_remediation_plan.ai", ai_e)

        parts, parts_total = _recommend_parts(dtcs)
        labor_hours, labor_total = _labor_estimate(dtcs)
        chargeback_total = round(parts_total + labor_total, 2)
        dtc_list = [{"code": d[0], "description": d[1], "severity": d[2], "is_false_positive": d[3]}
                    for d in dtcs]

        return jsonify({
            "vehicle_id": vehicle_id,
            "plan": plan,
            "preview": {
                "open_jobs_to_reassign": open_jobs,
                "active_dtcs": len(dtcs),
                "predicted_failure_date": str(v[5]) if v[5] else None,
                "will_create_work_order": True,
                "will_ground_vehicle": True,
                # Drill-down detail
                "jobs": jobs,
                "dtcs": dtc_list,
                "parts": parts,
                "parts_total": parts_total,
                "labor_hours": labor_hours,
                "labor_total": labor_total,
                "chargeback_total": chargeback_total,
                "chargeback_region": v[7],
            },
        })
    except Exception as e:
        log_error("fleet_remediation_plan", e)
        return jsonify({"error": str(e)}), 500


def _do_remediate(cur, vehicle_id, now, actions=("schedule", "reassign", "parts")):
    """Closed-loop remediation for ONE vehicle on an open cursor (caller owns the txn).

    Returns a summary dict. Raises ValueError if the vehicle is missing or already
    in the shop. Shared by the single-vehicle endpoint and the Planner batch execute.
    """
    cur.execute("""SELECT assigned_technician_id, region_id, make, model, status, risk_category
                   FROM field_service.fleet_vehicles WHERE vehicle_id = %s FOR UPDATE""", (vehicle_id,))
    v = cur.fetchone()
    if not v:
        raise ValueError("vehicle not found")
    if v[4] == 'in_shop':
        raise ValueError("vehicle already in shop")
    tech_id, region_id, make, model, _, risk = v
    summary = {"vehicle_id": vehicle_id, "actions": []}
    wo_id = None

    # 1. Schedule service: create maintenance WO + ground the vehicle
    if "schedule" in actions:
        wo_number = f"FMR-{now.strftime('%H%M%S')}-{vehicle_id}"  # VARCHAR(20) — keep short
        sla_hours = {"CRITICAL": 24, "HIGH": 72}.get(risk, 120)
        cur.execute("""
            INSERT INTO field_service.work_orders
                (work_order_number, category, subcategory, priority, status, title,
                 description, reported_issue, region_id, vehicle_id, sla_due_at, created_at, updated_at)
            VALUES (%s, 'maintenance', 'fleet_maintenance', %s, 'open', %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING work_order_id
        """, (wo_number, 'critical' if risk == 'CRITICAL' else 'high',
              f"Preventive service: {vehicle_id} ({make} {model})",
              f"Predictive-maintenance remediation for a {risk}-risk vehicle. Pull to depot for service.",
              "Predictive maintenance remediation", region_id, vehicle_id,
              now + timedelta(hours=sla_hours), now, now))
        wo_id = cur.fetchone()[0]
        cur.execute("""UPDATE field_service.fleet_vehicles
                       SET status = 'in_shop', updated_at = %s WHERE vehicle_id = %s""", (now, vehicle_id))
        summary["actions"].append({"type": "schedule_service",
                                   "work_order_number": wo_number, "vehicle_grounded": True})

    # 2. Reassign the grounded tech's open jobs to the best available techs
    if "reassign" in actions and tech_id:
        cur.execute("""SELECT work_order_id, work_order_number FROM field_service.work_orders
                       WHERE assigned_technician_id = %s AND status IN ('open','assigned','en_route')
                       ORDER BY CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                                WHEN 'medium' THEN 2 ELSE 3 END""", (tech_id,))
        open_wos = cur.fetchall()
        # Capacity cap = 8 (dispatch default); don't reference technicians.max_active_orders
        # (only added by dispatch_optimization.sql, may be absent).
        cur.execute("""
            SELECT t.technician_id, t.first_name || ' ' || t.last_name,
                   (SELECT count(*) FROM field_service.work_orders w
                     WHERE w.assigned_technician_id = t.technician_id
                       AND w.status IN ('open','assigned','en_route','in_progress')) AS load
            FROM field_service.technicians t
            LEFT JOIN field_service.fleet_vehicles fv2 ON fv2.assigned_technician_id = t.technician_id
            WHERE t.region_id = %s AND t.technician_id <> %s
              AND t.status NOT IN ('off_duty', 'on_leave')
              AND COALESCE(fv2.status, 'active') <> 'in_shop'
        """, (region_id, tech_id))
        cands = [{"id": r[0], "name": r[1], "cap": 8, "load": int(r[2])} for r in cur.fetchall()]
        reassigned = []
        for wo in open_wos:
            avail = sorted([c for c in cands if c["load"] < c["cap"]], key=lambda c: c["load"])
            if not avail:
                break
            tgt = avail[0]
            cur.execute("""UPDATE field_service.work_orders
                           SET assigned_technician_id = %s, updated_at = %s
                           WHERE work_order_id = %s""", (tgt["id"], now, wo[0]))
            tgt["load"] += 1
            reassigned.append({"work_order_number": wo[1], "to": tgt["name"]})
        summary["actions"].append({"type": "reassign_workload", "count": len(reassigned),
                                   "reassigned": reassigned[:10],
                                   "unassigned_remaining": len(open_wos) - len(reassigned)})

    # 3. Reserve parts costed from the genuine fault codes; persist parts+chargeback on the WO.
    if "parts" in actions and wo_id:
        cur.execute("""SELECT code, raw_description, COALESCE(ai_severity, raw_severity),
                              COALESCE(is_false_positive, FALSE)
                       FROM field_service.vehicle_dtc_codes
                       WHERE vehicle_id = %s AND status = 'active'""", (vehicle_id,))
        dtcs = cur.fetchall()
        parts, parts_total = _recommend_parts(dtcs)
        labor_hours, labor_total = _labor_estimate(dtcs)
        chargeback_total = round(parts_total + labor_total, 2)
        parts_note = ("\n\nParts ordered: "
                      + "; ".join(f"{p['part']} x{p['qty']} (${p['line_total']:.0f})" for p in parts)
                      + f"\nParts ${parts_total:.0f} + labor {labor_hours}h ${labor_total:.0f} "
                      + f"= chargeback ${chargeback_total:.0f} to region {region_id}.")
        cur.execute("""UPDATE field_service.work_orders
                       SET description = description || %s, updated_at = %s
                       WHERE work_order_id = %s""", (parts_note, now, wo_id))
        cur.execute("""SELECT inventory_id FROM field_service.equipment_inventory
                       WHERE region_id = %s AND status = 'in_stock'
                       ORDER BY inventory_id LIMIT %s FOR UPDATE SKIP LOCKED""",
                    (region_id, max(1, len(parts))))
        part_ids = [r[0] for r in cur.fetchall()]
        for inv_id in part_ids:
            cur.execute("""UPDATE field_service.equipment_inventory
                           SET status = 'assigned', last_serviced_at = %s
                           WHERE inventory_id = %s""", (now, inv_id))
        summary["actions"].append({
            "type": "reserve_parts", "reserved": len(part_ids),
            "parts": parts, "parts_total": parts_total,
            "labor_hours": labor_hours, "labor_total": labor_total,
            "chargeback_total": chargeback_total,
        })

    return summary


@fleet_bp.route("/api/fleet/remediate", methods=["POST"])
def fleet_remediate():
    """Execute the closed-loop remediation for one vehicle (human-approved, one click)."""
    data = request.json or {}
    vehicle_id = data.get("vehicle_id")
    actions = tuple(data.get("actions") or ["schedule", "reassign", "parts"])
    if not vehicle_id:
        return jsonify({"error": "vehicle_id required"}), 400
    try:
        now = datetime.now(timezone.utc)
        with get_pool().connection() as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                try:
                    summary = _do_remediate(cur, vehicle_id, now, actions)
                except ValueError as ve:
                    conn.rollback()
                    return jsonify({"error": str(ve)}), 409 if "shop" in str(ve) else 404
                conn.commit()
        return jsonify({"success": True, "summary": summary})
    except Exception as e:
        log_error("fleet_remediate", e)
        return jsonify({"error": str(e)}), 500


@fleet_bp.route("/api/fleet/planner", methods=["POST"])
def fleet_planner():
    """Fleet Maintenance Planner — AI-assisted decisions at scale.

    Takes operator constraints (daily budget, min service age/mileage gate, risk levels,
    per-region grounding cap, repair-vs-retire threshold), triages the WHOLE at-risk
    population, computes each vehicle's remediation chargeback + residual value, and
    returns a budget-bounded batch plan: repair / retire / defer — with an ai_query
    executive rationale. Deterministic optimizer + Foundation Model reasoning.
    """
    c = request.json or {}
    try:
        budget = float(c.get("daily_budget", 25000))
        min_years = float(c.get("min_service_years", 0))
        min_km = float(c.get("min_odometer_km", 0))
        risk_levels = c.get("risk_levels") or ["CRITICAL", "HIGH"]
        max_per_region = int(c.get("max_ground_per_region", 5))
        retire_age = float(c.get("retire_age_years", 8))
        retire_km = float(c.get("retire_mileage_km", 240000))
        retire_cost_pct = float(c.get("retire_cost_pct", 40))
        NOW_YEAR = 2025

        _, user_regions = get_user_role_and_regions()
        where = ["fv.status = 'active'", "fv.risk_category = ANY(%s)"]
        params = [risk_levels]
        if user_regions:
            where.append("fv.region_id = ANY(%s)")
            params.append(user_regions)
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT fv.vehicle_id, fv.make, fv.model, fv.model_year, fv.odometer_km,
                           fv.risk_category, fv.predicted_failure_date, fv.region_id, sr.region_code,
                           COALESCE(fv.purchase_cost, 40000), fv.assigned_technician_id,
                           (SELECT count(*) FROM field_service.work_orders w
                             WHERE w.assigned_technician_id = fv.assigned_technician_id
                               AND w.status IN ('open','assigned','en_route','in_progress')) AS jobs,
                           COALESCE(cs.annual_running_cost, 0), cs.fuel_cost_per_km,
                           cs.eff_trend_pct, COALESCE(cs.fuel_anomaly, FALSE)
                    FROM field_service.fleet_vehicles fv
                    LEFT JOIN field_service.service_regions sr ON sr.region_id = fv.region_id
                    LEFT JOIN field_service.v_vehicle_cost_summary cs ON cs.vehicle_id = fv.vehicle_id
                    WHERE {' AND '.join(where)}
                """, params)
                rows = cur.fetchall()
                vids = [r[0] for r in rows]
                dtc_by_v = {}
                if vids:
                    cur.execute("""SELECT vehicle_id, code, raw_description,
                                          COALESCE(ai_severity, raw_severity), COALESCE(is_false_positive, FALSE)
                                   FROM field_service.vehicle_dtc_codes
                                   WHERE vehicle_id = ANY(%s) AND status = 'active'""", (vids,))
                    for d in cur.fetchall():
                        dtc_by_v.setdefault(d[0], []).append((d[1], d[2], d[3], d[4]))

        cands = []
        for r in rows:
            (vid, make, model, my, odo, risk, pfd, region_id, region_code, pcost, _tech, jobs,
             annual_run, fuel_cpk, eff_trend, fuel_anomaly) = r
            my = int(my); odo = float(odo or 0); pcost = float(pcost or 40000); jobs = int(jobs)
            annual_run = float(annual_run or 0)
            age = NOW_YEAR - my
            dtcs = dtc_by_v.get(vid, [])
            parts, parts_total = _recommend_parts(dtcs)
            _lh, labor_total = _labor_estimate(dtcs)
            chargeback = round(parts_total + labor_total, 2)
            residual = max(500.0, round(pcost * (0.85 ** age) - (odo / 1000.0) * 15, 0))
            priority = {"CRITICAL": 100, "HIGH": 60}.get(risk, 20) + jobs * 8 + age * 2
            cands.append({"vehicle_id": vid, "label": f"{my} {make} {model}", "region_id": region_id,
                          "region_code": region_code, "risk": risk, "age": age, "odometer_km": odo,
                          "predicted_failure_date": str(pfd) if pfd else None, "jobs_stranded": jobs,
                          "chargeback": chargeback, "residual_value": residual, "priority": priority,
                          "annual_running_cost": annual_run,
                          "fuel_cost_per_km": float(fuel_cpk) if fuel_cpk is not None else None,
                          "eff_trend_pct": float(eff_trend) if eff_trend is not None else None,
                          "fuel_anomaly": bool(fuel_anomaly)})

        repair_pool, retire, defer = [], [], []
        for x in cands:
            gated = (min_years > 0 or min_km > 0)
            if gated and not (x["age"] >= min_years or x["odometer_km"] >= min_km):
                x["reason"] = "below service-age/mileage gate — monitor"
                defer.append(x); continue
            old_unit = (x["age"] >= retire_age) or (x["odometer_km"] >= retire_km)
            if old_unit and (x["chargeback"] > (retire_cost_pct / 100.0) * x["residual_value"]):
                x["reason"] = (f"repair ${x['chargeback']:.0f} exceeds {retire_cost_pct:.0f}% of "
                               f"${x['residual_value']:.0f} residual on a {x['age']}yr / "
                               f"{x['odometer_km']/1000:.0f}k-km unit — replace")
                retire.append(x); continue
            # Running-cost retire: an old unit that costs more per year to run (fuel +
            # maintenance) than it is worth is a replace, even if the repair itself is cheap.
            if old_unit and x["annual_running_cost"] > 0 and x["annual_running_cost"] > x["residual_value"]:
                x["reason"] = (f"annual running cost ${x['annual_running_cost']:.0f} exceeds "
                               f"${x['residual_value']:.0f} residual on a {x['age']}yr unit"
                               + (" (fuel economy declining)" if x["fuel_anomaly"] else "") + " — replace")
                retire.append(x); continue
            repair_pool.append(x)

        repair_pool.sort(key=lambda x: -x["priority"])
        selected, spent, per_region = [], 0.0, {}
        for x in repair_pool:
            if spent + x["chargeback"] > budget:
                x["reason"] = "deferred — exceeds remaining daily budget"; defer.append(x); continue
            if per_region.get(x["region_id"], 0) >= max_per_region:
                x["reason"] = f"deferred — region {x['region_code']} grounding cap ({max_per_region}) reached"
                defer.append(x); continue
            selected.append(x); spent += x["chargeback"]
            per_region[x["region_id"]] = per_region.get(x["region_id"], 0) + 1

        jobs_total = sum(x["jobs_stranded"] for x in selected)
        retire_annual_run = round(sum(x["annual_running_cost"] for x in retire), 0)
        fuel_flagged = sum(1 for x in cands if x["fuel_anomaly"])
        summary = {
            "candidates": len(cands), "repair_count": len(selected), "repair_cost": round(spent, 2),
            "retire_count": len(retire), "defer_count": len(defer),
            "budget": budget, "budget_remaining": round(budget - spent, 2),
            "jobs_reassigned": jobs_total,
            "by_region": {k: v for k, v in per_region.items()},
            "retire_annual_running_cost": retire_annual_run,
            "fuel_anomaly_count": fuel_flagged,
        }

        prompt = (
            f"You are a fleet maintenance director making decisions at scale. From {len(cands)} at-risk "
            f"vehicles, the plan: REPAIR {len(selected)} today for ${spent:.0f} (daily budget ${budget:.0f}); "
            f"RETIRE {len(retire)} where repair cost or annual running cost exceeds the vehicle's residual "
            f"value (eliminating ${retire_annual_run:.0f}/yr of running cost); DEFER {len(defer)}. "
            f"{fuel_flagged} vehicles show a declining fuel-economy trend (a leading mechanical-failure "
            f"signal from fuel-card data). Per-region grounding cap {max_per_region}; {jobs_total} field jobs "
            f"will be reassigned to protect SLAs. In 3-4 sentences give the executive rationale — budget "
            f"discipline, SLA protection, fuel-economy/running-cost economics, and why replacing the retire "
            f"candidates beats repairing them."
        )
        rationale = None
        try:
            rr = _run_sql("SELECT ai_query('databricks-claude-sonnet-4-5', '%s')" % prompt.replace("'", "''"))
            if rr and rr[0]:
                rationale = rr[0][0]
        except Exception as ai_e:
            log_error("fleet_planner.ai", ai_e)

        return jsonify({
            "summary": summary,
            "rationale": rationale,
            "repair": selected,
            "retire": retire,
            "defer": defer[:50],
            "defer_total": len(defer),
            "constraints": {"daily_budget": budget, "min_service_years": min_years,
                            "min_odometer_km": min_km, "risk_levels": risk_levels,
                            "max_ground_per_region": max_per_region, "retire_age_years": retire_age,
                            "retire_mileage_km": retire_km, "retire_cost_pct": retire_cost_pct},
        })
    except Exception as e:
        log_error("fleet_planner", e)
        return jsonify({"error": str(e)}), 500


@fleet_bp.route("/api/fleet/planner/execute", methods=["POST"])
def fleet_planner_execute():
    """Batch-execute a Planner plan: remediate the repair list + retire the retire list.

    Each vehicle commits independently so one failure doesn't roll back the batch.
    """
    data = request.json or {}
    repair = (data.get("repair") or [])[:200]
    retire = (data.get("retire") or [])[:200]
    try:
        now = datetime.now(timezone.utc)
        res = {"repaired": [], "repair_failed": [], "retired": [],
               "total_chargeback": 0.0, "jobs_reassigned": 0}
        with get_pool().connection() as conn:
            conn.autocommit = False
            for vid in repair:
                try:
                    with conn.cursor() as cur:
                        s = _do_remediate(cur, vid, now)
                    conn.commit()
                    cb = next((a.get("chargeback_total", 0) for a in s["actions"] if a["type"] == "reserve_parts"), 0)
                    rj = next((a.get("count", 0) for a in s["actions"] if a["type"] == "reassign_workload"), 0)
                    res["repaired"].append({"vehicle_id": vid, "chargeback": cb})
                    res["total_chargeback"] += cb
                    res["jobs_reassigned"] += rj
                except Exception as ie:
                    conn.rollback()
                    res["repair_failed"].append({"vehicle_id": vid, "error": str(ie)[:120]})
            for vid in retire:
                try:
                    with conn.cursor() as cur:
                        cur.execute("""UPDATE field_service.fleet_vehicles
                                       SET status = 'retired', updated_at = %s
                                       WHERE vehicle_id = %s AND status <> 'retired'""", (now, vid))
                    conn.commit()
                    res["retired"].append(vid)
                except Exception:
                    conn.rollback()
        res["total_chargeback"] = round(res["total_chargeback"], 2)
        return jsonify({"success": True, **res})
    except Exception as e:
        log_error("fleet_planner_execute", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Summary (cached — touches several tables)
# ---------------------------------------------------------------------------

_fleet_cache: dict = {}     # key → (data, timestamp)
_FLEET_CACHE_TTL = 60       # 1 minute — summary aggregates the whole fleet


@fleet_bp.route("/api/fleet/summary")
def fleet_summary():
    """Fleet KPI cards, per-region rollup, and the DTC false-alarm rate.

    Per-region rollup comes from the v_fleet_health_summary view.  Fleet-wide
    KPIs (totals, % needing maintenance, averages) are derived from
    fleet_vehicles.  The DTC false-alarm rate is the share of AI-interpreted
    active codes flagged as false positives; it is ``null`` when the AI step
    has not run yet (UI shows "—").

    RBAC: non-admins see only their assigned regions.
    """
    _role, user_regions = get_user_role_and_regions()
    cache_key = "summary:" + (",".join(map(str, sorted(user_regions))) if user_regions else "all")
    cached = _fleet_cache.get(cache_key)
    if cached and time.time() - cached[1] < _FLEET_CACHE_TTL:
        return jsonify(cached[0])

    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Per-region rollup from the summary view (RBAC-filtered)
                region_sql = """
                    /* page:fleet/summary:regions */
                    SELECT region_id, region_code, region_name, total_vehicles,
                           in_shop, critical, high_risk, medium_risk, low_risk,
                           avg_health_score, avg_odometer_km
                    FROM field_service.v_fleet_health_summary
                """
                params: list = []
                if user_regions:
                    region_sql += " WHERE region_id = ANY(%s)"
                    params.append(user_regions)
                region_sql += " ORDER BY critical DESC, high_risk DESC"
                cur.execute(region_sql, params)
                regions = [{
                    "region_id": r[0], "region_code": r[1], "region_name": r[2],
                    "total_vehicles": r[3] or 0, "in_shop": r[4] or 0,
                    "critical": r[5] or 0, "high_risk": r[6] or 0,
                    "medium_risk": r[7] or 0, "low_risk": r[8] or 0,
                    "avg_health_score": float(r[9]) if r[9] is not None else None,
                    "avg_odometer_km": float(r[10]) if r[10] is not None else None,
                } for r in cur.fetchall()]

                # Fleet-wide KPIs from fleet_vehicles (RBAC-filtered)
                kpi_where = ""
                kpi_params: list = []
                if user_regions:
                    kpi_where = "WHERE region_id = ANY(%s)"
                    kpi_params.append(user_regions)
                cur.execute(f"""
                    /* page:fleet/summary:kpis */
                    SELECT COUNT(*) AS total_vehicles,
                           COUNT(*) FILTER (WHERE status = 'in_shop') AS in_shop,
                           COUNT(*) FILTER (WHERE risk_category IN ('HIGH', 'CRITICAL')) AS needs_maintenance,
                           AVG(health_score) AS avg_health_score,
                           AVG(odometer_km) AS avg_odometer_km
                    FROM field_service.fleet_vehicles
                    {kpi_where}
                """, kpi_params)
                k = cur.fetchone()
                total = k[0] or 0
                kpis = {
                    "total_vehicles": total,
                    "in_shop": k[1] or 0,
                    "needs_maintenance": k[2] or 0,
                    "needs_maintenance_pct": round((k[2] or 0) * 100.0 / total, 1) if total else 0,
                    "avg_health_score": round(float(k[3]), 1) if k[3] is not None else None,
                    "avg_odometer_km": round(float(k[4])) if k[4] is not None else None,
                }

                # DTC false-alarm rate: false positives / AI-interpreted active codes
                # (region filter applied by joining fleet_vehicles)
                fa_where = "WHERE dc.status = 'active' AND dc.ai_interpreted_at IS NOT NULL"
                fa_params: list = []
                if user_regions:
                    fa_where += " AND fv.region_id = ANY(%s)"
                    fa_params.append(user_regions)
                cur.execute(f"""
                    /* page:fleet/summary:false_alarm */
                    SELECT COUNT(*) AS interpreted,
                           COUNT(*) FILTER (WHERE dc.is_false_positive IS TRUE) AS false_positives
                    FROM field_service.vehicle_dtc_codes dc
                    JOIN field_service.fleet_vehicles fv ON dc.vehicle_id = fv.vehicle_id
                    {fa_where}
                """, fa_params)
                f = cur.fetchone()
                interpreted = f[0] or 0
                false_positives = f[1] or 0
                kpis["dtc_false_alarm_rate"] = (
                    round(false_positives * 100.0 / interpreted, 1) if interpreted else None
                )
                kpis["dtc_interpreted_count"] = interpreted
                kpis["dtc_false_positive_count"] = false_positives

        result = {"kpis": kpis, "regions": regions}
        _fleet_cache[cache_key] = (result, time.time())
        return jsonify(result)
    except Exception as e:
        log_error("fleet_summary", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Vehicle list (filterable, paginated, risk-ordered)
# ---------------------------------------------------------------------------

@fleet_bp.route("/api/fleet/vehicles")
def fleet_vehicles():
    """Filterable, paginated vehicle list ordered by risk then health.

    Query params:
      - region (str): region_code filter
      - risk (str): risk_category filter (LOW/MEDIUM/HIGH/CRITICAL)
      - status (str): active/in_shop/retired
      - search (str): match vehicle_id, make, or model (ILIKE)
      - limit (int, max 200), offset (int)

    RBAC: non-admins see only their assigned regions.
    """
    try:
        region_filter = request.args.get("region", "")
        # Accept both 'risk' and 'risk_category' (the template sends risk_category).
        risk_filter = request.args.get("risk_category") or request.args.get("risk", "")
        status_filter = request.args.get("status", "")
        search = request.args.get("search", "").strip()
        limit = min(int(request.args.get("limit", 200)), 200)
        offset = max(int(request.args.get("offset", 0)), 0)

        _role, user_regions = get_user_role_and_regions()

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where = ["1=1"]
                params: list = []
                if region_filter:
                    where.append("sr.region_code = %s")
                    params.append(region_filter)
                elif user_regions:
                    where.append("fv.region_id = ANY(%s)")
                    params.append(user_regions)
                if risk_filter:
                    where.append("fv.risk_category = %s")
                    params.append(risk_filter)
                if status_filter:
                    where.append("fv.status = %s")
                    params.append(status_filter)
                if search:
                    where.append("(fv.vehicle_id ILIKE %s OR fv.make ILIKE %s OR fv.model ILIKE %s)")
                    like = f"%{search}%"
                    params.extend([like, like, like])

                where_sql = " AND ".join(where)
                cur.execute(f"""
                    /* page:fleet/vehicles */
                    SELECT fv.vehicle_id, fv.make, fv.model, fv.model_year,
                           fv.vehicle_type, fv.status, fv.health_profile,
                           fv.health_score, fv.risk_category, fv.predicted_failure_date,
                           fv.odometer_km, fv.region_id, sr.region_code, sr.region_name,
                           t.first_name || ' ' || t.last_name AS tech_name,
                           fv.assigned_technician_id
                    FROM field_service.fleet_vehicles fv
                    LEFT JOIN field_service.service_regions sr ON fv.region_id = sr.region_id
                    LEFT JOIN field_service.technicians t ON fv.assigned_technician_id = t.technician_id
                    WHERE {where_sql}
                    ORDER BY CASE fv.risk_category
                                WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1
                                WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 ELSE 4 END,
                             fv.health_score ASC NULLS LAST
                    LIMIT %s OFFSET %s
                """, params + [limit, offset])
                vehicles = [{
                    "vehicle_id": r[0], "make": r[1], "model": r[2], "model_year": r[3],
                    "vehicle_type": r[4], "status": r[5], "health_profile": r[6],
                    "health_score": float(r[7]) if r[7] is not None else None,
                    "risk_category": r[8],
                    "predicted_failure_date": str(r[9]) if r[9] else None,
                    "odometer_km": float(r[10]) if r[10] is not None else None,
                    "region_id": r[11], "region_code": r[12], "region_name": r[13],
                    "tech_name": r[14], "assigned_technician_id": r[15],
                } for r in cur.fetchall()]

        return jsonify({"vehicles": vehicles, "count": len(vehicles), "offset": offset, "limit": limit})
    except Exception as e:
        log_error("fleet_vehicles", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Vehicle detail
# ---------------------------------------------------------------------------

@fleet_bp.route("/api/fleet/vehicle/<vehicle_id>")
def fleet_vehicle_detail(vehicle_id):
    """Full detail for one vehicle: latest telemetry, 30-day trend, DTCs,
    maintenance history, and any open fleet-maintenance work orders.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Vehicle row
                cur.execute("""
                    /* page:fleet/vehicle:row */
                    SELECT fv.vehicle_id, fv.vin, fv.make, fv.model, fv.model_year,
                           fv.vehicle_type, fv.status, fv.health_profile, fv.health_score,
                           fv.risk_category, fv.predicted_failure_date, fv.odometer_km,
                           fv.in_service_date, fv.last_service_date, fv.last_service_odometer_km,
                           fv.next_service_due_km, fv.purchase_cost,
                           fv.current_latitude, fv.current_longitude,
                           sr.region_code, sr.region_name,
                           t.first_name || ' ' || t.last_name AS tech_name
                    FROM field_service.fleet_vehicles fv
                    LEFT JOIN field_service.service_regions sr ON fv.region_id = sr.region_id
                    LEFT JOIN field_service.technicians t ON fv.assigned_technician_id = t.technician_id
                    WHERE fv.vehicle_id = %s
                """, (vehicle_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Vehicle not found"}), 404
                vehicle = {
                    "vehicle_id": row[0], "vin": row[1], "make": row[2], "model": row[3],
                    "model_year": row[4], "vehicle_type": row[5], "status": row[6],
                    "health_profile": row[7],
                    "health_score": float(row[8]) if row[8] is not None else None,
                    "risk_category": row[9],
                    "predicted_failure_date": str(row[10]) if row[10] else None,
                    "odometer_km": float(row[11]) if row[11] is not None else None,
                    "in_service_date": str(row[12]) if row[12] else None,
                    "last_service_date": str(row[13]) if row[13] else None,
                    "last_service_odometer_km": float(row[14]) if row[14] is not None else None,
                    "next_service_due_km": float(row[15]) if row[15] is not None else None,
                    "purchase_cost": float(row[16]) if row[16] is not None else None,
                    "current_latitude": float(row[17]) if row[17] is not None else None,
                    "current_longitude": float(row[18]) if row[18] is not None else None,
                    "region_code": row[19], "region_name": row[20], "tech_name": row[21],
                }

                # Latest telemetry
                cur.execute("""
                    /* page:fleet/vehicle:latest_telemetry */
                    SELECT recorded_at, odometer_km, speed_avg_kph, engine_temp_c,
                           oil_life_pct, battery_voltage, fuel_level_pct, tire_pressure_psi,
                           engine_rpm_avg, harsh_brake_count, harsh_accel_count, idle_minutes,
                           dtc_active_count, health_score
                    FROM field_service.vehicle_telemetry
                    WHERE vehicle_id = %s
                    ORDER BY recorded_at DESC
                    LIMIT 1
                """, (vehicle_id,))
                t = cur.fetchone()
                latest_telemetry = None
                if t:
                    latest_telemetry = {
                        "recorded_at": str(t[0]) if t[0] else None,
                        "odometer_km": float(t[1]) if t[1] is not None else None,
                        "speed_avg_kph": float(t[2]) if t[2] is not None else None,
                        "engine_temp_c": float(t[3]) if t[3] is not None else None,
                        "oil_life_pct": float(t[4]) if t[4] is not None else None,
                        "battery_voltage": float(t[5]) if t[5] is not None else None,
                        "fuel_level_pct": float(t[6]) if t[6] is not None else None,
                        "tire_pressure_psi": float(t[7]) if t[7] is not None else None,
                        "engine_rpm_avg": float(t[8]) if t[8] is not None else None,
                        "harsh_brake_count": t[9], "harsh_accel_count": t[10],
                        "idle_minutes": float(t[11]) if t[11] is not None else None,
                        "dtc_active_count": t[12],
                        "health_score": float(t[13]) if t[13] is not None else None,
                    }

                # 30-day telemetry trend (for sparklines)
                cur.execute("""
                    /* page:fleet/vehicle:trend */
                    SELECT recorded_at, engine_temp_c, oil_life_pct,
                           battery_voltage, health_score
                    FROM field_service.vehicle_telemetry
                    WHERE vehicle_id = %s
                      AND recorded_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                    ORDER BY recorded_at ASC
                    LIMIT 200
                """, (vehicle_id,))
                trend = [{
                    "recorded_at": str(r[0]) if r[0] else None,
                    "engine_temp_c": float(r[1]) if r[1] is not None else None,
                    "oil_life_pct": float(r[2]) if r[2] is not None else None,
                    "battery_voltage": float(r[3]) if r[3] is not None else None,
                    "health_score": float(r[4]) if r[4] is not None else None,
                } for r in cur.fetchall()]

                # Active DTC codes (with AI interpretation)
                cur.execute("""
                    /* page:fleet/vehicle:dtc */
                    SELECT dtc_id, code, raw_description, dtc_system, reported_at,
                           odometer_at_report, raw_severity, status,
                           ai_severity, ai_explanation, is_false_positive, ai_interpreted_at
                    FROM field_service.vehicle_dtc_codes
                    WHERE vehicle_id = %s AND status = 'active'
                    ORDER BY reported_at DESC
                    LIMIT 50
                """, (vehicle_id,))
                dtc_codes = [{
                    "dtc_id": r[0], "code": r[1], "raw_description": r[2],
                    "dtc_system": r[3], "reported_at": str(r[4]) if r[4] else None,
                    "odometer_at_report": float(r[5]) if r[5] is not None else None,
                    "raw_severity": r[6], "status": r[7],
                    "ai_severity": r[8], "ai_explanation": r[9],
                    "is_false_positive": r[10],
                    "ai_interpreted_at": str(r[11]) if r[11] else None,
                } for r in cur.fetchall()]

                # Recent maintenance history
                cur.execute("""
                    /* page:fleet/vehicle:history */
                    SELECT record_id, service_date, service_type,
                           odometer_at_service, cost, was_predicted, notes
                    FROM field_service.vehicle_maintenance_history
                    WHERE vehicle_id = %s
                    ORDER BY service_date DESC
                    LIMIT 20
                """, (vehicle_id,))
                history = [{
                    "record_id": r[0], "service_date": str(r[1]) if r[1] else None,
                    "service_type": r[2],
                    "odometer_at_service": float(r[3]) if r[3] is not None else None,
                    "cost": float(r[4]) if r[4] is not None else None,
                    "was_predicted": r[5], "notes": r[6],
                } for r in cur.fetchall()]

                # Open fleet-maintenance work orders for this vehicle
                cur.execute("""
                    /* page:fleet/vehicle:work_orders */
                    SELECT work_order_id, work_order_number, title, priority,
                           status, created_at, sla_due_at
                    FROM field_service.work_orders
                    WHERE category = 'maintenance'
                      AND subcategory = 'fleet_maintenance'
                      AND vehicle_id = %s
                      AND status NOT IN ('completed', 'cancelled')
                    ORDER BY created_at DESC
                    LIMIT 20
                """, (vehicle_id,))
                work_orders = [{
                    "work_order_id": r[0], "work_order_number": r[1], "title": r[2],
                    "priority": r[3], "status": r[4],
                    "created_at": str(r[5]) if r[5] else None,
                    "sla_due_at": str(r[6]) if r[6] else None,
                } for r in cur.fetchall()]

                # Cost + fuel-efficiency summary (off-Sheets fuel data + maintenance)
                cost = None
                try:
                    cur.execute("""
                        /* page:fleet/vehicle:cost */
                        SELECT fills_60d, fuel_cost_60d, distance_60d, efficiency_kmpl,
                               recent_kmpl, older_kmpl, eff_trend_pct, fuel_anomaly,
                               fuel_cost_per_km, external_invoices, maint_cost_365d,
                               annual_running_cost
                        FROM field_service.v_vehicle_cost_summary
                        WHERE vehicle_id = %s
                    """, (vehicle_id,))
                    cr = cur.fetchone()
                    if cr:
                        cost = {
                            "fills_60d": int(cr[0]) if cr[0] is not None else 0,
                            "fuel_cost_60d": float(cr[1]) if cr[1] is not None else None,
                            "distance_60d": float(cr[2]) if cr[2] is not None else None,
                            "efficiency_kmpl": float(cr[3]) if cr[3] is not None else None,
                            "recent_kmpl": float(cr[4]) if cr[4] is not None else None,
                            "older_kmpl": float(cr[5]) if cr[5] is not None else None,
                            "eff_trend_pct": float(cr[6]) if cr[6] is not None else None,
                            "fuel_anomaly": bool(cr[7]),
                            "fuel_cost_per_km": float(cr[8]) if cr[8] is not None else None,
                            "external_invoices": int(cr[9]) if cr[9] is not None else 0,
                            "maint_cost_365d": float(cr[10]) if cr[10] is not None else None,
                            "annual_running_cost": float(cr[11]) if cr[11] is not None else None,
                        }
                except Exception as ce:
                    # Cost view may not exist yet (fuel data not deployed) — degrade gracefully.
                    log_error("fleet_vehicle_detail.cost", ce)

        return jsonify({
            "vehicle": vehicle,
            "latest_telemetry": latest_telemetry,
            "trend": trend,
            "dtc_codes": dtc_codes,
            "maintenance_history": history,
            "work_orders": work_orders,
            "cost": cost,
        })
    except Exception as e:
        log_error("fleet_vehicle_detail", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Fleet-wide DTC codes (AI interpreted) — the data-trust panel
# ---------------------------------------------------------------------------

@fleet_bp.route("/api/fleet/dtc-codes")
def fleet_dtc_codes():
    """Active DTC codes across the fleet with AI interpretation.

    Query params:
      - region (str): region_code filter
      - sort (str): reported_at | severity (default reported_at)
      - false_positives (str): 'true' to show only flagged false positives
      - limit (int, max 200)

    RBAC: non-admins see only their assigned regions.  AI columns may be NULL
    (UI shows "Pending AI review").
    """
    try:
        region_filter = request.args.get("region", "")
        sort_by = request.args.get("sort", "reported_at")
        only_fp = request.args.get("false_positives", "").lower() == "true"
        limit = min(int(request.args.get("limit", 100)), 200)

        _role, user_regions = get_user_role_and_regions()

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where = ["dc.status = 'active'"]
                params: list = []
                if region_filter:
                    where.append("sr.region_code = %s")
                    params.append(region_filter)
                elif user_regions:
                    where.append("fv.region_id = ANY(%s)")
                    params.append(user_regions)
                if only_fp:
                    where.append("dc.is_false_positive IS TRUE")
                where_sql = " AND ".join(where)

                # sort_by validated against an allow-list before interpolation
                if sort_by == "severity":
                    order_sql = """ORDER BY CASE dc.ai_severity
                                        WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                                        WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END,
                                     dc.reported_at DESC"""
                else:
                    order_sql = "ORDER BY dc.reported_at DESC"

                cur.execute(f"""
                    /* page:fleet/dtc_codes */
                    SELECT dc.dtc_id, dc.vehicle_id, fv.make, fv.model,
                           sr.region_code, dc.code, dc.raw_description, dc.dtc_system,
                           dc.reported_at, dc.raw_severity, dc.ai_severity,
                           dc.ai_explanation, dc.is_false_positive, dc.ai_interpreted_at
                    FROM field_service.vehicle_dtc_codes dc
                    JOIN field_service.fleet_vehicles fv ON dc.vehicle_id = fv.vehicle_id
                    LEFT JOIN field_service.service_regions sr ON fv.region_id = sr.region_id
                    WHERE {where_sql}
                    {order_sql}
                    LIMIT %s
                """, params + [limit])
                codes = [{
                    "dtc_id": r[0], "vehicle_id": r[1], "make": r[2], "model": r[3],
                    "region_code": r[4], "code": r[5], "raw_description": r[6],
                    "dtc_system": r[7], "reported_at": str(r[8]) if r[8] else None,
                    "raw_severity": r[9], "ai_severity": r[10],
                    "ai_explanation": r[11], "is_false_positive": r[12],
                    "ai_interpreted_at": str(r[13]) if r[13] else None,
                } for r in cur.fetchall()]

        return jsonify({"dtc_codes": codes, "count": len(codes), "sort_by": sort_by})
    except Exception as e:
        log_error("fleet_dtc_codes", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Fleet map (vehicles with coordinates + risk)
# ---------------------------------------------------------------------------

@fleet_bp.route("/api/fleet/map")
def fleet_map():
    """Vehicles with current coordinates, risk category, and health score.

    Only returns vehicles that have a known location.  RBAC: non-admins see
    only their assigned regions.
    """
    try:
        _role, user_regions = get_user_role_and_regions()
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where = [
                    "fv.current_latitude IS NOT NULL",
                    "fv.current_longitude IS NOT NULL",
                    "fv.status <> 'retired'",
                ]
                params: list = []
                if user_regions:
                    where.append("fv.region_id = ANY(%s)")
                    params.append(user_regions)
                where_sql = " AND ".join(where)
                cur.execute(f"""
                    /* page:fleet/map */
                    SELECT fv.vehicle_id, fv.make, fv.model, fv.status,
                           fv.risk_category, fv.health_score,
                           fv.current_latitude, fv.current_longitude,
                           sr.region_code,
                           t.first_name || ' ' || t.last_name AS tech_name
                    FROM field_service.fleet_vehicles fv
                    LEFT JOIN field_service.service_regions sr ON fv.region_id = sr.region_id
                    LEFT JOIN field_service.technicians t ON fv.assigned_technician_id = t.technician_id
                    WHERE {where_sql}
                    LIMIT 2000
                """, params)
                vehicles = [{
                    "vehicle_id": r[0], "make": r[1], "model": r[2], "status": r[3],
                    "risk_category": r[4],
                    "health_score": float(r[5]) if r[5] is not None else None,
                    "latitude": float(r[6]) if r[6] is not None else None,
                    "longitude": float(r[7]) if r[7] is not None else None,
                    "region_code": r[8], "tech_name": r[9],
                } for r in cur.fetchall()]

        return jsonify({"vehicles": vehicles, "count": len(vehicles)})
    except Exception as e:
        log_error("fleet_map", e)
        return jsonify({"error": str(e)}), 500

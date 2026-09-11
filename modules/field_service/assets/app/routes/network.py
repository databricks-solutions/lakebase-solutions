"""
Network Health, Skills Matrix, and Predictive Maintenance blueprint.

Purpose
=======
Serves three API endpoints that power the Network Health dashboard, the
Skills Matrix page, and the Predictive Maintenance work-order list.

- **Network Health** queries Unity Catalog gold tables (produced by a DLT
  Medallion pipeline) via the SQL Statement Execution API.
- **Skills Matrix** queries Lakebase PG for technician certifications and
  skill gap analysis.
- **Predictive Maintenance** queries Lakebase PG for work orders created
  by the ML model scoring pipeline (subcategory = 'predictive_maintenance').

Routes (3)
==========
GET  /api/network-health/summary    Node risk, regional summary, outages, IoT health
GET  /api/skills/matrix             Technician-skill assignments and gap analysis
GET  /api/predictive-maintenance    ML-generated predictive maintenance work orders

Data sources
============
- SQL Warehouse (Statement Execution API)   gold_node_maintenance_risk,
    gold_regional_network_summary, gold_daily_outage_summary,
    gold_iot_device_health
- Lakebase PostgreSQL                       technician_skills, skill_types,
    technicians, service_regions, work_orders

Related files
=============
- app/templates/network_health.html     Network health dashboard UI
- app/templates/skills_matrix.html      Skills matrix UI
- app/shared.py                         _run_sql, get_pool, log_error
- notebooks/dlt_pipeline.py             DLT pipeline producing gold tables
"""

import logging
import os
import time
import threading

from flask import Blueprint, jsonify, request

from shared import _run_sql, get_pool, log_error

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint creation
# ---------------------------------------------------------------------------

network_bp = Blueprint("network", __name__)


# ── Network Health Summary (cached + parallel) ────────────────────────────

_nh_cache: dict = {}          # key → (data, timestamp)
_NH_CACHE_TTL = 120           # 2 minutes — warehouse queries are expensive


@network_bp.route("/api/network-health/summary")
def network_health_summary():
    """Surface gold pipeline tables: node health, outages, maintenance risk.

    Runs 4 SQL warehouse queries IN PARALLEL (was sequential — 4× faster).
    Results are cached for 2 minutes to avoid hammering the warehouse.
    Each sub-query is wrapped in its own try/except so that a single table
    failure doesn't break the entire response.
    """
    # Check cache first
    cached = _nh_cache.get("summary")
    if cached:
        data, ts = cached
        if time.time() - ts < _NH_CACHE_TTL:
            return jsonify(data)

    try:
        catalog = os.environ.get("PIPELINE_CATALOG", "dba-lakebase-network")
        result = {}

        # Run all 4 SQL warehouse queries in parallel threads
        def _query_risk():
            try:
                rows = _run_sql(f"""
                    SELECT node_id, node_name, node_type, region_code,
                           maintenance_risk_score, risk_category, recent_avg_health,
                           days_since_maintenance, recent_outage_count
                    FROM `{catalog}`.network_data.gold_node_maintenance_risk
                    ORDER BY maintenance_risk_score DESC LIMIT 20
                """, catalog=catalog)
                result["maintenance_risk"] = [
                    {
                        "node_id": r[0], "name": r[1], "type": r[2], "region": r[3],
                        "risk_score": float(r[4] or 0), "risk_category": r[5],
                        "health_score": float(r[6] or 0),
                        "days_since_maint": int(r[7] or 0), "total_outages": int(r[8] or 0),
                    }
                    for r in rows
                ]
            except Exception as e:
                result["maintenance_risk"] = []
                result["maintenance_risk_error"] = str(e)

        def _query_regional():
            try:
                rows = _run_sql(f"""
                    SELECT region_code, avg_health_score, active_nodes,
                           healthy_node_count, unhealthy_node_count, 0 AS critical_nodes,
                           region_total_errors, region_avg_latency_ms
                    FROM `{catalog}`.network_data.gold_regional_network_summary
                    WHERE measurement_date = (SELECT MAX(measurement_date) FROM `{catalog}`.network_data.gold_regional_network_summary)
                    ORDER BY avg_health_score ASC
                """, catalog=catalog)
                result["regional_summary"] = [
                    {
                        "region": r[0], "avg_health": float(r[1] or 0),
                        "total_nodes": int(r[2] or 0), "healthy": int(r[3] or 0),
                        "degraded": int(r[4] or 0), "critical": int(r[5] or 0),
                        "outages": int(r[6] or 0), "avg_signal": float(r[7] or 0),
                    }
                    for r in rows
                ]
            except Exception as e:
                result["regional_summary"] = []
                result["regional_summary_error"] = str(e)

        def _query_outages():
            try:
                rows = _run_sql(f"""
                    SELECT outage_date, region_code, severity,
                           outage_count, ROUND(total_outage_minutes / 60.0, 1), ROUND(avg_duration_minutes / 60.0, 1)
                    FROM `{catalog}`.network_data.gold_daily_outage_summary
                    WHERE outage_date >= CURRENT_DATE - INTERVAL '7' DAY
                    ORDER BY outage_date DESC, outage_count DESC
                    LIMIT 50
                """, catalog=catalog)
                result["outage_summary"] = [
                    {
                        "date": str(r[0]), "region": r[1], "severity": r[2],
                        "count": int(r[3] or 0), "duration_hours": float(r[4] or 0),
                        "avg_repair_hours": float(r[5] or 0),
                    }
                    for r in rows
                ]
            except Exception as e:
                result["outage_summary"] = []
                result["outage_summary_error"] = str(e)

        def _query_iot():
            try:
                rows = _run_sql(f"""
                    SELECT infrastructure_id, device_count,
                           avg_battery_pct, avg_signal_dbm,
                           total_errors, CASE WHEN iot_health_score < 50 THEN 1 ELSE 0 END AS warning_devices,
                           CASE WHEN iot_health_score >= 50 THEN 1 ELSE 0 END AS healthy_devices
                    FROM `{catalog}`.network_data.gold_iot_device_health
                    ORDER BY total_errors DESC LIMIT 20
                """, catalog=catalog)
                result["iot_health"] = [
                    {
                        "infra_id": r[0], "device_count": int(r[1] or 0),
                        "avg_battery": float(r[2] or 0), "avg_signal": float(r[3] or 0),
                        "critical": int(r[4] or 0), "warning": int(r[5] or 0),
                        "healthy": int(r[6] or 0),
                    }
                    for r in rows
                ]
            except Exception as e:
                result["iot_health"] = []
                result["iot_health_error"] = str(e)

        # Fire all 4 queries in parallel
        threads = [
            threading.Thread(target=_query_risk),
            threading.Thread(target=_query_regional),
            threading.Thread(target=_query_outages),
            threading.Thread(target=_query_iot),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)  # 2 min max per thread

        # KPI rollups derived from the warehouse data
        risk_data = result.get("maintenance_risk", [])
        result["kpis"] = {
            "critical_nodes": len([r for r in risk_data if (r.get("risk_category") or '').lower() == "critical"]),
            "high_risk_nodes": len([r for r in risk_data if (r.get("risk_category") or '').lower() in ("critical", "high")]),
            "avg_health": round(
                sum(float(r.get("health_score") or 0) for r in risk_data) / max(len(risk_data), 1), 1
            ),
            "regions_monitored": len(result.get("regional_summary", [])),
        }

        # Cache the assembled result for 2 minutes
        _nh_cache["summary"] = (result, time.time())

        return jsonify(result)
    except Exception as e:
        log_error("network_health_summary", e)
        # Return stale cache on error if available
        cached = _nh_cache.get("summary")
        if cached:
            data, _ = cached
            data["_stale"] = True
            return jsonify(data)
        return jsonify({"error": str(e)}), 500


# ── Network Health PG Data (fast — Lakebase only) ────────────────────────

@network_bp.route("/api/network-health/pg-summary")
def network_health_pg_summary():
    """Fast PG-backed data: customer impact and network-linked repairs.

    Returns instantly from Lakebase — no SQL Warehouse dependency.
    """
    result = {}
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sr.region_name, sr.region_code,
                           COALESCE(cc.cnt, 0) AS customer_count,
                           COALESCE(wo_agg.active_wos, 0) AS active_work_orders,
                           COALESCE(wo_agg.active_repairs, 0) AS active_repairs
                    FROM field_service.service_regions sr
                    LEFT JOIN (
                        SELECT region_id, COUNT(*) AS cnt
                        FROM field_service.customers
                        GROUP BY region_id
                    ) cc ON cc.region_id = sr.region_id
                    LEFT JOIN (
                        SELECT region_id,
                               COUNT(*) AS active_wos,
                               COUNT(*) FILTER (WHERE category = 'repair') AS active_repairs
                        FROM field_service.work_orders
                        WHERE status NOT IN ('completed', 'cancelled')
                        GROUP BY region_id
                    ) wo_agg ON wo_agg.region_id = sr.region_id
                    ORDER BY customer_count DESC
                """)
                result["customer_impact"] = [{
                    "region": r[0],
                    "region_code": r[1],
                    "customers": r[2],
                    "active_wos": r[3],
                    "active_repairs": r[4],
                } for r in cur.fetchall()]

                cur.execute("""
                    SELECT wo.category, wo.priority, COUNT(*) as cnt,
                           COUNT(*) FILTER (WHERE wo.sla_met = false) as sla_breached
                    FROM field_service.work_orders wo
                    WHERE wo.category = 'repair'
                      AND wo.status NOT IN ('cancelled')
                      AND wo.created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
                    GROUP BY wo.category, wo.priority
                    ORDER BY cnt DESC
                """)
                result["network_work_orders"] = [{
                    "category": r[0], "priority": r[1],
                    "count": r[2], "sla_breached": r[3],
                } for r in cur.fetchall()]

        return jsonify(result)
    except Exception as e:
        log_error("network_health_pg_summary", e)
        return jsonify({"error": str(e)}), 500


# ── Skills Matrix ─────────────────────────────────────────────────────────

@network_bp.route("/api/skills/matrix")
def skills_matrix():
    """Return technician-skill matrix with certification data.

    Three queries:
    1. All skill types (categories, cert requirements)
    2. Technician-skill assignments with proficiency and expiration
    3. Skill gap analysis (which skills have fewest qualified techs)
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # All skill types
                cur.execute("""
                    /* page:skills/matrix:skill_types */
                    SELECT skill_id, skill_name, skill_category, certification_required
                    FROM field_service.skill_types
                    ORDER BY skill_category, skill_name
                """)
                skills = [
                    {"id": r[0], "name": r[1], "category": r[2], "cert_required": r[3]}
                    for r in cur.fetchall()
                ]

                # Technician-skill assignments with proficiency
                cur.execute("""
                    /* page:skills/matrix:assignments */
                    SELECT t.technician_id, t.first_name || ' ' || t.last_name as name,
                           t.certification_level, sr.region_name,
                           ts.skill_id, ts.proficiency_level,
                           ts.certified_at, ts.expires_at
                    FROM field_service.technician_skills ts
                    JOIN field_service.technicians t ON ts.technician_id = t.technician_id
                    JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    WHERE t.is_active = true
                    ORDER BY t.last_name, t.first_name, ts.skill_id
                """)
                assignments = []
                for r in cur.fetchall():
                    assignments.append({
                        "tech_id": r[0], "name": r[1], "cert_level": r[2],
                        "region": r[3], "skill_id": r[4], "proficiency": r[5],
                        "certified_at": str(r[6]) if r[6] else None,
                        "expires_at": str(r[7]) if r[7] else None,
                    })

                # Skill gap summary: which skills have fewest qualified techs
                cur.execute("""
                    /* page:skills/matrix:gaps */
                    SELECT st.skill_name, st.skill_category,
                           COUNT(ts.technician_id) as qualified_count,
                           COUNT(CASE WHEN ts.proficiency_level = 'expert' THEN 1 END) as expert_count,
                           COUNT(CASE WHEN ts.expires_at < CURRENT_DATE + INTERVAL '90 days' THEN 1 END) as expiring_soon
                    FROM field_service.skill_types st
                    LEFT JOIN field_service.technician_skills ts ON st.skill_id = ts.skill_id
                    GROUP BY st.skill_id, st.skill_name, st.skill_category
                    ORDER BY qualified_count ASC
                """)
                gaps = [
                    {"skill": r[0], "category": r[1], "qualified": r[2],
                     "experts": r[3], "expiring": r[4]}
                    for r in cur.fetchall()
                ]

        return jsonify({"skills": skills, "assignments": assignments, "gaps": gaps})
    except Exception as e:
        log_error("skills_matrix", e)
        return jsonify({"error": str(e)}), 500


# ── Predictive Maintenance ────────────────────────────────────────────────

@network_bp.route("/api/predictive-maintenance")
def predictive_maintenance_list():
    """Return predictive maintenance work orders (created by ML model scoring).

    These are work orders with subcategory='predictive_maintenance', sorted by
    the model's confidence_score descending so the highest-confidence
    predictions appear first.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:predictive_maintenance */
                    SELECT wo.work_order_id, wo.work_order_number, wo.title, wo.priority,
                           wo.status, wo.confidence_score, wo.description,
                           wo.created_at, wo.sla_due_at, wo.region_id,
                           sr.region_name
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.service_regions sr ON wo.region_id = sr.region_id
                    WHERE wo.subcategory = 'predictive_maintenance'
                    ORDER BY wo.confidence_score DESC NULLS LAST, wo.created_at DESC
                    LIMIT 50
                """)
                cols = [d[0] for d in cur.description]
                rows = [
                    {
                        cols[i]: (
                            str(r[i])
                            if r[i] is not None and not isinstance(r[i], (int, float, bool))
                            else r[i]
                        )
                        for i in range(len(cols))
                    }
                    for r in cur.fetchall()
                ]
                return jsonify({"work_orders": rows, "count": len(rows)})
    except Exception as e:
        log_error("predictive_maintenance", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Network Incidents (Correlated Alarms)
# ═══════════════════════════════════════════════════════════════════════════


@network_bp.route("/api/network-health/incidents")
def network_incidents():
    """Correlated network incidents — raw alarms grouped into root causes."""
    try:
        status_filter = request.args.get("status", "")
        limit = min(int(request.args.get("limit", 50)), 200)

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where = "WHERE 1=1"
                params: list = []
                if status_filter:
                    where += " AND status = %s"
                    params.append(status_filter)

                cur.execute(f"""
                    SELECT incident_id, incident_number, severity, classification,
                           status, region, root_cause, affected_nodes,
                           affected_customers, raw_alarm_count,
                           detected_at, resolved_at, mttr_minutes
                    FROM field_service.network_incidents
                    {where}
                    ORDER BY detected_at DESC
                    LIMIT %s
                """, params + [limit])

                incidents = [{
                    'incident_id': r[0],
                    'incident_number': r[1],
                    'severity': r[2],
                    'classification': r[3],
                    'status': r[4],
                    'region': r[5],
                    'root_cause': r[6],
                    'affected_nodes': r[7],
                    'affected_customers': r[8],
                    'raw_alarm_count': r[9],
                    'detected_at': str(r[10]) if r[10] else None,
                    'resolved_at': str(r[11]) if r[11] else None,
                    'mttr_minutes': r[12],
                } for r in cur.fetchall()]

                # Summary stats
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE status IN ('open', 'investigating')) as active,
                        COUNT(*) FILTER (WHERE severity = 'critical' AND status != 'resolved') as critical_active,
                        SUM(affected_customers) FILTER (WHERE status IN ('open', 'investigating')) as customers_affected,
                        ROUND(AVG(mttr_minutes) FILTER (WHERE mttr_minutes IS NOT NULL)) as avg_mttr
                    FROM field_service.network_incidents
                """)
                sr = cur.fetchone()
                summary = {
                    'active_incidents': sr[0] or 0,
                    'critical_active': sr[1] or 0,
                    'customers_affected': sr[2] or 0,
                    'avg_mttr_minutes': int(sr[3]) if sr[3] else 0,
                }

        return jsonify({'incidents': incidents, 'summary': summary})
    except Exception as e:
        log_error("network_incidents", e)
        return jsonify({'error': str(e)}), 500

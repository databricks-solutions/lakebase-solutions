"""
SLA Analytics + SLA Engine + Technician API Blueprint.

This blueprint serves the Analytics page (SLA compliance, breach drilldown),
the SLA Engine panel (risk heatmap, leaderboard, live risk, matview refresh),
and the Technician roster/detail endpoints.

Route groups
============
* ``/api/analytics/sla``       -- Cached SLA compliance breakdown (sampled at scale)
* ``/api/technicians/roster``  -- Technician roster with skills (cached, analytics pool)
* ``/api/technicians/<id>``    -- Technician detail with active orders and completions
* ``/api/sla/risk-heatmap``    -- Regional SLA risk from ``mv_regional_sla``
* ``/api/sla/leaderboard``     -- Technician performance from ``mv_technician_leaderboard``
* ``/api/sla/live-risk``       -- Real-time at-risk work orders (score >= 70)
* ``/api/sla/refresh``         -- On-demand materialized view refresh

Dependencies
------------
* ``shared.get_pool`` -- interactive PG pool (30 s timeout)
* ``shared.get_analytics_pool`` -- heavy analytics pool (300 s timeout)
* ``shared._get_or_refresh`` / ``shared._run_cached_query`` -- query caching
* ``shared.log_error`` -- ring-buffer error logger

NOTE: The technician detail endpoint references ``TELCO_INFRASTRUCTURE``,
``INFRA_TYPES``, and ``_haversine_km`` from the main ``app.py`` module for
nearby infrastructure lookups. These are injected at registration time via
``init_app()`` or are accessed through the app's module-level globals.
"""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request

from shared import (
    get_pool,
    get_analytics_pool,
    log_error,
    _get_or_refresh,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint definition
# ---------------------------------------------------------------------------
# We use two separate url_prefix values, so we register routes manually via
# the ``/api/analytics``, ``/api/sla``, and ``/api/technicians`` paths.
analytics_bp = Blueprint("analytics", __name__)


# ═══════════════════════════════════════════════════════════════════════════
# SLA Analytics (cached, sampled for 5M-scale performance)
# ═══════════════════════════════════════════════════════════════════════════

def _compute_analytics_sla():
    """SLA analytics using sampled queries for live accuracy at scale.

    Uses ``TABLESAMPLE SYSTEM(1)`` for statistically representative results
    without scanning 5M work_orders. Materialized views are used only for risk
    scores. Scale factors are computed against ``pg_class.reltuples`` for
    instant approximate totals.
    """
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Approximate total from pg_class statistics (instant, no scan)
            cur.execute("""
                /* page:analytics/sla:approx_counts */
                SELECT reltuples::bigint FROM pg_class
                WHERE relname = 'work_orders' AND relnamespace = (
                    SELECT oid FROM pg_namespace WHERE nspname = 'field_service'
                )
            """)
            approx_total = cur.fetchone()[0]

            # Priority breakdown -- sample 1% for speed
            cur.execute("""
                /* page:analytics/sla:by_priority_sampled */
                SELECT priority,
                       COUNT(*) FILTER (WHERE sla_met = true) as met,
                       COUNT(*) FILTER (WHERE sla_met = false) as breached
                FROM field_service.work_orders TABLESAMPLE SYSTEM(1)
                WHERE status = 'completed' AND sla_met IS NOT NULL
                GROUP BY priority
            """)
            priority_order = {"critical": 1, "high": 2, "medium": 3, "low": 4}
            sampled_priority = cur.fetchall()
            sampled_total = sum(r[1] + r[2] for r in sampled_priority)
            # Scale sampled counts back to approximate totals
            scale_factor = approx_total * 0.9 / max(sampled_total, 1)
            approx_completed = int(approx_total * 0.9)

            by_priority = sorted([
                {
                    "priority": r[0],
                    "met": int(r[1] * scale_factor),
                    "breached": int(r[2] * scale_factor),
                    "rate": round(r[1] / max(r[1] + r[2], 1) * 100, 1),
                }
                for r in sampled_priority
            ], key=lambda x: priority_order.get(x["priority"], 5))

            # Overall compliance derived from sampled data (always fresh)
            total_sampled_met = sum(r[1] for r in sampled_priority)
            overall_rate = round(total_sampled_met / max(sampled_total, 1) * 100, 1)
            total_met = int(approx_completed * overall_rate / 100)
            total_breached = approx_completed - total_met

            compliance = {
                "met": total_met, "breached": total_breached,
                "pending": 0, "total": approx_completed,
                "rate": overall_rate,
            }

            # By region -- sampled 1% with region join
            cur.execute("""
                /* page:analytics/sla:by_region_sampled */
                SELECT sr.region_name,
                       COUNT(*) FILTER (WHERE wo.sla_met = true) as met,
                       COUNT(*) FILTER (WHERE wo.sla_met = false) as breached
                FROM field_service.work_orders wo TABLESAMPLE SYSTEM(1)
                JOIN field_service.service_regions sr ON wo.region_id = sr.region_id
                WHERE wo.status = 'completed' AND wo.sla_met IS NOT NULL
                GROUP BY sr.region_name ORDER BY sr.region_name
            """)
            by_region = [
                {
                    "region": r[0],
                    "met": int(r[1] * scale_factor),
                    "breached": int(r[2] * scale_factor),
                    "rate": round(r[1] / max(r[1] + r[2], 1) * 100, 1),
                }
                for r in cur.fetchall()
            ]

            # Resolution time by category -- sample 1%
            cur.execute("""
                /* page:analytics/sla:resolution_sampled */
                SELECT category,
                       ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600)::numeric, 1) as avg_hours
                FROM field_service.work_orders TABLESAMPLE SYSTEM(1)
                WHERE status = 'completed' AND resolved_at IS NOT NULL
                GROUP BY category ORDER BY avg_hours
            """)
            resolution_by_category = [{"category": r[0], "hours": float(r[1])} for r in cur.fetchall()]

            # Revenue at risk -- uses partial index on active SLA-breached orders
            cur.execute("""
                /* page:analytics/sla:revenue_at_risk */
                SELECT COALESCE(SUM(
                    sp.penalty_per_hour * LEAST(
                        GREATEST(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - wo.sla_due_at)) / 3600, 0),
                        720
                    )
                ), 0)::numeric(18,2)
                FROM field_service.work_orders wo
                JOIN field_service.sla_policies sp ON wo.sla_id = sp.sla_id
                WHERE wo.status NOT IN ('completed', 'cancelled')
                  AND wo.sla_due_at < CURRENT_TIMESTAMP
            """)
            revenue_at_risk = float(cur.fetchone()[0])

            # First-time fix rate by region (small table -- always fast)
            cur.execute("""
                /* page:analytics/sla:ftfr_by_region */
                SELECT sr.region_name,
                       ROUND(AVG(t.first_fix_rate)::numeric, 1) as avg_ftfr
                FROM field_service.technicians t
                JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                WHERE t.is_active = true AND t.first_fix_rate IS NOT NULL
                GROUP BY sr.region_name ORDER BY sr.region_name
            """)
            ftfr_by_region = [{"region": r[0], "rate": float(r[1])} for r in cur.fetchall()]

    return {
        "compliance": compliance, "by_region": by_region,
        "by_priority": by_priority, "revenue_at_risk": revenue_at_risk,
        "ftfr_by_region": ftfr_by_region, "resolution_by_category": resolution_by_category,
    }


@analytics_bp.route("/api/analytics/sla")
def analytics_sla():
    """Return cached SLA compliance data (background-refreshed every 45s)."""
    try:
        data = _get_or_refresh("analytics_sla", _compute_analytics_sla)
        return jsonify(data)
    except Exception as e:
        log_error("analytics_sla", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Technician Roster (cached, analytics pool)
# ═══════════════════════════════════════════════════════════════════════════

def _compute_technicians_roster():
    """Heavy query: technician roster with skills (analytics pool, 300s timeout).

    Split into two fast queries instead of one slow join against 5M work_orders:
    1. Base technician info with region join
    2. Skills aggregation from the small technician_skills table
    """
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Base technician data
            cur.execute("""
                /* page:technicians/roster:base */
                SELECT t.technician_id, t.employee_id, t.first_name, t.last_name,
                       t.status, t.shift, t.certification_level,
                       t.avg_rating, t.first_fix_rate,
                       t.jobs_completed_mtd, t.jobs_completed_ytd,
                       sr.region_name
                FROM field_service.technicians t
                JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                WHERE t.is_active = true
                ORDER BY t.last_name, t.first_name
            """)
            techs = []
            tech_ids = []
            for r in cur.fetchall():
                tech_ids.append(r[0])
                techs.append({
                    "id": r[0], "employee_id": r[1], "name": f"{r[2]} {r[3]}",
                    "first_name": r[2], "last_name": r[3],
                    "status": r[4], "shift": r[5], "cert_level": r[6],
                    "rating": float(r[7]) if r[7] else None,
                    "ftfr": float(r[8]) if r[8] else None,
                    "jobs_mtd": r[9], "jobs_ytd": r[10],
                    "region": r[11], "skills": "", "active_orders": 0,
                })

            # Skills aggregation (small tables -- always fast)
            if tech_ids:
                cur.execute("""
                    /* page:technicians/roster:skills */
                    SELECT ts.technician_id, string_agg(st.skill_name, ', ' ORDER BY st.skill_name)
                    FROM field_service.technician_skills ts
                    JOIN field_service.skill_types st ON ts.skill_id = st.skill_id
                    GROUP BY ts.technician_id
                """)
                skill_map = {r[0]: r[1] for r in cur.fetchall()}
                for t in techs:
                    t["skills"] = skill_map.get(t["id"], "")

            return {"technicians": techs}


@analytics_bp.route("/api/technicians/roster")
def technicians_roster():
    """Return cached technician roster (60s TTL, background refresh)."""
    try:
        data = _get_or_refresh("technicians_roster", _compute_technicians_roster, ttl=60)
        return jsonify(data)
    except Exception as e:
        log_error("technicians_roster", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Technician Detail
# ═══════════════════════════════════════════════════════════════════════════

@analytics_bp.route("/api/technicians/<int:tech_id>")
def technician_detail(tech_id):
    """Full technician profile with skills, active orders, completions, and performance.

    Also includes nearby infrastructure (within 50 km) when the technician
    has GPS coordinates and infrastructure data is available from app.py globals.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Set tight timeout — this endpoint must be fast for map clicks
                cur.execute("SET LOCAL statement_timeout = '5000'")  # 5s max

                # Core tech info with region
                cur.execute("""
                    /* page:technicians/detail */
                    SELECT t.technician_id, t.employee_id, t.first_name, t.last_name,
                           t.status, t.shift, t.certification_level, t.phone, t.email,
                           t.avg_rating, t.first_fix_rate,
                           t.jobs_completed_mtd, t.jobs_completed_ytd,
                           t.current_latitude, t.current_longitude,
                           sr.region_name, t.hire_date
                    FROM field_service.technicians t
                    JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    WHERE t.technician_id = %s
                """, (tech_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Technician not found"}), 404

                tech = {
                    "id": row[0], "employee_id": row[1],
                    "name": f"{row[2]} {row[3]}", "first_name": row[2], "last_name": row[3],
                    "status": row[4], "shift": row[5], "cert_level": row[6],
                    "phone": row[7], "email": row[8],
                    "rating": float(row[9]) if row[9] else None,
                    "ftfr": float(row[10]) if row[10] else None,
                    "jobs_mtd": row[11], "jobs_ytd": row[12],
                    "lat": float(row[13]) if row[13] else None,
                    "lng": float(row[14]) if row[14] else None,
                    "region": row[15],
                    "hire_date": str(row[16]) if row[16] else None,
                }

                # Skills with proficiency levels
                cur.execute("""
                    /* page:technicians/detail:skills */
                    SELECT st.skill_name, ts.proficiency_level
                    FROM field_service.technician_skills ts
                    JOIN field_service.skill_types st ON ts.skill_id = st.skill_id
                    WHERE ts.technician_id = %s ORDER BY st.skill_name
                """, (tech_id,))
                tech["skills"] = [{"name": r[0], "level": r[1]} for r in cur.fetchall()]

                # Active work orders (with location for map display)
                cur.execute("""
                    /* page:technicians/detail:active_orders */
                    SELECT wo.work_order_id, wo.work_order_number, wo.status, wo.priority,
                           wo.category, wo.title, wo.sla_due_at,
                           c.first_name || ' ' || c.last_name as customer_name,
                           wo.address_line1, wo.city, wo.state_province,
                           wo.latitude, wo.longitude
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.customers c ON wo.customer_id = c.customer_id
                    WHERE wo.assigned_technician_id = %s
                      AND wo.status NOT IN ('completed', 'cancelled')
                    ORDER BY CASE wo.priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                             WHEN 'medium' THEN 3 ELSE 4 END
                """, (tech_id,))
                tech["active_orders"] = [{
                    "id": r[0], "number": r[1], "status": r[2],
                    "priority": r[3], "category": r[4], "title": r[5],
                    "sla_due": str(r[6]) if r[6] else None,
                    "customer": r[7],
                    "address": ", ".join(filter(None, [r[8], r[9], r[10]])),
                    "lat": float(r[11]) if r[11] else None,
                    "lng": float(r[12]) if r[12] else None,
                } for r in cur.fetchall()]

                # Recent completions (last 10)
                cur.execute("""
                    /* page:technicians/detail:completions */
                    SELECT wo.work_order_id, wo.work_order_number, wo.priority,
                           wo.category, wo.resolved_at,
                           a.customer_rating, a.customer_feedback
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.appointments a ON a.work_order_id = wo.work_order_id
                    WHERE wo.assigned_technician_id = %s AND wo.status = 'completed'
                    ORDER BY wo.resolved_at DESC NULLS LAST LIMIT 10
                """, (tech_id,))
                tech["recent_completions"] = [{
                    "id": r[0], "number": r[1], "priority": r[2],
                    "category": r[3], "resolved_at": str(r[4]) if r[4] else None,
                    "rating": float(r[5]) if r[5] else None,
                    "feedback": r[6],
                } for r in cur.fetchall()]

                # Avg resolution hours this month
                cur.execute("""
                    /* page:technicians/detail:avg_resolution */
                    SELECT ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600)::numeric, 1)
                    FROM field_service.work_orders
                    WHERE assigned_technician_id = %s AND status = 'completed'
                      AND resolved_at >= CURRENT_DATE - INTERVAL '30 days'
                """, (tech_id,))
                r = cur.fetchone()
                tech["avg_resolution_hours"] = float(r[0]) if r and r[0] else None

        # Nearby infrastructure (from app.py in-memory data)
        # These globals are defined in app.py and available at import time
        try:
            from flask import current_app
            # Access the map module globals via the app's TELCO_INFRASTRUCTURE
            import app as main_app
            TELCO_INFRASTRUCTURE = getattr(main_app, "TELCO_INFRASTRUCTURE", [])
            INFRA_TYPES = getattr(main_app, "INFRA_TYPES", {})
            _haversine_km = getattr(main_app, "_haversine_km", None)

            if tech.get("lat") and tech.get("lng") and TELCO_INFRASTRUCTURE and _haversine_km:
                nearby = []
                for item in TELCO_INFRASTRUCTURE:
                    dist = _haversine_km(tech["lat"], tech["lng"], item["lat"], item["lng"])
                    if dist < 50:  # within 50km
                        itype = INFRA_TYPES.get(item["type"], {})
                        nearby.append({
                            "id": item["id"], "name": item["name"],
                            "type_label": itype.get("label", item["type"]),
                            "status": item["status"], "distance_km": round(dist, 1),
                            "lat": item["lat"], "lng": item["lng"],
                            "region": item["region"],
                        })
                nearby.sort(key=lambda x: x["distance_km"])
                tech["nearby_infrastructure"] = nearby[:5]
        except Exception:
            pass  # Infrastructure data not available -- skip silently

        return jsonify(tech)
    except Exception as e:
        log_error("technician_detail", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# SLA Engine (Materialized Views + Live Risk)
# ═══════════════════════════════════════════════════════════════════════════

@analytics_bp.route("/api/sla/risk-heatmap")
def sla_risk_heatmap():
    """Regional SLA risk heatmap from the ``mv_regional_sla`` materialized view."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:sla/risk_heatmap */
                    SELECT region_id, region_name, open_orders, critical_risk,
                           high_risk, medium_risk, low_risk, avg_risk_score,
                           overall_sla_pct
                    FROM field_service.mv_regional_sla
                    ORDER BY avg_risk_score DESC
                """)
                rows = cur.fetchall()
                regions = [{
                    "region_id": r[0], "region_name": r[1],
                    "open_orders": r[2], "critical_risk": r[3],
                    "high_risk": r[4], "medium_risk": r[5], "low_risk": r[6],
                    "avg_risk_score": float(r[7]) if r[7] else 0,
                    "overall_sla_pct": float(r[8]) if r[8] else 0,
                } for r in rows]
        return jsonify({"regions": regions})
    except Exception as e:
        log_error("sla_risk_heatmap", e)
        return jsonify({"error": str(e)}), 500


@analytics_bp.route("/api/sla/leaderboard")
def sla_leaderboard():
    """Technician performance leaderboard from ``mv_technician_leaderboard``."""
    try:
        limit = min(int(request.args.get("limit", 20)), 200)
        sort_by = request.args.get("sort", "sla_pct")
        allowed_sorts = {"sla_pct", "completed_count", "avg_resolution_hours"}
        if sort_by not in allowed_sorts:
            sort_by = "sla_pct"  # safe default
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    /* page:sla/leaderboard */
                    SELECT technician_id, name, region_id, region_name,
                           completed_count, sla_met_count, avg_resolution_hours, sla_pct
                    FROM field_service.mv_technician_leaderboard
                    WHERE completed_count > 0
                    ORDER BY {sort_by} DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                techs = [{
                    "technician_id": r[0], "name": r[1], "region_id": r[2],
                    "region_name": r[3], "completed_count": r[4],
                    "sla_met_count": r[5],
                    "avg_resolution_hours": float(r[6]) if r[6] else None,
                    "sla_pct": float(r[7]) if r[7] else None,
                } for r in rows]
        return jsonify({"leaderboard": techs, "sort_by": sort_by})
    except Exception as e:
        log_error("sla_leaderboard", e)
        return jsonify({"error": str(e)}), 500


@analytics_bp.route("/api/sla/live-risk")
def sla_live_risk():
    """Live SLA risk scores -- real-time from work_orders (not matview).

    Returns active work orders with ``sla_risk_score >= 70``, ordered by
    descending risk score. This provides the "at risk right now" view.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:sla/live_risk */
                    SELECT wo.work_order_id, wo.work_order_number, wo.priority,
                           wo.status, wo.sla_risk_score, wo.sla_hours_remaining,
                           wo.sla_due_at, wo.category,
                           r.region_name,
                           t.first_name || ' ' || t.last_name AS tech_name
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.service_regions r ON wo.region_id = r.region_id
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    WHERE wo.status NOT IN ('completed', 'cancelled')
                      AND wo.sla_risk_score >= 70
                    ORDER BY wo.sla_risk_score DESC, wo.sla_hours_remaining ASC
                    LIMIT 50
                """)
                rows = cur.fetchall()
                at_risk = [{
                    "work_order_id": r[0], "work_order_number": r[1],
                    "priority": r[2], "status": r[3],
                    "sla_risk_score": r[4],
                    "sla_hours_remaining": float(r[5]) if r[5] else None,
                    "sla_due_at": r[6].isoformat() if r[6] else None,
                    "category": r[7], "region_name": r[8], "tech_name": r[9],
                } for r in rows]
        return jsonify({"at_risk_orders": at_risk, "count": len(at_risk)})
    except Exception as e:
        log_error("sla_live_risk", e)
        return jsonify({"error": str(e)}), 500


@analytics_bp.route("/api/sla/refresh", methods=["POST"])
def sla_refresh_matviews():
    """Refresh SLA materialized views on demand.

    Calls the ``field_service.refresh_sla_matviews()`` SECURITY DEFINER function
    which runs as the admin role, allowing the app role to trigger refreshes.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT field_service.refresh_sla_matviews()")
                conn.commit()
        return jsonify({"success": True, "message": "Materialized views refreshed"})
    except Exception as e:
        log_error("sla_refresh", e)
        return jsonify({"error": str(e)}), 500

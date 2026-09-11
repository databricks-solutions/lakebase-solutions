"""
Dispatch Board API — Flask Blueprint
=====================================

All endpoints that power the Dispatch Board page:
  - Summary KPIs (cached, background-refreshed)
  - Recent work orders with customer/tech joins
  - SLA urgency queue with time-remaining calculation
  - Technician roster with active order counts
  - Technician list with assignment counts
  - Customer search (for new WO creation)
  - Work order creation with auto-SLA and skill mapping
  - Auto-assign unassigned WOs using skill+rating ranking
  - Available techs with skill match scoring
  - Category/subcategory listing

Data structures defined here:
  - CATEGORY_SKILL_MAP: maps (category, subcategory) -> skill_type_id
  - SLA_DEFAULTS: maps priority -> {sla_id, hours}
  - REGION_COORDS: maps region_id -> (lat, lng) for coordinate generation

Usage in app.py:
    from routes.dispatch import dispatch_bp
    app.register_blueprint(dispatch_bp)
"""

import json
import logging
import random
import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, jsonify, request

from shared import (
    get_pool,
    get_analytics_pool,
    log_error,
    validate_identifier,
    _get_or_refresh,
    get_active_wo_counts_by_tech,
    get_user_role_and_regions,
    mask_phone,
    GENIE_SPACES,
)

log = logging.getLogger(__name__)

dispatch_bp = Blueprint("dispatch", __name__)


# ── Fleet vehicle health awareness ────────────────────────────────────────
# The fleet predictive-maintenance feature (data/fleet_management.sql) adds a
# fleet_vehicles table linking each technician's vehicle health to dispatch.
# Guard on its presence so the dispatch board keeps working on workspaces
# where the fleet feature hasn't been deployed yet. Cached for the process
# lifetime (schema changes require a redeploy).
_fleet_tables_present_cache: bool | None = None


def _fleet_tables_present(cur) -> bool:
    """True if field_service.fleet_vehicles exists (cached)."""
    global _fleet_tables_present_cache
    if _fleet_tables_present_cache is None:
        try:
            cur.execute("SELECT to_regclass('field_service.fleet_vehicles') IS NOT NULL")
            _fleet_tables_present_cache = bool(cur.fetchone()[0])
        except Exception:
            _fleet_tables_present_cache = False
    return _fleet_tables_present_cache


# ── Region coordinate lookup ────────────────────────────────────────────
# Used by create-work-order to generate lat/lng from the customer's region.
REGION_COORDS = {
    1: (47.6, -122.33),   # Pacific Northwest (Seattle)
    2: (33.45, -112.07),  # Southwest (Phoenix)
    3: (32.78, -96.80),   # South Central (Dallas)
    4: (33.75, -84.39),   # Southeast (Atlanta)
    5: (41.88, -87.63),   # Midwest (Chicago)
    6: (40.71, -74.01),   # Northeast (NYC)
    7: (39.74, -104.99),  # Mountain West (Denver)
    8: (37.77, -122.42),  # Bay Area (San Francisco)
}


# ── Category → Skill mapping ───────────────────────────────────────────
# Maps (category, subcategory) to the skill_type_id a technician should have.
# Used by create-work-order to set required_skill_id and by auto-assign to
# derive the skill when work orders lack one.
CATEGORY_SKILL_MAP = {
    ('install', 'fiber_install'): 1,           # Fiber Optic Installation
    ('install', 'ont_setup'): 15,              # ONT Configuration
    ('install', 'router_install'): 16,         # Router Configuration
    ('install', 'copper_install'): 2,          # Copper Line Installation
    ('install', 'satellite_install'): 4,       # Satellite Dish Installation
    ('install', 'fixed_wireless_install'): 5,  # 5G Small Cell Installation
    ('repair', 'no_service'): 8,               # Network Troubleshooting
    ('repair', 'no_signal'): 8,
    ('repair', 'slow_speed'): 8,
    ('repair', 'intermittent'): 8,
    ('repair', 'intermittent_connection'): 8,
    ('repair', 'equipment_failure'): 9,        # CPE Troubleshooting
    ('repair', 'fiber_cut'): 6,                # Fiber Optic Repair
    ('repair', 'noise_on_line'): 7,            # Copper Line Repair
    ('repair', 'line_damage'): 7,
    ('maintenance', 'firmware_update'): 11,    # Preventive Maintenance
    ('maintenance', 'line_test'): 11,
    ('maintenance', 'preventive'): 11,
    ('upgrade', 'speed_upgrade'): 8,           # Network Troubleshooting
    ('upgrade', 'equipment_upgrade'): 9,       # CPE Troubleshooting
    ('upgrade', 'service_tier_change'): 16,    # Router Configuration
    ('disconnect', 'service_disconnect'): 9,
}


# ── SLA defaults by priority ───────────────────────────────────────────
# Each priority maps to an SLA policy id and the number of hours until breach.
SLA_DEFAULTS = {
    'emergency': {'sla_id': 1, 'hours': 4},
    'high':      {'sla_id': 2, 'hours': 8},
    'medium':    {'sla_id': 3, 'hours': 24},
    'low':       {'sla_id': 4, 'hours': 48},
}


# ── Heavy query: dispatch summary ──────────────────────────────────────
def _compute_dispatch_summary():
    """Compute the full dispatch summary using the analytics pool.

    Split into targeted queries instead of a full-table GROUP BY:
      1. Active WOs only — uses idx_wo_active partial index (~2-5% of table)
      2. Today's completions — small result set (resolved_at >= today)
      3. Technician availability (small table, always fast)
      4. Active WOs by region — uses idx_wo_active_region partial index

    The analytics pool (300 s timeout) is used because aggregations at
    the 5M work-order scale can still take a few seconds even with indexes.
    """
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # ── 1. Active WOs only (uses partial index, ~100K-200K rows) ──
            cur.execute("""
                /* page:dispatch/summary:active */
                SELECT
                    status,
                    category,
                    COUNT(*) as cnt,
                    COUNT(*) FILTER (
                        WHERE sla_due_at IS NOT NULL
                          AND sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours'
                          AND sla_due_at > CURRENT_TIMESTAMP
                    ) as sla_at_risk,
                    COUNT(*) FILTER (
                        WHERE sla_due_at IS NOT NULL AND sla_due_at < CURRENT_TIMESTAMP
                    ) as sla_breached
                FROM field_service.work_orders
                WHERE status NOT IN ('completed', 'cancelled')
                GROUP BY status, category
            """)
            rows = cur.fetchall()

            status_counts = {}
            sla_at_risk = 0
            sla_breached = 0
            by_category = {}
            for row in rows:
                st, cat, cnt, at_risk, breached = row
                status_counts[st] = status_counts.get(st, 0) + cnt
                sla_at_risk += at_risk
                sla_breached += breached
                by_category[cat] = by_category.get(cat, 0) + cnt

            # ── 2. Today's completions (small set — only today's resolved) ──
            cur.execute("""
                /* page:dispatch/summary:completions_today */
                SELECT
                    COUNT(*) as completed_today,
                    COALESCE(
                        ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600)::numeric, 1),
                        0
                    ) as avg_completion_hours,
                    COUNT(*) FILTER (WHERE sla_met = true) as sla_met_today,
                    COUNT(*) FILTER (WHERE sla_met IS NOT NULL) as sla_total_today
                FROM field_service.work_orders
                WHERE status = 'completed' AND resolved_at >= CURRENT_DATE
            """)
            comp_row = cur.fetchone()
            completed_today = comp_row[0] or 0
            avg_completion_hours = float(comp_row[1] or 0)
            sla_met_today = comp_row[2] or 0
            sla_total_today = comp_row[3] or 0
            sla_compliance_pct = round((sla_met_today / max(sla_total_today, 1)) * 100, 1)

            # Add completed + cancelled counts using reltuples estimate
            # (avoids scanning 4.8M completed rows just for a count)
            cur.execute("""
                /* page:dispatch/summary:total_estimate */
                SELECT GREATEST(reltuples::bigint, 0)
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'field_service' AND c.relname = 'work_orders'
            """)
            total_estimate = cur.fetchone()[0] or 0
            active_total = sum(status_counts.values())
            # completed + cancelled = total - active
            status_counts['completed'] = max(total_estimate - active_total, 0)

            # ── 3. Technician availability (small table, always fast) ──
            cur.execute("""
                /* page:dispatch/summary:tech_status */
                SELECT status, COUNT(*) as cnt
                FROM field_service.technicians
                GROUP BY status
            """)
            tech_counts = {row[0]: row[1] for row in cur.fetchall()}

            # ── 4. Active work orders by region ──
            cur.execute("""
                /* page:dispatch/summary:by_region */
                SELECT sr.region_name, COUNT(*) as cnt
                FROM field_service.work_orders wo
                JOIN field_service.service_regions sr ON wo.region_id = sr.region_id
                WHERE wo.status NOT IN ('completed', 'cancelled')
                GROUP BY sr.region_name
                ORDER BY cnt DESC
            """)
            by_region = {row[0]: row[1] for row in cur.fetchall()}

    return {
        'work_orders': status_counts,
        'technicians': tech_counts,
        'sla': {'at_risk': sla_at_risk, 'breached': sla_breached},
        'today': {
            'completed': completed_today,
            'avg_completion_hours': avg_completion_hours,
            'sla_compliance_pct': sla_compliance_pct,
        },
        'by_category': by_category,
        'by_region': by_region,
    }


# ═════════════════════════════════════════════════════════════════════════
# Routes
# ═════════════════════════════════════════════════════════════════════════


@dispatch_bp.route('/api/dispatch/summary')
def dispatch_summary():
    """Get dispatch board summary (cached, background-refreshed)."""
    try:
        data = _get_or_refresh('dispatch_summary', _compute_dispatch_summary)
        return jsonify(data)
    except Exception as e:
        log_error("dispatch_summary", e)
        return jsonify({'error': str(e)}), 500


@dispatch_bp.route('/api/dispatch/recent-orders')
def dispatch_recent_orders():
    """Get recent work orders with customer and technician details.

    Query params:
      - limit (int, default 25, max 100)
      - status (str, optional filter)
    """
    try:
        limit = min(int(request.args.get('limit', 25)), 100)
        status_filter = request.args.get('status', '')
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where_clause = ""
                params = []
                if status_filter:
                    where_clause = "WHERE wo.status = %s"
                    params.append(status_filter)

                cur.execute(f"""
                    /* page:dispatch/recent_orders */
                    SELECT
                        wo.work_order_id, wo.status, wo.priority, wo.category,
                        wo.subcategory, wo.reported_issue,
                        c.first_name || ' ' || c.last_name as customer_name,
                        c.city, c.state_province,
                        COALESCE(t.first_name || ' ' || t.last_name, 'Unassigned') as technician_name,
                        wo.sla_due_at,
                        wo.created_at,
                        sr.region_name,
                        wo.appointment_window_start,
                        wo.appointment_window_end,
                        so.service_order_number
                    FROM field_service.work_orders wo
                    JOIN field_service.customers c ON wo.customer_id = c.customer_id
                    JOIN field_service.service_regions sr ON c.region_id = sr.region_id
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    LEFT JOIN field_service.service_orders so ON wo.service_order_id = so.service_order_id
                    {where_clause}
                    ORDER BY
                        CASE wo.priority
                            WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                            WHEN 'medium' THEN 2 ELSE 3
                        END,
                        wo.created_at DESC
                    LIMIT %s
                """, params + [limit])

                col_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                data = [dict(zip(col_names, [str(v) if v is not None else None for v in row])) for row in rows]

        return jsonify({'orders': data})
    except Exception as e:
        log_error("dispatch_recent_orders", e)
        return jsonify({'error': str(e)}), 500


@dispatch_bp.route('/api/dispatch/urgent-queue')
def dispatch_urgent_queue():
    """Get work orders sorted by SLA urgency -- dispatchers need this view.

    Query params:
      - region (str, optional)
      - category (str, optional)
      - limit (int, default 50, max 100)

    Computes minutes_remaining and sla_status (ok/at_risk/breached) for
    each order based on the current timestamp vs sla_due_at.
    """
    try:
        region_filter = request.args.get('region', '')
        category_filter = request.args.get('category', '')
        limit = min(int(request.args.get('limit', 50)), 100)

        # RBAC: restrict to user's assigned regions
        _role, _user_regions = get_user_role_and_regions()

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where_clauses = ["wo.status NOT IN ('completed', 'cancelled')"]
                params = []
                if region_filter:
                    where_clauses.append("sr.region_name = %s")
                    params.append(region_filter)
                elif _user_regions:
                    where_clauses.append("wo.region_id = ANY(%s)")
                    params.append(_user_regions)
                if category_filter:
                    where_clauses.append("wo.category = %s")
                    params.append(category_filter)

                where_sql = " AND ".join(where_clauses)

                cur.execute(f"""
                    /* page:dispatch_board/urgent_queue */
                    SELECT wo.work_order_id, wo.work_order_number, wo.title, wo.status,
                           wo.priority, wo.category, wo.subcategory,
                           wo.reported_issue,
                           wo.address_line1, wo.city, wo.state_province,
                           wo.sla_due_at,
                           EXTRACT(EPOCH FROM (wo.sla_due_at - CURRENT_TIMESTAMP)) / 60 as mins_remaining,
                           wo.created_at,
                           c.first_name || ' ' || c.last_name as customer_name,
                           c.phone as customer_phone,
                           t.first_name || ' ' || t.last_name as tech_name,
                           t.technician_id,
                           sr.region_name,
                           wo.resolution_notes,
                           wo.appointment_window_start,
                           wo.appointment_window_end,
                           wo.estimated_duration_min,
                           so.service_order_number
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.customers c ON wo.customer_id = c.customer_id
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    LEFT JOIN field_service.service_regions sr ON wo.region_id = sr.region_id
                    LEFT JOIN field_service.service_orders so ON wo.service_order_id = so.service_order_id
                    WHERE {where_sql}
                    ORDER BY
                        COALESCE(wo.appointment_window_end, wo.sla_due_at) ASC NULLS LAST,
                        CASE wo.priority
                            WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                            WHEN 'medium' THEN 2 ELSE 3
                        END
                    LIMIT %s
                """, params + [limit])

                orders = []
                for r in cur.fetchall():
                    mins = float(r[12]) if r[12] is not None else None
                    # Determine SLA status from minutes remaining
                    sla_status = 'ok'
                    if mins is not None:
                        if mins < 0:
                            sla_status = 'breached'
                        elif mins < 120:
                            sla_status = 'at_risk'

                    orders.append({
                        'work_order_id': r[0],
                        'work_order_number': r[1],
                        'title': r[2],
                        'status': r[3],
                        'priority': r[4],
                        'category': r[5],
                        'subcategory': r[6],
                        'reported_issue': r[7],
                        'address': f"{r[8] or ''}, {r[9] or ''}, {r[10] or ''}".strip(', '),
                        'sla_due_at': str(r[11]) if r[11] else None,
                        'minutes_remaining': round(mins, 1) if mins is not None else None,
                        'sla_status': sla_status,
                        'created_at': str(r[13]) if r[13] else None,
                        'customer_name': r[14],
                        'customer_phone': mask_phone(r[15]),
                        'tech_name': r[16],
                        'tech_id': r[17],
                        'region': r[18],
                        'resolution_notes': r[19],
                        'appointment_window_start': str(r[20]) if r[20] else None,
                        'appointment_window_end': str(r[21]) if r[21] else None,
                        'estimated_duration_min': r[22],
                        'service_order_number': r[23],
                    })

        return jsonify({'orders': orders, 'total': len(orders)})
    except Exception as e:
        log_error("dispatch_urgent_queue", e)
        return jsonify({'error': str(e)}), 500


@dispatch_bp.route('/api/dispatch/technician-roster')
def dispatch_technician_roster():
    """Get individual technician details for the dispatch board.

    Uses a LATERAL join to fetch each tech's highest-priority active work
    order without a correlated subquery per row.

    Query params:
      - region (str, optional filter)
    """
    try:
        region_filter = request.args.get('region', '')
        # Get cached active WO counts (shared across dispatch + map)
        wo_counts = get_active_wo_counts_by_tech()

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where_clause = ""
                params = []
                if region_filter:
                    where_clause = "AND sr.region_name = %s"
                    params.append(region_filter)

                # Fleet tie-in: surface each tech's vehicle health for a board badge.
                if _fleet_tables_present(cur):
                    fleet_select = (", fv.health_score AS veh_health, "
                                    "fv.risk_category AS veh_risk, fv.status AS veh_status")
                    fleet_join = ("LEFT JOIN field_service.fleet_vehicles fv "
                                  "ON fv.vehicle_id = t.vehicle_id")
                else:
                    fleet_select = ""
                    fleet_join = ""

                cur.execute(f"""
                    /* page:dispatch/technician_roster */
                    SELECT t.technician_id,
                           t.first_name || ' ' || t.last_name as name,
                           t.status,
                           t.certification_level,
                           sr.region_name,
                           t.phone,
                           -- Current active work order (if any)
                           cwo.work_order_number as current_wo,
                           cwo.title as current_wo_title,
                           cwo.priority as current_wo_priority,
                           cwo.category as current_wo_category
                           {fleet_select}
                    FROM field_service.technicians t
                    LEFT JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    {fleet_join}
                    LEFT JOIN LATERAL (
                        SELECT work_order_number, title, priority, category
                        FROM field_service.work_orders
                        WHERE assigned_technician_id = t.technician_id
                          AND status IN ('assigned', 'en_route', 'in_progress')
                        ORDER BY
                            CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                            WHEN 'medium' THEN 2 ELSE 3 END
                        LIMIT 1
                    ) cwo ON true
                    WHERE t.status != 'inactive' {where_clause}
                    ORDER BY
                        CASE t.status WHEN 'available' THEN 0 WHEN 'en_route' THEN 1
                        WHEN 'on_site' THEN 2 ELSE 3 END,
                        t.last_name
                """, params)

                techs = []
                for r in cur.fetchall():
                    tech_id = r[0]
                    tech = {
                        'technician_id': tech_id,
                        'name': r[1],
                        'status': r[2],
                        'certification_level': r[3],
                        'region': r[4],
                        'phone': r[5],
                        'current_wo': r[6],
                        'current_wo_title': r[7],
                        'current_wo_priority': r[8],
                        'current_wo_category': r[9],
                        'completed_today': 0,
                        'active_orders': wo_counts.get(tech_id, 0),
                    }
                    # Vehicle health badge (present only when fleet feature deployed)
                    if len(r) > 10:
                        tech['vehicle_health_score'] = float(r[10]) if r[10] is not None else None
                        tech['vehicle_risk_category'] = r[11]
                        tech['vehicle_status'] = r[12]
                    techs.append(tech)

        return jsonify({'technicians': techs, 'total': len(techs)})
    except Exception as e:
        log_error("dispatch_technician_roster", e)
        return jsonify({'error': str(e)}), 500


@dispatch_bp.route('/api/dispatch/technicians')
def dispatch_technicians():
    """Get technician list with current assignment counts."""
    try:
        # Get cached active WO counts (shared across dispatch + map)
        wo_counts = get_active_wo_counts_by_tech()

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:dispatch/technicians */
                    SELECT
                        t.technician_id, t.first_name, t.last_name,
                        t.employee_id, t.status, t.certification_level,
                        sr.region_name
                    FROM field_service.technicians t
                    JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    WHERE t.is_active = TRUE
                    ORDER BY t.status, t.last_name
                """)
                data = []
                for row in cur.fetchall():
                    tech_id = row[0]
                    data.append({
                        'technician_id': str(tech_id),
                        'first_name': row[1],
                        'last_name': row[2],
                        'employee_id': str(row[3]) if row[3] else None,
                        'status': row[4],
                        'certification_level': row[5],
                        'region_name': row[6],
                        'active_orders': str(wo_counts.get(tech_id, 0)),
                        'completed_today': '0',
                    })

        return jsonify({'technicians': data})
    except Exception as e:
        log_error("dispatch_technicians", e)
        return jsonify({'error': str(e)}), 500


# ── Customer search (used by dispatch for WO creation) ─────────────────

@dispatch_bp.route('/api/customers/search')
def customer_search():
    """Search customers by name, phone, or account number for WO creation.

    Query params:
      - q (str, min 2 chars): search term matched against first_name,
        last_name, phone, and email via ILIKE.

    Returns up to 20 matching active customers with region and address info.
    """
    try:
        q = request.args.get('q', '').strip()
        if len(q) < 2:
            return jsonify([])

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.customer_id, c.first_name || ' ' || c.last_name AS name,
                           c.phone, c.email,
                           COALESCE(c.address_line1, '') || ', ' || COALESCE(c.city, '') || ', ' || COALESCE(c.state_province, '') || ' ' || COALESCE(c.postal_code, '') AS service_address,
                           sr.region_name, c.region_id, c.service_type
                    FROM field_service.customers c
                    JOIN field_service.service_regions sr ON c.region_id = sr.region_id
                    WHERE c.account_status = 'active'
                      AND (
                        c.first_name ILIKE %s OR c.last_name ILIKE %s
                        OR c.phone ILIKE %s OR c.email ILIKE %s
                      )
                    ORDER BY c.last_name, c.first_name
                    LIMIT 20
                """, (f'%{q}%', f'%{q}%', f'%{q}%', f'%{q}%'))
                results = [{'id': r[0], 'name': r[1], 'phone': r[2], 'email': r[3],
                            'address': r[4], 'region': r[5], 'region_id': r[6],
                            'service_type': r[7]} for r in cur.fetchall()]
        return jsonify(results)
    except Exception as e:
        log_error("customer_search", e)
        return jsonify({'error': str(e)}), 500


# ── Work order creation ────────────────────────────────────────────────

@dispatch_bp.route('/api/dispatch/create-work-order', methods=['POST'])
def dispatch_create_work_order():
    """Create a new work order with auto-set SLA and required skill.

    JSON body:
      - customer_id (int, required)
      - category (str, default 'repair')
      - subcategory (str, default 'no_service')
      - priority (str, default 'medium')
      - title (str, optional -- auto-generated if blank)
      - description (str, optional)

    Uses CATEGORY_SKILL_MAP to determine required_skill_id and
    SLA_DEFAULTS to set the SLA deadline based on priority.
    """
    try:
        data = request.json or {}
        customer_id = data.get('customer_id')
        category = data.get('category', 'repair')
        subcategory = data.get('subcategory', 'no_service')
        priority = data.get('priority', 'medium')
        title = data.get('title', '')
        description = data.get('description', '')

        if not customer_id:
            return jsonify({'error': 'customer_id is required'}), 400

        # Determine required skill from category/subcategory
        skill_id = CATEGORY_SKILL_MAP.get((category, subcategory),
                   CATEGORY_SKILL_MAP.get((category, None), 8))

        # Determine SLA deadline from priority
        sla_info = SLA_DEFAULTS.get(priority, SLA_DEFAULTS['medium'])

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get customer info for region + address
                cur.execute("""
                    SELECT region_id, COALESCE(address_line1, '') || ', ' || COALESCE(city, '') || ', ' || COALESCE(state_province, '') AS service_address FROM field_service.customers
                    WHERE customer_id = %s
                """, (customer_id,))
                cust = cur.fetchone()
                if not cust:
                    return jsonify({'error': 'Customer not found'}), 404

                region_id = cust[0]

                # Generate coordinates from region center with jitter
                rc = REGION_COORDS.get(region_id, (39.8, -98.5))
                wo_lat = rc[0] + random.uniform(-0.25, 0.25)
                wo_lng = rc[1] + random.uniform(-0.25, 0.25)

                sla_due = datetime.now(timezone.utc) + timedelta(hours=sla_info['hours'])
                wo_number = f"WO-{int(time.time()*1000) % 10000000:07d}"

                if not title:
                    title = f"{category.title()} - {subcategory.replace('_', ' ').title()}"

                cur.execute("""
                    INSERT INTO field_service.work_orders (
                        work_order_number, customer_id, category, subcategory, priority,
                        status, title, reported_issue,
                        sla_id, sla_due_at, region_id, latitude, longitude,
                        required_skill_id
                    ) VALUES (%s, %s, %s, %s, %s, 'open', %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING work_order_id, work_order_number
                """, (wo_number, customer_id, category, subcategory, priority,
                      title, description or title,
                      sla_info['sla_id'], sla_due, region_id, wo_lat, wo_lng, skill_id))
                row = cur.fetchone()
                wo_id = row[0]

                # Log the creation event (late import to avoid circular dependency)
                import app as _app
                _app.log_event(conn, 'work_order.created', 'work_order', wo_id,
                               'Dispatch', {'category': category, 'subcategory': subcategory,
                                            'priority': priority, 'customer_id': customer_id,
                                            'required_skill_id': skill_id})
                conn.commit()

        return jsonify({'success': True, 'work_order_id': wo_id, 'number': row[1],
                        'required_skill_id': skill_id})
    except Exception as e:
        log_error("dispatch_create_wo", e)
        return jsonify({'error': str(e)}), 500


# ── Available techs with skill match scoring ───────────────────────────

@dispatch_bp.route('/api/dispatch/available-techs-with-skills')
def dispatch_available_techs_with_skills():
    """Get available technicians ranked by skill match for a work order.

    Query params:
      - wo_id (int, required): work order to match skills against.

    Returns techs in the same region, ordered by proficiency match then
    avg_rating descending. Each tech includes a skill_match field:
    expert > intermediate > basic > none.
    """
    try:
        wo_id = request.args.get('wo_id', type=int)
        if not wo_id:
            return jsonify({'error': 'wo_id required'}), 400

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.technician_id,
                           t.first_name || ' ' || t.last_name AS name,
                           sr.region_name,
                           t.certification_level,
                           t.avg_rating,
                           t.current_latitude, t.current_longitude,
                           ts.proficiency_level,
                           st.skill_name,
                           wo.required_skill_id
                    FROM field_service.technicians t
                    JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    CROSS JOIN (SELECT required_skill_id, region_id
                                FROM field_service.work_orders WHERE work_order_id = %s) wo
                    LEFT JOIN field_service.technician_skills ts
                        ON ts.technician_id = t.technician_id
                        AND ts.skill_id = wo.required_skill_id
                    LEFT JOIN field_service.skill_types st
                        ON st.skill_id = wo.required_skill_id
                    WHERE t.is_active = true AND t.status = 'available'
                      AND t.region_id = wo.region_id
                    ORDER BY
                        CASE WHEN ts.proficiency_level IS NOT NULL THEN 0 ELSE 1 END,
                        CASE ts.proficiency_level
                            WHEN 'expert' THEN 0
                            WHEN 'intermediate' THEN 1
                            WHEN 'basic' THEN 2
                            ELSE 3
                        END,
                        t.avg_rating DESC NULLS LAST
                """, (wo_id,))
                techs = []
                for r in cur.fetchall():
                    prof = r[7]
                    techs.append({
                        'id': r[0], 'name': r[1], 'region': r[2],
                        'cert': r[3],
                        'rating': float(r[4]) if r[4] else None,
                        'lat': float(r[5]) if r[5] else None,
                        'lng': float(r[6]) if r[6] else None,
                        'proficiency': prof,
                        'required_skill': r[8],
                        'skill_match': 'expert' if prof == 'expert'
                                       else 'intermediate' if prof == 'intermediate'
                                       else 'basic' if prof == 'basic'
                                       else 'none',
                    })
        return jsonify(techs)
    except Exception as e:
        log_error("dispatch_available_techs_skills", e)
        return jsonify({'error': str(e)}), 500


# ── Auto-assign unassigned work orders ─────────────────────────────────

@dispatch_bp.route('/api/dispatch/auto-assign', methods=['POST'])
def dispatch_auto_assign():
    """Auto-assign all unassigned work orders using skill+rating ranking.

    For each open/unassigned WO (ordered by priority then SLA urgency):
      1. Derive the required skill from CATEGORY_SKILL_MAP if not set.
      2. Find the best available tech in the same region by skill match
         then avg_rating.
      3. Assign the WO, mark the tech as en_route, add a system note.

    Returns the count of assigned and skipped (no available tech) orders.
    """
    try:
        pool = get_pool()
        assigned_count = 0
        skipped = 0

        # Fetch open WOs first (read-only, no lock contention)
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT work_order_id, region_id, required_skill_id, category, subcategory
                    FROM field_service.work_orders
                    WHERE status = 'open' AND assigned_technician_id IS NULL
                    ORDER BY
                        CASE priority
                            WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                            WHEN 'medium' THEN 2 ELSE 3
                        END,
                        sla_due_at ASC NULLS LAST
                """)
                open_wos = cur.fetchall()

        # Assign each WO in its own short transaction to avoid deadlocks
        # with the simulator (which also updates technicians concurrently)
        import app as _app
        for wo in open_wos:
            wo_id, region_id, req_skill, cat, subcat = wo
            if not req_skill:
                req_skill = CATEGORY_SKILL_MAP.get((cat, subcat), 8)

            for attempt in range(3):
                try:
                    with pool.connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT t.technician_id
                                FROM field_service.technicians t
                                LEFT JOIN field_service.technician_skills ts
                                    ON ts.technician_id = t.technician_id AND ts.skill_id = %s
                                WHERE t.is_active = true AND t.status = 'available'
                                  AND t.region_id = %s
                                ORDER BY
                                    CASE WHEN ts.proficiency_level IS NOT NULL THEN 0 ELSE 1 END,
                                    CASE ts.proficiency_level
                                        WHEN 'expert' THEN 0 WHEN 'intermediate' THEN 1
                                        WHEN 'basic' THEN 2 ELSE 3
                                    END,
                                    t.avg_rating DESC NULLS LAST
                                LIMIT 1
                            """, (req_skill, region_id))
                            tech_row = cur.fetchone()

                            if not tech_row:
                                skipped += 1
                                break

                            tech_id = tech_row[0]

                            cur.execute("""
                                UPDATE field_service.work_orders
                                SET status = 'assigned', assigned_technician_id = %s,
                                    required_skill_id = COALESCE(required_skill_id, %s),
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE work_order_id = %s
                            """, (tech_id, req_skill, wo_id))

                            cur.execute("""
                                UPDATE field_service.technicians
                                SET status = 'en_route', updated_at = CURRENT_TIMESTAMP
                                WHERE technician_id = %s
                            """, (tech_id,))

                            cur.execute("""
                                INSERT INTO field_service.work_order_notes
                                    (work_order_id, author, note_type, content)
                                VALUES (%s, 'AutoDispatch', 'system',
                                        'Auto-assigned based on skill match and technician rating.')
                            """, (wo_id,))

                            _app.log_event(conn, 'work_order.auto_assigned', 'work_order', wo_id,
                                           'AutoDispatch', {'technician_id': tech_id,
                                                            'required_skill_id': req_skill})
                        conn.commit()
                    assigned_count += 1
                    break  # success — move to next WO
                except Exception as e:
                    if 'deadlock' in str(e).lower() and attempt < 2:
                        import time
                        time.sleep(0.1 * (attempt + 1))
                        continue
                    if attempt == 2:
                        skipped += 1
                    else:
                        raise

        return jsonify({'success': True, 'assigned': assigned_count, 'skipped': skipped,
                        'message': f'Auto-assigned {assigned_count} work orders, {skipped} skipped (no available techs)'})
    except Exception as e:
        log_error("dispatch_auto_assign", e)
        return jsonify({'error': str(e)}), 500


# ── Category listing ───────────────────────────────────────────────────

@dispatch_bp.route('/api/dispatch/categories')
def dispatch_categories():
    """Return available work order categories and subcategories.

    Derives the category tree from CATEGORY_SKILL_MAP keys so the UI
    dropdown stays in sync with the skill mapping logic.
    """
    cats = {}
    for (cat, subcat) in CATEGORY_SKILL_MAP:
        if subcat:
            cats.setdefault(cat, []).append(subcat)
    # Deduplicate and sort
    for cat in cats:
        cats[cat] = sorted(set(cats[cat]))
    return jsonify(cats)


# ═════════════════════════════════════════════════════════════════════════
# Smart Dispatch Optimization
# ═════════════════════════════════════════════════════════════════════════


@dispatch_bp.route('/api/dispatch/smart-assign', methods=['POST'])
def dispatch_smart_assign():
    """Intelligent work order assignment using multi-factor scoring.

    Scores every open/unassigned WO against eligible technicians using:
      - Skill match (0-30 pts)
      - Geographic distance via haversine (0-25 pts)
      - Capacity utilization (0-20 pts)
      - SLA urgency (0-15 pts)
      - Technician rating (0-10 pts)

    Then assigns WOs to the highest-scoring tech with available capacity
    (max 8 active orders per tech).

    Query params:
      - region (str, optional): restrict to a specific region
      - limit (int, default 500): max WOs to process per batch
      - dry_run (bool, default false): score only, don't assign
    """
    try:
        region_filter = request.args.get('region', '')
        limit = min(int(request.args.get('limit', 500)), 2000)
        dry_run = request.args.get('dry_run', 'false').lower() == 'true'

        pool = get_analytics_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Resolve region_id if name provided
                region_id = None
                if region_filter:
                    cur.execute(
                        "SELECT region_id FROM field_service.service_regions WHERE region_name = %s",
                        (region_filter,)
                    )
                    row = cur.fetchone()
                    region_id = row[0] if row else None

                # Phase 1: Compute scores via PG function
                cur.execute(
                    "SELECT * FROM field_service.compute_dispatch_scores(%s, %s)",
                    (region_id, limit)
                )
                stats_row = cur.fetchone()
                scored_count = stats_row[0] if stats_row else 0
                wo_count = stats_row[1] if stats_row else 0
                tech_count = stats_row[2] if stats_row else 0
                avg_score = float(stats_row[3]) if stats_row and stats_row[3] else 0
                avg_distance = float(stats_row[4]) if stats_row and stats_row[4] else 0

        if dry_run:
            return jsonify({
                'mode': 'dry_run',
                'scored_pairs': scored_count,
                'work_orders': wo_count,
                'technicians': tech_count,
                'avg_score': avg_score,
                'avg_distance_km': avg_distance,
            })

        # Phase 2: Assign using ranked scores with capacity enforcement
        assigned = 0
        skipped = 0
        total_distance = 0
        total_score = 0
        assignments = []

        pool = get_pool()
        with pool.connection() as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # Fleet tie-in: a technician whose van is in the shop (pulled in by
                # predictive maintenance) has no working vehicle and is excluded from
                # assignment. Guarded so the board works before the fleet feature ships.
                if _fleet_tables_present(cur):
                    fleet_join = ("LEFT JOIN field_service.fleet_vehicles fv "
                                  "ON fv.vehicle_id = t.vehicle_id")
                    fleet_filter = "AND COALESCE(fv.status, 'active') <> 'in_shop'"
                else:
                    fleet_join = ""
                    fleet_filter = ""
                # Get ranked best-tech per WO, respecting capacity
                cur.execute(f"""
                    WITH tech_load AS (
                        SELECT assigned_technician_id, COUNT(*) as cnt
                        FROM field_service.work_orders
                        WHERE status NOT IN ('completed', 'cancelled')
                          AND assigned_technician_id IS NOT NULL
                        GROUP BY assigned_technician_id
                    ),
                    ranked AS (
                        SELECT ds.work_order_id, ds.technician_id,
                               ds.total_score, ds.distance_km,
                               ds.skill_score, ds.distance_score,
                               ds.capacity_score, ds.sla_score, ds.rating_score,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ds.work_order_id
                                   ORDER BY ds.total_score DESC
                               ) as rank
                        FROM field_service.dispatch_scores ds
                        JOIN field_service.technicians t ON ds.technician_id = t.technician_id
                        LEFT JOIN tech_load tl ON tl.assigned_technician_id = ds.technician_id
                        {fleet_join}
                        WHERE COALESCE(tl.cnt, 0) < COALESCE(t.max_active_orders, 8)
                        {fleet_filter}
                    )
                    SELECT work_order_id, technician_id, total_score, distance_km
                    FROM ranked
                    WHERE rank = 1
                    ORDER BY total_score DESC
                """)
                candidates = cur.fetchall()

                # Track capacity in-memory during assignment loop
                tech_capacity_used: dict[int, int] = {}

                for wo_id, tech_id, score, dist_km in candidates:
                    # Check in-memory capacity (accounts for assignments made this batch)
                    used = tech_capacity_used.get(tech_id, 0)
                    # Get max from DB (cached in the query, default 8)
                    if used >= 8:
                        skipped += 1
                        continue

                    # Assign
                    cur.execute("""
                        UPDATE field_service.work_orders
                        SET status = 'assigned',
                            assigned_technician_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE work_order_id = %s
                          AND status = 'open'
                          AND assigned_technician_id IS NULL
                    """, (tech_id, wo_id))

                    if cur.rowcount > 0:
                        # Update tech status
                        cur.execute("""
                            UPDATE field_service.technicians
                            SET status = CASE WHEN status = 'available' THEN 'en_route' ELSE status END
                            WHERE technician_id = %s
                        """, (tech_id,))

                        tech_capacity_used[tech_id] = used + 1
                        assigned += 1
                        total_distance += float(dist_km or 0)
                        total_score += float(score or 0)
                        assignments.append({
                            'work_order_id': wo_id,
                            'technician_id': tech_id,
                            'score': float(score),
                            'distance_km': round(float(dist_km or 0), 1),
                        })
                    else:
                        skipped += 1

                conn.commit()

        return jsonify({
            'assigned': assigned,
            'skipped': skipped,
            'scored_pairs': scored_count,
            'avg_score': round(total_score / max(assigned, 1), 1),
            'avg_distance_km': round(total_distance / max(assigned, 1), 1),
            'techs_used': len(tech_capacity_used),
            'assignments': assignments[:50],  # first 50 for UI display
        })
    except Exception as e:
        log_error("dispatch_smart_assign", e)
        return jsonify({'error': str(e)}), 500


@dispatch_bp.route('/api/dispatch/assignment-scores/<int:wo_id>')
def dispatch_assignment_scores(wo_id):
    """Return scored technician candidates for a specific work order.

    Shows the score breakdown (skill, distance, capacity, SLA, rating)
    for each eligible tech, powering the "Why this tech?" explainability panel.
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ds.technician_id,
                           t.first_name || ' ' || t.last_name as name,
                           t.status, t.certification_level,
                           sr.region_name,
                           ds.skill_score, ds.distance_score, ds.capacity_score,
                           ds.sla_score, ds.rating_score, ds.total_score,
                           ds.distance_km
                    FROM field_service.dispatch_scores ds
                    JOIN field_service.technicians t ON ds.technician_id = t.technician_id
                    JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    WHERE ds.work_order_id = %s
                    ORDER BY ds.total_score DESC
                    LIMIT 10
                """, (wo_id,))
                candidates = [{
                    'technician_id': r[0], 'name': r[1],
                    'status': r[2], 'cert_level': r[3], 'region': r[4],
                    'scores': {
                        'skill': float(r[5]), 'distance': float(r[6]),
                        'capacity': float(r[7]), 'sla': float(r[8]),
                        'rating': float(r[9]),
                    },
                    'total_score': float(r[10]),
                    'distance_km': round(float(r[11]), 1),
                } for r in cur.fetchall()]

        return jsonify({'work_order_id': wo_id, 'candidates': candidates})
    except Exception as e:
        log_error("dispatch_assignment_scores", e)
        return jsonify({'error': str(e)}), 500


@dispatch_bp.route('/api/dispatch/capacity-overview')
def dispatch_capacity_overview():
    """Per-technician workload utilization for the capacity dashboard."""
    try:
        wo_counts = get_active_wo_counts_by_tech()

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.technician_id,
                           t.first_name || ' ' || t.last_name as name,
                           t.status, sr.region_name,
                           COALESCE(t.max_active_orders, 8) as max_orders
                    FROM field_service.technicians t
                    JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    WHERE t.is_active = true
                    ORDER BY t.status, sr.region_name, t.last_name
                """)
                techs = []
                region_summary: dict[str, dict] = {}
                for r in cur.fetchall():
                    tech_id = r[0]
                    active = wo_counts.get(tech_id, 0)
                    max_orders = r[4]
                    utilization = round((active / max(max_orders, 1)) * 100, 1)
                    region = r[3]
                    techs.append({
                        'technician_id': tech_id,
                        'name': r[1],
                        'status': r[2],
                        'region': region,
                        'active_orders': active,
                        'max_orders': max_orders,
                        'utilization_pct': utilization,
                    })
                    # Aggregate by region
                    rs = region_summary.setdefault(region, {
                        'total_techs': 0, 'available': 0,
                        'total_active': 0, 'total_capacity': 0,
                    })
                    rs['total_techs'] += 1
                    if r[2] == 'available':
                        rs['available'] += 1
                    rs['total_active'] += active
                    rs['total_capacity'] += max_orders

        # Compute region utilization percentages
        regions = []
        for name, rs in sorted(region_summary.items()):
            rs['region'] = name
            rs['utilization_pct'] = round(
                (rs['total_active'] / max(rs['total_capacity'], 1)) * 100, 1
            )
            regions.append(rs)

        return jsonify({
            'technicians': techs[:200],  # limit for UI performance
            'regions': regions,
            'total_techs': len(techs),
            'avg_utilization': round(
                sum(t['utilization_pct'] for t in techs) / max(len(techs), 1), 1
            ),
        })
    except Exception as e:
        log_error("dispatch_capacity_overview", e)
        return jsonify({'error': str(e)}), 500

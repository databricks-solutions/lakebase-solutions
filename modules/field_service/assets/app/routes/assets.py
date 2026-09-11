"""
Assets, Inventory, and Events blueprint.

Purpose
=======
Handles equipment inventory management, parts reservation (ACID
transactions), reorder tracking, and the event-sourcing activity feed.
All queries hit the Lakebase PostgreSQL connection pool.

Routes (9)
==========
GET   /api/assets/summary                        Equipment summary (cached, analytics pool)
POST  /api/inventory/reserve                     Transactional parts reservation (ACID demo)
GET   /api/inventory/status                      Stock levels by region and equipment type
GET   /api/inventory/reorders                    Pending/recent reorder requests
GET   /api/events/feed                           Live activity feed (latest events)
GET   /api/events/entity/<entity_type>/<id>      Event history for a specific entity
GET   /api/events/stats                          Event counts by type over a time period
GET   /api/sla/risk-heatmap                      Regional SLA risk from materialized view
GET   /api/sla/leaderboard                       Technician performance leaderboard (matview)
GET   /api/sla/live-risk                         Real-time high-risk work orders
POST  /api/sla/refresh                           Refresh SLA materialized views on demand

Data sources
============
- Lakebase PostgreSQL (interactive pool)     equipment_inventory, equipment_catalog,
    service_regions, reorder_requests, events, work_order_parts, work_orders,
    mv_regional_sla, mv_technician_leaderboard
- Lakebase PostgreSQL (analytics pool)       Heavy aggregations for /api/assets/summary

Related files
=============
- app/templates/assets.html          Assets/inventory UI
- app/shared.py                      get_pool, get_analytics_pool, _get_or_refresh,
                                     log_error, _column_exists_cache, _column_exists_lock
- data/field_service_schema.sql      Table definitions and seed data
- data/lakebase_features.sql         Triggers, matviews, SLA engine
"""

import json
import logging

from flask import Blueprint, jsonify, request

from shared import (
    _get_or_refresh,
    get_analytics_pool,
    get_pool,
    log_error,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint creation
# ---------------------------------------------------------------------------

assets_bp = Blueprint("assets", __name__)


# ---------------------------------------------------------------------------
# Helper: log_event (event sourcing)
# ---------------------------------------------------------------------------


def _log_event(conn, event_type, entity_type, entity_id, actor="system", payload=None):
    """Log a structured event to the events table (event sourcing).

    This is a local helper that mirrors the log_event function in app.py.
    It writes directly to the events table within the provided connection.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO field_service.events
                    (event_type, entity_type, entity_id, actor, payload)
                VALUES (%s, %s, %s, %s, %s)
            """, (event_type, entity_type, str(entity_id), actor,
                  json.dumps(payload) if payload else None))
    except Exception as e:
        log.warning(f"Event log failed ({event_type}): {e}")


# ---------------------------------------------------------------------------
# Assets Summary (cached via analytics pool)
# ---------------------------------------------------------------------------


def _compute_assets_summary():
    """Heavy query: assets summary (analytics pool for 3M equipment rows).

    Computes status breakdown, category counts, regional distribution,
    total inventory value, and the 50 most recently added items.
    """
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:assets/summary:by_status */
                SELECT status, COUNT(*) FROM field_service.equipment_inventory
                GROUP BY status ORDER BY status
            """)
            by_status = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute("""
                /* page:assets/summary:by_category */
                SELECT ec.category, COUNT(*)
                FROM field_service.equipment_inventory ei
                JOIN field_service.equipment_catalog ec ON ei.equipment_type_id = ec.equipment_type_id
                GROUP BY ec.category ORDER BY COUNT(*) DESC
            """)
            by_category = [{"category": r[0], "count": r[1]} for r in cur.fetchall()]

            cur.execute("""
                /* page:assets/summary:by_region */
                SELECT sr.region_name, COUNT(*)
                FROM field_service.equipment_inventory ei
                JOIN field_service.service_regions sr ON ei.region_id = sr.region_id
                GROUP BY sr.region_name ORDER BY COUNT(*) DESC
            """)
            by_region = [{"region": r[0], "count": r[1]} for r in cur.fetchall()]

            cur.execute("""
                /* page:assets/summary:total_value */
                SELECT COALESCE(SUM(ec.unit_cost), 0)::numeric(12,2)
                FROM field_service.equipment_inventory ei
                JOIN field_service.equipment_catalog ec ON ei.equipment_type_id = ec.equipment_type_id
                WHERE ei.status IN ('in_stock', 'assigned', 'installed')
            """)
            total_value = float(cur.fetchone()[0])

            cur.execute("""
                /* page:assets/summary:recent */
                SELECT ei.inventory_id, ec.equipment_name, ec.manufacturer,
                       ei.serial_number, ei.status, ei.warehouse_location,
                       sr.region_name, ec.category
                FROM field_service.equipment_inventory ei
                JOIN field_service.equipment_catalog ec ON ei.equipment_type_id = ec.equipment_type_id
                JOIN field_service.service_regions sr ON ei.region_id = sr.region_id
                ORDER BY ei.inventory_id DESC LIMIT 50
            """)
            items = [
                {
                    "id": r[0], "name": r[1], "manufacturer": r[2],
                    "serial": r[3], "status": r[4], "warehouse": r[5],
                    "region": r[6], "category": r[7],
                }
                for r in cur.fetchall()
            ]

    return {
        "by_status": by_status, "by_category": by_category,
        "by_region": by_region, "total_value": total_value,
        "total": sum(by_status.values()), "items": items,
    }


# ── Assets Summary (cached, 60s TTL) ─────────────────────────────────────

@assets_bp.route("/api/assets/summary")
def assets_summary():
    try:
        data = _get_or_refresh("assets_summary", _compute_assets_summary, ttl=60)
        return jsonify(data)
    except Exception as e:
        log_error("assets_summary", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Inventory endpoints
# ---------------------------------------------------------------------------

# ── Transactional parts reservation (ACID demo) ──────────────────────────

@assets_bp.route("/api/inventory/reserve", methods=["POST"])
def inventory_reserve():
    """Transactional parts reservation for a work order.

    Demonstrates ACID transactions: SELECT...FOR UPDATE SKIP LOCKED to grab
    available stock, UPDATE inventory status, INSERT work_order_parts record,
    all within a single transaction.  Rolls back on insufficient stock.
    """
    try:
        data = request.json or {}
        wo_id = data.get("work_order_id")
        equipment_type_id = data.get("equipment_type_id")
        quantity = int(data.get("quantity", 1))
        if not wo_id or not equipment_type_id:
            return jsonify({"error": "work_order_id and equipment_type_id required"}), 400

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get the work order's region for regional stock lookup
                cur.execute("""
                    /* page:inventory/reserve:get_wo */
                    SELECT region_id, status FROM field_service.work_orders
                    WHERE work_order_id = %s
                """, (wo_id,))
                wo = cur.fetchone()
                if not wo:
                    return jsonify({"error": "Work order not found"}), 404
                region_id = wo[0]

                # Lock and select available inventory items in this region
                # FOR UPDATE SKIP LOCKED prevents blocking on concurrent reservations
                cur.execute("""
                    /* page:inventory/reserve:lock_stock */
                    SELECT inventory_id, serial_number
                    FROM field_service.equipment_inventory
                    WHERE equipment_type_id = %s
                      AND region_id = %s
                      AND status = 'in_stock'
                    ORDER BY inventory_id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (equipment_type_id, region_id, quantity))
                available = cur.fetchall()

                if len(available) < quantity:
                    conn.rollback()
                    return jsonify({
                        "error": "Insufficient stock",
                        "requested": quantity,
                        "available": len(available),
                        "region_id": region_id,
                    }), 409

                reserved_ids = []
                for inv_id, serial in available:
                    # Update inventory status to 'assigned'
                    cur.execute("""
                        /* page:inventory/reserve:assign */
                        UPDATE field_service.equipment_inventory
                        SET status = 'assigned',
                            assigned_technician_id = (
                                SELECT assigned_technician_id
                                FROM field_service.work_orders WHERE work_order_id = %s
                            ),
                            last_serviced_at = CURRENT_TIMESTAMP
                        WHERE inventory_id = %s
                    """, (wo_id, inv_id))

                    # Create work_order_parts record linking inventory to work order
                    cur.execute("""
                        /* page:inventory/reserve:add_part */
                        INSERT INTO field_service.work_order_parts
                            (work_order_id, inventory_id, quantity, action)
                        VALUES (%s, %s, 1, 'installed')
                    """, (wo_id, inv_id))
                    reserved_ids.append(inv_id)

                conn.commit()

                # Log the reservation event (separate commit for audit trail)
                _log_event(conn, "inventory.reserved", "work_order", wo_id,
                           "api", {"equipment_type_id": equipment_type_id,
                                   "quantity": quantity, "inventory_ids": reserved_ids,
                                   "region_id": region_id})
                conn.commit()

        return jsonify({
            "success": True,
            "reserved": len(reserved_ids),
            "inventory_ids": reserved_ids,
            "work_order_id": wo_id,
        })
    except Exception as e:
        log_error("inventory_reserve", e)
        return jsonify({"error": str(e)}), 500


# ── Stock levels by region and equipment type ─────────────────────────────

@assets_bp.route("/api/inventory/status")
def inventory_status():
    """Stock levels by region and equipment type, plus low-stock alerts."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:inventory/status:stock_levels */
                    SELECT r.region_name, r.region_id,
                           ec.equipment_name, ec.category, ei.equipment_type_id,
                           COUNT(*) FILTER (WHERE ei.status = 'in_stock') AS in_stock,
                           COUNT(*) FILTER (WHERE ei.status = 'assigned') AS assigned,
                           COUNT(*) FILTER (WHERE ei.status = 'installed') AS installed,
                           COUNT(*) FILTER (WHERE ei.status = 'defective') AS defective,
                           COUNT(*) AS total
                    FROM field_service.equipment_inventory ei
                    JOIN field_service.equipment_catalog ec ON ei.equipment_type_id = ec.equipment_type_id
                    JOIN field_service.service_regions r ON ei.region_id = r.region_id
                    GROUP BY r.region_name, r.region_id, ec.equipment_name, ec.category, ei.equipment_type_id
                    ORDER BY r.region_name, ec.category, ec.equipment_name
                """)
                rows = cur.fetchall()
                stock = [
                    {
                        "region_name": r[0], "region_id": r[1],
                        "equipment_name": r[2], "category": r[3],
                        "equipment_type_id": r[4],
                        "in_stock": r[5], "assigned": r[6],
                        "installed": r[7], "defective": r[8], "total": r[9],
                    }
                    for r in rows
                ]

                # Low stock alerts: items with fewer than 10 in stock
                cur.execute("""
                    /* page:inventory/status:low_stock */
                    SELECT r.region_name, ec.equipment_name, COUNT(*) as stock_count
                    FROM field_service.equipment_inventory ei
                    JOIN field_service.equipment_catalog ec ON ei.equipment_type_id = ec.equipment_type_id
                    JOIN field_service.service_regions r ON ei.region_id = r.region_id
                    WHERE ei.status = 'in_stock'
                    GROUP BY r.region_name, ec.equipment_name, ei.equipment_type_id, ei.region_id
                    HAVING COUNT(*) < 10
                    ORDER BY COUNT(*) ASC
                """)
                alerts = [
                    {"region": r[0], "equipment": r[1], "remaining": r[2]}
                    for r in cur.fetchall()
                ]

        return jsonify({"stock_levels": stock, "low_stock_alerts": alerts})
    except Exception as e:
        log_error("inventory_status", e)
        return jsonify({"error": str(e)}), 500


# ── Pending and recent reorder requests ───────────────────────────────────

@assets_bp.route("/api/inventory/reorders")
def inventory_reorders():
    """Pending and recent reorder requests (created by auto-reorder trigger)."""
    try:
        status_filter = request.args.get("status", "pending")
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:inventory/reorders */
                    SELECT rr.reorder_id, ec.equipment_name, ec.category,
                           r.region_name, rr.quantity, rr.status,
                           rr.requested_by, rr.created_at
                    FROM field_service.reorder_requests rr
                    JOIN field_service.equipment_catalog ec ON rr.equipment_type_id = ec.equipment_type_id
                    JOIN field_service.service_regions r ON rr.region_id = r.region_id
                    WHERE rr.status = %s
                    ORDER BY rr.created_at DESC
                    LIMIT 100
                """, (status_filter,))
                rows = cur.fetchall()
                reorders = [
                    {
                        "reorder_id": r[0], "equipment_name": r[1], "category": r[2],
                        "region_name": r[3], "quantity": r[4], "status": r[5],
                        "requested_by": r[6],
                        "created_at": r[7].isoformat() if r[7] else None,
                    }
                    for r in rows
                ]
        return jsonify({"reorders": reorders, "count": len(reorders)})
    except Exception as e:
        log_error("inventory_reorders", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Event Sourcing endpoints
# ---------------------------------------------------------------------------

# ── Live activity feed ────────────────────────────────────────────────────

@assets_bp.route("/api/events/feed")
def events_feed():
    """Live activity feed -- latest events across the system.

    Optional query params:
    - limit (int, max 200): number of events to return
    - type (str): filter by event_type
    """
    try:
        limit = min(int(request.args.get("limit", 50)), 200)
        event_type = request.args.get("type")
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                if event_type:
                    cur.execute("""
                        /* page:events/feed */
                        SELECT event_id, event_type, entity_type, entity_id,
                               actor, payload, created_at
                        FROM field_service.events
                        WHERE event_type = %s
                        ORDER BY created_at DESC LIMIT %s
                    """, (event_type, limit))
                else:
                    cur.execute("""
                        /* page:events/feed */
                        SELECT event_id, event_type, entity_type, entity_id,
                               actor, payload, created_at
                        FROM field_service.events
                        ORDER BY created_at DESC LIMIT %s
                    """, (limit,))
                rows = cur.fetchall()
                events = [
                    {
                        "event_id": r[0], "event_type": r[1], "entity_type": r[2],
                        "entity_id": r[3], "actor": r[4], "payload": r[5],
                        "created_at": r[6].isoformat() if r[6] else None,
                    }
                    for r in rows
                ]
        return jsonify({"events": events, "count": len(events)})
    except Exception as e:
        log_error("events_feed", e)
        return jsonify({"error": str(e)}), 500


# ── Entity event history ──────────────────────────────────────────────────

@assets_bp.route("/api/events/entity/<entity_type>/<entity_id>")
def events_entity_history(entity_type, entity_id):
    """Full event history for a specific entity (chronological order)."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:events/entity_history */
                    SELECT event_id, event_type, entity_type, entity_id,
                           actor, payload, created_at
                    FROM field_service.events
                    WHERE entity_type = %s AND entity_id = %s
                    ORDER BY created_at ASC
                """, (entity_type, entity_id))
                rows = cur.fetchall()
                events = [
                    {
                        "event_id": r[0], "event_type": r[1], "entity_type": r[2],
                        "entity_id": r[3], "actor": r[4], "payload": r[5],
                        "created_at": r[6].isoformat() if r[6] else None,
                    }
                    for r in rows
                ]
        return jsonify({
            "events": events, "count": len(events),
            "entity_type": entity_type, "entity_id": entity_id,
        })
    except Exception as e:
        log_error("events_entity", e)
        return jsonify({"error": str(e)}), 500


# ── Event statistics ──────────────────────────────────────────────────────

@assets_bp.route("/api/events/stats")
def events_stats():
    """Event statistics -- counts by type over a recent period.

    Optional query param: hours (int, default 24).
    """
    try:
        hours = int(request.args.get("hours", 24))
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:events/stats:by_type */
                    SELECT event_type, COUNT(*) as cnt
                    FROM field_service.events
                    WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '%s hours'
                    GROUP BY event_type
                    ORDER BY cnt DESC
                """, (hours,))
                stats = [{"event_type": r[0], "count": r[1]} for r in cur.fetchall()]
                cur.execute("/* page:events/stats:total */ SELECT COUNT(*) FROM field_service.events")
                total = cur.fetchone()[0]
        return jsonify({"stats": stats, "total_events": total, "period_hours": hours})
    except Exception as e:
        log_error("events_stats", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# SLA Engine endpoints
# ---------------------------------------------------------------------------

# ── Regional SLA risk heatmap ─────────────────────────────────────────────

@assets_bp.route("/api/sla/risk-heatmap")
def sla_risk_heatmap():
    """Regional SLA risk heatmap from materialized view."""
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
                regions = [
                    {
                        "region_id": r[0], "region_name": r[1],
                        "open_orders": r[2], "critical_risk": r[3],
                        "high_risk": r[4], "medium_risk": r[5], "low_risk": r[6],
                        "avg_risk_score": float(r[7]) if r[7] else 0,
                        "overall_sla_pct": float(r[8]) if r[8] else 0,
                    }
                    for r in rows
                ]
        return jsonify({"regions": regions})
    except Exception as e:
        log_error("sla_risk_heatmap", e)
        return jsonify({"error": str(e)}), 500


# ── Technician performance leaderboard ────────────────────────────────────

@assets_bp.route("/api/sla/leaderboard")
def sla_leaderboard():
    """Technician performance leaderboard from materialized view.

    Optional query params:
    - limit (int, max 200): number of techs to return
    - sort (str): column to sort by (sla_pct, completed_count, avg_resolution_hours)
    """
    try:
        limit = min(int(request.args.get("limit", 20)), 200)
        sort_by = request.args.get("sort", "sla_pct")
        allowed_sorts = {"sla_pct", "completed_count", "avg_resolution_hours"}
        if sort_by not in allowed_sorts:
            sort_by = "sla_pct"
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # sort_by is validated against allowed_sorts above — safe to interpolate
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
                techs = [
                    {
                        "technician_id": r[0], "name": r[1], "region_id": r[2],
                        "region_name": r[3], "completed_count": r[4],
                        "sla_met_count": r[5],
                        "avg_resolution_hours": float(r[6]) if r[6] else None,
                        "sla_pct": float(r[7]) if r[7] else None,
                    }
                    for r in rows
                ]
        return jsonify({"leaderboard": techs, "sort_by": sort_by})
    except Exception as e:
        log_error("sla_leaderboard", e)
        return jsonify({"error": str(e)}), 500


# ── Live SLA risk scores ─────────────────────────────────────────────────

@assets_bp.route("/api/sla/live-risk")
def sla_live_risk():
    """Live SLA risk scores -- real-time from work_orders (not matview).

    Returns the top 50 open orders with sla_risk_score >= 70, sorted by
    risk score descending.
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
                at_risk = [
                    {
                        "work_order_id": r[0], "work_order_number": r[1],
                        "priority": r[2], "status": r[3],
                        "sla_risk_score": r[4],
                        "sla_hours_remaining": float(r[5]) if r[5] else None,
                        "sla_due_at": r[6].isoformat() if r[6] else None,
                        "category": r[7], "region_name": r[8], "tech_name": r[9],
                    }
                    for r in rows
                ]
        return jsonify({"at_risk_orders": at_risk, "count": len(at_risk)})
    except Exception as e:
        log_error("sla_live_risk", e)
        return jsonify({"error": str(e)}), 500


# ── Refresh SLA materialized views ────────────────────────────────────────

@assets_bp.route("/api/sla/refresh", methods=["POST"])
def sla_refresh_matviews():
    """Refresh SLA materialized views on demand.

    Calls the SECURITY DEFINER function refresh_sla_matviews() which runs
    as the owner (admin) but is callable by the app role.
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

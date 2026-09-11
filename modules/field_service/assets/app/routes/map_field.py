"""
Field Map + Work Order Detail API — Flask Blueprint
=====================================================

All endpoints that power the Field Map page and work order detail panel:

  Map data:
    - /api/map/data — technicians + open work orders with GPS coordinates
    - /api/map/infrastructure — telco infrastructure with maintenance status
    - /api/map/infrastructure/<id>/linked — WOs and techs near an asset
    - /api/map/infrastructure/<id>/iot-telemetry — IoT diagnostics via SQL Warehouse

  Map actions:
    - /api/map/reassign — reassign a WO to a different technician
    - /api/map/escalate — escalate a WO to critical priority
    - /api/map/close — complete/close a WO from the map
    - /api/map/available-techs — available techs for reassignment dropdown

  Work order detail:
    - /api/work-orders/<id> — full WO detail with notes, parts, appointments
    - /api/work-orders/<id>/notes — add a note to a WO

This blueprint references several app-level data structures (TELCO_INFRASTRUCTURE,
INFRA_TYPES, _haversine_km, _tech_movements, etc.) via late imports from the
main app module to avoid circular dependencies.

Usage in app.py:
    from routes.map_field import map_field_bp
    app.register_blueprint(map_field_bp)
"""

import json
import logging
import math
import os
import re
import time

from flask import Blueprint, jsonify, request

from shared import (
    get_pool,
    get_analytics_pool,
    log_error,
    validate_identifier,
    _get_or_refresh,
    get_active_wo_counts_by_tech,
    get_user_role_and_regions,
    GENIE_SPACES,
    get_workspace_client,
    _run_sql,
)

log = logging.getLogger(__name__)

map_field_bp = Blueprint("map_field", __name__)


# ── Local helpers ───────────────────────────────────────────────────────

def _haversine_km(lat1, lng1, lat2, lng2):
    """Haversine distance between two lat/lng points in kilometres."""
    R = 6371.0
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlng / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _get_infra_data():
    """Late-import infrastructure data from the main app module.

    Returns (TELCO_INFRASTRUCTURE, INFRA_TYPES) from app.py.
    These are module-level constants generated at import time.
    """
    import app as _app
    return _app.TELCO_INFRASTRUCTURE, _app.INFRA_TYPES


# ═════════════════════════════════════════════════════════════════════════
# Map Data Routes
# ═════════════════════════════════════════════════════════════════════════


@map_field_bp.route('/api/map/data')
def map_data():
    """Return technicians and open work orders with GPS coordinates.

    Queries:
      1. Active technicians with valid lat/lng and their active order counts
      2. Open work orders (limit 500) ordered by priority then SLA urgency
      3. Server-side stats (total open, critical, SLA at risk/breached)
         computed across ALL open WOs (not just the LIMIT 500)
    """
    try:
        # Get cached active WO counts (shared across dispatch + map)
        wo_counts = get_active_wo_counts_by_tech()

        # RBAC: restrict to user's assigned regions
        _role, _user_regions = get_user_role_and_regions()
        _region_clause = ""
        _region_params: list = []
        if _user_regions:
            _region_clause = "AND t.region_id = ANY(%s)"
            _region_params = [_user_regions]

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # ── Technicians with valid coordinates ──
                cur.execute(f"""
                    /* page:map/technicians */
                    SELECT t.technician_id, t.first_name, t.last_name, t.employee_id,
                           t.status, t.certification_level, t.current_latitude, t.current_longitude,
                           sr.region_name, COALESCE(t.max_active_orders, 8)
                    FROM field_service.technicians t
                    JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                    WHERE t.is_active = true
                      AND t.current_latitude IS NOT NULL
                      AND t.current_longitude IS NOT NULL
                      {_region_clause}
                """, _region_params if _region_params else None)
                techs = [{'id': r[0], 'name': f"{r[1]} {r[2]}", 'employee_id': r[3],
                          'status': r[4], 'cert_level': r[5],
                          'lat': float(r[6]), 'lng': float(r[7]),
                          'region': r[8], 'active_orders': wo_counts.get(r[0], 0),
                          'max_capacity': r[9]} for r in cur.fetchall()]

                # ── Open work orders with valid coordinates ──
                _wo_region = ""
                if _user_regions:
                    _wo_region = "AND wo.region_id = ANY(%s)"
                cur.execute(f"""
                    /* page:map/work_orders */
                    SELECT wo.work_order_id, wo.work_order_number, wo.status, wo.priority,
                           wo.category, wo.title, wo.latitude, wo.longitude,
                           wo.sla_due_at,
                           c.first_name || ' ' || c.last_name as customer_name,
                           COALESCE(t.first_name || ' ' || t.last_name, 'Unassigned') as tech_name
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.customers c ON wo.customer_id = c.customer_id
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    WHERE wo.status NOT IN ('completed', 'cancelled')
                      AND wo.latitude IS NOT NULL
                      AND wo.longitude IS NOT NULL
                      {_wo_region}
                    ORDER BY
                        CASE wo.priority
                            WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                            WHEN 'medium' THEN 2 ELSE 3
                        END,
                        wo.sla_due_at ASC NULLS LAST
                    LIMIT 500
                """, _region_params if _region_params else None)
                orders = [{'id': r[0], 'number': r[1], 'status': r[2], 'priority': r[3],
                           'category': r[4], 'title': r[5] or '',
                           'lat': float(r[6]), 'lng': float(r[7]),
                           'sla_due': str(r[8]) if r[8] else None,
                           'customer': r[9] or '', 'technician': r[10]} for r in cur.fetchall()]

                # ── Server-side stats (unbiased, counts ALL open WOs) ──
                cur.execute("""
                    /* page:map/stats */
                    SELECT
                        COUNT(*) as total_open,
                        COUNT(*) FILTER (WHERE priority = 'critical') as critical,
                        COUNT(*) FILTER (WHERE sla_due_at IS NOT NULL
                            AND sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours'
                            AND sla_due_at > CURRENT_TIMESTAMP) as sla_at_risk,
                        COUNT(*) FILTER (WHERE sla_due_at IS NOT NULL
                            AND sla_due_at <= CURRENT_TIMESTAMP) as sla_breached
                    FROM field_service.work_orders
                    WHERE status NOT IN ('completed', 'cancelled')
                """)
                sr = cur.fetchone()
                stats = {
                    'total_open': sr[0], 'critical': sr[1],
                    'sla_at_risk': sr[2], 'sla_breached': sr[3],
                    'total_techs': len(techs),
                }

        import datetime
        return jsonify({
            'technicians': techs,
            'work_orders': orders,
            'stats': stats,
            'server_time': datetime.datetime.utcnow().isoformat() + 'Z',
        })
    except Exception as e:
        log_error("map_data", e)
        return jsonify({'error': str(e)}), 500


# ── Infrastructure API ─────────────────────────────────────────────────

@map_field_bp.route('/api/map/infrastructure')
def map_infrastructure():
    """Return telco infrastructure with maintenance status.

    Query params:
      - region (str, optional filter)

    Enriches each item with type_label and color from INFRA_TYPES.
    """
    TELCO_INFRASTRUCTURE, INFRA_TYPES = _get_infra_data()
    region_filter = request.args.get('region')
    infra = TELCO_INFRASTRUCTURE
    if region_filter:
        infra = [i for i in infra if i['region'] == region_filter]
    result = []
    for item in infra:
        itype = INFRA_TYPES.get(item['type'], {})
        result.append({
            **item,
            'type_label': itype.get('label', item['type']),
            'color': itype.get('color', '#888'),
        })
    return jsonify({'infrastructure': result})


@map_field_bp.route('/api/map/infrastructure/<infra_id>/linked')
def infrastructure_linked(infra_id):
    """Find work orders and their assigned techs near an infrastructure asset.

    Data-driven: queries actual WOs from the DB within ~5 km of the asset,
    and returns the assigned technicians for those WOs. Every line drawn
    on the map represents a real record in Lakebase.
    """
    try:
        TELCO_INFRASTRUCTURE, _ = _get_infra_data()
        item = next((i for i in TELCO_INFRASTRUCTURE if i['id'] == infra_id), None)
        if not item:
            return jsonify({'error': 'Infrastructure not found'}), 404

        # ~5 km in degrees (rough approximation)
        lat_range = 0.045
        lng_range = 0.06
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:map/infra_linked */
                    SELECT wo.work_order_id, wo.work_order_number, wo.status, wo.priority,
                           wo.category, wo.latitude, wo.longitude,
                           t.technician_id, t.first_name || ' ' || t.last_name as tech_name,
                           t.employee_id, t.status as tech_status,
                           t.certification_level,
                           t.current_latitude, t.current_longitude
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    WHERE wo.status NOT IN ('completed', 'cancelled')
                      AND wo.latitude IS NOT NULL AND wo.longitude IS NOT NULL
                      AND wo.latitude BETWEEN %s AND %s
                      AND wo.longitude BETWEEN %s AND %s
                    ORDER BY wo.priority = 'critical' DESC, wo.updated_at DESC
                    LIMIT 10
                """, (item['lat'] - lat_range, item['lat'] + lat_range,
                      item['lng'] - lng_range, item['lng'] + lng_range))
                work_orders = []
                techs_seen = {}
                techs = []
                for r in cur.fetchall():
                    wo_lat, wo_lng = float(r[5]), float(r[6])
                    dist = _haversine_km(item['lat'], item['lng'], wo_lat, wo_lng)
                    work_orders.append({
                        'id': r[0], 'number': r[1], 'status': r[2], 'priority': r[3],
                        'category': r[4], 'lat': wo_lat, 'lng': wo_lng,
                        'distance_km': round(dist, 1),
                        'tech_id': r[7], 'tech_name': r[8],
                    })
                    # Collect unique assigned technicians
                    if r[7] and r[7] not in techs_seen:
                        techs_seen[r[7]] = True
                        tech_lat = float(r[12]) if r[12] else None
                        tech_lng = float(r[13]) if r[13] else None
                        techs.append({
                            'id': r[7], 'name': r[8], 'employee_id': r[9],
                            'status': r[10], 'cert': r[11],
                            'lat': tech_lat, 'lng': tech_lng,
                        })
        return jsonify({'work_orders': work_orders, 'techs': techs})
    except Exception as e:
        log_error("infrastructure_linked", e)
        return jsonify({'error': str(e)}), 500


@map_field_bp.route('/api/map/infrastructure/<infra_id>/iot-telemetry')
def infrastructure_iot_telemetry(infra_id):
    """IoT diagnostics for an infrastructure asset.

    Queries DLT pipeline gold/silver tables via SQL Warehouse (Statement
    Execution API), NOT Lakebase. Returns:
      - summary: aggregated health metrics from gold_iot_device_health
      - devices: per-device detail from silver_iot_telemetry (latest 20)

    If the DLT tables are not yet populated, returns available=False with
    a user-friendly message.
    """
    warehouse_id = os.environ.get('SQL_WAREHOUSE_ID')
    catalog = os.environ.get('PIPELINE_CATALOG', 'dba-lakebase-network')

    if not warehouse_id:
        return jsonify({'available': False, 'reason': 'SQL_WAREHOUSE_ID not configured'})

    # Validate infra_id to prevent SQL injection (alphanumeric + hyphens + underscores)
    if not re.match(r'^[a-zA-Z0-9_-]+$', infra_id):
        return jsonify({'error': 'Invalid infrastructure ID'}), 400

    TELCO_INFRASTRUCTURE, _ = _get_infra_data()
    item = next((i for i in TELCO_INFRASTRUCTURE if i['id'] == infra_id), None)
    if not item:
        return jsonify({'error': 'Infrastructure not found'}), 404

    try:
        w = get_workspace_client()

        # ── Gold: aggregated health summary ──
        summary_sql = f"""
            SELECT infrastructure_id, device_count, avg_signal_dbm, avg_throughput_mbps,
                   avg_latency_ms, avg_packet_loss_pct, avg_temperature_c, avg_battery_pct,
                   total_connected_clients, total_errors, iot_health_score, last_reading_time
            FROM {catalog}.network_data.gold_iot_device_health
            WHERE infrastructure_id = '{infra_id}'
            LIMIT 1
        """
        summary_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=summary_sql,
            wait_timeout='30s',
        )

        summary = None
        if summary_result.result and summary_result.result.data_array:
            cols = [c.name for c in summary_result.manifest.schema.columns]
            row = summary_result.result.data_array[0]
            summary = {cols[i]: row[i] for i in range(len(cols))}

        # ── Silver: per-device detail (latest 20 readings) ──
        devices_sql = f"""
            SELECT device_id, device_type, timestamp, signal_strength_dbm, throughput_mbps,
                   latency_ms, packet_loss_pct, temperature_celsius, battery_pct,
                   connected_clients, error_count, firmware_version
            FROM {catalog}.network_data.silver_iot_telemetry
            WHERE infrastructure_id = '{infra_id}'
            ORDER BY timestamp DESC
            LIMIT 20
        """
        devices_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=devices_sql,
            wait_timeout='30s',
        )

        devices = []
        if devices_result.result and devices_result.result.data_array:
            cols = [c.name for c in devices_result.manifest.schema.columns]
            for row in devices_result.result.data_array:
                devices.append({cols[i]: row[i] for i in range(len(cols))})

        return jsonify({
            'available': True,
            'summary': summary,
            'devices': devices,
        })

    except Exception as e:
        err_msg = str(e).lower()
        if 'table_or_view_not_found' in err_msg or 'not found' in err_msg or 'does not exist' in err_msg:
            return jsonify({'available': False, 'reason': 'IoT tables not yet populated — start the data simulator to generate telemetry data'})
        log_error("iot_telemetry_api", e)
        return jsonify({'available': False, 'reason': str(e)})


# ═════════════════════════════════════════════════════════════════════════
# Work Order Detail Routes
# ═════════════════════════════════════════════════════════════════════════


@map_field_bp.route('/api/work-orders/<int:wo_id>')
def work_order_detail(wo_id):
    """Full work order detail with notes, parts, appointments, and nearby infrastructure.

    Returns a rich object including:
      - work_order: all columns plus joined customer/tech/region/SLA info
      - notes: last 50 notes ordered by recency
      - parts: equipment used/reserved for this WO
      - appointments: scheduled and actual visit times with ratings
      - nearby_infrastructure: up to 3 closest infra assets within 50 km
    """
    try:
        TELCO_INFRASTRUCTURE, INFRA_TYPES = _get_infra_data()
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # ── Main work order with joins ──
                cur.execute("""
                    /* page:work_orders/detail */
                    SELECT wo.*, c.first_name || ' ' || c.last_name as customer_name,
                           c.email as customer_email, c.phone as customer_phone,
                           c.customer_tier, c.account_number,
                           t.first_name || ' ' || t.last_name as tech_name,
                           t.employee_id as tech_employee_id, t.certification_level,
                           t.current_latitude as tech_lat, t.current_longitude as tech_lng,
                           sr.region_name,
                           sp.sla_name, sp.response_hours, sp.resolution_hours, sp.penalty_per_hour
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.customers c ON wo.customer_id = c.customer_id
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    LEFT JOIN field_service.service_regions sr ON wo.region_id = sr.region_id
                    LEFT JOIN field_service.sla_policies sp ON wo.sla_id = sp.sla_id
                    WHERE wo.work_order_id = %s
                """, [wo_id])
                row = cur.fetchone()
                if not row:
                    return jsonify({'error': 'Work order not found'}), 404
                cols = [d[0] for d in cur.description]
                wo = {cols[i]: (str(row[i]) if row[i] is not None and not isinstance(row[i], (int, float, bool)) else row[i]) for i in range(len(cols))}

                # ── Notes ──
                cur.execute("""
                    /* page:work_orders/detail:notes */
                    SELECT note_id, author, note_type, content, created_at
                    FROM field_service.work_order_notes
                    WHERE work_order_id = %s ORDER BY created_at DESC LIMIT 50
                """, [wo_id])
                notes = [{'id': r[0], 'author': r[1], 'type': r[2], 'content': r[3], 'time': str(r[4])} for r in cur.fetchall()]

                # ── Parts ──
                cur.execute("""
                    /* page:work_orders/detail:parts */
                    SELECT wop.quantity, wop.action, ec.equipment_name, ec.manufacturer, ec.model_number
                    FROM field_service.work_order_parts wop
                    JOIN field_service.equipment_inventory ei ON wop.inventory_id = ei.inventory_id
                    JOIN field_service.equipment_catalog ec ON ei.equipment_type_id = ec.equipment_type_id
                    WHERE wop.work_order_id = %s
                """, [wo_id])
                parts = [{'qty': r[0], 'action': r[1], 'name': r[2], 'manufacturer': r[3], 'model': r[4]} for r in cur.fetchall()]

                # ── Appointments ──
                cur.execute("""
                    /* page:work_orders/detail:appointments */
                    SELECT a.appointment_id, a.scheduled_start, a.scheduled_end,
                           a.actual_start, a.actual_end, a.status,
                           a.travel_time_min, a.on_site_time_min,
                           a.customer_rating, a.customer_feedback,
                           t.first_name || ' ' || t.last_name as tech_name
                    FROM field_service.appointments a
                    LEFT JOIN field_service.technicians t ON a.technician_id = t.technician_id
                    WHERE a.work_order_id = %s ORDER BY a.scheduled_start DESC
                """, [wo_id])
                appts = [{'id': r[0], 'sched_start': str(r[1]) if r[1] else None,
                          'sched_end': str(r[2]) if r[2] else None,
                          'actual_start': str(r[3]) if r[3] else None,
                          'actual_end': str(r[4]) if r[4] else None,
                          'status': r[5], 'travel_min': r[6], 'onsite_min': r[7],
                          'rating': r[8], 'feedback': r[9], 'tech': r[10]} for r in cur.fetchall()]

                # ── Nearby infrastructure based on WO location ──
                nearby_infra = []
                wo_lat = wo.get('latitude')
                wo_lng = wo.get('longitude')
                if wo_lat and wo_lng:
                    wo_lat_f, wo_lng_f = float(wo_lat), float(wo_lng)
                    for item in TELCO_INFRASTRUCTURE:
                        dist = _haversine_km(wo_lat_f, wo_lng_f, item['lat'], item['lng'])
                        if dist < 50:  # within 50 km
                            itype = INFRA_TYPES.get(item['type'], {})
                            nearby_infra.append({
                                'id': item['id'], 'name': item['name'],
                                'type_label': itype.get('label', item['type']),
                                'status': item['status'], 'distance_km': round(dist, 1),
                                'lat': item['lat'], 'lng': item['lng'],
                            })
                    nearby_infra.sort(key=lambda x: x['distance_km'])

        return jsonify({'work_order': wo, 'notes': notes, 'parts': parts,
                        'appointments': appts, 'nearby_infrastructure': nearby_infra[:3]})
    except Exception as e:
        log_error("work_order_detail", e)
        return jsonify({'error': str(e)}), 500


@map_field_bp.route('/api/work-orders/<int:wo_id>/communications')
def work_order_communications(wo_id):
    """Customer communications history for a work order."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT comm_id, channel, direction, comm_type,
                           subject, status, sent_at, delivered_at, read_at
                    FROM field_service.customer_communications
                    WHERE work_order_id = %s
                    ORDER BY sent_at DESC
                    LIMIT 20
                """, (wo_id,))
                comms = [{
                    'comm_id': r[0], 'channel': r[1], 'direction': r[2],
                    'type': r[3], 'subject': r[4], 'status': r[5],
                    'sent_at': str(r[6]) if r[6] else None,
                    'delivered_at': str(r[7]) if r[7] else None,
                    'read_at': str(r[8]) if r[8] else None,
                } for r in cur.fetchall()]
        return jsonify({'communications': comms, 'count': len(comms)})
    except Exception as e:
        log_error("work_order_communications", e)
        return jsonify({'communications': [], 'error': str(e)})


@map_field_bp.route('/api/work-orders/<int:wo_id>/notes', methods=['POST'])
def add_work_order_note(wo_id):
    """Add a note to a work order.

    JSON body:
      - content (str, required)
      - type (str, default 'general')
      - author (str, default 'App User')
    """
    try:
        data = request.get_json()
        content = (data.get('content') or '').strip()
        note_type = data.get('type', 'general')
        author = data.get('author', 'App User')
        if not content:
            return jsonify({'error': 'Note content is required'}), 400
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:work_orders/add_note */
                    INSERT INTO field_service.work_order_notes
                    (work_order_id, author, note_type, content)
                    VALUES (%s, %s, %s, %s)
                    RETURNING note_id, created_at
                """, (wo_id, author, note_type, content))
                row = cur.fetchone()
                conn.commit()
        return jsonify({'note_id': row[0], 'created_at': str(row[1])})
    except Exception as e:
        log_error("add_work_order_note", e)
        return jsonify({'error': str(e)}), 500


# ═════════════════════════════════════════════════════════════════════════
# Map Action Routes
# ═════════════════════════════════════════════════════════════════════════


@map_field_bp.route('/api/map/reassign', methods=['POST'])
def map_reassign():
    """Reassign a work order to a different technician.

    JSON body:
      - work_order_id (int, required)
      - technician_id (int, required)

    Side effects:
      - Sets old tech back to 'available'
      - Assigns new tech and sets them to 'en_route'
      - Triggers GPS route animation for the new tech
      - Adds a system note to the work order
    """
    try:
        data = request.json or {}
        wo_id = data.get('work_order_id')
        new_tech_id = data.get('technician_id')
        if not wo_id or not new_tech_id:
            return jsonify({'error': 'work_order_id and technician_id required'}), 400

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get WO info (need coords for route animation)
                cur.execute("""
                    /* page:map/reassign:get_wo */
                    SELECT status, latitude, longitude FROM field_service.work_orders
                    WHERE work_order_id = %s
                """, (wo_id,))
                wo = cur.fetchone()
                if not wo:
                    return jsonify({'error': 'Work order not found'}), 404

                # Release old tech back to available
                cur.execute("""
                    /* page:map/reassign:release_old_tech */
                    UPDATE field_service.technicians
                    SET status = 'available', updated_at = CURRENT_TIMESTAMP
                    WHERE technician_id = (
                        SELECT assigned_technician_id FROM field_service.work_orders
                        WHERE work_order_id = %s
                    ) AND status IN ('en_route', 'on_site')
                """, (wo_id,))

                # Reassign the work order
                cur.execute("""
                    /* page:map/reassign:update_wo */
                    UPDATE field_service.work_orders
                    SET assigned_technician_id = %s, status = 'assigned', updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s
                """, (new_tech_id, wo_id))

                # Set new tech to en_route
                cur.execute("""
                    /* page:map/reassign:set_tech_enroute */
                    UPDATE field_service.technicians
                    SET status = 'en_route', updated_at = CURRENT_TIMESTAMP
                    WHERE technician_id = %s
                """, (new_tech_id,))

                # Create movement animation (late import for app-level state)
                cur.execute("SELECT current_latitude, current_longitude FROM field_service.technicians WHERE technician_id = %s", (new_tech_id,))
                pos = cur.fetchone()
                if pos and pos[0] and pos[1] and wo[1] and wo[2]:
                    import app as _app
                    waypoints, seg_durs = _app._generate_street_route(float(pos[0]), float(pos[1]), float(wo[1]), float(wo[2]))
                    with _app._tech_movements_lock:
                        _app._tech_movements[new_tech_id] = {
                            'waypoints': waypoints, 'seg_durations': seg_durs,
                            'current_idx': 0, 'seg_start': time.time(),
                            'wo_id': wo_id, 'region_id': None,
                        }
                    _app._ensure_position_thread()

                # Add system note
                cur.execute("""
                    /* page:map/reassign:add_note */
                    INSERT INTO field_service.work_order_notes (work_order_id, author, note_type, content)
                    VALUES (%s, 'Dispatch', 'system', 'Work order reassigned to new technician from Field Map.')
                """, (wo_id,))
                conn.commit()

        return jsonify({'success': True})
    except Exception as e:
        log_error("map_reassign", e)
        return jsonify({'error': str(e)}), 500


@map_field_bp.route('/api/map/escalate', methods=['POST'])
def map_escalate():
    """Escalate a work order to critical priority.

    JSON body:
      - work_order_id (int, required)

    Also logs an event and adds a system note.
    """
    try:
        data = request.json or {}
        wo_id = data.get('work_order_id')
        if not wo_id:
            return jsonify({'error': 'work_order_id required'}), 400

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:map/escalate:update_priority */
                    UPDATE field_service.work_orders
                    SET priority = 'critical', updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s
                    RETURNING work_order_number
                """, (wo_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({'error': 'Work order not found'}), 404

                cur.execute("""
                    /* page:map/escalate:add_note */
                    INSERT INTO field_service.work_order_notes (work_order_id, author, note_type, content)
                    VALUES (%s, 'Dispatch', 'system', 'ESCALATED to critical priority from Field Map command center.')
                """, (wo_id,))
                # Log event (late import)
                import app as _app
                _app.log_event(conn, 'work_order.escalated', 'work_order', wo_id,
                               'Dispatch', {'new_priority': 'critical'})
                conn.commit()

        return jsonify({'success': True, 'number': row[0]})
    except Exception as e:
        log_error("map_escalate", e)
        return jsonify({'error': str(e)}), 500


@map_field_bp.route('/api/map/close', methods=['POST'])
def map_close_order():
    """Close out / complete a work order from the map.

    JSON body:
      - work_order_id (int, required)

    Side effects:
      - Marks WO as completed with resolved_at timestamp
      - Releases assigned tech back to 'available'
      - Stops any active GPS movement animation for the tech
      - Completes any open appointments
      - Logs a closure event
    """
    try:
        data = request.json or {}
        wo_id = data.get('work_order_id')
        if not wo_id:
            return jsonify({'error': 'work_order_id required'}), 400

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:map/close:complete_wo */
                    UPDATE field_service.work_orders
                    SET status = 'completed', resolved_at = CURRENT_TIMESTAMP,
                        resolution_notes = 'Closed from Field Map command center.',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s
                    RETURNING assigned_technician_id, work_order_number
                """, (wo_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({'error': 'Work order not found'}), 404

                tech_id = row[0]
                if tech_id:
                    # Release tech
                    cur.execute("""
                        /* page:map/close:release_tech */
                        UPDATE field_service.technicians
                        SET status = 'available', updated_at = CURRENT_TIMESTAMP
                        WHERE technician_id = %s
                    """, (tech_id,))
                    # Stop movement animation (late import for app-level state)
                    import app as _app
                    with _app._tech_movements_lock:
                        _app._tech_movements.pop(tech_id, None)

                # Add system note
                cur.execute("""
                    /* page:map/close:add_note */
                    INSERT INTO field_service.work_order_notes (work_order_id, author, note_type, content)
                    VALUES (%s, 'Dispatch', 'system', 'Work order completed and closed from Field Map.')
                """, (wo_id,))
                # Complete open appointments
                cur.execute("""
                    /* page:map/close:complete_appts */
                    UPDATE field_service.appointments
                    SET status = 'completed', actual_end = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s AND status != 'completed'
                """, (wo_id,))
                # Log event (late import)
                import app as _app
                _app.log_event(conn, 'work_order.closed', 'work_order', wo_id,
                               'Dispatch', {'new_status': 'completed',
                                            'source': 'Field Map command center',
                                            'technician_id': tech_id})
                conn.commit()

        return jsonify({'success': True, 'number': row[1]})
    except Exception as e:
        log_error("map_close", e)
        return jsonify({'error': str(e)}), 500


@map_field_bp.route('/api/map/available-techs')
def map_available_techs():
    """Get available technicians for reassignment dropdown.

    If wo_id is provided, filters to techs in the same region as the WO
    so reassignments stay local.

    Query params:
      - wo_id (int, optional): work order to filter by region
    """
    try:
        wo_id = request.args.get('wo_id', type=int)
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                if wo_id:
                    # Region-filtered: only techs in the same region as the WO
                    cur.execute("""
                        /* page:map/available_techs */
                        SELECT t.technician_id, t.first_name || ' ' || t.last_name,
                               sr.region_name, t.certification_level,
                               t.avg_rating, t.current_latitude, t.current_longitude
                        FROM field_service.technicians t
                        JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                        WHERE t.is_active = true AND t.status = 'available'
                          AND t.region_id = (SELECT region_id FROM field_service.work_orders WHERE work_order_id = %s)
                        ORDER BY t.avg_rating DESC NULLS LAST, t.last_name
                    """, (wo_id,))
                else:
                    # All available techs across all regions
                    cur.execute("""
                        /* page:map/available_techs */
                        SELECT t.technician_id, t.first_name || ' ' || t.last_name,
                               sr.region_name, t.certification_level,
                               t.avg_rating, t.current_latitude, t.current_longitude
                        FROM field_service.technicians t
                        JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                        WHERE t.is_active = true AND t.status = 'available'
                        ORDER BY sr.region_name, t.last_name
                    """)
                techs = [{'id': r[0], 'name': r[1], 'region': r[2], 'cert': r[3],
                          'rating': float(r[4]) if r[4] else None,
                          'lat': float(r[5]) if r[5] else None,
                          'lng': float(r[6]) if r[6] else None} for r in cur.fetchall()]
        return jsonify(techs)
    except Exception as e:
        log_error("map_available_techs", e)
        return jsonify({'error': str(e)}), 500


# ── GPS Breadcrumb Trail ──────────────────────────────────────────────

@map_field_bp.route('/api/map/breadcrumbs/<int:tech_id>')
def map_breadcrumbs(tech_id):
    """GPS breadcrumb trail for a technician (last N hours).

    Returns position samples for rendering a trail polyline on the map.
    Also computes average speed for ETA estimation.
    """
    try:
        hours = min(int(request.args.get('hours', 8)), 24)
        limit = min(int(request.args.get('limit', 500)), 2000)

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT latitude, longitude, speed_kmh, heading, event_type, recorded_at
                    FROM field_service.gps_breadcrumbs
                    WHERE technician_id = %s
                      AND recorded_at > CURRENT_TIMESTAMP - (%s || ' hours')::interval
                    ORDER BY recorded_at ASC
                    LIMIT %s
                """, (tech_id, str(hours), limit))
                points = [{
                    'lat': float(r[0]), 'lng': float(r[1]),
                    'speed': float(r[2]) if r[2] else 0,
                    'heading': float(r[3]) if r[3] else 0,
                    'event': r[4],
                    'time': str(r[5]),
                } for r in cur.fetchall()]

                # Compute average speed (for ETA estimation)
                speeds = [p['speed'] for p in points if p['speed'] > 0]
                avg_speed = round(sum(speeds) / max(len(speeds), 1), 1) if speeds else 0

        return jsonify({
            'technician_id': tech_id,
            'points': points,
            'count': len(points),
            'avg_speed_kmh': avg_speed,
            'hours': hours,
        })
    except Exception as e:
        log_error("map_breadcrumbs", e)
        return jsonify({'points': [], 'error': str(e)})


# ── Production Road Routing (multi-provider with caching) ────────────

import hashlib as _hashlib

_route_cache: dict = {}  # key → (data, timestamp)
_ROUTE_CACHE_TTL = 300   # 5 minutes


def _parse_waypoints(waypoints_str: str) -> list[tuple[float, float]]:
    """Parse 'lat1,lng1,lat2,lng2,...' into [(lat, lng), ...]."""
    coords = waypoints_str.split(',')
    if len(coords) < 4 or len(coords) % 2 != 0:
        raise ValueError('Need at least 2 waypoints')
    return [(float(coords[i]), float(coords[i + 1])) for i in range(0, len(coords), 2)]


def _route_mapbox(points: list[tuple[float, float]]) -> dict | None:
    """Call Mapbox Directions API for road routing (primary provider).

    Requires MAPBOX_ACCESS_TOKEN env var. Returns None if unavailable.
    100K free requests/month, then $5/1K requests.
    """
    import requests as http_req
    token = os.environ.get('MAPBOX_ACCESS_TOKEN', '')
    if not token:
        return None
    try:
        coords = ';'.join(f"{lng},{lat}" for lat, lng in points)
        resp = http_req.get(
            f"https://api.mapbox.com/directions/v5/mapbox/driving/{coords}",
            params={
                'access_token': token,
                'overview': 'full',
                'geometries': 'geojson',
                'steps': 'true',
                'annotations': 'duration,distance',
            },
            timeout=8,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get('code') != 'Ok' or not data.get('routes'):
            return None

        route = data['routes'][0]
        steps = []
        for leg in route.get('legs', []):
            for step in leg.get('steps', []):
                maneuver = step.get('maneuver', {})
                instruction = step.get('name', '')
                modifier = maneuver.get('modifier', '')
                step_type = maneuver.get('type', '')
                if step_type == 'depart':
                    text = f"Head {modifier} on {instruction}" if instruction else f"Depart {modifier}"
                elif step_type == 'arrive':
                    text = "Arrive at destination"
                elif step_type in ('turn', 'new name', 'end of road'):
                    text = f"Turn {modifier} onto {instruction}" if instruction else f"Turn {modifier}"
                elif step_type == 'merge':
                    text = f"Merge onto {instruction}" if instruction else "Merge"
                elif step_type == 'fork':
                    text = f"Keep {modifier} onto {instruction}" if instruction else f"Keep {modifier}"
                elif step_type in ('on ramp', 'off ramp'):
                    text = f"Take the ramp onto {instruction}" if instruction else "Take the ramp"
                elif step_type == 'continue':
                    text = f"Continue on {instruction}" if instruction else "Continue straight"
                elif step_type == 'roundabout':
                    text = f"Enter roundabout, exit onto {instruction}" if instruction else "Enter roundabout"
                elif step_type == 'rotary':
                    text = f"Enter rotary, exit onto {instruction}" if instruction else "Enter rotary"
                else:
                    text = f"{step_type.replace('_', ' ').title()} onto {instruction}" if instruction else step_type.replace('_', ' ').title()

                if step.get('distance', 0) > 10:
                    steps.append({
                        'instruction': text.strip(),
                        'distance_km': round(step.get('distance', 0) / 1000, 2),
                        'duration_min': round(step.get('duration', 0) / 60, 1),
                    })

        return {
            'geometry': route['geometry'],
            'distance_km': round(route['distance'] / 1000, 1),
            'duration_min': round(route['duration'] / 60, 1),
            'steps': steps,
            'source': 'mapbox',
        }
    except Exception:
        return None


def _route_osrm(points: list[tuple[float, float]]) -> dict | None:
    """Call OSRM for road routing (fallback provider). Returns None on failure."""
    import requests as http_req
    try:
        # OSRM uses lng,lat order, semicolon-separated
        osrm_coords = ';'.join(f"{lng},{lat}" for lat, lng in points)
        resp = http_req.get(
            f"https://router.project-osrm.org/route/v1/driving/{osrm_coords}",
            params={'overview': 'full', 'geometries': 'geojson', 'steps': 'true'},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get('code') != 'Ok' or not data.get('routes'):
            return None

        route = data['routes'][0]
        # Extract turn-by-turn steps from legs
        steps = []
        for leg in route.get('legs', []):
            for step in leg.get('steps', []):
                maneuver = step.get('maneuver', {})
                instruction = step.get('name', '')
                modifier = maneuver.get('modifier', '')
                step_type = maneuver.get('type', '')
                if step_type == 'depart':
                    text = f"Head {modifier} on {instruction}" if instruction else f"Depart {modifier}"
                elif step_type == 'arrive':
                    text = "Arrive at destination"
                elif step_type in ('turn', 'new name', 'end of road'):
                    text = f"Turn {modifier} onto {instruction}" if instruction else f"Turn {modifier}"
                elif step_type == 'merge':
                    text = f"Merge onto {instruction}" if instruction else "Merge"
                elif step_type == 'fork':
                    text = f"Keep {modifier} onto {instruction}" if instruction else f"Keep {modifier}"
                elif step_type in ('on ramp', 'off ramp'):
                    text = f"Take the ramp onto {instruction}" if instruction else "Take the ramp"
                elif step_type == 'continue':
                    text = f"Continue on {instruction}" if instruction else "Continue straight"
                elif step_type == 'roundabout':
                    text = f"Enter roundabout, exit onto {instruction}" if instruction else "Enter roundabout"
                else:
                    text = f"{step_type.replace('_', ' ').title()} onto {instruction}" if instruction else step_type.replace('_', ' ').title()

                if step.get('distance', 0) > 10:  # skip trivial steps
                    steps.append({
                        'instruction': text.strip(),
                        'distance_km': round(step.get('distance', 0) / 1000, 2),
                        'duration_min': round(step.get('duration', 0) / 60, 1),
                    })

        return {
            'geometry': route['geometry'],
            'distance_km': round(route['distance'] / 1000, 1),
            'duration_min': round(route['duration'] / 60, 1),
            'steps': steps,
            'source': 'osrm',
        }
    except Exception:
        return None


def _route_crow_flies(points: list[tuple[float, float]]) -> dict:
    """Fallback: straight-line distance when routing APIs are unavailable."""
    import math
    total_dist = 0
    coords_geojson = []
    for i, (lat, lng) in enumerate(points):
        coords_geojson.append([lng, lat])
        if i > 0:
            dlat = math.radians(lat - points[i - 1][0])
            dlng = math.radians(lng - points[i - 1][1])
            a = (math.sin(dlat / 2) ** 2 +
                 math.cos(math.radians(points[i - 1][0])) * math.cos(math.radians(lat)) *
                 math.sin(dlng / 2) ** 2)
            total_dist += 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return {
        'geometry': {'type': 'LineString', 'coordinates': coords_geojson},
        'distance_km': round(total_dist, 1),
        'duration_min': round(total_dist / 50 * 60, 1),  # assume 50 km/h
        'steps': [],
        'source': 'straight_line',
    }


@map_field_bp.route('/api/map/route')
def map_route():
    """Production road routing with caching and fallback chain.

    Tries: cache → OSRM → straight-line fallback.
    Returns GeoJSON geometry + distance + duration + turn-by-turn steps.

    Query params:
        waypoints: comma-separated lat,lng pairs (e.g., "40.7,-74.0,40.8,-74.1")
    """
    waypoints_str = request.args.get('waypoints', '')
    if not waypoints_str:
        return jsonify({'error': 'Missing waypoints parameter'}), 400

    try:
        points = _parse_waypoints(waypoints_str)

        # Check cache
        cache_key = _hashlib.md5(waypoints_str.encode()).hexdigest()
        cached = _route_cache.get(cache_key)
        if cached:
            data, ts = cached
            if time.time() - ts < _ROUTE_CACHE_TTL:
                data['cached'] = True
                return jsonify(data)

        # Routing chain: Mapbox (primary) → OSRM (fallback) → crow-flies (last resort)
        result = _route_mapbox(points)
        if not result:
            result = _route_osrm(points)
        if result:
            result['cached'] = False
            _route_cache[cache_key] = (result, time.time())
            # Cleanup old cache entries
            if len(_route_cache) > 500:
                _route_cache.clear()
            return jsonify(result)

        # Last resort: straight-line distance (clearly marked as fallback)
        result = _route_crow_flies(points)
        result['cached'] = False
        result['fallback'] = True  # Frontend shows warning for straight-line routes
        return jsonify(result)

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        log_error("map_route", e)
        return jsonify({'error': str(e)}), 500


# ═════════════════════════════════════════════════════════════════════════
# Territory Visualization API
# ═════════════════════════════════════════════════════════════════════════


def _generate_territory_polygon(center_lat: float, center_lng: float,
                                radius_km: float, num_points: int = 32) -> list:
    """Generate a GeoJSON polygon (circle approximation) around a center point.

    Returns a list of [lng, lat] coordinate pairs forming a closed ring.
    """
    coords = []
    for i in range(num_points):
        angle = 2 * math.pi * i / num_points
        # Approximate: 1 degree lat ≈ 111km, 1 degree lng ≈ 111km * cos(lat)
        dlat = (radius_km / 111.0) * math.sin(angle)
        dlng = (radius_km / (111.0 * math.cos(math.radians(center_lat)))) * math.cos(angle)
        coords.append([round(center_lng + dlng, 6), round(center_lat + dlat, 6)])
    coords.append(coords[0])  # close the ring
    return coords


@map_field_bp.route('/api/map/territories')
def map_territories():
    """Return metro territory boundaries as GeoJSON with utilization stats.

    Each territory includes:
      - GeoJSON polygon boundary (circular approximation)
      - Tech count and active WO count (for utilization coloring)
      - Territory metadata (name, region, center lat/lng)
    """
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get territories with tech/WO counts
                cur.execute("""
                    SELECT mt.territory_id, mt.territory_name, mt.territory_code,
                           mt.center_latitude, mt.center_longitude, mt.radius_km,
                           sr.region_name, sr.region_code,
                           COUNT(DISTINCT t.technician_id) FILTER (
                               WHERE t.is_active AND t.status NOT IN ('off_duty', 'on_leave')
                           ) AS active_techs,
                           COUNT(DISTINCT t.technician_id) AS total_techs
                    FROM field_service.metro_territories mt
                    JOIN field_service.service_regions sr ON mt.region_id = sr.region_id
                    LEFT JOIN field_service.technicians t ON t.region_id = mt.region_id
                        AND t.current_latitude BETWEEN mt.center_latitude - (mt.radius_km / 111.0)
                                                    AND mt.center_latitude + (mt.radius_km / 111.0)
                        AND t.current_longitude BETWEEN mt.center_longitude - (mt.radius_km / (111.0 * COS(RADIANS(mt.center_latitude))))
                                                     AND mt.center_longitude + (mt.radius_km / (111.0 * COS(RADIANS(mt.center_latitude))))
                    GROUP BY mt.territory_id, mt.territory_name, mt.territory_code,
                             mt.center_latitude, mt.center_longitude, mt.radius_km,
                             sr.region_name, sr.region_code
                    ORDER BY sr.region_name, mt.territory_name
                """)
                territories = []
                for r in cur.fetchall():
                    tid, name, code, lat, lng, radius, region, region_code, \
                        active_techs, total_techs = r
                    lat_f = float(lat)
                    lng_f = float(lng)
                    radius_f = float(radius)

                    # Utilization: ratio of active techs to total (0-1)
                    utilization = round(active_techs / max(total_techs, 1), 2)

                    territories.append({
                        'id': code,
                        'name': name,
                        'region': region,
                        'region_code': region_code,
                        'center': {'lat': lat_f, 'lng': lng_f},
                        'radius_km': radius_f,
                        'active_techs': active_techs,
                        'total_techs': total_techs,
                        'utilization': utilization,
                        'geojson': {
                            'type': 'Feature',
                            'properties': {
                                'name': name,
                                'code': code,
                                'utilization': utilization,
                            },
                            'geometry': {
                                'type': 'Polygon',
                                'coordinates': [
                                    _generate_territory_polygon(lat_f, lng_f, radius_f)
                                ],
                            },
                        },
                    })

        return jsonify({'territories': territories})
    except Exception as e:
        log_error("map_territories", e)
        return jsonify({'territories': [], 'error': str(e)})

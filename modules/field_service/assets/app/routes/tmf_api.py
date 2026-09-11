"""
TMF 621 Trouble Ticket REST API — Flask Blueprint
===================================================

Industry-standard REST API conforming to TMF 621 (Trouble Ticket Management).
This makes the Lakebase FSM app compatible with OSS/BSS integration patterns
used by every major telco (AT&T, Verizon, Comcast, Lumen).

Routes
------
GET    /api/tmf/troubleTickets           List/search tickets
POST   /api/tmf/troubleTickets           Create ticket
GET    /api/tmf/troubleTickets/<id>      Get ticket by ID
PATCH  /api/tmf/troubleTickets/<id>      Update ticket status/fields
GET    /api/tmf/troubleTickets/<id>/notes  Get ticket activity log

TMF 621 field mapping:
    TMF ticketId         → work_order_id
    TMF severity         → priority (critical/high/medium/low)
    TMF status           → status (submitted→open, acknowledged→assigned, etc.)
    TMF description      → reported_issue
    TMF reportedBy       → customer_name
    TMF assignedTo       → technician_name
    TMF expectedResolution → sla_due_at
    TMF resolutionDate   → resolved_at

Usage in app.py:
    from routes.tmf_api import tmf_bp
    app.register_blueprint(tmf_bp)
"""

import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from shared import get_pool, log_error

log = logging.getLogger(__name__)

tmf_bp = Blueprint("tmf_api", __name__)


def _wo_to_tmf(row, col_names) -> dict:
    """Convert a work_order row to TMF 621 TroubleTicket format."""
    d = dict(zip(col_names, row))

    # TMF status mapping
    status_map = {
        'open': 'submitted',
        'assigned': 'acknowledged',
        'en_route': 'inProgress',
        'in_progress': 'inProgress',
        'on_hold': 'held',
        'completed': 'resolved',
        'cancelled': 'cancelled',
    }

    return {
        '@type': 'TroubleTicket',
        'id': str(d.get('work_order_id', '')),
        'href': f"/api/tmf/troubleTickets/{d.get('work_order_id', '')}",
        'externalId': d.get('work_order_number', ''),
        'severity': d.get('priority', 'medium'),
        'status': status_map.get(d.get('status', ''), d.get('status', '')),
        'statusChangeReason': d.get('resolution_notes', ''),
        'description': d.get('reported_issue', d.get('title', '')),
        'name': d.get('title', ''),
        'category': d.get('category', ''),
        'subCategory': d.get('subcategory', ''),
        'priority': d.get('priority', 'medium'),
        'creationDate': str(d['created_at']) if d.get('created_at') else None,
        'lastUpdate': str(d['updated_at']) if d.get('updated_at') else None,
        'expectedResolutionDate': str(d['sla_due_at']) if d.get('sla_due_at') else None,
        'resolutionDate': str(d['resolved_at']) if d.get('resolved_at') else None,
        'channel': {'name': 'FieldOps App'},
        'relatedParty': [
            {
                'id': str(d.get('customer_id', '')),
                'name': d.get('customer_name', ''),
                'role': 'reportedBy',
            },
            {
                'id': str(d.get('technician_id', '')) if d.get('technician_id') else '',
                'name': d.get('tech_name', 'Unassigned'),
                'role': 'assignedTo',
            },
        ],
        'place': {
            'name': d.get('address', ''),
            'geographicLocation': {
                'latitude': str(d['latitude']) if d.get('latitude') else None,
                'longitude': str(d['longitude']) if d.get('longitude') else None,
            },
        },
        'note': [],
        'serviceOrder': {
            'id': str(d.get('service_order_id', '')) if d.get('service_order_id') else None,
        },
    }


@tmf_bp.route('/api/tmf/troubleTickets')
def tmf_list_tickets():
    """TMF 621: List/search trouble tickets.

    Query params:
        status (str): Filter by TMF status (submitted, acknowledged, inProgress, resolved)
        severity (str): Filter by severity (critical, high, medium, low)
        limit (int): Max results (default 20, max 100)
        offset (int): Pagination offset
    """
    try:
        # Map TMF status back to internal
        tmf_status = request.args.get('status', '')
        reverse_status = {
            'submitted': 'open', 'acknowledged': 'assigned',
            'inProgress': "('en_route','in_progress')",
            'resolved': 'completed', 'cancelled': 'cancelled',
        }

        severity = request.args.get('severity', '')
        limit = min(int(request.args.get('limit', 20)), 100)
        offset = int(request.args.get('offset', 0))

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                where_clauses = []
                params = []

                if tmf_status:
                    internal = reverse_status.get(tmf_status, tmf_status)
                    if internal.startswith('('):
                        where_clauses.append(f"wo.status IN {internal}")
                    else:
                        where_clauses.append("wo.status = %s")
                        params.append(internal)

                if severity:
                    where_clauses.append("wo.priority = %s")
                    params.append(severity)

                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                cur.execute(f"""
                    SELECT wo.work_order_id, wo.work_order_number, wo.title,
                           wo.status, wo.priority, wo.category, wo.subcategory,
                           wo.reported_issue, wo.resolution_notes,
                           wo.created_at, wo.updated_at, wo.sla_due_at, wo.resolved_at,
                           wo.customer_id, wo.latitude, wo.longitude,
                           wo.assigned_technician_id as technician_id,
                           wo.service_order_id,
                           c.first_name || ' ' || c.last_name as customer_name,
                           COALESCE(t.first_name || ' ' || t.last_name, 'Unassigned') as tech_name,
                           wo.address_line1 || ', ' || wo.city || ', ' || wo.state_province as address
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.customers c ON wo.customer_id = c.customer_id
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    {where_sql}
                    ORDER BY wo.created_at DESC
                    LIMIT %s OFFSET %s
                """, params + [limit, offset])

                col_names = [desc[0] for desc in cur.description]
                tickets = [_wo_to_tmf(row, col_names) for row in cur.fetchall()]

        return jsonify(tickets)
    except Exception as e:
        log_error("tmf_list_tickets", e)
        return jsonify({'error': str(e)}), 500


@tmf_bp.route('/api/tmf/troubleTickets', methods=['POST'])
def tmf_create_ticket():
    """TMF 621: Create a trouble ticket."""
    try:
        data = request.json or {}
        description = data.get('description', '')
        severity = data.get('severity', 'medium')
        category = data.get('category', 'repair')
        name = data.get('name', description[:100])

        # Map TMF severity to internal priority
        priority = severity if severity in ('critical', 'high', 'medium', 'low') else 'medium'

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO field_service.work_orders (
                        work_order_number, title, reported_issue,
                        priority, category, status, created_at
                    ) VALUES (
                        'TMF-' || nextval('field_service.work_orders_work_order_id_seq'),
                        %s, %s, %s, %s, 'open', now()
                    ) RETURNING work_order_id, work_order_number
                """, (name, description, priority, category))
                row = cur.fetchone()
                conn.commit()

        return jsonify({
            '@type': 'TroubleTicket',
            'id': str(row[0]),
            'externalId': row[1],
            'status': 'submitted',
            'severity': severity,
        }), 201
    except Exception as e:
        log_error("tmf_create_ticket", e)
        return jsonify({'error': str(e)}), 500


@tmf_bp.route('/api/tmf/troubleTickets/<int:ticket_id>')
def tmf_get_ticket(ticket_id):
    """TMF 621: Get a trouble ticket by ID."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT wo.work_order_id, wo.work_order_number, wo.title,
                           wo.status, wo.priority, wo.category, wo.subcategory,
                           wo.reported_issue, wo.resolution_notes,
                           wo.created_at, wo.updated_at, wo.sla_due_at, wo.resolved_at,
                           wo.customer_id, wo.latitude, wo.longitude,
                           wo.assigned_technician_id as technician_id,
                           wo.service_order_id,
                           c.first_name || ' ' || c.last_name as customer_name,
                           COALESCE(t.first_name || ' ' || t.last_name, 'Unassigned') as tech_name,
                           wo.address_line1 || ', ' || wo.city || ', ' || wo.state_province as address
                    FROM field_service.work_orders wo
                    LEFT JOIN field_service.customers c ON wo.customer_id = c.customer_id
                    LEFT JOIN field_service.technicians t ON wo.assigned_technician_id = t.technician_id
                    WHERE wo.work_order_id = %s
                """, (ticket_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({'error': 'TroubleTicket not found'}), 404
                col_names = [desc[0] for desc in cur.description]
                ticket = _wo_to_tmf(row, col_names)

                # Add notes
                cur.execute("""
                    SELECT note_type, content, created_at,
                           t.first_name || ' ' || t.last_name as author
                    FROM field_service.work_order_notes n
                    LEFT JOIN field_service.technicians t ON n.technician_id = t.technician_id
                    WHERE n.work_order_id = %s
                    ORDER BY n.created_at DESC LIMIT 20
                """, (ticket_id,))
                ticket['note'] = [{
                    'date': str(r[2]) if r[2] else None,
                    'author': r[3] or 'System',
                    'text': r[1],
                    'noteType': r[0],
                } for r in cur.fetchall()]

        return jsonify(ticket)
    except Exception as e:
        log_error("tmf_get_ticket", e)
        return jsonify({'error': str(e)}), 500


@tmf_bp.route('/api/tmf/troubleTickets/<int:ticket_id>', methods=['PATCH'])
def tmf_update_ticket(ticket_id):
    """TMF 621: Update a trouble ticket (status, severity, notes)."""
    try:
        data = request.json or {}
        updates = []
        params = []

        # Map TMF status to internal
        if 'status' in data:
            tmf_to_internal = {
                'submitted': 'open', 'acknowledged': 'assigned',
                'inProgress': 'in_progress', 'resolved': 'completed',
                'cancelled': 'cancelled',
            }
            internal_status = tmf_to_internal.get(data['status'], data['status'])
            updates.append("status = %s")
            params.append(internal_status)

            if internal_status == 'completed':
                updates.append("resolved_at = now()")

        if 'severity' in data:
            updates.append("priority = %s")
            params.append(data['severity'])

        if 'statusChangeReason' in data:
            updates.append("resolution_notes = %s")
            params.append(data['statusChangeReason'])

        if not updates:
            return jsonify({'error': 'No fields to update'}), 400

        updates.append("updated_at = now()")
        params.append(ticket_id)

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE field_service.work_orders SET {', '.join(updates)} WHERE work_order_id = %s",
                    params
                )
                if cur.rowcount == 0:
                    return jsonify({'error': 'TroubleTicket not found'}), 404
                conn.commit()

        return jsonify({'id': str(ticket_id), 'status': 'updated'})
    except Exception as e:
        log_error("tmf_update_ticket", e)
        return jsonify({'error': str(e)}), 500

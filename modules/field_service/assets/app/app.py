"""
Lakebase FSM — Flask Application Core
======================================

Thin core that creates the Flask app, registers 14 blueprints, and holds
module-level data that blueprints import via ``import app as _app``:

* PAGE_ARCHITECTURE / _PAGE_TEMPLATES — context processor data
* Data generators — TELCO_INFRASTRUCTURE, IOT_DEVICES, STB_DEVICES, etc.
* Simulator thread functions — _run_generator, _run_iot_generator, etc.
* Movement tracking — _tech_movements, _generate_street_route, etc.
* Iceberg demo / Migration state — _iceberg_state, _migration_state, etc.
* What-If scenario functions — _whatif_get_branch_conn, etc.
* Startup bootstrap — _startup_bootstrap, _refresh_dates_if_stale, etc.
* log_event — used by dispatch, map_field, simulator blueprints
"""

from flask import Flask, render_template, jsonify, request, Response, redirect
import psycopg
from psycopg_pool import ConnectionPool
import os
import sys
import io
import csv
from pathlib import Path
import json
import re
import time
import math
import random
import threading
import logging
from datetime import datetime, timezone, timedelta
from databricks.sdk import WorkspaceClient

app = Flask(__name__)

# ── AsBuilt refresh API (live re-discovery from the config panel) ─────────
try:
    from asbuilt_api import asbuilt_bp
    app.register_blueprint(asbuilt_bp)
except ImportError:
    pass  # asbuilt_api.py not present — refresh won't be available

# ── Blueprint Registration ────────────────────────────────────────────────
from routes.pages import pages_bp
from routes.dispatch import dispatch_bp
from routes.map_field import map_field_bp
from routes.analytics import analytics_bp
from routes.admin import admin_bp
from routes.genie_agent import genie_agent_bp
from routes.explorer import explorer_bp
from routes.health import health_bp
from routes.simulator import simulator_bp
from routes.whatif import whatif_bp
from routes.network import network_bp
from routes.assets import assets_bp
from routes.fleet import fleet_bp
from routes.demos import demos_bp
from routes.architecture import architecture_bp  # powers the </> code-behind panel on every page
from routes.tmf_api import tmf_bp
from routes.lakebase import lakebase_bp  # Lakebase Control Tower aggregation (/api/lakebase/overview)
from routes.data_api import data_api_bp  # Lakebase Data API (PostgREST) demo (/api/data-api/*)

app.register_blueprint(pages_bp)
app.register_blueprint(dispatch_bp)
app.register_blueprint(map_field_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(genie_agent_bp)
app.register_blueprint(explorer_bp)
app.register_blueprint(health_bp)
app.register_blueprint(simulator_bp)
app.register_blueprint(whatif_bp)
app.register_blueprint(network_bp)
app.register_blueprint(assets_bp)
app.register_blueprint(fleet_bp)
app.register_blueprint(demos_bp)
app.register_blueprint(architecture_bp)  # /api/page-architecture/<page_id> for the </> panel
app.register_blueprint(tmf_bp)
app.register_blueprint(lakebase_bp)  # Lakebase Control Tower overview API
app.register_blueprint(data_api_bp)  # Lakebase Data API (PostgREST) demo API

# Production: disable debug, enable JSON logging
app.config['DEBUG'] = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'

# Structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ── App start time for uptime tracking ────────────────────────────────────
APP_START_TIME = datetime.now(timezone.utc)

# ── Recent errors ring buffer ─────────────────────────────────────────────
_error_log = []
_error_log_lock = threading.Lock()
MAX_ERROR_LOG = 50

def log_error(source, error):
    """Log error to both stderr and the in-memory ring buffer."""
    log.error(f"{source}: {error}")
    with _error_log_lock:
        _error_log.append({
            'time': datetime.now(timezone.utc).isoformat(),
            'source': source,
            'error': str(error)[:500]
        })
        if len(_error_log) > MAX_ERROR_LOG:
            _error_log.pop(0)

# ── Genie Space IDs ──────────────────────────────────────────────────────
GENIE_SPACES = {
    'postgres': {
        'id': os.environ.get('GENIE_SPACE_POSTGRES', ''),
        'name': 'PostgresAdmin',
        'description': 'PostgreSQL monitoring and performance data'
    },
    'field_ops': {
        'id': os.environ.get('GENIE_SPACE_FIELD_OPS', ''),
        'name': 'Field Service Operations',
        'description': 'Work orders, technicians, dispatch, and SLA data'
    },
    'network_health': {
        'id': os.environ.get('GENIE_SPACE_NETWORK_HEALTH', ''),
        'name': 'Network Health & Telemetry',
        'description': 'Network node health, outages, IoT telemetry, and maintenance risk'
    },
    'sla_workforce': {
        'id': os.environ.get('GENIE_SPACE_SLA_WORKFORCE', ''),
        'name': 'SLA & Workforce Analytics',
        'description': 'SLA compliance, technician performance, and regional analytics'
    }
}

# ── Database Connection Pool ──────────────────────────────────────────────
connection_pool = None
workspace_client = None

# Input validation: only allow alphanumeric, underscore, hyphen in identifiers
IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_-]*$')

def validate_identifier(name, label='identifier'):
    """Validate SQL identifier to prevent injection."""
    if not IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {label}: {name}")
    return name

# Cache for schema column checks — avoids information_schema queries on every poll
_column_exists_cache = {}
_column_exists_lock = threading.Lock()

def _build_conninfo():
    pg_password = os.environ.get('PGPASSWORD') or os.environ.get('DATABRICKS_TOKEN', '')
    return (
        f"dbname={os.environ.get('PGDATABASE')} "
        f"user={os.environ.get('PGUSER')} "
        f"password={pg_password} "
        f"host={os.environ.get('PGHOST')} "
        f"port=5432 sslmode=require"
    )

def get_pool():
    global connection_pool
    if connection_pool is None:
        connection_pool = ConnectionPool(
            conninfo=_build_conninfo(),
            min_size=5,
            max_size=20,
            open=False,
            timeout=30,
            kwargs={"options": "-c statement_timeout=30000"},
        )
        connection_pool.open(wait=False)
        log.info("Database connection pool opened (min_size=5, max_size=20, growing in background)")
    return connection_pool

# Separate pool for heavy analytics queries (longer timeout for background cache refresh)
_analytics_pool = None
def get_analytics_pool():
    global _analytics_pool
    if _analytics_pool is None:
        _analytics_pool = ConnectionPool(
            conninfo=_build_conninfo(),
            min_size=1,
            max_size=4,
            open=False,
            timeout=30,
            kwargs={"options": "-c statement_timeout=300000"},  # 5 minutes for heavy aggregations at scale
        )
        _analytics_pool.open(wait=False)
        log.info("Analytics pool opened (statement_timeout=120s)")
    return _analytics_pool

def get_workspace_client():
    global workspace_client
    if workspace_client is None:
        workspace_client = WorkspaceClient()
        log.info("WorkspaceClient initialized")
    return workspace_client

# ── Query result cache ────────────────────────────────────────────────────
# Caches expensive aggregation queries (dispatch summary, SLA, technician roster)
# so they don't block the UI on every page load. Background refresh keeps data fresh.
_query_cache = {}
_query_cache_lock = threading.Lock()
QUERY_CACHE_TTL = 45  # seconds — dashboard data refreshes every 45s
_bg_refresh_in_progress = set()

def _run_cached_query(cache_key, query_fn, ttl=QUERY_CACHE_TTL):
    """Return cached result if fresh, otherwise run query_fn (with fallback to stale cache on error)."""
    now = time.time()
    with _query_cache_lock:
        entry = _query_cache.get(cache_key)
        if entry and now < entry['expires']:
            return entry['data']
        stale_data = entry['data'] if entry else None

    # Try to compute fresh data
    try:
        data = query_fn()
        with _query_cache_lock:
            _query_cache[cache_key] = {'data': data, 'expires': now + ttl}
        return data
    except Exception as e:
        log.warning(f"Query cache miss for {cache_key}: {e}")
        if stale_data is not None:
            return stale_data
        raise

def _bg_refresh_cache(cache_key, query_fn, ttl=QUERY_CACHE_TTL):
    """Trigger a background refresh if not already running. Returns stale data immediately."""
    if cache_key in _bg_refresh_in_progress:
        return
    _bg_refresh_in_progress.add(cache_key)
    def _refresh():
        try:
            data = query_fn()
            with _query_cache_lock:
                _query_cache[cache_key] = {'data': data, 'expires': time.time() + ttl}
        except Exception as e:
            log.warning(f"Background refresh failed for {cache_key}: {e}")
        finally:
            _bg_refresh_in_progress.discard(cache_key)
    threading.Thread(target=_refresh, daemon=True).start()

def _get_or_refresh(cache_key, query_fn, ttl=QUERY_CACHE_TTL):
    """Best-effort cached query: return cache if available, refresh in background if stale."""
    now = time.time()
    with _query_cache_lock:
        entry = _query_cache.get(cache_key)
        if entry and now < entry['expires']:
            return entry['data']
        if entry:
            # Stale — return stale data but trigger background refresh
            _bg_refresh_cache(cache_key, query_fn, ttl)
            return entry['data']
    # No cache at all — must block and compute
    return _run_cached_query(cache_key, query_fn, ttl)

# ── Table list cache ──────────────────────────────────────────────────────
_table_cache = {'data': None, 'expires': 0}
_table_cache_lock = threading.Lock()
TABLE_CACHE_TTL = 300  # seconds — tables rarely change at runtime

def get_cached_tables():
    """Return cached table list (Lakebase + UC), refreshing if expired."""
    now = time.time()
    with _table_cache_lock:
        if _table_cache['data'] is not None and now < _table_cache['expires']:
            return _table_cache['data']

    # ── Lakebase tables ──
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """)
            tables = [{'schema': row[0], 'table': row[1], 'source': 'lakebase'}
                      for row in cur.fetchall()]

    # ── Unity Catalog tables (network_data) ──
    try:
        catalog = os.environ.get('PIPELINE_CATALOG', 'dba-lakebase-network')
        uc_schema = 'network_data'
        rows = _run_sql(
            f"SHOW TABLES IN `{catalog}`.`{uc_schema}`",
            catalog=catalog
        )
        for row in rows:
            # SHOW TABLES returns [database, tableName, isTemporary]
            tbl_name = row[1] if len(row) > 1 else row[0]
            tables.append({
                'schema': f'{uc_schema}',
                'table': tbl_name,
                'source': 'uc',
                'catalog': catalog
            })
    except Exception as e:
        log.warning(f"Could not fetch UC tables: {e}")

    with _table_cache_lock:
        _table_cache['data'] = tables
        _table_cache['expires'] = time.time() + TABLE_CACHE_TTL

    return tables

# ── Simple rate limiting (in-memory, per-IP) ─────────────────────────────
_rate_limit_store: dict = {}  # ip → [timestamps]
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = {
    '/api/agent/ask': 10,
    '/api/agent/ask-stream': 10,
    '/api/dispatch/smart-assign': 20,
    '/api/dispatch/auto-assign': 10,
    '/api/admin/pg-dump': 3,
    '/api/admin/trigger-rotation': 3,
}

@app.before_request
def rate_limit():
    """Simple in-memory rate limiter for sensitive endpoints."""
    import time as _time
    path = request.path
    max_req = RATE_LIMIT_MAX.get(path)
    if not max_req:
        return None
    ip = request.remote_addr or 'unknown'
    key = f"{ip}:{path}"
    now = _time.time()
    hits = _rate_limit_store.get(key, [])
    hits = [t for t in hits if t > now - RATE_LIMIT_WINDOW]
    if len(hits) >= max_req:
        return jsonify({'error': 'Rate limit exceeded. Try again later.'}), 429
    hits.append(now)
    _rate_limit_store[key] = hits
    # Cleanup old entries periodically
    if len(_rate_limit_store) > 1000:
        cutoff = now - RATE_LIMIT_WINDOW * 2
        _rate_limit_store.clear()
    return None


# ── CSRF protection: require JSON Content-Type on POST requests ──────────
@app.before_request
def csrf_check():
    """Reject POST requests without application/json Content-Type.

    This prevents CSRF attacks because browsers cannot send JSON cross-origin
    without a CORS preflight (which we don't allow). Form submissions and
    <img> tags send form-encoded or no content type, not JSON.
    """
    if request.method == 'POST':
        ct = request.content_type or ''
        if 'application/json' not in ct and 'multipart/form-data' not in ct:
            return jsonify({'error': 'Content-Type must be application/json'}), 415


# ── Security + cache headers ────────────────────────────────────────────
@app.after_request
def add_security_and_cache_headers(response):
    """Add security headers + cache headers on every response."""
    # Security headers (OWASP recommendations)
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    if request.is_secure:
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'

    # Cache headers
    if request.path.startswith('/static/'):
        if request.path.endswith(('.woff2', '.svg', '.png', '.ico', '.webp')):
            response.headers['Cache-Control'] = 'public, max-age=86400'
        elif request.path.endswith('.css'):
            response.headers['Cache-Control'] = 'public, max-age=300'
    elif response.content_type and 'text/html' in response.content_type:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

# ── View Source: map page_id -> template file for workspace links ─────────
_PAGE_TEMPLATES = {
    'index': 'index.html', 'dispatch': 'dispatch_board.html', 'view': 'view_data.html',
    'genie': 'genie.html', 'supervisor': 'supervisor.html', 'operations': 'operations_center.html',
    'simulator': 'simulator.html', 'health': 'health.html', 'map': 'map.html',
    'analytics': 'analytics.html', 'technicians': 'technicians.html', 'assets': 'assets.html',
    'network_health': 'network_health.html', 'skills': 'skills_matrix.html',
    'fleet': 'fleet.html',
    'architecture': 'architecture.html', 'asbuilt': 'asbuilt.html', 'whatif': 'whatif.html',
    'data_api': 'data_api.html',
    'admin': 'admin.html',
}

PAGE_ARCHITECTURE = {
    'index': {
        'title': 'Dashboard',
        'description': 'Operations overview with KPIs, SLA compliance trend, regional load, activity feed, and Databricks services status.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'OLTP queries via psycopg connection pool (optimized with partial indexes)'},
            {'name': 'Genie Spaces', 'icon': 'ai', 'color': '#FF6F00', 'detail': 'Service status monitoring'},
            {'name': 'AI Agent', 'icon': 'ai', 'color': '#9C27B0', 'detail': 'Endpoint availability check'},
            {'name': 'SQL Warehouse', 'icon': 'wh', 'color': '#1B7FE3', 'detail': 'DLT pipeline status check'},
        ],
        'tables': ['work_orders', 'technicians', 'service_regions'],
        'apis': [
            '/api/dispatch/summary', '/api/dashboard/health-lite', '/api/dashboard/activity-feed',
            '/api/dashboard/regional-load', '/api/dashboard/sla-trend', '/api/dashboard/services-status',
        ],
        'source_files': ['app/templates/index.html', 'app/routes/health.py', 'app/routes/dispatch.py'],
        'config_keys': ['pg_host', 'catalog_name', 'genie_space_ids', 'warehouse_id'],
    },
    'dispatch': {
        'title': 'Dispatch Board',
        'description': 'Intelligent technician dispatch with multi-factor scoring (skill, distance, capacity, SLA urgency, rating), SLA queue, and ML-ready optimization.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'OLTP read/write + PG scoring function (compute_dispatch_scores)'},
            {'name': 'MLflow', 'icon': 'ai', 'color': '#0194E2', 'detail': 'Dispatch scoring model training and experiment tracking'},
        ],
        'tables': ['work_orders', 'technicians', 'customers', 'technician_skills', 'skill_types', 'service_regions', 'dispatch_scores'],
        'apis': [
            '/api/dispatch/summary', '/api/dispatch/recent-orders', '/api/dispatch/urgent-queue',
            '/api/dispatch/technician-roster', '/api/dispatch/technicians',
            '/api/dispatch/create-work-order', '/api/dispatch/auto-assign',
            '/api/dispatch/smart-assign', '/api/dispatch/assignment-scores/<wo_id>',
            '/api/dispatch/capacity-overview',
            '/api/dispatch/available-techs-with-skills', '/api/customers/search',
        ],
        'source_files': ['app/templates/dispatch_board.html', 'app/routes/dispatch.py', 'data/dispatch_optimization.sql'],
        'config_keys': ['pg_host', 'schema'],
    },
    'map': {
        'title': 'Field Map',
        'description': 'Production-grade dispatch map with Mapbox/OSRM road routing, metro territory visualization, turn-by-turn directions, mobile-responsive bottom sheet, real-time health monitoring, and IoT telemetry drill-down.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Work orders, technicians, metro territories, infrastructure'},
            {'name': 'SQL Warehouse', 'icon': 'wh', 'color': '#1B7FE3', 'detail': 'IoT telemetry from DLT gold tables'},
            {'name': 'Mapbox Directions API', 'icon': 'map', 'color': '#4264FB', 'detail': 'Primary road routing with turn-by-turn (OSRM fallback)'},
        ],
        'tables': ['work_orders', 'technicians', 'customers', 'metro_territories', 'infrastructure_assets', 'gold_iot_device_health', 'silver_iot_telemetry'],
        'apis': [
            '/api/map/data', '/api/map/infrastructure', '/api/map/infrastructure/<id>/iot-telemetry',
            '/api/map/route', '/api/map/territories', '/api/map/breadcrumbs/<id>',
            '/api/technicians/<id>', '/api/work-orders/<id>',
            '/api/map/reassign', '/api/map/escalate', '/api/map/close',
        ],
        'source_files': ['app/templates/map.html', 'app/routes/map_field.py', 'data/field_service_schema.sql'],
        'config_keys': ['pg_host', 'warehouse_id', 'pipeline_catalog', 'MAPBOX_ACCESS_TOKEN'],
    },
    'analytics': {
        'title': 'SLA Analytics',
        'description': 'SLA compliance rates, breach trends, and technician performance powered by materialized views.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Analytics pool (300s timeout) for heavy aggregations'},
        ],
        'tables': ['work_orders', 'mv_technician_leaderboard', 'mv_regional_sla', 'technicians', 'service_regions'],
        'apis': [
            '/api/analytics/sla', '/api/sla/risk-heatmap', '/api/sla/leaderboard',
            '/api/sla/live-risk', '/api/sla/refresh',
        ],
        'source_files': ['app/templates/analytics.html', 'app/routes/analytics.py'],
        'config_keys': ['pg_host'],
    },
    'supervisor': {
        'title': 'AI Supervisor',
        'description': 'Multi-agent chat powered by LangGraph on Model Serving. Routes questions across 4 Genie spaces and synthesizes answers.',
        'services': [
            {'name': 'Model Serving', 'icon': 'ml', 'color': '#9333EA', 'detail': 'LangGraph agent endpoint invocation'},
            {'name': 'MLflow', 'icon': 'ml', 'color': '#0194E2', 'detail': 'Inference trace retrieval'},
            {'name': 'Genie API', 'icon': 'ai', 'color': '#FF6F00', 'detail': 'Agent routes to 4 specialized Genie spaces'},
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Agent long-term memory: conversations + messages (ai_memory schema)'},
        ],
        'tables': ['ai_memory.conversations', 'ai_memory.messages'],
        'apis': ['/api/agent/ask', '/api/agent/ask-stream', '/api/agent/status', '/api/agent/traces'],
        'source_files': ['app/templates/supervisor.html', 'app/routes/genie_agent.py', 'deployment/agent_supervisor.py'],
        'config_keys': ['agent_endpoint_name', 'agent_model_name', 'genie_space_ids'],
    },
    'genie': {
        'title': 'Genie AI',
        'description': 'Direct natural language Q&A with 4 AI/BI Genie spaces covering field ops, DB admin, network health, and SLA analytics.',
        'services': [
            {'name': 'Genie API', 'icon': 'ai', 'color': '#FF6F00', 'detail': 'Space query + conversation via Databricks Genie REST API'},
        ],
        'tables': [],
        'apis': ['/api/genie/spaces', '/api/genie/spaces/<key>/questions', '/api/genie/ask'],
        'source_files': ['app/templates/genie.html', 'app/routes/genie_agent.py'],
        'config_keys': ['genie_space_ids', 'warehouse_id'],
    },
    'network_health': {
        'title': 'Network Health',
        'description': 'Infrastructure risk assessment with D3 topology graph, alarm correlation (raw alerts → root cause incidents), and IoT telemetry from DLT pipeline.',
        'services': [
            {'name': 'SQL Warehouse', 'icon': 'wh', 'color': '#1B7FE3', 'detail': 'Queries DLT gold tables via Statement Execution API'},
            {'name': 'DLT Pipeline', 'icon': 'pipe', 'color': '#00BFA5', 'detail': 'Medallion architecture: raw -> enriched -> aggregated'},
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Network incidents (correlated alarms) stored in Lakebase'},
        ],
        'tables': ['gold_node_maintenance_risk', 'gold_regional_network_summary', 'gold_daily_outage_summary', 'gold_iot_device_health', 'network_incidents'],
        'apis': ['/api/network-health/summary', '/api/network-health/incidents'],
        'source_files': ['app/templates/network_health.html', 'app/routes/network.py'],
        'config_keys': ['warehouse_id', 'pipeline_catalog'],
    },
    'technicians': {
        'title': 'Technicians',
        'description': 'Technician roster with skills, certifications, availability, and performance metrics.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Analytics pool for roster aggregation'},
        ],
        'tables': ['technicians', 'technician_skills', 'skill_types', 'service_regions', 'work_orders'],
        'apis': ['/api/technicians/roster', '/api/technicians/<id>'],
        'source_files': ['app/templates/technicians.html', 'app/routes/analytics.py'],
        'config_keys': ['pg_host'],
    },
    'assets': {
        'title': 'Assets',
        'description': 'Equipment inventory management with regional stock tracking and reorder automation.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Equipment CRUD + reorder triggers'},
        ],
        'tables': ['equipment_inventory', 'equipment_catalog', 'service_regions', 'reorder_requests'],
        'apis': ['/api/inventory/status', '/api/inventory/reorders', '/api/inventory/reserve'],
        'source_files': ['app/templates/assets.html', 'app/routes/assets.py'],
        'config_keys': ['pg_host'],
    },
    'fleet': {
        'title': 'Fleet Health',
        'description': 'Predictive Maintenance Action Center: telemetry-driven health scoring + ML failure prediction + AI-interpreted fault codes feed a closed-loop remediation queue. One click turns a prediction into real operations — creates a maintenance work order, grounds the vehicle, reassigns the technician\'s open jobs to available crews via the dispatch engine, and reserves parts. A Fleet Planner agent decides at scale: under operator constraints (daily budget, service-age gate, per-region grounding cap, repair-vs-retire) it triages the whole at-risk fleet into a budget-bounded repair/retire/defer plan with an ai_query rationale, then batch-executes. Fuel-card data (previously stranded in Google Sheets/AppSheet) and third-party shop invoices are consolidated onto the lakehouse via Auto Loader — surfacing fuel-economy decline as a LEADING failure signal and true cost-per-km / running-cost TCO that feeds the Planner\'s retire-vs-repair economics.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'OLTP fleet inventory, telemetry, DTC codes, maintenance history, fuel transactions, and external-provider invoices'},
            {'name': 'UC Volume Ingestion', 'icon': 'pipe', 'color': '#00BFA5', 'detail': 'Fuel-card + external-maintenance CSV exports (the "off Google Sheets / AppSheet" path) land in a UC Volume and are ingested into the Bronze Managed Iceberg layer — the same Volume landing-zone pattern as the telemetry medallion'},
            {'name': 'Managed Iceberg', 'icon': 'pipe', 'color': '#00BFA5', 'detail': 'Bronze/Silver/Gold medallion in governed Iceberg tables — telemetry (gold_vehicle_health) trains the PdM model and powers "Vehicle Model Reliability"; fuel + maintenance (gold_vehicle_cost) powers cost-per-km / running-cost analytics'},
            {'name': 'MLflow + UC Model Registry', 'icon': 'ml', 'color': '#0194E2', 'detail': 'LightGBM gradient-boosted classifier (vs RandomForest/LogReg, best-by-F1) that learns 7-day telematics trends -> real unplanned-repair outcomes; tracked in MLflow, registered in Unity Catalog as fleet_maintenance_model@production, batch-scored daily'},
            {'name': 'AI Functions (ai_query)', 'icon': 'ai', 'color': '#FF6F00', 'detail': 'Interprets raw DTC codes (grades severity, flags false positives) and writes the Fleet Planner executive rationale'},
            {'name': 'Lakehouse Monitoring', 'icon': 'wh', 'color': '#9333EA', 'detail': 'Data quality and drift monitoring on fleet telemetry'},
            {'name': 'SQL Warehouse', 'icon': 'wh', 'color': '#1B7FE3', 'detail': 'Serverless analytics over gold fleet tables'},
        ],
        'tables': ['fleet_vehicles', 'vehicle_telemetry', 'vehicle_dtc_codes', 'vehicle_maintenance_history',
                   'fuel_transactions', 'external_maintenance', 'v_vehicle_cost_summary',
                   'gold_vehicle_health', 'gold_fleet_summary', 'gold_vehicle_cost'],
        'apis': [
            '/api/fleet/summary', '/api/fleet/vehicles', '/api/fleet/vehicle/<vehicle_id>',
            '/api/fleet/dtc-codes', '/api/fleet/map', '/api/fleet/reliability-by-model',
            '/api/fleet/action-queue', '/api/fleet/remediation-plan/<vehicle_id>', '/api/fleet/remediate',
            '/api/fleet/planner', '/api/fleet/planner/execute',
            '/api/fleet/cost-summary', '/api/fleet/fuel-anomalies',
        ],
        'source_files': [
            'app/routes/fleet.py', 'app/templates/fleet.html', 'data/fleet_management.sql',
            'data/fleet_fuel_and_costs.sql', 'pipelines/iceberg_streaming_pipeline.py',
            'notebooks/ingest_fuel_external.py', 'notebooks/fleet_predictive_maintenance.py',
            'notebooks/score_fleet_work_orders.py', 'notebooks/interpret_dtc_codes.py',
        ],
        'config_keys': ['pg_host', 'pipeline_catalog', 'warehouse_id'],
    },
    'skills': {
        'title': 'Skills Matrix',
        'description': 'Certification matrix showing technician qualifications, skill gaps, and expiration tracking.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Skills and certification data'},
        ],
        'tables': ['technicians', 'technician_skills', 'skill_types'],
        'apis': ['/api/skills/matrix'],
        'source_files': ['app/templates/skills_matrix.html', 'app/routes/network.py'],
        'config_keys': ['pg_host'],
    },
    'operations': {
        'title': 'Operations Center',
        'description': 'Executive dashboard embedded from Databricks AI/BI Lakeview. The dashboard is created by 06_create_dashboard.py during deployment, which publishes it with embed credentials. The embed URL is stored in config.yaml and injected as DASHBOARD_EMBED_URL env var. At runtime, the app renders it as an iframe — no direct Databricks API calls from Flask.',
        'services': [
            {'name': 'Lakeview Dashboard API', 'icon': 'dash', 'color': '#E91E63', 'detail': 'Created via /api/2.0/lakeview/dashboards at deploy time, embedded via iframe at runtime'},
            {'name': 'SQL Warehouse', 'icon': 'wh', 'color': '#1B7FE3', 'detail': 'Executes dashboard SQL queries over UC foreign tables backed by Lakebase'},
            {'name': 'Unity Catalog', 'icon': 'uc', 'color': '#FF6F00', 'detail': 'Foreign tables expose Lakebase PG data to the dashboard SQL'},
        ],
        'tables': ['work_orders (via UC foreign table)', 'technicians (via UC foreign table)', 'service_regions (via UC foreign table)'],
        'apis': [],
        'source_files': ['app/templates/operations_center.html', 'app/routes/pages.py', 'deployment/06_create_dashboard.py'],
        'config_keys': ['dashboard_id', 'warehouse_id', 'catalog_name'],
    },
    'simulator': {
        'title': 'Simulator',
        'description': 'Real-time data generator that creates work orders, dispatches, and completions directly into Lakebase.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'DML writes: INSERT/UPDATE work orders, events, notes'},
        ],
        'tables': ['work_orders', 'technicians', 'events', 'work_order_notes'],
        'apis': ['/api/simulator/start', '/api/simulator/status', '/api/simulator/stop', '/api/simulator/live-stats'],
        'source_files': ['app/templates/simulator.html', 'app/routes/simulator.py', 'notebooks/run_data_generator'],
        'config_keys': ['pg_host', 'scale_preset', 'scale'],
    },
    'whatif': {
        'title': 'What-If Analysis',
        'description': 'Scenario planning using Lakebase Autoscaling database branching. Fork production data instantly, test changes, compare results.',
        'services': [
            {'name': 'Lakebase Autoscaling API', 'icon': 'branch', 'color': '#00BCD4', 'detail': 'Branch creation (copy-on-write) + endpoint provisioning'},
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Run scenario DML on isolated branch'},
        ],
        'tables': ['work_orders', 'technicians', 'service_regions'],
        'apis': ['/api/whatif/scenarios', '/api/whatif/create', '/api/whatif/cleanup'],
        'source_files': ['app/templates/whatif.html', 'app/routes/whatif.py'],
        'config_keys': ['autoscaling_project_id', 'lakebase_type', 'pg_host'],
    },
    'data_api': {
        'title': 'Lakebase Data API',
        'description': 'PostgREST-compatible REST API over Lakebase Postgres: filtering, pagination, multi-table joins (resource embedding), aggregations & nested queries via RPC, and bulk read/write — secured by Databricks OAuth + row-level security.',
        'services': [
            {'name': 'Lakebase Data API', 'icon': 'db', 'color': '#00A972', 'detail': 'PostgREST REST front end (OAuth bearer auth) over the Lakebase instance'},
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Backing OLTP data + SETOF RPC functions (aggregation / nested queries)'},
        ],
        'tables': ['work_orders', 'customers', 'appointments', 'technicians', 'service_regions', 'data_api_demo'],
        'apis': ['/api/data-api/filter-paginate', '/api/data-api/embed', '/api/data-api/rpc-aggregate', '/api/data-api/rpc-nested', '/api/data-api/large-result', '/api/data-api/bulk-write'],
        'source_files': ['app/templates/data_api.html', 'app/routes/data_api.py', 'data/data_api_demo.sql'],
        'config_keys': ['data_api_url', 'autoscaling_project_id', 'pg_host'],
    },
    'view': {
        'title': 'Data Explorer',
        'description': 'Browse and query Lakebase tables with pagination, search, sorting, and CSV export.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Full schema access via information_schema + psycopg pool'},
            {'name': 'Unity Catalog REST', 'icon': 'uc', 'color': '#FF6F00', 'detail': 'Optional: browse Managed Iceberg tables'},
        ],
        'tables': ['All field_service.* tables', 'information_schema.columns'],
        'apis': ['/api/tables', '/api/columns/<schema>/<table>', '/api/query/<schema>/<table>/paginated', '/api/export/<schema>/<table>/csv'],
        'source_files': ['app/templates/view_data.html', 'app/routes/explorer.py'],
        'config_keys': ['pg_host', 'catalog_name'],
    },
    'architecture': {
        'title': 'Architecture',
        'description': 'Animated data flow diagram showing how the app integrates with the Databricks platform.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Feature introspection (extensions, triggers, roles)'},
        ],
        'tables': [],
        'apis': ['/api/lakebase/capabilities'],
        'source_files': ['app/templates/architecture.html', 'app/routes/architecture.py'],
        'config_keys': ['pg_host', 'lakebase_type'],
    },
    'asbuilt': {
        'title': 'AsBuilt',
        'description': 'Auto-discovered workspace resources overlaid on the IDEA Databricks Data Intelligence Platform map (fork-and-overlay): active services illuminated, unused dimmed, with core-flow and feature-dependency lines.',
        'services': [
            {'name': 'Workspace API', 'icon': 'wk', 'color': '#607D8B', 'detail': 'Resource discovery via Databricks SDK (20+ API calls)'},
        ],
        'tables': [],
        'apis': ['/asbuilt/refresh'],
        'source_files': ['app/templates/asbuilt.html', 'app/static/asbuilt/index.html', 'app/static/asbuilt/overlay.js', 'app/asbuilt_api.py', 'app/generate_overlay.py'],
        'config_keys': ['prefix', 'profile'],
    },
    'health': {
        'title': 'System Health',
        'description': 'Comprehensive health checks across all Databricks services: database, warehouse, pipeline, Genie, and agent.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'Connection health + pool metrics'},
            {'name': 'SQL Warehouse', 'icon': 'wh', 'color': '#1B7FE3', 'detail': 'Warehouse state check'},
            {'name': 'DLT Pipeline', 'icon': 'pipe', 'color': '#00BFA5', 'detail': 'Pipeline health via SDK'},
            {'name': 'Genie API', 'icon': 'ai', 'color': '#FF6F00', 'detail': 'Space availability'},
        ],
        'tables': ['pg_stat_activity'],
        'apis': ['/api/health', '/api/data-freshness', '/api/refresh-dates'],
        'source_files': ['app/templates/health.html', 'app/routes/health.py'],
        'config_keys': ['pg_host', 'warehouse_id', 'pipeline_catalog', 'genie_space_ids', 'agent_endpoint_name'],
    },
    'admin': {
        'title': 'Lakebase Admin',
        'description': 'DBA control center with live query dashboard, ASH history, password rotation, backup/restore, and cluster topology.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'DBA queries: pg_stat_statements, sessions, locks, bloat'},
            {'name': 'Jobs API', 'icon': 'job', 'color': '#D4A76A', 'detail': 'Password rotation job submission'},
            {'name': 'UC Volumes', 'icon': 'uc', 'color': '#FF6F00', 'detail': 'pg_dump backup storage'},
            {'name': 'Lakebase Autoscaling API', 'icon': 'branch', 'color': '#00BCD4', 'detail': 'Cluster topology + CU status'},
        ],
        'tables': ['pg_stat_statements', 'pg_stat_activity', 'ash_history', 'ash_query_log', 'work_orders'],
        'apis': [
            '/api/admin/instance-info', '/api/admin/live-dashboard/summary',
            '/api/admin/live-dashboard/history', '/api/admin/live-dashboard/sessions',
            '/api/admin/cluster-status', '/api/admin/table-bloat',
            '/api/admin/rotation-status', '/api/admin/trigger-rotation',
        ],
        'source_files': ['app/templates/admin.html', 'app/routes/admin.py', 'notebooks/ash_sampler', 'notebooks/rotate_pg_password'],
        'config_keys': ['pg_host', 'autoscaling_project_id', 'lakebase_type', 'instance_name'],
    },
    'lakebase': {
        'title': 'Lakebase Control Tower',
        'description': 'The Lakebase wing landing page. Live view of what sets Lakebase apart from a self-managed Postgres — copy-on-write branching, serverless autoscaling, and lakehouse-native governance — plus the managed Postgres essentials (triggers, agent memory, pooling, observability) backing this app.',
        'services': [
            {'name': 'Lakebase PostgreSQL', 'icon': 'db', 'color': '#00A972', 'detail': 'OLTP metrics: sessions, pool, work-order volume, SLA-risk trigger'},
            {'name': 'Lakebase Autoscaling API', 'icon': 'branch', 'color': '#00BCD4', 'detail': 'Endpoint state, CU limits, open What-If branch count'},
        ],
        'tables': ['work_orders', 'ai_memory.conversations', 'ai_memory.messages', 'pg_stat_activity'],
        'apis': [
            '/api/lakebase/overview', '/api/lakebase/trigger-demo', '/api/lakebase/two-engines',
            '/api/lakebase/undo/start', '/api/lakebase/undo/status',
            '/api/lakebase/universes/start', '/api/lakebase/universes/status',
            '/api/lakebase/load/start', '/api/lakebase/load/status',
            '/api/admin/cluster-status', '/api/whatif/status',
        ],
        'source_files': ['app/templates/lakebase_hub.html', 'app/routes/lakebase.py'],
        'config_keys': ['pg_host', 'autoscaling_project_id', 'lakebase_type'],
    },
}

# Page IDs whose architecture includes a Lakebase-backed service. Drives the
# "Served by Lakebase" chip in the top bar — derived from PAGE_ARCHITECTURE so it
# stays correct automatically as pages are added/changed.
LAKEBASE_PAGES = {
    pid for pid, meta in PAGE_ARCHITECTURE.items()
    if any('Lakebase' in s.get('name', '') for s in meta.get('services', []))
}

@app.context_processor
def inject_source_url():
    """Inject source_url and page architecture into every template."""
    host = os.environ.get('DATABRICKS_HOST', '')
    repo_path = os.environ.get('SOURCE_REPO_PATH', '')
    source_base = ''
    if host and repo_path:
        if not host.startswith('http'):
            host = f"https://{host}"
        source_base = f"{host}/#workspace{repo_path}"
    return {
        'source_base_url': source_base,
        'page_templates': _PAGE_TEMPLATES,
        'page_arch': PAGE_ARCHITECTURE,
        'lakebase_pages': LAKEBASE_PAGES,
    }


# Keys that must NEVER be shown in the architecture panel
_CONFIG_BLOCKLIST = {'app_password', 'token', 'password', 'secret', 'pgpassword'}

# Cached config.yaml (loaded once, refreshed on deploy)
_config_yaml_cache = {'data': None, 'loaded': False}

def _load_config_yaml():
    """Load config.yaml from the deployment directory (best-effort, cached)."""
    if _config_yaml_cache['loaded']:
        return _config_yaml_cache['data']
    try:
        import yaml as _yaml
        # Try workspace path first, then local
        for candidate in [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'deployment', 'config.yaml'),
            '/Workspace/deployment/config.yaml',
        ]:
            try:
                with open(candidate) as f:
                    _config_yaml_cache['data'] = _yaml.safe_load(f) or {}
                    _config_yaml_cache['loaded'] = True
                    return _config_yaml_cache['data']
            except Exception:
                continue
        # Fallback: try reading from env-var-based deployment path
        deploy_dir = os.environ.get('DEPLOYMENT_CONFIG_PATH', '')
        if deploy_dir:
            with open(deploy_dir) as f:
                _config_yaml_cache['data'] = _yaml.safe_load(f) or {}
                _config_yaml_cache['loaded'] = True
                return _config_yaml_cache['data']
    except Exception:
        pass
    _config_yaml_cache['loaded'] = True
    _config_yaml_cache['data'] = {}
    return {}


# ── Line number index for source links ──────────────────────────────────
# Scans app.py at startup to find actual line numbers for routes and API endpoints.
# This way source links go to app.py#L614 instead of just app.py.
_LINE_INDEX = {}  # {'@app.route("/dispatch")': 312, '/api/dispatch/summary': 1800, ...}

def _build_line_index():
    """Scan app.py to build a mapping of route patterns -> line numbers."""
    try:
        app_path = os.path.abspath(__file__)
        with open(app_path) as f:
            for lineno, line in enumerate(f, 1):
                stripped = line.strip()
                # Match @app.route('/path') decorators
                if stripped.startswith("@app.route("):
                    m = re.search(r"['\"]([^'\"]+)['\"]", stripped)
                    if m:
                        _LINE_INDEX[m.group(1)] = lineno
                # Match def function_name(): after route decorator
                elif stripped.startswith("def ") and stripped.endswith(":"):
                    func_name = stripped[4:stripped.index("(")]
                    _LINE_INDEX[f"def:{func_name}"] = lineno
    except Exception:
        pass

_build_line_index()


# ═══════════════════════════════════════════════════════════════════════════
# SIMULATOR STATE + DATA GENERATORS
# ═══════════════════════════════════════════════════════════════════════════
# These stay in app.py because multiple blueprints import them:
#   - simulator blueprint imports _run_generator, _run_iot_generator, etc.
#   - health blueprint imports _sim_state, _pipeline_run_id, _get_iceberg_catalog
#   - map_field blueprint imports TELCO_INFRASTRUCTURE, INFRA_TYPES, _tech_movements, etc.
#   - analytics blueprint imports TELCO_INFRASTRUCTURE, INFRA_TYPES, _haversine_km
#   - dispatch blueprint imports log_event

# ── In-Process Simulator ─────────────────────────────────────────────────
_sim_lock = threading.Lock()
_sim_state = {
    'running': False,
    'thread': None,
    'iot_thread': None,
    'stop_event': threading.Event(),
    'start_time': None,
    'duration_minutes': 0,
    'speed_factor': 0,
    'error': None,
    'iot_files_generated': 0,
    'stb_thread': None,
    'stb_files_generated': 0,
    'incident_thread': None,
    'incidents_generated': 0,
    'pipeline_thread': None,
    'pipeline_phase': 'idle',
    'pipeline_phase_detail': '',
    'pipeline_started_at': None,
    'pipeline_completed_at': None,
}

# Region center coordinates for generating lat/lng on new work orders
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

# ── Telco Infrastructure ─────────────────────────────────────────────────
# Represents cell towers, data centers, fiber hubs, and central offices
# that the telco owns and maintains across the country.
INFRA_TYPES = {
    'cell_tower': {'label': 'Cell Tower', 'icon': 'tower', 'color': '#60A5FA'},
    'data_center': {'label': 'Data Center', 'icon': 'dc', 'color': '#A78BFA'},
    'fiber_hub': {'label': 'Fiber Distribution Hub', 'icon': 'hub', 'color': '#34D399'},
    'central_office': {'label': 'Central Office', 'icon': 'co', 'color': '#F59E0B'},
    'small_cell': {'label': 'Small Cell Node', 'icon': 'sc', 'color': '#6EE7B7'},
}

# ── Metro Area Definitions ─────────────────────────────────────────────
# (code, name, region, lat, lng, size)
# size: 'large' = 14 nodes, 'medium' = 9, 'small' = 5
METRO_AREAS = [
    # Pacific Northwest
    ('SEA', 'Seattle', 'Pacific Northwest', 47.6062, -122.3321, 'large'),
    ('PDX', 'Portland', 'Pacific Northwest', 45.5152, -122.6784, 'medium'),
    ('BOI', 'Boise', 'Pacific Northwest', 43.6150, -116.2023, 'small'),
    ('TAC', 'Tacoma', 'Pacific Northwest', 47.2529, -122.4443, 'small'),
    ('SPO', 'Spokane', 'Pacific Northwest', 47.6588, -117.4260, 'small'),
    # Mountain West / Southwest
    ('DEN', 'Denver', 'Mountain West', 39.7392, -104.9903, 'large'),
    ('PHX', 'Phoenix', 'Southwest', 33.4484, -112.0740, 'large'),
    ('SLC', 'Salt Lake City', 'Mountain West', 40.7608, -111.8910, 'medium'),
    ('ABQ', 'Albuquerque', 'Southwest', 35.0844, -106.6504, 'small'),
    ('LVG', 'Las Vegas', 'Southwest', 36.1699, -115.1398, 'medium'),
    ('TUC', 'Tucson', 'Southwest', 32.2226, -110.9747, 'small'),
    ('COS', 'Colorado Springs', 'Mountain West', 38.8339, -104.8214, 'small'),
    # Bay Area / West
    ('SFO', 'San Francisco', 'Bay Area', 37.7749, -122.4194, 'large'),
    ('LAX', 'Los Angeles', 'Bay Area', 34.0522, -118.2437, 'large'),
    ('SAN', 'San Diego', 'Bay Area', 32.7157, -117.1611, 'medium'),
    ('SAC', 'Sacramento', 'Bay Area', 38.5816, -121.4944, 'small'),
    ('SJC', 'San Jose', 'Bay Area', 37.3382, -121.8863, 'medium'),
    # South Central
    ('DAL', 'Dallas', 'South Central', 32.7767, -96.7970, 'large'),
    ('HOU', 'Houston', 'South Central', 29.7604, -95.3698, 'large'),
    ('AUS', 'Austin', 'South Central', 30.2672, -97.7431, 'medium'),
    ('SAT', 'San Antonio', 'South Central', 29.4241, -98.4936, 'medium'),
    ('OKC', 'Oklahoma City', 'South Central', 35.4676, -97.5164, 'small'),
    ('TUL', 'Tulsa', 'South Central', 36.1540, -95.9928, 'small'),
    # Southeast
    ('ATL', 'Atlanta', 'Southeast', 33.7490, -84.3880, 'large'),
    ('MIA', 'Miami', 'Southeast', 25.7617, -80.1918, 'large'),
    ('TPA', 'Tampa', 'Southeast', 27.9506, -82.4572, 'medium'),
    ('CLT', 'Charlotte', 'Southeast', 35.2271, -80.8431, 'medium'),
    ('ORL', 'Orlando', 'Southeast', 28.5383, -81.3792, 'medium'),
    ('NSH', 'Nashville', 'Southeast', 36.1627, -86.7816, 'small'),
    ('JAX', 'Jacksonville', 'Southeast', 30.3322, -81.6557, 'small'),
    # Midwest
    ('CHI', 'Chicago', 'Midwest', 41.8781, -87.6298, 'large'),
    ('DET', 'Detroit', 'Midwest', 42.3314, -83.0458, 'medium'),
    ('COL', 'Columbus', 'Midwest', 39.9612, -82.9988, 'medium'),
    ('IND', 'Indianapolis', 'Midwest', 39.7684, -86.1581, 'medium'),
    ('MKE', 'Milwaukee', 'Midwest', 43.0389, -87.9065, 'small'),
    ('MSP', 'Minneapolis', 'Midwest', 44.9778, -93.2650, 'medium'),
    ('STL', 'St. Louis', 'Midwest', 38.6270, -90.1994, 'small'),
    ('KCI', 'Kansas City', 'Midwest', 39.0997, -94.5786, 'small'),
    # Northeast
    ('NYC', 'New York', 'Northeast', 40.7484, -73.9857, 'large'),
    ('PHL', 'Philadelphia', 'Northeast', 39.9526, -75.1652, 'large'),
    ('BOS', 'Boston', 'Northeast', 42.3601, -71.0589, 'medium'),
    ('PIT', 'Pittsburgh', 'Northeast', 40.4406, -79.9959, 'small'),
    ('BWI', 'Baltimore', 'Northeast', 39.2904, -76.6122, 'medium'),
    ('DCA', 'Washington DC', 'Northeast', 38.9072, -77.0369, 'large'),
]

# Node templates per metro size: (type, name_suffix)
_LARGE_METRO_NODES = [
    ('central_office', 'Central Office'),
    ('data_center', 'Data Center'),
    ('fiber_hub', 'Fiber Hub North'),
    ('fiber_hub', 'Fiber Hub South'),
    ('fiber_hub', 'Fiber Hub West'),
    ('cell_tower', 'Tower Downtown'),
    ('cell_tower', 'Tower North'),
    ('cell_tower', 'Tower East'),
    ('cell_tower', 'Tower South'),
    ('cell_tower', 'Tower West'),
    ('small_cell', 'Small Cell 1'),
    ('small_cell', 'Small Cell 2'),
    ('small_cell', 'Small Cell 3'),
    ('small_cell', 'Small Cell 4'),
]
_MEDIUM_METRO_NODES = [
    ('central_office', 'Central Office'),
    ('data_center', 'Data Center'),
    ('fiber_hub', 'Fiber Hub'),
    ('fiber_hub', 'Fiber Hub South'),
    ('cell_tower', 'Tower Downtown'),
    ('cell_tower', 'Tower North'),
    ('cell_tower', 'Tower South'),
    ('small_cell', 'Small Cell 1'),
    ('small_cell', 'Small Cell 2'),
]
_SMALL_METRO_NODES = [
    ('central_office', 'Central Office'),
    ('fiber_hub', 'Fiber Hub'),
    ('cell_tower', 'Tower Downtown'),
    ('cell_tower', 'Tower North'),
    ('small_cell', 'Small Cell 1'),
]

_NODE_TEMPLATES = {'large': _LARGE_METRO_NODES, 'medium': _MEDIUM_METRO_NODES, 'small': _SMALL_METRO_NODES}
_STATUS_CYCLE = ['healthy', 'healthy', 'healthy', 'healthy', 'healthy',
                 'healthy', 'healthy', 'maintenance_due', 'maintenance_due', 'critical']


def _generate_infrastructure():
    """Programmatically generate ~500 infrastructure nodes from metro definitions."""
    nodes = []
    for code, name, region, lat, lng, size in METRO_AREAS:
        templates = _NODE_TEMPLATES[size]
        for idx, (node_type, suffix) in enumerate(templates):
            node_id = f"{code}-{node_type[:2].upper()}-{idx + 1:02d}"
            status = _STATUS_CYCLE[(hash(node_id) % len(_STATUS_CYCLE))]
            nodes.append({
                'id': node_id,
                'name': f'{name} {suffix}',
                'type': node_type,
                'lat': round(lat + (idx * 0.012 - len(templates) * 0.006), 4),
                'lng': round(lng + (idx * 0.015 - len(templates) * 0.0075), 4),
                'status': status,
                'region': region,
            })
    return nodes


TELCO_INFRASTRUCTURE = _generate_infrastructure()

# ── IoT Device Registry ─────────────────────────────────────────────────
# Each infrastructure asset gets 2-4 IoT devices based on type.
# Deterministic: same devices every time for stable IDs.
IOT_DEVICE_TEMPLATES = {
    'cell_tower': [
        ('RU', 'radio_unit'),
        ('PM', 'power_monitor'),
        ('ES', 'env_sensor'),
        ('GW', 'gateway'),
    ],
    'data_center': [
        ('PM', 'power_monitor'),
        ('ES', 'env_sensor'),
        ('NS', 'network_switch'),
        ('CS', 'cooling_sensor'),
    ],
    'fiber_hub': [
        ('OA', 'optical_amplifier'),
        ('PM', 'power_monitor'),
        ('ES', 'env_sensor'),
    ],
    'central_office': [
        ('RU', 'radio_unit'),
        ('PM', 'power_monitor'),
        ('ES', 'env_sensor'),
        ('GW', 'gateway'),
    ],
    'small_cell': [
        ('RU', 'radio_unit'),
        ('PM', 'power_monitor'),
    ],
}

IOT_DEVICES = []
for _infra in TELCO_INFRASTRUCTURE:
    _templates = IOT_DEVICE_TEMPLATES.get(_infra['type'], [('PM', 'power_monitor')])
    for _idx, (_suffix, _dev_type) in enumerate(_templates, 1):
        IOT_DEVICES.append({
            'device_id': f"IOT-{_infra['id']}-{_suffix}-{_idx:02d}",
            'infrastructure_id': _infra['id'],
            'device_type': _dev_type,
            'lat': _infra['lat'] + (_idx * 0.0001),
            'lng': _infra['lng'] + (_idx * 0.0001),
            'infra_status': _infra['status'],
            'firmware_version': f"v{2 + (_idx % 3)}.{_idx}.{hash(_infra['id']) % 20}",
        })

log.info(f"IoT device registry: {len(IOT_DEVICES)} devices across {len(TELCO_INFRASTRUCTURE)} infrastructure assets")

# ── STB (Set Top Box) Device Registry ─────────────────────────────────
# Simulates residential set-top boxes across subscriber households.
# Each household gets 1-3 STBs (living room, bedroom, basement).
STB_MODELS = [
    ('XG2v2-P', 'rev3', True, True),    # Premium 4K DVR
    ('XG1v4-A', 'rev2', True, False),    # Standard DVR
    ('Xi6-T', 'rev1', False, True),      # 4K streaming box (no DVR)
    ('Xi5-S', 'rev1', False, False),     # Basic streaming box
    ('XG2v2-P', 'rev4', True, True),     # Newer premium
    ('AX061AEI', 'rev2', True, False),   # Pace DVR
]

STB_REGIONS = {
    'Mountain West': {'center': (39.74, -104.99), 'spread': 0.15, 'households': 1200},
    'Bay Area': {'center': (37.77, -122.42), 'spread': 0.20, 'households': 1400},
    'Northeast': {'center': (40.75, -73.99), 'spread': 0.15, 'households': 1600},
    'South Central': {'center': (32.78, -96.80), 'spread': 0.12, 'households': 1000},
    'Midwest': {'center': (41.88, -87.63), 'spread': 0.15, 'households': 1200},
    'Pacific Northwest': {'center': (47.61, -122.33), 'spread': 0.10, 'households': 800},
    'Southeast': {'center': (33.75, -84.39), 'spread': 0.12, 'households': 800},
    'Southwest': {'center': (33.45, -112.07), 'spread': 0.12, 'households': 600},
}

STB_SERVICE_TIERS = ['basic', 'standard', 'premium']
STB_LOCATION_TYPES = ['living_room', 'bedroom', 'basement', 'office']
STB_FIRMWARE_VERSIONS = [
    'PROD_22.3.1_build.42', 'PROD_22.4.0_build.15', 'PROD_23.1.0_build.8',
    'PROD_23.2.1_build.30', 'PROD_24.1.0_build.3', 'BETA_24.2.0_build.1',
]

STB_DEVICES = []
_stb_hh_idx = 0
random.seed(42)  # Deterministic registry
for _region_name, _region_cfg in STB_REGIONS.items():
    _clat, _clng = _region_cfg['center']
    _spread = _region_cfg['spread']
    for _hh in range(_region_cfg['households']):
        _stb_hh_idx += 1
        _hh_id = f"HH-{_region_name[:3].upper()}-{_stb_hh_idx:05d}"
        _hh_lat = _clat + random.uniform(-_spread, _spread)
        _hh_lng = _clng + random.uniform(-_spread, _spread)
        _tier = random.choice(STB_SERVICE_TIERS)
        _num_boxes = 1 if _tier == 'basic' else (random.choice([2, 3]) if _tier == 'premium' else random.choice([1, 2]))
        # Assign a health profile to each household (determines telemetry quality)
        _hh_health = random.choices(['healthy', 'degrading', 'failing'], weights=[75, 18, 7])[0]
        for _box_idx in range(_num_boxes):
            _model, _hw_rev, _dvr, _is_4k = STB_MODELS[(_stb_hh_idx + _box_idx) % len(STB_MODELS)]
            _fw = STB_FIRMWARE_VERSIONS[(_stb_hh_idx + _box_idx) % len(STB_FIRMWARE_VERSIONS)]
            _loc = STB_LOCATION_TYPES[_box_idx % len(STB_LOCATION_TYPES)]
            STB_DEVICES.append({
                'device_id': f"STB-{_region_name[:3].upper()}-{_stb_hh_idx:05d}-{chr(65 + _box_idx)}",
                'household_id': _hh_id,
                'model': _model,
                'hardware_revision': _hw_rev,
                'firmware_version': _fw,
                'install_date': f"202{random.randint(1, 4)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
                'mac_address': ':'.join(f"{random.randint(0,255):02X}" for _ in range(6)),
                'location_type': _loc,
                'lat': round(_hh_lat + (_box_idx * 0.00005), 6),
                'lng': round(_hh_lng + (_box_idx * 0.00005), 6),
                'region': _region_name,
                'service_tier': _tier,
                'dvr_enabled': _dvr,
                'is_4k': _is_4k,
                'health_profile': _hh_health,
            })
random.seed()  # Reset seed for runtime randomness

log.info(f"STB device registry: {len(STB_DEVICES)} set-top boxes across {_stb_hh_idx} households")


def _generate_stb_telemetry_file():
    """Generate one pipe-delimited CSV file with a reading from every STB device."""
    now = datetime.now(timezone.utc)
    filename = f"stb_telemetry_{now.strftime('%Y%m%d_%H%M%S')}_{random.randint(0, 999):03d}.csv"
    header = "device_id|household_id|timestamp|signal_snr_db|signal_power_dbmv|downstream_freq_mhz|upstream_power_dbmv|corrected_errors|uncorrected_errors|cpu_utilization_pct|memory_utilization_pct|temperature_celsius|uptime_hours|boot_count_30d|tuner_lock_failures|hdmi_connection_status|wifi_signal_strength_dbm|content_type|channel_number|stream_bitrate_mbps|buffering_events|playback_errors|firmware_version|error_codes|dvr_disk_usage_pct|lat|lng"
    rows = [header]

    # Time-of-day content correlation (UTC)
    hour = now.hour
    is_primetime = 0 <= hour <= 5 or hour >= 23  # ~6pm-midnight ET
    is_daytime = 12 <= hour <= 22                  # morning/afternoon ET

    for dev in STB_DEVICES:
        hp = dev['health_profile']

        # Content type based on time of day
        if is_primetime:
            content_type = random.choices(['live_tv', 'dvr', 'vod', 'app'], weights=[50, 20, 20, 10])[0]
        elif is_daytime:
            content_type = random.choices(['live_tv', 'dvr', 'vod', 'app', 'idle'], weights=[15, 10, 20, 15, 40])[0]
        else:
            content_type = random.choices(['dvr', 'idle'], weights=[20, 80])[0]

        # Base telemetry correlated with health profile
        if hp == 'failing':
            snr = round(random.uniform(15, 28), 1)
            sig_power = round(random.uniform(-15, -8), 1)
            corrected = random.randint(50, 500)
            uncorrected = random.randint(10, 100)
            cpu = round(random.uniform(60, 95), 1)
            memory = round(random.uniform(70, 95), 1)
            temp = round(random.uniform(60, 82), 1)
            uptime = round(random.uniform(0.5, 72), 1)
            boot_count = random.randint(5, 20)
            tuner_fails = random.randint(3, 25)
            buffering = random.randint(5, 30)
            playback_err = random.randint(3, 15)
            bitrate = round(random.uniform(1.0, 8.0), 2)
            error_codes = random.choice(['E101', 'E101|E205', 'E301|E101', 'E205|E410', 'E301'])
            wifi_dbm = round(random.uniform(-85, -65), 1)
        elif hp == 'degrading':
            snr = round(random.uniform(28, 35), 1)
            sig_power = round(random.uniform(-8, -2), 1)
            corrected = random.randint(10, 80)
            uncorrected = random.randint(1, 15)
            cpu = round(random.uniform(40, 70), 1)
            memory = round(random.uniform(50, 75), 1)
            temp = round(random.uniform(45, 62), 1)
            uptime = round(random.uniform(48, 500), 1)
            boot_count = random.randint(2, 6)
            tuner_fails = random.randint(1, 5)
            buffering = random.randint(2, 8)
            playback_err = random.randint(1, 5)
            bitrate = round(random.uniform(5.0, 15.0), 2)
            error_codes = random.choice(['', '', 'E205', 'E101'])
            wifi_dbm = round(random.uniform(-70, -50), 1)
        else:  # healthy
            snr = round(random.uniform(35, 42), 1)
            sig_power = round(random.uniform(-2, 8), 1)
            corrected = random.randint(0, 15)
            uncorrected = random.randint(0, 2)
            cpu = round(random.uniform(15, 45), 1)
            memory = round(random.uniform(30, 55), 1)
            temp = round(random.uniform(32, 48), 1)
            uptime = round(random.uniform(200, 2000), 1)
            boot_count = random.randint(0, 2)
            tuner_fails = random.randint(0, 1)
            buffering = random.randint(0, 2)
            playback_err = random.randint(0, 1)
            bitrate = round(random.uniform(12.0, 25.0), 2)
            error_codes = ''
            wifi_dbm = round(random.uniform(-50, -30), 1)

        # Adjust for content type (idle devices use less)
        if content_type == 'idle':
            cpu = round(cpu * 0.3, 1)
            bitrate = 0.0
            buffering = 0
            playback_err = 0

        hdmi = 'connected' if random.random() > 0.05 else 'disconnected'
        downstream_freq = random.choice([549, 555, 561, 567, 573, 579, 585])
        upstream_power = round(random.uniform(35, 50), 1)
        channel = random.randint(1, 999) if content_type == 'live_tv' else ''
        dvr_disk = round(random.uniform(20, 95), 1) if dev['dvr_enabled'] else 0.0

        row = f"{dev['device_id']}|{dev['household_id']}|{now.isoformat()}|{snr}|{sig_power}|{downstream_freq}|{upstream_power}|{corrected}|{uncorrected}|{cpu}|{memory}|{temp}|{uptime}|{boot_count}|{tuner_fails}|{hdmi}|{wifi_dbm}|{content_type}|{channel}|{bitrate}|{buffering}|{playback_err}|{dev['firmware_version']}|{error_codes}|{dvr_disk}|{dev['lat']}|{dev['lng']}"
        rows.append(row)

    return '\n'.join(rows), filename


def _run_stb_generator(speed_factor=1):
    """Background thread: generate STB telemetry CSV files and upload to UC Volume."""
    stop = _sim_state['stop_event']
    interval = max(15 / max(speed_factor, 0.1), 2)
    catalog = os.environ.get('PIPELINE_CATALOG', 'dba-lakebase-network')
    volume_path = f'/Volumes/{catalog}/stb_data/raw_files/'

    log.info(f"STB generator started (interval={interval:.1f}s, {len(STB_DEVICES)} devices)")

    while not stop.is_set():
        try:
            csv_content, filename = _generate_stb_telemetry_file()
            w = get_workspace_client()
            w.files.upload(
                file_path=volume_path + filename,
                contents=io.BytesIO(csv_content.encode('utf-8')),
                overwrite=True,
            )
            _sim_state['stb_files_generated'] = _sim_state.get('stb_files_generated', 0) + 1
            log.info(f"STB telemetry uploaded: {filename} ({len(STB_DEVICES)} readings)")
        except Exception as e:
            log_error("stb_generator", e)

        stop.wait(interval)

    log.info("STB generator stopped")


def _generate_iot_telemetry_file():
    """Generate one pipe-delimited CSV file with a reading from every IoT device."""
    now = datetime.now(timezone.utc)
    filename = f"iot_telemetry_{now.strftime('%Y%m%d_%H%M%S')}_{random.randint(0, 999):03d}.csv"
    header = "device_id|infrastructure_id|device_type|timestamp|signal_strength_dbm|throughput_mbps|latency_ms|packet_loss_pct|temperature_celsius|battery_pct|connected_clients|error_count|firmware_version|lat|lng"
    rows = [header]

    for dev in IOT_DEVICES:
        status = dev['infra_status']
        # Base values correlated with infrastructure status
        if status == 'critical':
            signal = round(random.uniform(-110, -75), 1)
            throughput = round(random.uniform(5, 80), 2)
            latency = round(random.uniform(30, 200), 2)
            packet_loss = round(random.uniform(1.0, 8.0), 2)
            temp = round(random.uniform(45, 85), 1)
            battery = round(random.uniform(10, 40), 1)
            clients = random.randint(0, 20)
            errors = random.randint(5, 50)
        elif status == 'maintenance_due':
            signal = round(random.uniform(-85, -50), 1)
            throughput = round(random.uniform(50, 200), 2)
            latency = round(random.uniform(10, 60), 2)
            packet_loss = round(random.uniform(0.3, 2.0), 2)
            temp = round(random.uniform(30, 55), 1)
            battery = round(random.uniform(30, 70), 1)
            clients = random.randint(5, 80)
            errors = random.randint(1, 15)
        else:  # healthy
            signal = round(random.uniform(-65, -20), 1)
            throughput = round(random.uniform(100, 500), 2)
            latency = round(random.uniform(1, 15), 2)
            packet_loss = round(random.uniform(0.0, 0.5), 2)
            temp = round(random.uniform(20, 40), 1)
            battery = round(random.uniform(70, 100), 1)
            clients = random.randint(10, 200)
            errors = random.randint(0, 3)

        row = f"{dev['device_id']}|{dev['infrastructure_id']}|{dev['device_type']}|{now.isoformat()}|{signal}|{throughput}|{latency}|{packet_loss}|{temp}|{battery}|{clients}|{errors}|{dev['firmware_version']}|{dev['lat']}|{dev['lng']}"
        rows.append(row)

    return '\n'.join(rows), filename


def _run_incident_generator(speed_factor=1):
    """Background thread: generate network incidents to keep the Network Health page alive.

    Creates, escalates, and resolves incidents on a realistic lifecycle so
    there are always 3-6 active/investigating incidents during a demo.
    """
    stop = _sim_state['stop_event']
    interval = max(25 / max(speed_factor, 0.1), 5)
    SCHEMA = 'field_service'

    SEVERITIES = ['critical', 'major', 'minor', 'warning']
    SEV_WEIGHTS = [0.10, 0.25, 0.40, 0.25]
    CLASSIFICATIONS = ['fiber_cut', 'power_outage', 'equipment_failure', 'capacity_exceeded', 'software_fault']
    REGIONS = ['Pacific Northwest', 'Southwest', 'South Central', 'Southeast', 'Midwest', 'Northeast']
    ROOT_CAUSES = {
        'fiber_cut': ['Construction crew severed underground fiber conduit', 'Vehicle collision damaged aerial fiber span', 'Rodent damage to buried fiber cable'],
        'power_outage': ['Utility power failure affecting site', 'UPS battery exhaustion after extended outage', 'Generator fuel depletion during backup operation'],
        'equipment_failure': ['OLT line card failure', 'GPON splitter malfunction', 'Optical amplifier degradation', 'Switch fabric error'],
        'capacity_exceeded': ['Peak usage exceeded port capacity', 'Bandwidth saturation on backbone link', 'Connection pool exhausted on aggregation node'],
        'software_fault': ['Firmware crash after auto-update', 'Memory leak in routing daemon', 'Configuration sync failure between redundant nodes'],
    }

    # Get starting sequence number
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COALESCE(MAX(CAST(SUBSTRING(incident_number FROM 5) AS INTEGER)), 0) FROM {SCHEMA}.network_incidents")
                seq = cur.fetchone()[0]
    except Exception:
        seq = 200

    log.info(f"Incident generator started (interval={interval:.1f}s, seq starts at INC-{seq+1:06d})")

    while not stop.is_set():
        try:
            pool = get_pool()
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    # Create new incident (60% chance)
                    if random.random() < 0.60:
                        seq += 1
                        severity = random.choices(SEVERITIES, weights=SEV_WEIGHTS)[0]
                        classification = random.choice(CLASSIFICATIONS)
                        region = random.choice(REGIONS)
                        root_cause = random.choice(ROOT_CAUSES[classification])
                        sev_mult = {'critical': 4, 'major': 3, 'minor': 2, 'warning': 1}[severity]
                        affected_nodes = random.randint(1, 5) * sev_mult
                        affected_customers = affected_nodes * random.randint(50, 350)
                        raw_alarms = affected_nodes * random.randint(3, 8)
                        cur.execute(f"""
                            INSERT INTO {SCHEMA}.network_incidents
                                (incident_number, severity, classification, status, region,
                                 root_cause, affected_nodes, affected_customers, raw_alarm_count,
                                 first_alarm_at, detected_at, created_by)
                            VALUES (%s, %s, %s, 'open', %s, %s, %s, %s, %s,
                                    CURRENT_TIMESTAMP - interval '2 minutes', CURRENT_TIMESTAMP, 'correlation_engine')
                        """, (f'INC-{seq:06d}', severity, classification, region,
                              root_cause, affected_nodes, affected_customers, raw_alarms))
                        with _sim_lock:
                            _sim_state['incidents_generated'] = _sim_state.get('incidents_generated', 0) + 1

                    # Escalate an open incident (30% chance)
                    if random.random() < 0.30:
                        cur.execute(f"""
                            UPDATE {SCHEMA}.network_incidents
                            SET status = CASE status WHEN 'open' THEN 'investigating' WHEN 'investigating' THEN 'mitigating' ELSE status END,
                                notes = COALESCE(notes, '') || E'\\n' || %s
                            WHERE incident_id = (
                                SELECT incident_id FROM {SCHEMA}.network_incidents
                                WHERE status IN ('open', 'investigating')
                                ORDER BY RANDOM() LIMIT 1
                            )
                        """, (f"[{datetime.now(timezone.utc).strftime('%H:%M')}] NOC team investigating — dispatching field crew.",))

                    # Resolve an old incident (40% chance — targets incidents > 3 min old)
                    if random.random() < 0.40:
                        cur.execute(f"""
                            UPDATE {SCHEMA}.network_incidents
                            SET status = 'resolved',
                                resolved_at = CURRENT_TIMESTAMP,
                                mttr_minutes = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - detected_at)::INTEGER / 60
                            WHERE incident_id = (
                                SELECT incident_id FROM {SCHEMA}.network_incidents
                                WHERE status IN ('open', 'investigating', 'mitigating')
                                  AND detected_at < CURRENT_TIMESTAMP - interval '3 minutes'
                                ORDER BY detected_at ASC LIMIT 1
                            )
                        """)

                    conn.commit()
        except Exception as e:
            log_error("incident_generator", e)

        stop.wait(interval)

    log.info("Incident generator stopped")


def _run_iot_generator(speed_factor=1):
    """Background thread: generate IoT telemetry CSV files and upload to UC Volume.
    Also periodically triggers the DLT pipeline so new files get processed."""
    stop = _sim_state['stop_event']
    # Floor of 2s: below that the upload round trip dominates and we would just
    # queue requests. Volume above ~7x therefore scales readings per file instead
    # (see sweeps below) rather than shortening this interval further.
    interval = max(15 / max(speed_factor, 0.1), 2)  # 15s at 1x, faster at higher speed
    pipeline_interval = max(60 / max(speed_factor, 0.1), 30)  # re-trigger pipeline every ~60s
    catalog = os.environ.get('PIPELINE_CATALOG', 'dba-lakebase-network')
    volume_path = f'/Volumes/{catalog}/network_data/raw_files/'
    last_pipeline_trigger = 0

    log.info(f"IoT generator started (interval={interval:.1f}s, pipeline_interval={pipeline_interval:.0f}s, {len(IOT_DEVICES)} devices)")

    while not stop.is_set():
        try:
            # Throughput is bound by the upload round trip and the 2s floor below,
            # not by row count — so at higher volume settings emit more readings per
            # file rather than more small files. Small files are also the worst case
            # for the streaming pipeline that consumes this volume.
            sweeps = max(1, int(speed_factor / 10))
            csv_content, filename = _generate_iot_telemetry_file()
            if sweeps > 1:
                # Generate each extra sweep fresh rather than repeating rows —
                # duplicated readings would carry identical device ids, timestamps
                # and values, which is neither realistic telemetry nor useful input
                # for the pipeline.
                lines = csv_content.split("\n")
                header, body = lines[0], [l for l in lines[1:] if l]
                for _ in range(sweeps - 1):
                    extra, _fn = _generate_iot_telemetry_file()
                    body.extend(l for l in extra.split("\n")[1:] if l)
                csv_content = "\n".join([header] + body) + "\n"
            w = get_workspace_client()
            w.files.upload(
                file_path=volume_path + filename,
                contents=io.BytesIO(csv_content.encode('utf-8')),
                overwrite=True,
            )
            _sim_state['iot_files_generated'] += 1
            log.info(
                f"IoT telemetry uploaded: {filename} "
                f"({len(IOT_DEVICES) * sweeps} readings, {sweeps} sweep(s))"
            )
        except Exception as e:
            log_error("iot_generator", e)

        # Periodically re-trigger the DLT pipeline to process new files
        now = time.monotonic()
        if now - last_pipeline_trigger >= pipeline_interval:
            _trigger_iot_pipeline()
            last_pipeline_trigger = now

        stop.wait(interval)

    log.info("IoT generator stopped")


# In-memory technician movement tracking — waypoint-based for street-like paths
# tech_id -> { waypoints: [(lat,lng), ...], seg_durations: [float, ...],
#              current_idx: int, seg_start: float, wo_id }
_tech_movements = {}
_tech_movements_lock = threading.Lock()

# On-site timers: tech_id -> timestamp when they should transition back to available
_onsite_timers = {}

# Driving speed for demo visualization — fast enough to see clear movement on the map
# while still following the road geometry. Real city driving is ~45 km/h, but for a
# live demo we accelerate to make movement visually obvious within seconds.
_DRIVE_SPEED_KMH = 65  # Realistic city driving speed — looks like a real GPS tracker


def _haversine_km(lat1, lng1, lat2, lng2):
    """Haversine distance between two lat/lng points in km."""
    R = 6371.0
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlng / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _compute_segment_durations(waypoints, speed_kmh=_DRIVE_SPEED_KMH):
    """Compute per-segment travel time in seconds based on real distance."""
    durations = []
    for i in range(len(waypoints) - 1):
        dist_km = _haversine_km(waypoints[i][0], waypoints[i][1],
                                waypoints[i + 1][0], waypoints[i + 1][1])
        secs = (dist_km / speed_kmh) * 3600
        durations.append(max(secs, 0.2))
    return durations


def _synthetic_route(origin_lat, origin_lng, dest_lat, dest_lng, num_points=40):
    """Generate a fast synthetic curved route (no network call)."""
    waypoints = []
    for i in range(num_points + 1):
        t = i / num_points
        lat = origin_lat + (dest_lat - origin_lat) * t
        lng = origin_lng + (dest_lng - origin_lng) * t
        if 0 < i < num_points:
            perp = math.sin(t * math.pi) * 0.0008
            jitter_lat = random.uniform(-0.0002, 0.0002)
            jitter_lng = random.uniform(-0.0002, 0.0002)
            lat += perp + jitter_lat
            lng += perp * 0.5 + jitter_lng
        waypoints.append((lat, lng))
    seg_durs = _compute_segment_durations(waypoints)
    return waypoints, seg_durs


def _get_osrm_route(origin_lat, origin_lng, dest_lat, dest_lng):
    """Call OSRM public routing engine to get real road geometry."""
    import urllib.request

    url = (
        f"http://router.project-osrm.org/route/v1/driving/"
        f"{origin_lng},{origin_lat};{dest_lng},{dest_lat}"
        f"?overview=full&geometries=geojson"
    )
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'FieldOpsFSM/1.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())

        if data.get('code') != 'Ok' or not data.get('routes'):
            return None

        coords = data['routes'][0]['geometry']['coordinates']
        waypoints = [(c[1], c[0]) for c in coords]

        if len(waypoints) < 2:
            return None

        return waypoints
    except Exception as e:
        log.debug(f"OSRM route failed: {e}")
        return None


# LRU route cache to avoid repeated OSRM calls for similar routes
from functools import lru_cache as _lru_cache

@_lru_cache(maxsize=200)
def _get_cached_route_inner(key):
    """Internal cached route lookup (LRU eviction)."""
    return _get_osrm_route(key[0], key[1], key[2], key[3])


def _get_cached_route(origin_lat, origin_lng, dest_lat, dest_lng):
    """Get OSRM route with LRU caching. Rounds coords to ~100m grid for cache hits."""
    key = (round(origin_lat, 3), round(origin_lng, 3),
           round(dest_lat, 3), round(dest_lng, 3))
    return _get_cached_route_inner(key)


def _sample_waypoints(waypoints, max_points=40):
    """Downsample a dense polyline to a manageable number of waypoints."""
    if len(waypoints) <= max_points:
        return waypoints

    step = (len(waypoints) - 1) / (max_points - 1)
    sampled = []
    for i in range(max_points):
        idx = min(int(i * step), len(waypoints) - 1)
        sampled.append(waypoints[idx])
    sampled[-1] = waypoints[-1]
    return sampled


def _process_onsite_timers():
    """Transition on_site techs back to available when their timer expires."""
    now = time.time()
    expired = [tid for tid, expires_at in _onsite_timers.items() if now >= expires_at]
    if not expired:
        return
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                for tid in expired:
                    cur.execute("""
                        UPDATE field_service.technicians
                        SET status = 'available', updated_at = CURRENT_TIMESTAMP
                        WHERE technician_id = %s
                    """, (tid,))
                    del _onsite_timers[tid]
            conn.commit()
    except Exception as e:
        log_error("onsite_timer", e)


def _bootstrap_moving_techs(pool):
    """Create movement routes for techs and maintain a realistic status mix."""
    if not _sim_state.get('running'):
        return

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.technician_id, t.status, t.current_latitude, t.current_longitude, t.region_id,
                           wo.latitude, wo.longitude, wo.work_order_id
                    FROM field_service.technicians t
                    LEFT JOIN field_service.work_orders wo
                        ON wo.assigned_technician_id = t.technician_id
                        AND wo.status NOT IN ('completed', 'cancelled')
                        AND wo.latitude IS NOT NULL
                    WHERE t.is_active = true
                      AND t.current_latitude IS NOT NULL
                      AND t.current_longitude IS NOT NULL
                    ORDER BY wo.updated_at DESC NULLS LAST
                """)
                all_techs = cur.fetchall()

                status_counts = {}
                for r in all_techs:
                    s = r[1]
                    status_counts[s] = status_counts.get(s, 0) + 1
                total = len(all_techs)
                en_route_count = status_counts.get('en_route', 0)

                target_en_route = int(total * 0.30)
                need_dispatch = max(0, target_en_route - en_route_count)

                if need_dispatch > 0:
                    available_techs = [r for r in all_techs if r[1] == 'available']
                    random.shuffle(available_techs)
                    dispatched = 0
                    for r in available_techs[:need_dispatch]:
                        tech_id = r[0]
                        with _tech_movements_lock:
                            if tech_id in _tech_movements:
                                continue
                        tlat, tlng, region_id = float(r[2]), float(r[3]), r[4]
                        wo_lat, wo_lng, wo_id = r[5], r[6], r[7]
                        if wo_lat and wo_lng:
                            dest_lat, dest_lng = float(wo_lat), float(wo_lng)
                        else:
                            rc = REGION_COORDS.get(region_id, (39.8, -98.5))
                            dest_lat = rc[0] + random.uniform(-0.012, 0.012)
                            dest_lng = rc[1] + random.uniform(-0.012, 0.012)
                            wo_id = None
                        cur.execute("""
                            UPDATE field_service.technicians
                            SET status = 'en_route', updated_at = CURRENT_TIMESTAMP
                            WHERE technician_id = %s
                        """, (tech_id,))
                        waypoints, seg_durs = _synthetic_route(tlat, tlng, dest_lat, dest_lng)
                        with _tech_movements_lock:
                            _tech_movements[tech_id] = {
                                'waypoints': waypoints, 'seg_durations': seg_durs,
                                'current_idx': 0, 'seg_start': time.time(),
                                'wo_id': wo_id, 'region_id': region_id,
                            }
                        dispatched += 1
                    if dispatched:
                        conn.commit()
                        log.info(f"GPS feed: dispatched {dispatched} techs to en_route")

        # Create movement for any en_route/available techs not yet tracked
        created = 0
        for r in all_techs:
            tech_id, status, tlat, tlng, region_id = r[0], r[1], r[2], r[3], r[4]
            wo_lat, wo_lng, wo_id = r[5], r[6], r[7]
            if status not in ('en_route', 'available'):
                continue
            with _tech_movements_lock:
                if tech_id in _tech_movements:
                    continue

            origin_lat, origin_lng = float(tlat), float(tlng)
            if status == 'en_route' and wo_lat and wo_lng:
                dest_lat, dest_lng = float(wo_lat), float(wo_lng)
                route_wo_id = wo_id
            else:
                rc = REGION_COORDS.get(region_id, (39.8, -98.5))
                dest_lat = rc[0] + random.uniform(-0.012, 0.012)
                dest_lng = rc[1] + random.uniform(-0.012, 0.012)
                route_wo_id = None

            waypoints, seg_durs = _synthetic_route(origin_lat, origin_lng, dest_lat, dest_lng)
            with _tech_movements_lock:
                _tech_movements[tech_id] = {
                    'waypoints': waypoints, 'seg_durations': seg_durs,
                    'current_idx': 0, 'seg_start': time.time(),
                    'wo_id': route_wo_id, 'region_id': region_id,
                }
            created += 1

        if created:
            log.info(f"GPS feed: created routes for {created} techs")
    except Exception as e:
        log_error("bootstrap_moving_techs", e)


def _generate_street_route(origin_lat, origin_lng, dest_lat, dest_lng):
    """Generate a route between two points using real road geometry from OSRM.
    Falls back to a simple grid pattern if OSRM is unavailable."""
    road_route = _get_cached_route(origin_lat, origin_lng, dest_lat, dest_lng)
    if road_route:
        waypoints = _sample_waypoints(road_route, max_points=40)
        seg_durations = _compute_segment_durations(waypoints)
        total_time = sum(seg_durations)
        log.debug(f"OSRM route: {len(waypoints)} waypoints, {total_time:.1f}s total travel time")
        return waypoints, seg_durations

    log.debug(f"OSRM unavailable, using grid fallback for ({origin_lat:.3f},{origin_lng:.3f}) -> ({dest_lat:.3f},{dest_lng:.3f})")
    waypoints = [(origin_lat, origin_lng)]
    dlat = dest_lat - origin_lat
    dlng = dest_lng - origin_lng
    num_segments = max(4, min(12, int((abs(dlat) + abs(dlng)) / 0.008)))
    lat_first = random.random() < 0.5
    cur_lat, cur_lng = origin_lat, origin_lng

    for i in range(1, num_segments):
        p = i / num_segments
        jlat = random.uniform(-0.002, 0.002)
        jlng = random.uniform(-0.002, 0.002)
        if (lat_first and i % 2 == 1) or (not lat_first and i % 2 == 0):
            cur_lat = origin_lat + dlat * p + jlat
        else:
            cur_lng = origin_lng + dlng * p + jlng
        waypoints.append((round(cur_lat, 6), round(cur_lng, 6)))

    waypoints.append((dest_lat, dest_lng))
    seg_durations = _compute_segment_durations(waypoints)
    return waypoints, seg_durations

# ── FSM Work Order Templates ─────────────────────────────────────────────
WO_TEMPLATES = [
    {'category': 'install', 'subcategory': 'fiber_install', 'issue': 'New fiber service installation requested', 'priority_weights': [0.0, 0.1, 0.7, 0.2]},
    {'category': 'install', 'subcategory': 'ont_setup', 'issue': 'ONT device setup and activation', 'priority_weights': [0.0, 0.1, 0.6, 0.3]},
    {'category': 'install', 'subcategory': 'router_install', 'issue': 'Customer router installation and WiFi configuration', 'priority_weights': [0.0, 0.05, 0.7, 0.25]},
    {'category': 'repair', 'subcategory': 'no_service', 'issue': 'Customer reporting complete loss of service', 'priority_weights': [0.1, 0.4, 0.4, 0.1]},
    {'category': 'repair', 'subcategory': 'intermittent', 'issue': 'Intermittent connectivity issues reported', 'priority_weights': [0.0, 0.2, 0.6, 0.2]},
    {'category': 'repair', 'subcategory': 'slow_speed', 'issue': 'Customer experiencing slow download/upload speeds', 'priority_weights': [0.0, 0.15, 0.6, 0.25]},
    {'category': 'repair', 'subcategory': 'fiber_cut', 'issue': 'Suspected fiber cut — no optical signal', 'priority_weights': [0.2, 0.5, 0.3, 0.0]},
    {'category': 'repair', 'subcategory': 'equipment_failure', 'issue': 'CPE equipment malfunction — device unresponsive', 'priority_weights': [0.05, 0.3, 0.5, 0.15]},
    {'category': 'maintenance', 'subcategory': 'firmware_update', 'issue': 'Scheduled firmware update for customer equipment', 'priority_weights': [0.0, 0.0, 0.3, 0.7]},
    {'category': 'maintenance', 'subcategory': 'line_test', 'issue': 'Routine line quality test and optimization', 'priority_weights': [0.0, 0.0, 0.2, 0.8]},
    {'category': 'maintenance', 'subcategory': 'preventive', 'issue': 'Preventive maintenance — signal degradation detected', 'priority_weights': [0.0, 0.1, 0.5, 0.4]},
    {'category': 'upgrade', 'subcategory': 'speed_upgrade', 'issue': 'Service tier upgrade — higher speed plan provisioning', 'priority_weights': [0.0, 0.05, 0.6, 0.35]},
    {'category': 'upgrade', 'subcategory': 'equipment_upgrade', 'issue': 'Equipment upgrade — replace legacy ONT/router', 'priority_weights': [0.0, 0.1, 0.5, 0.4]},
    {'category': 'disconnect', 'subcategory': 'service_disconnect', 'issue': 'Customer requested service disconnection', 'priority_weights': [0.0, 0.0, 0.3, 0.7]},
]

PRIORITY_LABELS = ['emergency', 'high', 'medium', 'low']
CATEGORY_WEIGHTS = {'install': 0.22, 'repair': 0.40, 'maintenance': 0.18, 'upgrade': 0.15, 'disconnect': 0.05}

TECH_NOTES = [
    "Tested signal levels — downstream {ds_db} dBm, upstream {us_db} dBm. Within acceptable range.",
    "Replaced ONT unit SN:{sn}. New unit provisioned and tested. Service restored.",
    "Ran OTDR trace — found {otdr_dist}m fault. Repaired fiber splice at distribution point.",
    "Updated firmware from v{fw_old} to v{fw_new}. Customer CPE rebooted successfully.",
    "Verified WiFi coverage — {wifi_rooms} rooms tested. Signal strength acceptable in all areas.",
    "Replaced damaged fiber drop cable ({cable_m}m run). Tested end-to-end — no loss detected.",
    "Customer router factory reset and reconfigured. SSID and credentials provided to customer.",
    "Installed new Calix 844G ONT. Provisioned on PON port {pon_port}. All services verified.",
    "Speed test results: {down_mbps} Mbps down / {up_mbps} Mbps up. Matches provisioned tier.",
    "Checked splitter and fiber connections at terminal. Cleaned connectors. Signal improved by {improve_db} dB.",
]


def _pick_priority(template):
    """Choose a priority based on the template's weight distribution."""
    return random.choices(PRIORITY_LABELS, weights=template['priority_weights'])[0]


def _pick_template():
    """Choose a work order template weighted by category frequency."""
    categories = list(CATEGORY_WEIGHTS.keys())
    weights = [CATEGORY_WEIGHTS[c] for c in categories]
    chosen_cat = random.choices(categories, weights=weights)[0]
    templates_for_cat = [t for t in WO_TEMPLATES if t['category'] == chosen_cat]
    return random.choice(templates_for_cat)


def _generate_tech_note():
    """Generate a realistic technician note with random values."""
    template = random.choice(TECH_NOTES)
    return template.format(
        ds_db=round(random.uniform(-8, -2), 1),
        us_db=round(random.uniform(1, 4), 1),
        sn=f"CXNK{random.randint(10000000, 99999999)}",
        otdr_dist=random.randint(50, 2000),
        fw_old=f"{random.randint(20, 22)}.{random.randint(0, 3)}.{random.randint(0, 9)}",
        fw_new=f"{random.randint(22, 24)}.{random.randint(0, 3)}.{random.randint(0, 9)}",
        wifi_rooms=random.randint(3, 8),
        cable_m=random.randint(15, 100),
        pon_port=f"{random.randint(1, 8)}/{random.randint(1, 16)}",
        down_mbps=random.choice([100, 200, 300, 500, 940]),
        up_mbps=random.choice([50, 100, 200, 500]),
        improve_db=round(random.uniform(1.5, 6.0), 1),
    )


def _run_generator(duration_minutes, speed_factor=1):
    """Background thread: generate realistic FSM data into Lakebase."""
    SCHEMA = 'field_service'
    log.info(f"[GENERATOR] Thread started: duration={duration_minutes}m, speed={speed_factor}x")
    try:
        pool = get_pool()
        log.info(f"[GENERATOR] Pool acquired: {pool}")

        # Load reference data
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT customer_id, region_id, service_type FROM {SCHEMA}.customers WHERE account_status = 'active' ORDER BY RANDOM() LIMIT 50000")
                customers = [{'id': r[0], 'region_id': r[1], 'service_type': r[2]} for r in cur.fetchall()]

                cur.execute(f"SELECT technician_id, region_id, current_latitude, current_longitude FROM {SCHEMA}.technicians WHERE is_active = TRUE")
                technicians = [{'id': r[0], 'region_id': r[1],
                               'lat': float(r[2]) if r[2] else None,
                               'lng': float(r[3]) if r[3] else None} for r in cur.fetchall()]

                cur.execute(f"SELECT sla_id, sla_name, priority, response_hours, resolution_hours FROM {SCHEMA}.sla_policies")
                sla_lookup = {}
                sla_by_priority = {}
                for r in cur.fetchall():
                    sla_lookup[r[0]] = {'sla_name': r[1], 'priority': r[2], 'response_hours': r[3], 'resolution_hours': r[4]}
                    sla_by_priority[r[2]] = {'sla_id': r[0], 'resolution_hours': r[4]}

                # Get equipment for parts consumption
                cur.execute(f"""
                    SELECT inventory_id, equipment_type_id, region_id
                    FROM {SCHEMA}.equipment_inventory
                    WHERE status = 'in_stock'
                    ORDER BY RANDOM() LIMIT 10000
                """)
                inventory = [{'id': r[0], 'equipment_type_id': r[1], 'region_id': r[2]} for r in cur.fetchall()]

        if not customers:
            _sim_state['error'] = 'No active customers found'
            return

        log.info(f"Simulator: {len(customers)} customers, {len(technicians)} techs, {len(inventory)} parts (speed={speed_factor}x)")
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        stop = _sim_state['stop_event']
        sf = max(speed_factor, 0.1)

        # Track created work order IDs for lifecycle transitions
        open_orders = []
        assigned_orders = []

        while not stop.is_set() and datetime.now() < end_time:
            rand = random.random()
            try:
                with pool.connection() as conn:
                    with conn.cursor() as cur:
                        if rand < 0.20:
                            # ── Create new work order ── (20%) ────────────
                            customer = random.choice(customers)
                            wo_number = f"WO-SIM-{int(time.time()*1000) % 10000000:07d}"
                            rc = REGION_COORDS.get(customer['region_id'], (39.8, -98.5))
                            wo_lat = rc[0] + random.uniform(-0.25, 0.25)
                            wo_lng = rc[1] + random.uniform(-0.25, 0.25)

                            # 8% chance: predictive maintenance WO (ML-generated)
                            if random.random() < 0.08:
                                equip_types = ['ONT', 'OLT line card', 'fiber splice', 'power supply', 'UPS battery', 'GPON splitter']
                                equip = random.choice(equip_types)
                                conf = round(random.uniform(0.72, 0.98), 4)
                                priority = 'high' if conf > 0.90 else 'medium'
                                sla_info = sla_by_priority.get(priority, {'sla_id': 1, 'resolution_hours': 24})
                                sla_due = datetime.now(timezone.utc) + timedelta(hours=sla_info['resolution_hours'])
                                title = f"Predictive: {equip} replacement recommended"
                                issue = f"ML model flagged {equip} for potential failure within 14 days. Confidence: {round(conf*100, 1)}%. Proactive replacement recommended to avoid service disruption."
                                cur.execute(f"""
                                    INSERT INTO {SCHEMA}.work_orders (
                                        work_order_number, customer_id, category, subcategory, priority,
                                        status, title, reported_issue, confidence_score,
                                        sla_id, sla_due_at, region_id, latitude, longitude
                                    ) VALUES (%s, %s, 'maintenance', 'predictive_maintenance', %s,
                                              'open', %s, %s, %s, %s, %s, %s, %s, %s)
                                    RETURNING work_order_id
                                """, (wo_number, customer['id'], priority, title, issue, conf,
                                      sla_info['sla_id'], sla_due, customer['region_id'], wo_lat, wo_lng))
                                wo_id = cur.fetchone()[0]
                                log_event(conn, 'work_order.created', 'work_order', wo_id,
                                          'Simulator', {'category': 'maintenance', 'subcategory': 'predictive_maintenance',
                                                        'priority': priority, 'confidence_score': conf,
                                                        'customer_id': customer['id'], 'region_id': customer['region_id']})
                                conn.commit()
                                open_orders.append({'id': wo_id, 'region_id': customer['region_id'],
                                                  'customer_id': customer['id'],
                                                  'category': 'maintenance',
                                                  'lat': wo_lat, 'lng': wo_lng})
                            else:
                                template = _pick_template()
                                priority = _pick_priority(template)
                                sla_info = sla_by_priority.get(priority, {'sla_id': 1, 'resolution_hours': 24})
                                sla_due = datetime.now(timezone.utc) + timedelta(hours=sla_info['resolution_hours'])
                                cur.execute(f"""
                                    INSERT INTO {SCHEMA}.work_orders (
                                        work_order_number, customer_id, category, subcategory, priority,
                                        status, title, reported_issue,
                                        sla_id, sla_due_at, region_id, latitude, longitude
                                    ) VALUES (%s, %s, %s, %s, %s, 'open', %s, %s, %s, %s, %s, %s, %s)
                                    RETURNING work_order_id
                                """, (
                                    wo_number, customer['id'], template['category'], template['subcategory'],
                                    priority, template['issue'], template['issue'],
                                    sla_info['sla_id'], sla_due, customer['region_id'], wo_lat, wo_lng
                                ))
                                wo_id = cur.fetchone()[0]
                                log_event(conn, 'work_order.created', 'work_order', wo_id,
                                          'Simulator', {'category': template['category'],
                                                        'priority': priority,
                                                        'customer_id': customer['id'],
                                                        'sla_due_at': sla_due.isoformat(),
                                                        'region_id': customer['region_id']})
                                conn.commit()
                                open_orders.append({'id': wo_id, 'region_id': customer['region_id'],
                                                  'customer_id': customer['id'],
                                                  'category': template['category'],
                                                  'lat': wo_lat, 'lng': wo_lng})
                            time.sleep(random.uniform(1.0, 3.0) / sf)

                        elif rand < 0.45 and open_orders:
                            # ── Assign a work order to a technician ── (25%) ──
                            order = open_orders.pop(random.randint(0, min(len(open_orders) - 1, 4)))
                            regional_techs = [t for t in technicians if t['region_id'] == order['region_id']]
                            if regional_techs:
                                tech = random.choice(regional_techs)
                                scheduled = datetime.now(timezone.utc) + timedelta(hours=random.uniform(1, 8))
                                cur.execute(f"""
                                    UPDATE {SCHEMA}.work_orders
                                    SET status = 'assigned',
                                        assigned_technician_id = %s,
                                        updated_at = CURRENT_TIMESTAMP
                                    WHERE work_order_id = %s
                                """, (tech['id'], order['id']))

                                cur.execute(f"""
                                    UPDATE {SCHEMA}.technicians
                                    SET status = 'en_route', updated_at = CURRENT_TIMESTAMP
                                    WHERE technician_id = %s
                                """, (tech['id'],))

                                cur.execute(f"""
                                    INSERT INTO {SCHEMA}.appointments (
                                        work_order_id, technician_id,
                                        scheduled_start, scheduled_end, status
                                    ) VALUES (%s, %s, %s, %s, 'scheduled')
                                """, (order['id'], tech['id'], scheduled,
                                      scheduled + timedelta(hours=random.uniform(1, 3))))
                                log_event(conn, 'work_order.dispatched', 'work_order', order['id'],
                                          'Simulator', {'technician_id': tech['id'],
                                                        'old_status': 'open', 'new_status': 'assigned',
                                                        'region_id': order['region_id']})

                                # Customer communication: ETA notification
                                cust_id = order.get('customer_id')
                                if cust_id:
                                    eta_str = scheduled.strftime('%I:%M %p')
                                    cur.execute(f"""
                                        INSERT INTO {SCHEMA}.customer_communications
                                            (work_order_id, customer_id, channel, direction, comm_type, subject, body, status, sent_at, delivered_at)
                                        VALUES (%s, %s, 'sms', 'outbound', 'eta_notification',
                                                'Technician Dispatched', %s, 'delivered',
                                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + interval '3 seconds')
                                    """, (order['id'], cust_id,
                                          f"EchoStar: Your technician is on the way. Estimated arrival: {eta_str}. Track live at echostar.com/track/{order['id']}"))

                                conn.commit()

                                # Track movement: generate route to work order
                                dest_lat = order.get('lat')
                                dest_lng = order.get('lng')
                                cur.execute(f"SELECT current_latitude, current_longitude FROM {SCHEMA}.technicians WHERE technician_id = %s", (tech['id'],))
                                db_pos = cur.fetchone()
                                rc = REGION_COORDS.get(order['region_id'], (39.8, -98.5))
                                origin_lat = float(db_pos[0]) if db_pos and db_pos[0] else (tech.get('lat') or rc[0] + random.uniform(-0.1, 0.1))
                                origin_lng = float(db_pos[1]) if db_pos and db_pos[1] else (tech.get('lng') or rc[1] + random.uniform(-0.1, 0.1))
                                if not dest_lat or not dest_lng:
                                    dest_lat = rc[0] + random.uniform(-0.2, 0.2)
                                    dest_lng = rc[1] + random.uniform(-0.2, 0.2)
                                waypoints, seg_durs = _generate_street_route(origin_lat, origin_lng, dest_lat, dest_lng)
                                with _tech_movements_lock:
                                    _tech_movements[tech['id']] = {
                                        'waypoints': waypoints,
                                        'seg_durations': seg_durs,
                                        'current_idx': 0,
                                        'seg_start': time.time(),
                                        'wo_id': order['id'],
                                        'region_id': tech.get('region_id'),
                                    }
                                tech['lat'] = dest_lat
                                tech['lng'] = dest_lng

                                assigned_orders.append({**order, 'tech_id': tech['id']})
                            time.sleep(random.uniform(0.5, 2.0) / sf)

                        elif rand < 0.80 and assigned_orders:
                            # ── Complete a work order ── (35%) ────────────
                            order = assigned_orders.pop(random.randint(0, min(len(assigned_orders) - 1, 3)))
                            resolution_note = _generate_tech_note()
                            cur.execute(f"""
                                UPDATE {SCHEMA}.work_orders
                                SET status = 'completed',
                                    resolved_at = CURRENT_TIMESTAMP,
                                    resolution_notes = %s
                                WHERE work_order_id = %s
                            """, (resolution_note, order['id']))

                            tech_id = order.get('tech_id')
                            if tech_id:
                                cur.execute(f"""
                                    UPDATE {SCHEMA}.technicians
                                    SET status = 'available', updated_at = CURRENT_TIMESTAMP,
                                        jobs_completed_mtd = jobs_completed_mtd + 1
                                    WHERE technician_id = %s
                                """, (tech_id,))
                                cur.execute(f"SELECT current_latitude, current_longitude, region_id FROM {SCHEMA}.technicians WHERE technician_id = %s", (tech_id,))
                                trow = cur.fetchone()
                                if trow and trow[0] and trow[1]:
                                    rc = REGION_COORDS.get(trow[2], (39.8, -98.5))
                                    base_lat = rc[0] + random.uniform(-0.012, 0.012)
                                    base_lng = rc[1] + random.uniform(-0.012, 0.012)
                                    waypoints, seg_durs = _generate_street_route(
                                        float(trow[0]), float(trow[1]), base_lat, base_lng)
                                    with _tech_movements_lock:
                                        _tech_movements[tech_id] = {
                                            'waypoints': waypoints,
                                            'seg_durations': seg_durs,
                                            'current_idx': 0,
                                            'seg_start': time.time(),
                                            'wo_id': None,
                                            'region_id': trow[2],
                                        }
                                    tech_obj = next((t for t in technicians if t['id'] == tech_id), None)
                                    if tech_obj:
                                        tech_obj['lat'] = base_lat
                                        tech_obj['lng'] = base_lng
                                else:
                                    with _tech_movements_lock:
                                        _tech_movements.pop(tech_id, None)

                            cur.execute(f"""
                                INSERT INTO {SCHEMA}.work_order_notes (
                                    work_order_id, author, note_type, content
                                ) VALUES (%s, 'Simulator', 'tech_note', %s)
                            """, (order['id'], resolution_note))

                            cur.execute(f"""
                                UPDATE {SCHEMA}.appointments
                                SET status = 'completed',
                                    actual_start = scheduled_start,
                                    actual_end = CURRENT_TIMESTAMP
                                WHERE work_order_id = %s AND status != 'completed'
                            """, (order['id'],))

                            if order['category'] in ('install', 'repair') and inventory and random.random() < 0.6:
                                part = random.choice(inventory)
                                cur.execute(f"""
                                    INSERT INTO {SCHEMA}.work_order_parts (
                                        work_order_id, inventory_id, quantity, action
                                    )
                                    SELECT %s, %s, 1, 'installed'
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM {SCHEMA}.work_order_parts
                                        WHERE work_order_id = %s AND inventory_id = %s
                                    )
                                """, (order['id'], part['id'],
                                      order['id'], part['id']))
                                cur.execute(f"""
                                    UPDATE {SCHEMA}.equipment_inventory
                                    SET status = 'installed',
                                        installed_at = CURRENT_TIMESTAMP
                                    WHERE inventory_id = %s AND status = 'in_stock'
                                """, (part['id'],))

                            log_event(conn, 'work_order.completed', 'work_order', order['id'],
                                      'Simulator', {'old_status': 'assigned',
                                                    'new_status': 'completed',
                                                    'technician_id': order.get('tech_id'),
                                                    'resolution_notes': resolution_note[:100]})

                            # Customer communications: completion notice + survey
                            cust_id = order.get('customer_id')
                            if cust_id:
                                cur.execute(f"""
                                    INSERT INTO {SCHEMA}.customer_communications
                                        (work_order_id, customer_id, channel, direction, comm_type, subject, body, status, sent_at, delivered_at)
                                    VALUES (%s, %s, 'sms', 'outbound', 'completion_notice',
                                            'Service Complete', %s, 'delivered',
                                            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + interval '2 seconds')
                                """, (order['id'], cust_id,
                                      f"EchoStar: Your service request WO-{order['id']} has been completed. Thank you for choosing EchoStar."))
                                cur.execute(f"""
                                    INSERT INTO {SCHEMA}.customer_communications
                                        (work_order_id, customer_id, channel, direction, comm_type, subject, body, status, sent_at, delivered_at)
                                    VALUES (%s, %s, 'email', 'outbound', 'satisfaction_survey',
                                            'How was your service?', %s, 'delivered',
                                            CURRENT_TIMESTAMP + interval '5 minutes', CURRENT_TIMESTAMP + interval '5 minutes 4 seconds')
                                """, (order['id'], cust_id,
                                      f"Hi, we hope your recent service visit went well. Please take a moment to rate your experience: echostar.com/feedback?wo={order['id']}"))

                            conn.commit()
                            time.sleep(random.uniform(2.0, 5.0) / sf)

                        else:
                            # ── Add a note to an existing order ───────────
                            cur.execute(f"""
                                SELECT work_order_id FROM {SCHEMA}.work_orders
                                WHERE status IN ('open', 'assigned', 'in_progress')
                                ORDER BY RANDOM() LIMIT 1
                            """)
                            row = cur.fetchone()
                            if row:
                                note = random.choice([
                                    "Customer called to confirm appointment time.",
                                    "Updated priority per dispatch supervisor request.",
                                    "Customer not available — rescheduled for tomorrow.",
                                    "Parts ordered — ETA 2 business days.",
                                    "Technician reported access issue at premises. Trying alternate entry.",
                                    "Customer confirmed they will be available during service window.",
                                ])
                                cur.execute(f"""
                                    INSERT INTO {SCHEMA}.work_order_notes (
                                        work_order_id, author, note_type, content
                                    ) VALUES (%s, 'Simulator', 'system', %s)
                                """, (row[0], note))
                                conn.commit()
                            time.sleep(random.uniform(1.0, 4.0) / sf)

            except Exception as e:
                log_error("simulator_event", e)
                time.sleep(1)

            # Periodic sweep: every ~10 iterations, check for stranded en_route techs
            if random.random() < 0.1:
                try:
                    _bootstrap_moving_techs(pool)
                except Exception:
                    pass

        log.info(f"Simulator finished (duration={duration_minutes}m)")
    except Exception as e:
        log_error("simulator_fatal", e)
        _sim_state['error'] = str(e)
    finally:
        _sim_state['running'] = False
        # Clear all movements and timers when simulator stops
        with _tech_movements_lock:
            _tech_movements.clear()
        _onsite_timers.clear()


_position_thread_started = False
_position_thread_lock = threading.Lock()


def _ensure_position_thread():
    """Start the position update thread if not already running. Safe to call multiple times."""
    global _position_thread_started
    with _position_thread_lock:
        if _position_thread_started:
            return
        _position_thread_started = True
    t = threading.Thread(target=_update_tech_positions, daemon=True)
    t.start()
    log.info("Position update thread started (200ms tick, always-on)")


def _update_tech_positions():
    """Always-on background thread: walk techs along waypoint routes.
    Ticks every 200ms; writes positions to DB each tick."""
    _sweep_counter = 0
    while True:
        try:
            # Every ~8 seconds, re-bootstrap to dispatch and catch status changes
            _sweep_counter += 1
            if _sweep_counter >= 40:  # 40 * 0.2s = 8s
                _sweep_counter = 0
                try:
                    _bootstrap_moving_techs(get_pool())
                except Exception:
                    pass

            # Skip DB writes when no techs are moving
            with _tech_movements_lock:
                if not _tech_movements:
                    time.sleep(0.5)
                    continue

            now = time.time()
            updates = []
            wo_arrivals = []
            base_arrivals = []

            with _tech_movements_lock:
                for tech_id, mv in list(_tech_movements.items()):
                    waypoints = mv['waypoints']
                    seg_durations = mv['seg_durations']
                    idx = mv['current_idx']

                    if idx >= len(waypoints) - 1:
                        final = waypoints[-1]
                        updates.append((final[0], final[1], tech_id))
                        if mv.get('wo_id'):
                            wo_arrivals.append(tech_id)
                        else:
                            base_arrivals.append((tech_id, final[0], final[1], mv.get('region_id')))
                        del _tech_movements[tech_id]
                        continue

                    seg_dur = seg_durations[idx] if idx < len(seg_durations) else 1.0
                    elapsed = now - mv['seg_start']
                    progress = min(elapsed / seg_dur, 1.0) if seg_dur > 0 else 1.0

                    # Smooth ease-in-out within segment
                    t = progress * progress * (3 - 2 * progress)

                    p1 = waypoints[idx]
                    p2 = waypoints[idx + 1]
                    lat = p1[0] + (p2[0] - p1[0]) * t
                    lng = p1[1] + (p2[1] - p1[1]) * t
                    updates.append((lat, lng, tech_id))

                    if progress >= 1.0:
                        mv['current_idx'] = idx + 1
                        mv['seg_start'] = now

            if updates:
                pool = get_pool()
                with pool.connection(timeout=5) as conn:
                    with conn.cursor() as cur:
                        lats = [round(lat, 6) for lat, lng, tid in updates]
                        lngs = [round(lng, 6) for lat, lng, tid in updates]
                        tids = [tid for lat, lng, tid in updates]
                        cur.execute("""
                            UPDATE field_service.technicians t
                            SET current_latitude = v.lat,
                                current_longitude = v.lng,
                                updated_at = CURRENT_TIMESTAMP
                            FROM unnest(%s::double precision[], %s::double precision[], %s::int[])
                                 AS v(lat, lng, tid)
                            WHERE t.technician_id = v.tid
                        """, (lats, lngs, tids))
                        for tid in wo_arrivals:
                            cur.execute("""
                                UPDATE field_service.technicians
                                SET status = 'on_site', updated_at = CURRENT_TIMESTAMP
                                WHERE technician_id = %s
                            """, (tid,))
                            _onsite_timers[tid] = time.time() + random.uniform(10, 40)
                    conn.commit()

            # Transition on_site techs back to available after their timer expires
            if _sim_state.get('running'):
                _process_onsite_timers()

            # Re-queue patrol routes when simulator is running
            if _sim_state.get('running') and base_arrivals:
                for entry in base_arrivals:
                    tech_id, lat, lng, region_id = entry
                    try:
                        rc = REGION_COORDS.get(region_id, (39.8, -98.5))
                        dest_lat = rc[0] + random.uniform(-0.012, 0.012)
                        dest_lng = rc[1] + random.uniform(-0.012, 0.012)
                        waypoints, seg_durs = _synthetic_route(lat, lng, dest_lat, dest_lng)
                        with _tech_movements_lock:
                            _tech_movements[tech_id] = {
                                'waypoints': waypoints,
                                'seg_durations': seg_durs,
                                'current_idx': 0,
                                'seg_start': time.time(),
                                'wo_id': None,
                                'region_id': region_id,
                            }
                    except Exception:
                        pass
        except Exception as e:
            log_error("tech_position_update", e)
        time.sleep(0.2)


# ═══════════════════════════════════════════════════════════════════════════
# OPEN SOURCE ICEBERG DEMO
# ═══════════════════════════════════════════════════════════════════════════

_iceberg_state = {
    'running': False,
    'stop_event': None,
    'thread': None,
    'ops': [],
    'reads': 0,
    'writes': 0,
    'errors': 0,
    'last_error': None,
    'catalog_ok': False,
}
_iceberg_lock = threading.Lock()

ICEBERG_CATALOG_NAME = os.environ.get('PIPELINE_CATALOG', 'dba-lakebase-network')
ICEBERG_SCHEMA = 'network_data'
ICEBERG_WRITE_TABLE = 'oss_iceberg_analytics'


def _get_iceberg_catalog():
    """Create a PyIceberg RestCatalog pointing at Unity Catalog's Iceberg endpoint."""
    from pyiceberg.catalog.rest import RestCatalog
    w = get_workspace_client()
    config = w.config
    host = (config.host or '').rstrip('/')
    auth_headers = config.authenticate()
    token = auth_headers.get('Authorization', '').replace('Bearer ', '')
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


# ═══════════════════════════════════════════════════════════════════════════
# STB MIGRATION SIMULATOR STATE
# ═══════════════════════════════════════════════════════════════════════════

_migration_state = {
    'running': False,
    'stop_event': threading.Event(),
    'steps': [],
    'current_step': 0,
    'error': None,
    'started_at': None,
    'completed_at': None,
}
_migration_lock = threading.Lock()

MIGRATION_STEPS = [
    {'id': 'create_volume', 'label': 'Create UC Volume', 'tier': 'raw',
     'desc': 'Create stb_source_data volume for raw Parquet files'},
    {'id': 'gen_devices', 'label': 'Write Devices Parquet', 'tier': 'raw',
     'desc': '500 STB device records as Parquet files'},
    {'id': 'gen_telemetry', 'label': 'Write Telemetry Parquet', 'tier': 'raw',
     'desc': '10,000 telemetry readings as Parquet files'},
    {'id': 'gen_incidents', 'label': 'Write Incidents Parquet', 'tier': 'raw',
     'desc': '200 hardware incidents as Parquet files'},
    {'id': 'migrate_devices', 'label': 'CTAS Managed Devices', 'tier': 'managed',
     'desc': 'read_files() -> USING ICEBERG CLUSTER BY (device_id, region)'},
    {'id': 'migrate_telemetry', 'label': 'CTAS Managed Telemetry', 'tier': 'managed',
     'desc': 'read_files() -> USING ICEBERG CLUSTER BY (device_id, reading_date)'},
    {'id': 'migrate_incidents', 'label': 'CTAS Managed Incidents', 'tier': 'managed',
     'desc': 'read_files() -> USING ICEBERG CLUSTER BY (device_id, incident_type)'},
    {'id': 'validate', 'label': 'Validate Migration', 'tier': 'managed',
     'desc': 'Row count match + data integrity check'},
    {'id': 'show_benefits', 'label': 'Query Managed Iceberg', 'tier': 'managed',
     'desc': 'Analytics query on liquid-clustered Iceberg tables'},
]


# ═══════════════════════════════════════════════════════════════════════════
# SQL EXECUTION HELPER (used by multiple modules)
# ═══════════════════════════════════════════════════════════════════════════

def _run_sql(sql, catalog=None):
    """Execute SQL via the statement execution API and return results."""
    wh_id = os.environ.get('SQL_WAREHOUSE_ID', '')
    if not wh_id:
        raise ValueError("SQL_WAREHOUSE_ID not configured")
    w = get_workspace_client()
    body = {
        'warehouse_id': wh_id,
        'statement': sql,
        'wait_timeout': '50s',
    }
    if catalog:
        body['catalog'] = catalog
    resp = w.api_client.do('POST', '/api/2.0/sql/statements', body=body)
    stmt_id = resp.get('statement_id', '')
    state = resp.get('status', {}).get('state', '')

    # Poll with exponential backoff (100ms -> 2s cap, max ~10 min total)
    wait = 0.1
    elapsed = 0
    while state in ('PENDING', 'RUNNING') and elapsed < 600:
        time.sleep(wait)
        elapsed += wait
        wait = min(wait * 1.5, 2.0)
        resp = w.api_client.do('GET', f'/api/2.0/sql/statements/{stmt_id}')
        state = resp.get('status', {}).get('state', '')

    if state == 'FAILED':
        err = resp.get('status', {}).get('error', {}).get('message', 'SQL execution failed')
        raise RuntimeError(err)
    if state not in ('SUCCEEDED',):
        raise RuntimeError(f'SQL statement ended in unexpected state: {state}')
    return resp.get('result', {}).get('data_array', [])


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE TRIGGER
# ═══════════════════════════════════════════════════════════════════════════

ICEBERG_PIPELINE_NOTEBOOK = os.environ.get('ICEBERG_PIPELINE_PATH', '/Workspace/Shared/pipelines/iceberg_streaming_pipeline')
PIPELINE_CATALOG = os.environ.get('PIPELINE_CATALOG', 'dba-lakebase-network')
_pipeline_run_id = None  # Track the most recent pipeline run

def _run_source_refresh_and_pipeline(speed_factor=1):
    """Background thread: regenerate network source files, upload, run pipeline, track progress.

    This ensures the Iceberg gold tables have fresh data every time the simulator starts.
    The pipeline typically takes 3-5 minutes after source files are uploaded.
    """
    stop = _sim_state['stop_event']
    catalog = os.environ.get('PIPELINE_CATALOG', 'dba-lakebase-network')
    volume_path = f'/Volumes/{catalog}/network_data/raw_files'

    try:
        # ── Phase 1: Generate fresh source files ──
        with _sim_lock:
            _sim_state['pipeline_phase'] = 'generating'
            _sim_state['pipeline_phase_detail'] = 'Generating network nodes...'

        import tempfile, shutil
        tmpdir = tempfile.mkdtemp(prefix="sim_refresh_")
        try:
            if 'generate_raw_data' in sys.modules:
                del sys.modules['generate_raw_data']
            import generate_raw_data as raw_gen

            with _sim_lock:
                _sim_state['pipeline_phase_detail'] = 'Generating network nodes (2,000 nodes)...'
            nodes = raw_gen.generate_network_nodes(tmpdir)

            if stop.is_set():
                return

            with _sim_lock:
                _sim_state['pipeline_phase_detail'] = 'Generating performance metrics (500K rows)...'
            raw_gen.generate_performance_metrics(tmpdir, nodes)

            if stop.is_set():
                return

            with _sim_lock:
                _sim_state['pipeline_phase_detail'] = 'Generating outage events (3,000 events)...'
            raw_gen.generate_outage_events(tmpdir, nodes)
        except ImportError as e:
            log.warning(f"Source file generation unavailable: {e}")
            with _sim_lock:
                _sim_state['pipeline_phase'] = 'pipeline_failed'
                _sim_state['pipeline_phase_detail'] = f'Error: {e}'
            return

        if stop.is_set():
            shutil.rmtree(tmpdir, ignore_errors=True)
            return

        # ── Phase 2: Upload to UC Volume ──
        with _sim_lock:
            _sim_state['pipeline_phase'] = 'uploading'
            _sim_state['pipeline_phase_detail'] = 'Uploading files to UC Volume...'

        w = get_workspace_client()
        generated = list(Path(tmpdir).glob('*'))
        for i, f in enumerate(generated):
            with _sim_lock:
                _sim_state['pipeline_phase_detail'] = f'Uploading {f.name} ({i+1}/{len(generated)})...'
            with open(str(f), 'rb') as fh:
                w.files.upload(f'{volume_path}/{f.name}', fh, overwrite=True)
            log.info(f"Uploaded {f.name} to {volume_path}")

        shutil.rmtree(tmpdir, ignore_errors=True)

        if stop.is_set():
            return

        # ── Phase 3: Trigger pipeline and monitor ──
        with _sim_lock:
            _sim_state['pipeline_phase'] = 'pipeline_queued'
            _sim_state['pipeline_phase_detail'] = 'Submitting Iceberg pipeline job...'
            _sim_state['pipeline_started_at'] = time.time()
            _sim_state['pipeline_completed_at'] = None

        global _pipeline_run_id
        _trigger_iot_pipeline()
        run_id = _pipeline_run_id

        if not run_id:
            with _sim_lock:
                _sim_state['pipeline_phase'] = 'pipeline_failed'
                _sim_state['pipeline_phase_detail'] = 'Failed to submit pipeline job'
            return

        # Poll pipeline until complete or simulator stops
        while not stop.is_set():
            try:
                run = w.jobs.get_run(run_id=run_id)
                state = run.state.life_cycle_state.value if run.state else 'UNKNOWN'
                result_state = run.state.result_state.value if run.state and run.state.result_state else ''

                with _sim_lock:
                    if state in ('PENDING', 'QUEUED'):
                        _sim_state['pipeline_phase'] = 'pipeline_queued'
                        _sim_state['pipeline_phase_detail'] = 'Pipeline job queued — waiting for compute...'
                    elif state == 'RUNNING':
                        _sim_state['pipeline_phase'] = 'pipeline_running'
                        elapsed = int(time.time() - _sim_state['pipeline_started_at'])
                        _sim_state['pipeline_phase_detail'] = f'Processing bronze → silver → gold ({elapsed}s elapsed)...'
                    elif state == 'TERMINATED':
                        _sim_state['pipeline_completed_at'] = time.time()
                        if result_state == 'SUCCESS':
                            _sim_state['pipeline_phase'] = 'pipeline_complete'
                            elapsed = int(_sim_state['pipeline_completed_at'] - _sim_state['pipeline_started_at'])
                            _sim_state['pipeline_phase_detail'] = f'Pipeline complete — gold tables refreshed ({elapsed}s)'
                        else:
                            _sim_state['pipeline_phase'] = 'pipeline_failed'
                            _sim_state['pipeline_phase_detail'] = f'Pipeline failed: {result_state}'
                        return
                    else:
                        _sim_state['pipeline_phase'] = 'pipeline_failed'
                        _sim_state['pipeline_phase_detail'] = f'Pipeline unexpected state: {state}'
                        return
            except Exception as e:
                log.warning(f"Pipeline status check: {e}")

            stop.wait(10)

    except Exception as e:
        log_error("source_refresh_and_pipeline", e)
        with _sim_lock:
            _sim_state['pipeline_phase'] = 'pipeline_failed'
            _sim_state['pipeline_phase_detail'] = f'Error: {str(e)[:200]}'


def _trigger_iot_pipeline():
    """Submit the Iceberg streaming pipeline as a Spark job. Fire-and-forget."""
    global _pipeline_run_id
    try:
        w = get_workspace_client()
        if _pipeline_run_id:
            try:
                run = w.jobs.get_run(run_id=_pipeline_run_id)
                state = run.state.life_cycle_state.value if run.state else ''
                if state in ('PENDING', 'RUNNING', 'QUEUED'):
                    log.info(f"Pipeline run {_pipeline_run_id} still {state}, skipping")
                    return
            except Exception:
                pass

        from databricks.sdk.service.jobs import SubmitTask, NotebookTask, \
            Source, JobEnvironment
        from databricks.sdk.service.compute import Environment as ComputeEnvironment

        result = w.jobs.submit(
            run_name='iceberg-iot-pipeline',
            tasks=[SubmitTask(
                task_key='ingest',
                notebook_task=NotebookTask(
                    notebook_path=ICEBERG_PIPELINE_NOTEBOOK,
                    source=Source.WORKSPACE,
                    base_parameters={
                        'catalog': PIPELINE_CATALOG,
                        'schema': 'network_data',
                        'volume_path': f'/Volumes/{PIPELINE_CATALOG}/network_data/raw_files',
                    },
                ),
                environment_key='default',
            )],
            environments=[JobEnvironment(
                environment_key='default',
                spec=ComputeEnvironment(client='1'),
            )],
        )
        _pipeline_run_id = result.run_id
        log.info(f"Iceberg pipeline submitted: run_id={result.run_id}")
    except Exception as e:
        log.warning(f"Pipeline trigger: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# EVENT SOURCING (used by dispatch, map_field, simulator)
# ═══════════════════════════════════════════════════════════════════════════

def log_event(conn, event_type, entity_type, entity_id, actor='system', payload=None):
    """Log a structured event to the events table (event sourcing)."""
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


# ═══════════════════════════════════════════════════════════════════════════
# MULTI-AGENT SUPERVISOR (helper functions imported by genie_agent blueprint)
# ═══════════════════════════════════════════════════════════════════════════

AGENT_ENDPOINT_NAME = os.environ.get('AGENT_ENDPOINT_NAME', '')
AGENT_INFERENCE_TABLE = os.environ.get(
    'AGENT_INFERENCE_TABLE',
    'dba-lakebase-network.agents.multi_genie_supervisor_payload'
)


def _query_genie_space_internal(space_key, question):
    """Reusable Genie query — called by both /api/genie/ask and /api/agent/ask."""
    space_id = GENIE_SPACES[space_key]['id']
    if not space_id:
        return {'status': 'FAILED', 'error': f'Genie space "{space_key}" not configured'}

    w = get_workspace_client()

    response = w.api_client.do(
        method="POST",
        path=f"/api/2.0/genie/spaces/{space_id}/start-conversation",
        body={"content": question}
    )

    if 'message_id' in response:
        message_id = response['message_id']
        conversation_id = response.get('conversation_id') or response.get('conversation', {}).get('id')
    elif 'message' in response:
        message_id = response['message']['id']
        conversation_id = response['conversation']['id']
    elif 'id' in response:
        message_id = response['id']
        conversation_id = response.get('conversation_id')
    else:
        return {'status': 'FAILED', 'error': 'Unexpected API response'}

    for attempt in range(60):
        status_response = w.api_client.do(
            method="GET",
            path=f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        )

        msg_status = status_response.get('status', '')
        if msg_status in ('COMPLETED', 'COMPLETED_WITH_WARNING'):
            attachments = status_response.get('attachments', [])
            result = {'status': msg_status, 'space_key': space_key}
            for att in attachments:
                q = att.get('query', {})
                if q:
                    result['sql'] = q.get('query', '')
                    result['description'] = q.get('description', '')
                    columns = q.get('columns', [])
                    if columns:
                        result['columns'] = [c.get('name', '') for c in columns]
                text = att.get('text', {})
                if text:
                    result['text'] = text.get('content', '')
            return result

        if msg_status in ('FAILED', 'CANCELLED'):
            error_msg = status_response.get('error', {}).get('message', '') if isinstance(status_response.get('error'), dict) else str(status_response.get('error', ''))
            return {'status': 'FAILED', 'error': error_msg or f'Genie query failed: {msg_status}'}

        time.sleep(2)

    return {'status': 'TIMEOUT', 'error': 'Timeout waiting for Genie response'}


def _parse_agent_output(data):
    """Parse agent tags and extract text from ResponsesAgent output."""
    spaces_consulted = []
    text_parts = []

    if 'output' in data:
        for item in data['output']:
            if isinstance(item, dict) and item.get('type') == 'message':
                for c in item.get('content', []):
                    text = c.get('text', '')
                    agent_matches = re.findall(r'<agent>(\w+)</agent>', text)
                    for agent_name in agent_matches:
                        if agent_name not in ('supervisor', '__start__', '__end__') and agent_name not in spaces_consulted:
                            spaces_consulted.append(agent_name)
                    clean_text = re.sub(r'<agent>\w+</agent>\s*', '', text).strip()
                    if clean_text:
                        text_parts.append(clean_text)

    combined = '\n\n'.join(text_parts) if text_parts else str(data.get('output', ''))
    return combined, spaces_consulted


def _build_reasoning_chain(raw_spans):
    """Transform raw MLflow spans into a narrative reasoning story for the UI."""
    if not raw_spans:
        return []

    AGENT_DESCRIPTIONS = {
        'FieldOpsGenie': ('Field Service Operations', 'work orders, technicians, dispatch, and scheduling'),
        'PostgresAdminGenie': ('PostgresAdmin', 'database health, connections, query performance, and replication'),
        'NetworkHealthGenie': ('Network Health', 'network nodes, outages, IoT telemetry, and maintenance risk'),
        'SLAWorkforceGenie': ('SLA & Workforce', 'SLA compliance, technician performance, and regional analytics'),
    }

    span_data = []
    for s in raw_spans:
        name = s.get('name', '')
        attrs = s.get('attributes', {})
        span_type = attrs.get('mlflow.spanType', '')
        status = s.get('status', {})
        status_code = status.get('status_code', 'OK') if isinstance(status, dict) else str(status)
        start_ns = s.get('start_time_unix_nano', 0)
        end_ns = s.get('end_time_unix_nano', 0)
        duration_ms = round((end_ns - start_ns) / 1e6) if start_ns and end_ns else 0

        tokens = {}
        try:
            tokens = json.loads(attrs.get('mlflow.chat.tokenUsage', '{}'))
        except Exception:
            pass

        output_text = ''
        sql_query = ''
        if span_type == 'CHAT_MODEL':
            try:
                out = json.loads(attrs.get('mlflow.spanOutputs', '{}'))
                if isinstance(out, dict):
                    output_text = out.get('content', '') or ''
                    for tc in out.get('tool_calls', []):
                        if isinstance(tc, dict) and tc.get('name', '').startswith('transfer_to_'):
                            output_text = tc['name']
            except Exception:
                pass

        if name in ('ask_question', 'get_query_result', 'execute_query'):
            try:
                out = json.loads(attrs.get('mlflow.spanOutputs', '{}'))
                if isinstance(out, str):
                    if 'SELECT' in out.upper():
                        sql_query = out.strip()
                elif isinstance(out, dict):
                    sql_query = out.get('query', '') or out.get('sql', '') or ''
                    if not sql_query:
                        desc = out.get('description', '')
                        if desc:
                            output_text = desc
            except Exception:
                pass
            if not sql_query:
                try:
                    inp = json.loads(attrs.get('mlflow.spanInputs', '{}'))
                    if isinstance(inp, dict):
                        sql_query = inp.get('query', '') or inp.get('sql', '') or ''
                except Exception:
                    pass

        span_data.append({
            'name': name, 'type': span_type, 'status': status_code,
            'duration_ms': duration_ms, 'tokens': tokens, 'output': output_text,
            'sql': sql_query,
        })

    story = []
    total_llm_tokens = 0
    agents_seen = []
    agent_query_count = {}
    agent_total_time = {}
    agent_errors = {}
    agent_sql = {}
    supervisor_llm_calls = 0
    genie_total_time = 0

    for sd in span_data:
        if sd['type'] == 'CHAT_MODEL':
            total_llm_tokens += sd['tokens'].get('total_tokens', 0)
            supervisor_llm_calls += 1

        if sd['name'].startswith('transfer_to_'):
            agent_key = sd['name'].replace('transfer_to_', '')
            for real_name in AGENT_DESCRIPTIONS:
                if real_name.lower() == agent_key:
                    agent_key = real_name
                    break
            if agent_key not in agents_seen:
                agents_seen.append(agent_key)
            agent_query_count[agent_key] = agent_query_count.get(agent_key, 0) + 1

        if sd['name'] == 'ask_question':
            genie_total_time += sd['duration_ms']
            if agents_seen:
                last_agent = agents_seen[-1]
                agent_total_time[last_agent] = agent_total_time.get(last_agent, 0) + sd['duration_ms']
                if sd['status'] != 'OK':
                    agent_errors[last_agent] = True
                if sd.get('sql') and last_agent not in agent_sql:
                    agent_sql[last_agent] = sd['sql']

    total_duration_ms = 0
    for sd in span_data:
        if sd['name'] == 'predict' or sd['name'] == 'predict_stream':
            total_duration_ms = sd['duration_ms']
            break

    story.append({
        'type': 'narration', 'icon': 'brain',
        'text': 'Supervisor analyzed the question and evaluated which specialized agents could best answer it.',
        'detail': f'Considered {len(AGENT_DESCRIPTIONS)} available agents',
        'duration_ms': None,
    })

    for agent_key in agents_seen:
        friendly_name, domain = AGENT_DESCRIPTIONS.get(agent_key, (agent_key, ''))
        query_count = agent_query_count.get(agent_key, 1)
        query_time = agent_total_time.get(agent_key, 0)
        had_error = agent_errors.get(agent_key, False)
        time_str = f'{query_time / 1000:.1f}s' if query_time else ''

        story.append({
            'type': 'routing', 'icon': 'route',
            'text': f'Decided to consult **{friendly_name}** agent',
            'detail': f'This agent specializes in {domain}' if domain else '',
            'duration_ms': None,
        })

        sql = agent_sql.get(agent_key, '')
        if had_error:
            story.append({
                'type': 'error', 'icon': 'alert',
                'text': f'{friendly_name} encountered a permission error while querying',
                'detail': f'{query_count} {"queries" if query_count > 1 else "query"} attempted' + (f' over {time_str}' if time_str else ''),
                'duration_ms': query_time, 'sql': sql,
            })
        else:
            plural = f'Ran {query_count} queries' if query_count > 1 else 'Queried the Genie space'
            story.append({
                'type': 'agent', 'icon': 'search',
                'text': f'{friendly_name} retrieved data successfully',
                'detail': f'{plural}' + (f' in {time_str}' if time_str else ''),
                'duration_ms': query_time, 'sql': sql,
            })

    if agents_seen:
        if len(agents_seen) > 1:
            agent_names = [AGENT_DESCRIPTIONS.get(a, (a, ''))[0] for a in agents_seen]
            story.append({
                'type': 'narration', 'icon': 'merge',
                'text': f'Supervisor combined results from {len(agents_seen)} agents into a unified answer',
                'detail': ', '.join(agent_names),
                'duration_ms': None,
            })
        else:
            story.append({
                'type': 'narration', 'icon': 'compose',
                'text': 'Supervisor composed the final answer from the agent\'s data',
                'detail': None, 'duration_ms': None,
            })

    story.append({
        'type': 'summary', 'icon': 'stats', 'text': 'Execution complete',
        'stats': {
            'total_time': f'{total_duration_ms / 1000:.1f}s' if total_duration_ms else None,
            'llm_calls': supervisor_llm_calls,
            'total_tokens': total_llm_tokens,
            'genie_queries': sum(agent_query_count.values()),
            'genie_time': f'{genie_total_time / 1000:.1f}s' if genie_total_time else None,
        },
    })

    return story


def _agent_traces_mlflow_fallback(w, limit):
    """Fallback: use MLflow traces REST API when inference table is unavailable."""
    experiment_id = os.environ.get('AGENT_MLFLOW_EXPERIMENT_ID', '')
    if not experiment_id:
        try:
            ep = w.serving_endpoints.get(AGENT_ENDPOINT_NAME)
            served = ep.config.served_entities[0] if ep.config and ep.config.served_entities else None
            if served and served.environment_vars:
                experiment_id = served.environment_vars.get('MLFLOW_EXPERIMENT_ID', '')
        except Exception:
            pass

    if not experiment_id:
        return jsonify({'traces': [], 'error': 'No MLflow experiment configured'})

    data = w.api_client.do('GET', '/api/2.0/mlflow/traces', query={
        'experiment_ids': experiment_id,
        'max_results': str(limit),
    })

    traces = data.get('traces', []) if isinstance(data, dict) else []
    simplified = []
    for trace in traces:
        meta = {}
        for m in trace.get('request_metadata', []):
            meta[m.get('key', '')] = m.get('value', '')
        token_usage = json.loads(meta.get('mlflow.trace.tokenUsage', '{}')) if meta.get('mlflow.trace.tokenUsage') else {}
        span_stats = json.loads(meta.get('mlflow.trace.sizeStats', '{}')) if meta.get('mlflow.trace.sizeStats') else {}

        simplified.append({
            'request_id': trace.get('request_id', ''),
            'execution_time_ms': trace.get('execution_time_ms', 0),
            'status': trace.get('status', ''),
            'num_spans': span_stats.get('num_spans', 0),
            'total_tokens': token_usage.get('total_tokens', 0),
            'input_tokens': token_usage.get('input_tokens', 0),
            'output_tokens': token_usage.get('output_tokens', 0),
            'reasoning_chain': [],
            'source': 'mlflow_api',
        })

    return jsonify({'traces': simplified, 'source': 'mlflow_api'})


# ═══════════════════════════════════════════════════════════════════════════
# DATE REFRESH + STARTUP BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════════

def _rebalance_priorities(cur):
    """Ensure a realistic priority mix: ~5% critical, ~15% high, ~50% medium, ~30% low."""
    BATCH = 500_000
    cur.execute("SELECT MIN(work_order_id), MAX(work_order_id) FROM field_service.work_orders WHERE status NOT IN ('completed', 'cancelled')")
    row = cur.fetchone()
    if not row or not row[0]:
        return
    lo, hi = row
    start = lo
    while start <= hi:
        end = min(start + BATCH - 1, hi)
        cur.execute(f"""
            WITH ranked AS (
                SELECT work_order_id,
                       ROW_NUMBER() OVER (ORDER BY random()) as rn,
                       COUNT(*) OVER () as total
                FROM field_service.work_orders
                WHERE status NOT IN ('completed', 'cancelled')
                  AND work_order_id BETWEEN {start} AND {end}
            )
            UPDATE field_service.work_orders wo SET priority = CASE
                WHEN r.rn <= r.total * 0.05 THEN 'critical'
                WHEN r.rn <= r.total * 0.20 THEN 'high'
                WHEN r.rn <= r.total * 0.70 THEN 'medium'
                ELSE 'low'
            END
            FROM ranked r WHERE wo.work_order_id = r.work_order_id
        """)
        start = end + 1


_refresh_state = {'running': False, 'phase': '', 'progress': '', 'done': False, 'error': None}

REFRESH_BATCH = 500_000


def _batched_update(cur, table, pk, set_clause, where_extra=""):
    """Run a batched UPDATE on a table using PK ranges. Returns total rows updated."""
    cur.execute(f"SELECT MIN({pk}), MAX({pk}) FROM field_service.{table} {('WHERE ' + where_extra) if where_extra else ''}")
    row = cur.fetchone()
    if not row or row[0] is None:
        return 0
    lo, hi = row
    total = 0
    start = lo
    while start <= hi:
        end = min(start + REFRESH_BATCH - 1, hi)
        w = f"{pk} BETWEEN {start} AND {end}"
        if where_extra:
            w += f" AND {where_extra}"
        cur.execute(f"UPDATE field_service.{table} SET {set_clause} WHERE {w}")
        total += cur.rowcount
        start = end + 1
    return total


def _run_date_refresh(pool):
    """Background worker: shift all timestamps forward in batches."""
    global _refresh_state
    SCHEMA = 'field_service'
    t0 = time.time()
    try:
        with pool.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 0")

                cur.execute(f"SELECT MAX(created_at) FROM {SCHEMA}.work_orders WHERE work_order_number NOT LIKE 'WO-SIM-%%'")
                row = cur.fetchone()
                if not row or not row[0]:
                    _refresh_state.update(running=False, done=True, phase='skipped', progress='No seed data found')
                    return

                max_ts = row[0]
                if max_ts.tzinfo is None:
                    from datetime import timezone as tz
                    max_ts = max_ts.replace(tzinfo=tz.utc)

                now = datetime.now(timezone.utc)
                delta_hours = (now - max_ts).total_seconds() / 3600

                if delta_hours < 12:
                    _refresh_state.update(running=False, done=True, phase='fresh', progress=f'Data is {delta_hours:.1f}h old — still fresh')
                    return

                target_offset = f"{int(delta_hours) - 2} hours"
                ivl = f"INTERVAL '{target_offset}'"

                updates = [
                    ("work_orders", "work_orders", "work_order_id",
                     f"created_at = created_at + {ivl}, updated_at = updated_at + {ivl}, "
                     f"sla_due_at = CASE WHEN sla_due_at IS NOT NULL THEN sla_due_at + {ivl} END, "
                     f"first_response_at = CASE WHEN first_response_at IS NOT NULL THEN first_response_at + {ivl} END, "
                     f"resolved_at = CASE WHEN resolved_at IS NOT NULL THEN resolved_at + {ivl} END, "
                     f"closed_at = CASE WHEN closed_at IS NOT NULL THEN closed_at + {ivl} END"),
                    ("appointments", "appointments", "appointment_id",
                     f"scheduled_start = scheduled_start + {ivl}, scheduled_end = scheduled_end + {ivl}, "
                     f"actual_start = CASE WHEN actual_start IS NOT NULL THEN actual_start + {ivl} END, "
                     f"actual_end = CASE WHEN actual_end IS NOT NULL THEN actual_end + {ivl} END, "
                     f"created_at = created_at + {ivl}, updated_at = updated_at + {ivl}"),
                    ("work_order_notes", "work_order_notes", "note_id",
                     f"created_at = created_at + {ivl}"),
                    ("work_order_parts", "work_order_parts", "part_id",
                     f"created_at = created_at + {ivl}"),
                    ("customers", "customers", "customer_id",
                     f"contract_start = contract_start + {ivl}, contract_end = contract_end + {ivl}, "
                     f"created_at = created_at + {ivl}, updated_at = updated_at + {ivl}"),
                    ("technicians", "technicians", "technician_id",
                     f"updated_at = updated_at + {ivl}"),
                    ("equipment_inventory", "equipment_inventory", "equipment_id",
                     f"purchased_at = purchased_at + {ivl}, "
                     f"installed_at = CASE WHEN installed_at IS NOT NULL THEN installed_at + {ivl} END, "
                     f"last_serviced_at = CASE WHEN last_serviced_at IS NOT NULL THEN last_serviced_at + {ivl} END, "
                     f"created_at = created_at + {ivl}"),
                    ("technician_skills", "technician_skills", "skill_id",
                     f"certified_at = certified_at + {ivl}, "
                     f"expires_at = CASE WHEN expires_at IS NOT NULL THEN expires_at + {ivl} END"),
                ]

                total_tables = len(updates) + 2
                for i, (label, table, pk, set_clause) in enumerate(updates):
                    _refresh_state['phase'] = label
                    _refresh_state['progress'] = f"Updating {label} ({i+1}/{total_tables})..."
                    log.info(f"Date refresh: updating {label}...")
                    st = time.time()
                    rows = _batched_update(cur, table, pk, set_clause)
                    log.info(f"Date refresh: {label} done — {rows:,} rows in {time.time()-st:.1f}s")

                _refresh_state['phase'] = 'sla_fix'
                _refresh_state['progress'] = f"Fixing SLA due dates ({len(updates)+1}/{total_tables})..."
                log.info("Date refresh: fixing SLA due dates...")
                cur.execute("SELECT MIN(work_order_id), MAX(work_order_id) FROM field_service.work_orders WHERE status NOT IN ('completed', 'cancelled')")
                sla_range = cur.fetchone()
                if sla_range and sla_range[0] is not None:
                    sla_lo, sla_hi = sla_range
                    sla_start = sla_lo
                    while sla_start <= sla_hi:
                        sla_end = min(sla_start + REFRESH_BATCH - 1, sla_hi)
                        cur.execute(f"""
                            UPDATE field_service.work_orders wo SET
                                sla_due_at = CURRENT_TIMESTAMP + (sp.resolution_hours || ' hours')::INTERVAL
                            FROM field_service.sla_policies sp
                            WHERE wo.sla_id = sp.sla_id
                              AND wo.status NOT IN ('completed', 'cancelled')
                              AND wo.sla_due_at < CURRENT_TIMESTAMP
                              AND wo.work_order_id BETWEEN {sla_start} AND {sla_end}
                        """)
                        sla_start = sla_end + 1

                _refresh_state['phase'] = 'priorities'
                _refresh_state['progress'] = f"Rebalancing priorities ({total_tables}/{total_tables})..."
                log.info("Date refresh: rebalancing priorities...")
                _rebalance_priorities(cur)

                cur.execute("RESET statement_timeout")
                elapsed = time.time() - t0
                _refresh_state.update(running=False, done=True, phase='complete',
                                      progress=f'Refreshed in {elapsed:.0f}s')
                log.info(f"Date refresh complete: shifted by {target_offset} in {elapsed:.0f}s ({elapsed/60:.1f} min)")

    except Exception as e:
        log_error("date_refresh", e)
        _refresh_state.update(running=False, done=True, error=str(e), phase='error',
                              progress=f'Error: {str(e)[:100]}')


def _refresh_dates_if_stale(pool):
    """Kick off a batched date refresh in a background thread. Returns immediately."""
    global _refresh_state
    if _refresh_state.get('running'):
        return
    _refresh_state = {'running': True, 'phase': 'starting', 'progress': 'Starting date refresh...', 'done': False, 'error': None}
    t = threading.Thread(target=_run_date_refresh, args=(pool,), daemon=True)
    t.start()


def _ensure_new_regions(pool):
    """Add Denver and San Francisco regions if they don't exist, and distribute some techs/customers there."""
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM field_service.service_regions WHERE region_code = 'MTW'")
                if cur.fetchone()[0] == 0:
                    cur.execute("""
                        INSERT INTO field_service.service_regions (region_name, region_code, state_list, timezone, manager_name, population_weight, is_urban)
                        VALUES
                        ('Mountain West', 'MTW', 'CO,WY,MT', 'America/Denver', 'Michael Torres', 0.95, TRUE),
                        ('Bay Area',      'BAY', 'CA',       'America/Los_Angeles', 'Jennifer Wu', 1.10, TRUE)
                    """)
                    cur.execute("SELECT region_id FROM field_service.service_regions WHERE region_code = 'MTW'")
                    mtw_id = cur.fetchone()[0]
                    cur.execute("SELECT region_id FROM field_service.service_regions WHERE region_code = 'BAY'")
                    bay_id = cur.fetchone()[0]

                    cur.execute(f"""
                        WITH to_move AS (
                            SELECT technician_id FROM field_service.technicians
                            WHERE region_id IN (3, 5) ORDER BY random() LIMIT 25
                        )
                        UPDATE field_service.technicians SET region_id = {mtw_id},
                            current_latitude = 39.74 + (random() * 0.08 - 0.04),
                            current_longitude = -104.99 + (random() * 0.08 - 0.04)
                        FROM to_move WHERE field_service.technicians.technician_id = to_move.technician_id
                    """)
                    cur.execute(f"""
                        WITH to_move AS (
                            SELECT technician_id FROM field_service.technicians
                            WHERE region_id IN (1, 2) ORDER BY random() LIMIT 25
                        )
                        UPDATE field_service.technicians SET region_id = {bay_id},
                            current_latitude = 37.77 + (random() * 0.08 - 0.04),
                            current_longitude = -122.42 + (random() * 0.08 - 0.04)
                        FROM to_move WHERE field_service.technicians.technician_id = to_move.technician_id
                    """)

                    cur.execute(f"""
                        WITH to_move AS (
                            SELECT customer_id FROM field_service.customers
                            WHERE region_id IN (3, 5) ORDER BY random() LIMIT 5000
                        )
                        UPDATE field_service.customers SET region_id = {mtw_id}
                        FROM to_move WHERE field_service.customers.customer_id = to_move.customer_id
                    """)
                    cur.execute(f"""
                        WITH to_move AS (
                            SELECT customer_id FROM field_service.customers
                            WHERE region_id IN (1, 2) ORDER BY random() LIMIT 5000
                        )
                        UPDATE field_service.customers SET region_id = {bay_id}
                        FROM to_move WHERE field_service.customers.customer_id = to_move.customer_id
                    """)

                    REGION_COORDS[mtw_id] = (39.74, -104.99)
                    REGION_COORDS[bay_id] = (37.77, -122.42)
                    conn.commit()
                    log.info(f"Added Mountain West (id={mtw_id}) and Bay Area (id={bay_id}) regions with techs and customers")
                else:
                    cur.execute("SELECT region_id FROM field_service.service_regions WHERE region_code = 'MTW'")
                    mtw_id = cur.fetchone()[0]
                    cur.execute("SELECT region_id FROM field_service.service_regions WHERE region_code = 'BAY'")
                    bay_id = cur.fetchone()[0]
                    REGION_COORDS[mtw_id] = (39.74, -104.99)
                    REGION_COORDS[bay_id] = (37.77, -122.42)

                for new_id, coords in [(mtw_id, (39.74, -104.99)), (bay_id, (37.77, -122.42))]:
                    cur.execute("""
                        SELECT COUNT(*) FROM field_service.work_orders
                        WHERE region_id = %s AND status NOT IN ('completed', 'cancelled')
                    """, (new_id,))
                    wo_count = cur.fetchone()[0]
                    if wo_count < 30:
                        need = 60 - wo_count
                        cur.execute(f"""
                            WITH to_move AS (
                                SELECT work_order_id FROM field_service.work_orders
                                WHERE region_id NOT IN ({mtw_id}, {bay_id})
                                  AND status NOT IN ('completed', 'cancelled')
                                ORDER BY random() LIMIT {need}
                            )
                            UPDATE field_service.work_orders wo SET
                                region_id = {new_id},
                                latitude = {coords[0]} + (random() - 0.5) * 0.15,
                                longitude = {coords[1]} + (random() - 0.5) * 0.15,
                                updated_at = CURRENT_TIMESTAMP
                            FROM to_move tm WHERE wo.work_order_id = tm.work_order_id
                        """)
                        log.info(f"Moved {need} WOs to region {new_id} ({coords})")
                conn.commit()
    except Exception as e:
        log.warning(f"Region setup failed: {e}")


def _rightsize_data(pool):
    """Right-size the demo data to look like a realistic, well-run field service operation."""
    TARGET_OPEN = 500
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM field_service.work_orders
                    WHERE status NOT IN ('completed', 'cancelled')
                """)
                open_count = cur.fetchone()[0]
                log.info(f"Right-sizing: {open_count} open WOs, target ~{TARGET_OPEN}")

                if open_count > TARGET_OPEN:
                    excess = open_count - TARGET_OPEN
                    cur.execute(f"""
                        WITH to_close AS (
                            SELECT work_order_id
                            FROM field_service.work_orders
                            WHERE status NOT IN ('completed', 'cancelled')
                            ORDER BY created_at ASC
                            LIMIT {excess}
                        )
                        UPDATE field_service.work_orders wo SET
                            status = 'completed',
                            resolved_at = CURRENT_TIMESTAMP - (random() * INTERVAL '48 hours'),
                            sla_met = true,
                            updated_at = CURRENT_TIMESTAMP
                        FROM to_close tc
                        WHERE wo.work_order_id = tc.work_order_id
                    """)
                    log.info(f"Right-sizing: completed {excess} excess WOs")

                _rebalance_priorities(cur)

                cur.execute("""
                    WITH ranked AS (
                        SELECT work_order_id,
                               ROW_NUMBER() OVER (ORDER BY random()) as rn,
                               COUNT(*) OVER () as total
                        FROM field_service.work_orders
                        WHERE status NOT IN ('completed', 'cancelled')
                          AND sla_due_at IS NOT NULL
                    )
                    UPDATE field_service.work_orders wo SET sla_due_at = CASE
                        WHEN r.rn <= r.total * 0.05
                            THEN CURRENT_TIMESTAMP - (random() * INTERVAL '4 hours')
                        WHEN r.rn <= r.total * 0.15
                            THEN CURRENT_TIMESTAMP + (random() * INTERVAL '2 hours')
                        ELSE
                            CURRENT_TIMESTAMP + INTERVAL '2 hours' + (random() * INTERVAL '46 hours')
                    END
                    FROM ranked r WHERE wo.work_order_id = r.work_order_id
                """)

                conn.commit()
                cur.execute("""
                    SELECT COUNT(*) FROM field_service.work_orders
                    WHERE status NOT IN ('completed', 'cancelled')
                """)
                final_count = cur.fetchone()[0]
                log.info(f"Right-sizing complete: {final_count} open WOs with realistic priority/SLA distribution")
    except Exception as e:
        log.warning(f"Right-sizing failed: {e}")


def _assign_local_work_orders(pool):
    """Reassign ALL open WOs to local techs via round-robin."""
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE field_service.work_orders
                    SET assigned_technician_id = NULL
                    WHERE status NOT IN ('completed', 'cancelled')
                """)
                log.info(f"Cleared {cur.rowcount} WO assignments for clean re-assignment")

                cur.execute("""
                    SELECT DISTINCT region_id FROM field_service.work_orders
                    WHERE status NOT IN ('completed', 'cancelled')
                      AND region_id IS NOT NULL
                """)
                regions = [r[0] for r in cur.fetchall()]

                total_assigned = 0
                for region_id in regions:
                    cur.execute("""
                        SELECT work_order_id FROM field_service.work_orders
                        WHERE status NOT IN ('completed', 'cancelled')
                          AND region_id = %s
                        ORDER BY random()
                    """, (region_id,))
                    wo_ids = [r[0] for r in cur.fetchall()]
                    if not wo_ids:
                        continue

                    cur.execute("""
                        SELECT technician_id FROM field_service.technicians
                        WHERE is_active = true AND region_id = %s
                        ORDER BY random()
                    """, (region_id,))
                    tech_ids = [r[0] for r in cur.fetchall()]
                    if not tech_ids:
                        continue

                    for i, wo_id in enumerate(wo_ids):
                        tech_id = tech_ids[i % len(tech_ids)]
                        cur.execute("""
                            UPDATE field_service.work_orders
                            SET assigned_technician_id = %s, status = 'assigned',
                                updated_at = CURRENT_TIMESTAMP
                            WHERE work_order_id = %s
                        """, (tech_id, wo_id))
                        total_assigned += 1

                conn.commit()
                log.info(f"Round-robin assigned {total_assigned} WOs across {len(regions)} regions")
    except Exception as e:
        log.warning(f"Local WO assignment failed: {e}")


def _startup_bootstrap():
    """Run on app startup: refresh dates, right-size data, start GPS feed."""
    pool = None
    for _attempt in range(30):
        try:
            pool = get_pool()
            with pool.connection() as _conn:
                with _conn.cursor() as _cur:
                    _cur.execute("SELECT 1")
            break
        except Exception:
            time.sleep(0.2)
    if pool is None:
        log.warning("Could not initialize connection pool after 6s")
        return
    try:
        _ensure_new_regions(pool)
        _assign_local_work_orders(pool)
        _refresh_dates_if_stale(pool)
        _rightsize_data(pool)
        _ensure_position_thread()
        log.info("Startup complete — activate simulator to begin data generation and movement")
    except Exception as e:
        log.warning(f"Startup bootstrap deferred: {e}")

# Fire bootstrap in background on import (works with gunicorn/Databricks App runner)
threading.Thread(target=_startup_bootstrap, daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════
# WHAT-IF ANALYSIS (Lakebase Autoscaling Branching)
# ═══════════════════════════════════════════════════════════════════════════

LAKEBASE_PROJECT_ID = os.environ.get('LAKEBASE_PROJECT_ID', '')

_active_whatif = {}
_active_whatif_lock = threading.Lock()

WHATIF_SCENARIOS = {
    'reassign_breached': {
        'name': 'Reassign SLA-Breached to Top Performers',
        'description': 'Reassign all SLA-breached high-priority orders to the top 3 technicians per region by historical completion rate.',
        'icon': 'arrow-right-left',
    },
    'escalate_repairs': {
        'name': 'Escalate Medium Repairs to High Priority',
        'description': 'Promote all medium-priority repair orders to high priority to see the SLA impact.',
        'icon': 'arrow-up',
    },
    'redistribute_load': {
        'name': 'Redistribute Regional Workload',
        'description': 'Balance work orders evenly across available technicians to reduce hotspots.',
        'icon': 'scale',
    },
}


def _whatif_get_branch_conn(branch_id):
    """Connect to a specific branch on the Autoscaling project using native PG auth."""
    wc = get_workspace_client()
    endpoints = wc.api_client.do(
        "GET",
        f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT_ID}/branches/{branch_id}/endpoints"
    )
    ep = endpoints["endpoints"][0]
    host = ep["status"]["hosts"].get("host")
    log.info(f"Autoscaling branch conn: host={host} branch={branch_id}")

    pg_user = os.environ.get("PGUSER", "")
    pg_password = os.environ.get("PGPASSWORD", "")

    conn = psycopg.connect(
        host=host, port=5432, user=pg_user, password=pg_password,
        dbname="databricks_postgres", sslmode="require"
    )
    return conn, host


def _whatif_query_kpis(conn):
    """Query KPI metrics from a branch or production connection."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status NOT IN ('completed', 'cancelled')) as open_orders,
                COUNT(*) FILTER (WHERE status NOT IN ('completed', 'cancelled') AND sla_due_at < CURRENT_TIMESTAMP) as sla_breached,
                COUNT(*) FILTER (WHERE status NOT IN ('completed', 'cancelled') AND sla_due_at BETWEEN CURRENT_TIMESTAMP AND CURRENT_TIMESTAMP + INTERVAL '2 hours') as sla_at_risk,
                ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - created_at))/3600) FILTER (WHERE resolved_at IS NOT NULL)::numeric, 1) as avg_resolution_hours,
                COUNT(*) FILTER (WHERE sla_met = true AND resolved_at >= CURRENT_DATE) as sla_met_today,
                COUNT(*) FILTER (WHERE sla_met IS NOT NULL AND resolved_at >= CURRENT_DATE) as sla_total_today
            FROM field_service.work_orders
        """)
        row = cur.fetchone()
        sla_pct = round((row[4] / max(row[5], 1)) * 100, 1) if row[5] else 0
        return {
            'open_orders': row[0],
            'sla_breached': row[1],
            'sla_at_risk': row[2],
            'avg_resolution_hours': float(row[3]) if row[3] else 0,
            'sla_compliance_pct': sla_pct,
        }


def _whatif_run_reassign_breached(conn):
    """Scenario: reassign SLA-breached high-priority orders to top performers per region."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH breached AS (
                SELECT wo.work_order_id, wo.region_id
                FROM field_service.work_orders wo
                WHERE wo.status NOT IN ('completed', 'cancelled')
                  AND wo.priority IN ('critical', 'high')
                  AND wo.sla_due_at < CURRENT_TIMESTAMP
            ),
            top_techs AS (
                SELECT t.technician_id, t.region_id,
                       ROW_NUMBER() OVER (PARTITION BY t.region_id ORDER BY t.first_fix_rate DESC NULLS LAST, t.avg_rating DESC NULLS LAST) as rn
                FROM field_service.technicians t
                WHERE t.is_active = true AND t.status IN ('available', 'en_route')
            ),
            top3 AS (
                SELECT * FROM top_techs WHERE rn <= 3
            ),
            assignments AS (
                SELECT b.work_order_id,
                       (SELECT tt.technician_id FROM top3 tt
                        WHERE tt.region_id = b.region_id
                        ORDER BY tt.rn LIMIT 1) as new_tech
                FROM breached b
            )
            UPDATE field_service.work_orders wo SET
                assigned_technician_id = a.new_tech,
                status = 'assigned',
                updated_at = CURRENT_TIMESTAMP
            FROM assignments a
            WHERE wo.work_order_id = a.work_order_id
              AND a.new_tech IS NOT NULL
        """)
        reassigned = cur.rowcount
        conn.commit()
        return {'reassigned': reassigned}


def _whatif_run_escalate_repairs(conn):
    """Scenario: promote all medium-priority repair orders to high priority."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE field_service.work_orders
            SET priority = 'high',
                updated_at = CURRENT_TIMESTAMP
            WHERE status NOT IN ('completed', 'cancelled')
              AND category = 'repair'
              AND priority = 'medium'
        """)
        escalated = cur.rowcount
        conn.commit()
        return {'escalated': escalated}


def _whatif_run_redistribute(conn):
    """Scenario: balance work orders evenly across available technicians to reduce hotspots."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT region_id FROM field_service.work_orders
            WHERE status NOT IN ('completed', 'cancelled')
              AND region_id IS NOT NULL
        """)
        regions = [r[0] for r in cur.fetchall()]

        total_redistributed = 0
        for region_id in regions:
            cur.execute("""
                SELECT work_order_id FROM field_service.work_orders
                WHERE status NOT IN ('completed', 'cancelled')
                  AND region_id = %s
                ORDER BY random()
            """, (region_id,))
            wo_ids = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT technician_id FROM field_service.technicians
                WHERE is_active = true AND region_id = %s
                ORDER BY random()
            """, (region_id,))
            tech_ids = [r[0] for r in cur.fetchall()]

            if not tech_ids or not wo_ids:
                continue

            for i, wo_id in enumerate(wo_ids):
                tech_id = tech_ids[i % len(tech_ids)]
                cur.execute("""
                    UPDATE field_service.work_orders
                    SET assigned_technician_id = %s, status = 'assigned',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s
                """, (tech_id, wo_id))
                total_redistributed += 1

        conn.commit()
        return {'redistributed': total_redistributed, 'regions': len(regions)}


# ═══════════════════════════════════════════════════════════════════════════
# CACHE WARMUP + APP ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def _compute_dispatch_summary():
    """Heavy query: dispatch summary (uses analytics pool with 2-min timeout)."""
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:dispatch/summary:consolidated */
                SELECT
                    status,
                    COUNT(*) as cnt,
                    COUNT(*) FILTER (
                        WHERE sla_due_at IS NOT NULL
                          AND sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours'
                          AND sla_due_at > CURRENT_TIMESTAMP
                    ) as sla_at_risk,
                    COUNT(*) FILTER (
                        WHERE sla_due_at IS NOT NULL AND sla_due_at < CURRENT_TIMESTAMP
                    ) as sla_breached,
                    COUNT(*) FILTER (WHERE resolved_at >= CURRENT_DATE) as completed_today,
                    COALESCE(
                        ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600)
                              FILTER (WHERE resolved_at >= CURRENT_DATE)::numeric, 1),
                        0
                    ) as avg_completion_hours,
                    COUNT(*) FILTER (WHERE sla_met = true AND resolved_at >= CURRENT_DATE) as sla_met_today,
                    COUNT(*) FILTER (WHERE sla_met IS NOT NULL AND resolved_at >= CURRENT_DATE) as sla_total_today,
                    category
                FROM field_service.work_orders
                GROUP BY status, category
            """)
            rows = cur.fetchall()

            status_counts = {}
            sla_at_risk = 0
            sla_breached = 0
            completed_today = 0
            avg_hours_sum = 0
            avg_hours_count = 0
            sla_met_today = 0
            sla_total_today = 0
            by_category = {}

            for row in rows:
                st, cnt, at_risk, breached, comp_today, avg_h, met, total, cat = row
                status_counts[st] = status_counts.get(st, 0) + cnt
                if st not in ('completed', 'cancelled'):
                    sla_at_risk += at_risk
                    sla_breached += breached
                    by_category[cat] = by_category.get(cat, 0) + cnt
                if st == 'completed':
                    completed_today += comp_today
                    if comp_today > 0:
                        avg_hours_sum += float(avg_h) * comp_today
                        avg_hours_count += comp_today
                    sla_met_today += met
                    sla_total_today += total

            avg_completion_hours = round(avg_hours_sum / max(avg_hours_count, 1), 1)
            sla_compliance_pct = round((sla_met_today / max(sla_total_today, 1)) * 100, 1)

            cur.execute("""
                /* page:dispatch/summary:tech_status */
                SELECT status, COUNT(*) as cnt
                FROM field_service.technicians
                GROUP BY status
            """)
            tech_counts = {row[0]: row[1] for row in cur.fetchall()}

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


def _compute_analytics_sla():
    """Heavy query: SLA compliance breakdown."""
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:analytics/sla:sampled */
                SELECT priority,
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE sla_met = true) as met,
                       COUNT(*) FILTER (WHERE sla_met = false) as breached,
                       COALESCE(ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - created_at))/3600)::numeric, 1), 0) as avg_hours
                FROM field_service.work_orders
                WHERE resolved_at IS NOT NULL
                GROUP BY priority
            """)
            rows = cur.fetchall()
            breakdown = {}
            for row in rows:
                prio, total, met, breached, avg_h = row
                breakdown[prio] = {
                    'total': total, 'met': met, 'breached': breached,
                    'compliance_pct': round((met / max(total, 1)) * 100, 1),
                    'avg_resolution_hours': float(avg_h),
                }
            return breakdown


def _compute_technicians_roster():
    """Heavy query: technician roster with skills."""
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:technicians/roster */
                SELECT t.technician_id, t.first_name, t.last_name, t.email,
                       t.status, t.region_id, sr.region_name,
                       t.certification_level, t.avg_rating,
                       t.jobs_completed_mtd, t.first_fix_rate,
                       ARRAY_AGG(DISTINCT st.skill_name) FILTER (WHERE st.skill_name IS NOT NULL) as skills
                FROM field_service.technicians t
                LEFT JOIN field_service.service_regions sr ON t.region_id = sr.region_id
                LEFT JOIN field_service.technician_skills ts ON t.technician_id = ts.technician_id
                LEFT JOIN field_service.skill_types st ON ts.skill_type_id = st.skill_type_id
                WHERE t.is_active = true
                GROUP BY t.technician_id, t.first_name, t.last_name, t.email,
                         t.status, t.region_id, sr.region_name,
                         t.certification_level, t.avg_rating,
                         t.jobs_completed_mtd, t.first_fix_rate
                ORDER BY t.avg_rating DESC NULLS LAST
            """)
            techs = []
            for row in cur.fetchall():
                techs.append({
                    'technician_id': row[0], 'first_name': row[1], 'last_name': row[2],
                    'email': row[3], 'status': row[4], 'region_id': row[5],
                    'region_name': row[6], 'certification_level': row[7],
                    'avg_rating': float(row[8]) if row[8] else None,
                    'jobs_completed_mtd': row[9], 'first_fix_rate': float(row[10]) if row[10] else None,
                    'skills': row[11] or [],
                })
            return techs


def _compute_assets_summary():
    """Heavy query: equipment inventory summary by region and type."""
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:assets/summary */
                SELECT sr.region_name, ec.category_name, ei.status, COUNT(*) as cnt
                FROM field_service.equipment_inventory ei
                JOIN field_service.service_regions sr ON ei.region_id = sr.region_id
                JOIN field_service.equipment_catalog ec ON ei.equipment_type_id = ec.equipment_type_id
                GROUP BY sr.region_name, ec.category_name, ei.status
            """)
            summary = {}
            for row in cur.fetchall():
                region, category, status, cnt = row
                key = f"{region}|{category}"
                if key not in summary:
                    summary[key] = {'region': region, 'category': category, 'total': 0, 'by_status': {}}
                summary[key]['total'] += cnt
                summary[key]['by_status'][status] = cnt
            return list(summary.values())


def _compute_regional_load():
    """Heavy query: work order load and tech availability by region."""
    pool = get_analytics_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                /* page:dashboard/regional-load */
                SELECT sr.region_name, sr.region_id,
                       COUNT(wo.work_order_id) FILTER (WHERE wo.status NOT IN ('completed', 'cancelled')) as open_orders,
                       COUNT(wo.work_order_id) FILTER (WHERE wo.status NOT IN ('completed', 'cancelled') AND wo.sla_due_at < CURRENT_TIMESTAMP) as breached,
                       (SELECT COUNT(*) FROM field_service.technicians t WHERE t.region_id = sr.region_id AND t.is_active = true) as total_techs,
                       (SELECT COUNT(*) FROM field_service.technicians t WHERE t.region_id = sr.region_id AND t.is_active = true AND t.status = 'available') as available_techs
                FROM field_service.service_regions sr
                LEFT JOIN field_service.work_orders wo ON wo.region_id = sr.region_id
                GROUP BY sr.region_name, sr.region_id
                ORDER BY open_orders DESC
            """)
            regions = []
            for row in cur.fetchall():
                regions.append({
                    'region_name': row[0], 'region_id': row[1],
                    'open_orders': row[2], 'breached': row[3],
                    'total_techs': row[4], 'available_techs': row[5],
                })
            return regions


def _warmup_caches():
    """Pre-populate heavy query caches in background so first page load is fast."""
    time.sleep(5)  # Let the app fully start first
    log.info("Cache warmup starting...")
    for name, fn in [
        ('dispatch_summary', _compute_dispatch_summary),
        ('analytics_sla', _compute_analytics_sla),
        ('technicians_roster', _compute_technicians_roster),
        ('assets_summary', _compute_assets_summary),
        ('regional_load', _compute_regional_load),
    ]:
        try:
            data = fn()
            with _query_cache_lock:
                _query_cache[name] = {'data': data, 'expires': time.time() + QUERY_CACHE_TTL}
            log.info(f"  Cache warmed: {name}")
        except Exception as e:
            log.warning(f"  Cache warmup failed for {name}: {e}")
    log.info("Cache warmup complete")

if __name__ == '__main__':
    log.info("Starting Flask app...")
    threading.Thread(target=_warmup_caches, daemon=True).start()
    app.run(host='0.0.0.0', port=8000, debug=False)

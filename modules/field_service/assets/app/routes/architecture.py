"""
Architecture / Code Behind blueprint.

Purpose
=======
Serves the "Code Behind" slide-out panel data and the Lakebase capabilities
showcase endpoint.  The Code Behind panel appears on every page when the
user clicks the ``</>`` button -- it shows which Databricks services, APIs,
tables, and source files power that page.

Routes (2)
==========
GET  /api/page-architecture/<page_id>    Architecture metadata for a page
GET  /api/lakebase/capabilities          Summary of Lakebase PG features in use

Data sources
============
- PAGE_ARCHITECTURE dict (in-memory)     Service/table/API/source metadata per page
- _LINE_INDEX dict (in-memory)           Route -> line number mapping from app.py scan
- config.yaml (on disk)                  Live config values shown in the panel
- Lakebase PostgreSQL                    Triggers, matviews, event count, reorder count

Related files
=============
- app/shared.py                          _load_config_yaml, _CONFIG_BLOCKLIST, get_pool, log_error
- app/app.py                             PAGE_ARCHITECTURE dict (source of truth),
                                         _PAGE_TEMPLATES dict
- app/templates/architecture.html        Architecture diagram page
"""

import logging
import os
import re

from flask import Blueprint, jsonify

from shared import _CONFIG_BLOCKLIST, _load_config_yaml, get_pool, log_error

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint creation
# ---------------------------------------------------------------------------

architecture_bp = Blueprint("architecture", __name__)

# ---------------------------------------------------------------------------
# PAGE_ARCHITECTURE and _PAGE_TEMPLATES are defined in app.py.
# We import them lazily at request time to avoid circular imports
# (app.py registers this blueprint, so we can't import from app at module
# load time).
# ---------------------------------------------------------------------------

# ── Line number index for source links ────────────────────────────────────
# Scans app.py at startup to find actual line numbers for routes and API
# endpoints, so the Code Behind panel can link directly to e.g. app.py#L614
# instead of just app.py.

_LINE_INDEX = {}  # {'@app.route("/dispatch")': 312, '/api/dispatch/summary': 1800, ...}


def _build_line_index():
    """Scan app.py to build a mapping of route patterns -> line numbers.

    Matches both ``@app.route('/path')`` decorators and ``def func():``
    lines immediately following them.
    """
    try:
        # app.py is in the parent directory of this file's directory
        app_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "app.py")
        # Fall back to same directory if the above doesn't exist
        if not os.path.exists(app_path):
            app_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "app.py")
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


# Build the index at import time (same as app.py does)
_build_line_index()


# ── Map page_id -> route path (for line number lookup) ────────────────────
_PAGE_ROUTE_MAP = {
    "index": "/", "dispatch": "/dispatch", "map": "/map", "analytics": "/analytics",
    "supervisor": "/supervisor", "genie": "/genie", "network_health": "/network-health",
    "technicians": "/technicians", "assets": "/assets", "skills": "/skills",
    "operations": "/operations", "simulator": "/simulator", "whatif": "/what-if",
    "view": "/view-data", "architecture": "/architecture", "asbuilt": "/asbuilt",
    "health": "/health", "admin": "/admin",
}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# ── Code Behind panel data ────────────────────────────────────────────────

@architecture_bp.route("/api/page-architecture/<page_id>")
def get_page_architecture(page_id):
    """Return architecture metadata for a given page.

    Enriches the static PAGE_ARCHITECTURE data with:
    - Workspace source URLs (clickable links to the Databricks workspace)
    - Line numbers for source files and API endpoints
    - Live config values from config.yaml (secrets filtered out)
    """
    # Import PAGE_ARCHITECTURE and _PAGE_TEMPLATES from app.py at request time
    # to avoid circular imports during blueprint registration
    from app import PAGE_ARCHITECTURE, _PAGE_TEMPLATES  # noqa: F811

    arch = PAGE_ARCHITECTURE.get(page_id, {})
    if not arch:
        return jsonify({"error": "Unknown page"}), 404

    # Build workspace source URL prefix
    source_base = ""
    host = os.environ.get("DATABRICKS_HOST", "")
    repo_path = os.environ.get("SOURCE_REPO_PATH", "")
    if host and repo_path:
        if not host.startswith("http"):
            host = f"https://{host}"
        # Databricks workspace URLs need /Workspace prefix for #workspace anchor
        source_base = f"{host}/#workspace{repo_path}"

    result = dict(arch)
    result["source_base_url"] = source_base

    # Enrich source_files with line numbers from the index
    enriched_files = []
    for sf in arch.get("source_files", []):
        entry = {"path": sf, "name": sf.split("/")[-1]}
        if sf == "app/app.py":
            # Find the page route line number using the page_id -> route mapping
            route = _PAGE_ROUTE_MAP.get(page_id)
            if route and route in _LINE_INDEX:
                entry["line"] = _LINE_INDEX[route]
                entry["name"] = f"app.py (route, L{_LINE_INDEX[route]})"
        enriched_files.append(entry)

    # Enrich API endpoints with line numbers and HTTP methods
    enriched_apis = []
    for api_path in arch.get("apis", []):
        # Normalize parametric paths for matching
        clean = api_path.split("?")[0]  # strip query params
        line = _LINE_INDEX.get(clean)
        # Infer HTTP method from the endpoint name
        method = (
            "POST"
            if any(
                kw in api_path
                for kw in [
                    "create", "assign", "start", "stop", "refresh", "cleanup",
                    "reserve", "trigger", "cancel", "vacuum", "reindex",
                ]
            )
            else "GET"
        )
        enriched_apis.append({"path": api_path, "method": method, "line": line})

    result["enriched_apis"] = enriched_apis
    result["enriched_files"] = enriched_files

    # Resolve live config values for the page's config_keys
    config_keys = arch.get("config_keys", [])
    if config_keys:
        cfg = _load_config_yaml()
        config_values = {}
        for key in config_keys:
            # Skip keys that look like secrets
            if any(bl in key.lower() for bl in _CONFIG_BLOCKLIST):
                continue
            val = cfg.get(key)
            if val is not None:
                # Flatten nested dicts for display
                if isinstance(val, dict):
                    for sk, sv in val.items():
                        config_values[f"{key}.{sk}"] = str(sv)
                else:
                    config_values[key] = str(val)
        result["config_values"] = config_values

    return jsonify(result)


# ── Lakebase capabilities showcase ────────────────────────────────────────

@architecture_bp.route("/api/lakebase/capabilities")
def lakebase_capabilities():
    """Summary of Lakebase-specific PostgreSQL features in use.

    Introspects the live database to report on:
    - PG triggers (SLA risk scoring, auto-reorder)
    - Materialized views (leaderboards, SLA dashboards)
    - Event sourcing (JSONB event count)
    - ACID transactions (reorder request count)
    """
    try:
        pool = get_pool()
        capabilities = []
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Check triggers
                cur.execute("""
                    /* page:admin/capabilities:triggers */
                    SELECT trigger_name, event_object_table, action_timing, event_manipulation
                    FROM information_schema.triggers
                    WHERE trigger_schema = 'field_service'
                """)
                triggers = [
                    {"name": r[0], "table": r[1], "timing": r[2], "event": r[3]}
                    for r in cur.fetchall()
                ]
                capabilities.append({
                    "feature": "PG Triggers",
                    "description": "Server-side compute for SLA risk scoring and auto-reorder",
                    "count": len(triggers),
                    "details": triggers,
                })

                # Check materialized views
                cur.execute("""
                    /* page:admin/capabilities:matviews */
                    SELECT matviewname FROM pg_matviews
                    WHERE schemaname = 'field_service'
                """)
                matviews = [r[0] for r in cur.fetchall()]
                capabilities.append({
                    "feature": "Materialized Views",
                    "description": "Pre-computed leaderboards and SLA dashboards",
                    "count": len(matviews),
                    "details": matviews,
                })

                # Event count (demonstrates JSONB event sourcing)
                cur.execute("/* page:admin/capabilities:events */ SELECT COUNT(*) FROM field_service.events")
                event_count = cur.fetchone()[0]
                capabilities.append({
                    "feature": "Event Sourcing (JSONB)",
                    "description": "Immutable audit log with structured JSONB payloads",
                    "count": event_count,
                    "details": "GIN-indexed JSONB for flexible querying",
                })

                # ACID transaction count (reorder_requests auto-created by trigger)
                cur.execute("/* page:admin/capabilities:reorders */ SELECT COUNT(*) FROM field_service.reorder_requests")
                reorder_count = cur.fetchone()[0]
                capabilities.append({
                    "feature": "ACID Transactions",
                    "description": "Multi-table transactional parts reservation and auto-reorder",
                    "count": reorder_count,
                    "details": "SELECT...FOR UPDATE + multi-table commits",
                })

        return jsonify({"capabilities": capabilities})
    except Exception as e:
        log_error("lakebase_capabilities", e)
        return jsonify({"error": str(e)}), 500

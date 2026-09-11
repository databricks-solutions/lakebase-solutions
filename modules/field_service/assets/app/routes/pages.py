"""
Page routes blueprint — all 19 HTML page endpoints.

Purpose
=======
Serves every user-facing page in the application.  Each route renders a
Jinja2 template and passes a ``page_id`` used by the Code Behind panel
to look up architecture metadata.  No database queries happen here; the
templates fetch data asynchronously via the API routes in other blueprints.

Routes (19)
===========
GET  /                  Dashboard (index.html)
GET  /view-data         Data Explorer (view_data.html)
GET  /insert-data       Legacy redirect -> /view-data
GET  /genie             Genie AI chat (genie.html)
GET  /operations        Executive dashboard iframe (operations_center.html)
GET  /supervisor        Multi-agent AI supervisor (supervisor.html)
GET  /simulator         Real-time data generator (simulator.html)
GET  /health            System health (health.html)
GET  /dispatch          Dispatch board (dispatch_board.html)
GET  /map               Field map (map.html)
GET  /analytics         SLA analytics (analytics.html)
GET  /technicians       Technician roster (technicians.html)
GET  /assets            Equipment inventory (assets.html)
GET  /network-health    Network health (network_health.html)
GET  /skills            Skills matrix (skills_matrix.html)
GET  /architecture      Architecture diagram (architecture.html)
GET  /asbuilt           As-Built discovery panel (asbuilt.html)
GET  /what-if           What-If branching analysis (whatif.html)
GET  /admin             Admin panel (admin.html)

Data sources
============
None directly — pages are static HTML shells that call API endpoints.

Related files
=============
- app/templates/*.html          Jinja2 templates rendered by these routes
- app/shared.py                 GENIE_SPACES, AGENT_ENDPOINT_NAME config
- app/routes/architecture.py    Code Behind panel data for each page
"""

import hashlib
import os
import time

from flask import Blueprint, redirect, render_template, request

from shared import AGENT_ENDPOINT_NAME, GENIE_SPACES

# ---------------------------------------------------------------------------
# Blueprint creation
# ---------------------------------------------------------------------------

pages_bp = Blueprint("pages", __name__)


# ── Dashboard ─────────────────────────────────────────────────────────────

@pages_bp.route("/")
def home():
    return render_template("index.html", page_id="index")


# ── Data Explorer ─────────────────────────────────────────────────────────

@pages_bp.route("/view-data")
def view_data():
    return render_template("view_data.html", page_id="view")


@pages_bp.route("/insert-data")
def insert_data():
    # Legacy route — redirect to the unified Data Explorer page
    return redirect("/view-data")


# ── Genie AI ──────────────────────────────────────────────────────────────

@pages_bp.route("/genie")
def genie():
    return render_template("genie.html", page_id="genie", spaces=GENIE_SPACES)


# ── Operations Center (embedded Lakeview dashboard) ───────────────────────

@pages_bp.route("/operations")
def operations():
    dashboard_url = os.environ.get("DASHBOARD_EMBED_URL", "")
    return render_template(
        "operations_center.html", page_id="operations", dashboard_url=dashboard_url
    )


# ── AI Supervisor (LangGraph multi-agent) ─────────────────────────────────

@pages_bp.route("/supervisor")
def supervisor_page():
    # Build the list of configured Genie spaces for the agent selector
    agent_spaces = [
        {"key": k, "name": v["name"], "description": v["description"]}
        for k, v in GENIE_SPACES.items()
        if v.get("id")
    ]
    return render_template(
        "supervisor.html",
        page_id="supervisor",
        spaces=GENIE_SPACES,
        agent_endpoint=AGENT_ENDPOINT_NAME,
        agent_spaces=agent_spaces,
    )


# ── Simulator ─────────────────────────────────────────────────────────────

@pages_bp.route("/simulator")
def simulator():
    return render_template("simulator.html", page_id="simulator")


# ── System Health ─────────────────────────────────────────────────────────

@pages_bp.route("/health")
def health_page():
    return render_template("health.html", page_id="health")


# ── Dispatch Board ────────────────────────────────────────────────────────

@pages_bp.route("/dispatch")
def dispatch():
    return render_template("dispatch_board.html", page_id="dispatch")


# ── Field Map ─────────────────────────────────────────────────────────────

@pages_bp.route("/map")
def map_page():
    return render_template("map.html", page_id="map")


# ── SLA Analytics ─────────────────────────────────────────────────────────

@pages_bp.route("/analytics")
def analytics_page():
    return render_template("analytics.html", page_id="analytics")


# ── Technicians ───────────────────────────────────────────────────────────

@pages_bp.route("/technicians")
def technicians_page():
    return render_template("technicians.html", page_id="technicians")


# ── Assets / Inventory ────────────────────────────────────────────────────

@pages_bp.route("/assets")
def assets_page():
    return render_template("assets.html", page_id="assets")


# ── Network Health ────────────────────────────────────────────────────────

@pages_bp.route("/network-health")
def network_health_page():
    return render_template("network_health.html", page_id="network_health")


# ── Skills Matrix ─────────────────────────────────────────────────────────

@pages_bp.route("/skills")
def skills_page():
    return render_template("skills_matrix.html", page_id="skills")


# ── Fleet Health ──────────────────────────────────────────────────────────

@pages_bp.route("/fleet")
def fleet_page():
    return render_template("fleet.html", page_id="fleet")


# ── Architecture Diagram (archived — AsBuilt covers this) ─────────────────
# @pages_bp.route("/architecture")
# def architecture_page():
#     return render_template("architecture.html", page_id="architecture")


# ── As-Built Discovery Panel ──────────────────────────────────────────────

@pages_bp.route("/asbuilt")
def asbuilt_page():
    # Cache-busting redirect: append a version hash based on the template's
    # last-modified time so aggressive browser caches (Arc, Safari) pick up
    # template changes immediately after a deploy.
    # The AsBuilt is a self-contained IDEA document served from static/asbuilt/.
    # Databricks Apps set X-Frame-Options: DENY on every response, so it cannot be
    # embedded in an iframe (even same-origin) — serve it as a FULL PAGE via
    # top-level navigation instead. ?home=/ gives overlay.js a "Back to app" link;
    # ?v= is a cache-bust keyed on the overlay state so redeploys refresh cleanly.
    # ?v= is keyed on the NEWEST mtime across the whole bundle (index.html +
    # overlay.css/js + state), so ANY overlay change forces a fresh page load —
    # not just state changes. (index.html then cache-busts overlay.css/js itself.)
    from flask import current_app

    bundle = os.path.join(current_app.static_folder, "asbuilt")
    try:
        mtimes = [os.path.getmtime(os.path.join(bundle, f))
                  for f in ("index.html", "overlay.css", "overlay.js", "asbuilt_state.json")
                  if os.path.exists(os.path.join(bundle, f))]
        v = hashlib.md5(str(max(mtimes)).encode()).hexdigest()[:8]
    except Exception:
        v = str(int(time.time()))
    # Quick-links for the AsBuilt page's top nav bar (the app's own sidebar can't
    # be embedded there — Databricks Apps set X-Frame-Options: DENY — so the map
    # is a full page). overlay.js renders these + a "Back to FieldOps" home button.
    import json as _json
    import urllib.parse as _url
    _nav = [
        {"l": "Dashboard", "h": "/"},
        {"l": "Dispatch", "h": "/dispatch"},
        {"l": "Data Explorer", "h": "/view-data"},
        {"l": "Genie", "h": "/genie"},
        {"l": "AI Supervisor", "h": "/supervisor"},
        {"l": "Field Map", "h": "/map"},
        {"l": "Analytics", "h": "/analytics"},
        {"l": "Lakebase", "h": "/lakebase"},
        {"l": "Data API", "h": "/data-api"},
        {"l": "Admin", "h": "/admin"},
        {"l": "System Health", "h": "/health"},
    ]
    nav_q = _url.quote(_json.dumps(_nav, separators=(",", ":")))
    return redirect(f"/static/asbuilt/index.html?home=/&app=FieldOps&nav={nav_q}&v={v}")


# ── What-If Analysis ──────────────────────────────────────────────────────

@pages_bp.route("/what-if")
def whatif_page():
    # The template shows a "not configured" banner when no project ID is set
    project_id = os.environ.get("LAKEBASE_PROJECT_ID", "")
    return render_template(
        "whatif.html", page_id="whatif", project_configured=bool(project_id)
    )


# ── Lakebase Control Tower ────────────────────────────────────────────────

@pages_bp.route("/lakebase")
def lakebase_page():
    """Lakebase wing landing page — live view of the Lakebase differentiators."""
    project_id = os.environ.get("LAKEBASE_PROJECT_ID", "")
    return render_template(
        "lakebase_hub.html", page_id="lakebase", project_configured=bool(project_id)
    )


# ── Lakebase Data API ─────────────────────────────────────────────────────

@pages_bp.route("/data-api")
def data_api_page():
    """Lakebase Data API (PostgREST) demo — filtering, joins, RPC, bulk writes."""
    return render_template("data_api.html", page_id="data_api")


# ── Admin Panel ───────────────────────────────────────────────────────────

@pages_bp.route("/admin")
def admin_page():
    return render_template("admin.html", page_id="admin")


# ── Technician Mobile View ────────────────────────────────────────────

@pages_bp.route("/mobile")
def mobile_page():
    return render_template("mobile.html", page_id="mobile")

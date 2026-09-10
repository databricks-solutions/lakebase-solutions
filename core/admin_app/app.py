"""
Lakebase Admin Console — standalone Flask application.

A single-page Databricks App that serves a generic Postgres/Lakebase DBA
console. It registers one blueprint (``admin``) exposing the ``/api/admin/*``
API, renders the admin page, and provides a lightweight health check.

Point it at any Lakebase (or plain PostgreSQL) instance via the ``PG*``
environment variables; choose the schema it administers with ``TARGET_SCHEMA``
(default ``public``). See ``app.yaml.example`` and the README for the full
configuration surface.
"""

from __future__ import annotations

import logging
import os
import sys
import time

from flask import Flask, jsonify, redirect, render_template

# Blueprints live in the ``routes`` package next to this file.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared import APP_START_TIME, get_pool, log_error  # noqa: E402
from routes.admin import admin_bp, SCHEMA  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("admin_app")

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.register_blueprint(admin_bp)


@app.route("/")
def index():
    """Root redirects to the admin console."""
    return redirect("/admin")


@app.route("/admin")
def admin_page():
    """Render the Lakebase Admin console page."""
    return render_template("admin.html", page_id="admin", schema=SCHEMA)


@app.route("/api/health")
def health():
    """Liveness + a best-effort database ping.

    Returns 200 with ``db: "connected"`` when a connection can be checked out
    and ``SELECT 1`` succeeds; still 200 with ``db: "error"`` otherwise, so the
    app itself is considered healthy even while the database warms up.
    """
    uptime = round(time.time() - APP_START_TIME.timestamp(), 1)
    db_status = "unknown"
    try:
        pool = get_pool()
        with pool.connection(timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        db_status = "connected"
    except Exception as e:
        log_error("health", e)
        db_status = "error"
    return jsonify({
        "status": "ok",
        "app": "lakebase-admin-console",
        "schema": SCHEMA,
        "uptime_seconds": uptime,
        "db": db_status,
    })


if __name__ == "__main__":
    # Local dev. In a Databricks App, app.yaml's `command` runs the server.
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)

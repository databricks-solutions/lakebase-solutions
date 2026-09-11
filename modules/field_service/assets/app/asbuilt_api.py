"""
AsBuilt Refresh API — Flask Blueprint

Provides a POST /asbuilt/refresh endpoint that re-discovers workspace
resources using the Databricks SDK and regenerates the AsBuilt HTML.

Usage in app.py:
    from asbuilt_api import asbuilt_bp
    app.register_blueprint(asbuilt_bp)

The SDK auto-configures from the runtime environment (DATABRICKS_HOST +
DATABRICKS_TOKEN are injected by Databricks Apps at runtime).
"""

import json
import os
import traceback
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify

asbuilt_bp = Blueprint("asbuilt", __name__)


def _sdk_discover(demo_path=None):
    """Discover workspace resources using the Databricks SDK."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    manifest = {
        "workspace": {
            "host": w.config.host or "",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        },
        "errors": [],
    }

    def phase(name, fn):
        """Run a discovery phase, storing results and logging errors."""
        try:
            result = fn()
            manifest[name] = result
            return result
        except Exception as e:
            manifest[name] = []
            manifest["errors"].append(f"{name}: {e}")
            return []

    # Catalogs
    catalogs = phase("catalogs", lambda: [
        c.as_dict() for c in w.catalogs.list()
        if c.name and not c.name.startswith("__") and c.name != "system"
    ])

    # Schemas
    all_schemas = []
    def _schemas():
        for cat in catalogs[:20]:
            cat_name = cat.get("name", "")
            if not cat_name:
                continue
            try:
                for s in w.schemas.list(catalog_name=cat_name):
                    d = s.as_dict()
                    d["_catalog"] = cat_name
                    all_schemas.append(d)
            except Exception:
                pass
        return all_schemas
    phase("schemas", _schemas)

    # Tables (sample — first 10 schemas, up to 100 tables each)
    all_tables = []
    def _tables():
        for sch in all_schemas[:10]:
            cat = sch.get("_catalog", "")
            sch_name = sch.get("name", "")
            if not cat or not sch_name or sch_name == "information_schema":
                continue
            try:
                for t in list(w.tables.list(catalog_name=cat, schema_name=sch_name))[:100]:
                    d = t.as_dict()
                    d["_catalog"] = cat
                    d["_schema"] = sch_name
                    all_tables.append(d)
            except Exception:
                pass
        return all_tables
    phase("tables", _tables)

    # Volumes
    all_volumes = []
    def _volumes():
        for sch in all_schemas[:10]:
            cat = sch.get("_catalog", "")
            sch_name = sch.get("name", "")
            if not cat or not sch_name or sch_name == "information_schema":
                continue
            try:
                for v in w.volumes.list(catalog_name=cat, schema_name=sch_name):
                    d = v.as_dict()
                    d["_catalog"] = cat
                    d["_schema"] = sch_name
                    all_volumes.append(d)
            except Exception:
                pass
        return all_volumes
    phase("volumes", _volumes)

    # Pipelines
    phase("pipelines", lambda: [
        p.as_dict() for p in (w.pipelines.list_pipelines() or [])
    ])

    # Jobs
    phase("jobs", lambda: [j.as_dict() for j in w.jobs.list()])

    # Warehouses
    phase("warehouses", lambda: [wh.as_dict() for wh in w.warehouses.list()])

    # Clusters
    phase("clusters", lambda: [c.as_dict() for c in w.clusters.list()])

    # Apps
    def _apps():
        try:
            return [a.as_dict() for a in w.apps.list()]
        except Exception:
            return []
    phase("apps", _apps)

    # Serving endpoints
    phase("serving_endpoints", lambda: [
        ep.as_dict() for ep in w.serving_endpoints.list()
    ])

    # Repos
    phase("repos", lambda: [r.as_dict() for r in w.repos.list()])

    # Dashboards (Lakeview)
    def _dashboards():
        try:
            return [d.as_dict() for d in w.lakeview.list()]
        except Exception:
            return []
    phase("dashboards", _dashboards)

    # Vector Search endpoints
    def _vector_search():
        try:
            return [ep.as_dict() for ep in w.vector_search_endpoints.list_endpoints()]
        except Exception:
            return []
    phase("vector_search", _vector_search)

    # Lakebase — provisioned instances AND autoscaling projects.
    # Autoscaling is the current default; querying only /database/instances
    # missed it, leaving UC federated foreign tables to falsely light the box.
    def _lakebase():
        results = []
        try:
            resp = w.api_client.do("GET", "/api/2.0/database/instances")
            if isinstance(resp, dict):
                for inst in resp.get("instances", resp.get("items", [])):
                    inst["_lakebase_kind"] = "provisioned"
                    results.append(inst)
        except Exception:
            pass
        try:
            proj_resp = w.api_client.do("GET", "/api/2.0/postgres/projects")
            if isinstance(proj_resp, dict):
                for proj in proj_resp.get("projects", []):
                    raw = proj.get("name", "") or ""
                    proj["name"] = raw.split("/")[-1] if raw else proj.get("project_id", "")
                    proj["_lakebase_kind"] = "autoscaling"
                    state = (proj.get("status") or {}).get("current_state", "")
                    if state:
                        proj["state"] = state
                    results.append(proj)
        except Exception:
            pass
        return results
    phase("lakebase_instances", _lakebase)

    # Connections
    phase("connections", lambda: [c.as_dict() for c in w.connections.list()])

    # Registered models
    def _models():
        try:
            return [m.as_dict() for m in w.registered_models.list()]
        except Exception:
            return []
    phase("models", _models)

    # Secret scopes
    def _scopes():
        try:
            return [s.as_dict() for s in w.secrets.list_scopes()]
        except Exception:
            return []
    phase("secret_scopes", _scopes)

    # Genie spaces (no list API)
    manifest["genie_spaces"] = []

    # Demo assets
    if demo_path:
        def _demo_assets():
            try:
                return [item.as_dict() for item in w.workspace.list(demo_path)]
            except Exception:
                return []
        phase("demo_assets", _demo_assets)
        manifest["workspace"]["demo_path"] = demo_path

    total = sum(len(v) for k, v in manifest.items() if isinstance(v, list) and k != "errors")
    manifest["workspace"]["total_resources"] = total

    return manifest


def _state_out_path():
    """Where asbuilt_state.json lives — the folder overlay.js fetches it from.

    Order: ASBUILT_STATE_PATH env override → the static overlay bundle
    (static/asbuilt/) if present → next to this module. This matches the
    framework-agnostic bundle layout (static/asbuilt/{index.html,overlay.*,
    asbuilt_state.json}) without hardcoding an absolute runtime path."""
    env = os.environ.get("ASBUILT_STATE_PATH")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    bundle = os.path.join(here, "static", "asbuilt")
    if os.path.isdir(bundle):
        return os.path.join(bundle, "asbuilt_state.json")
    return os.path.join(here, "asbuilt_state.json")


@asbuilt_bp.route("/asbuilt/refresh", methods=["POST"])
def asbuilt_refresh():
    """Re-discover workspace resources and regenerate the overlay state JSON.

    Fork-and-overlay model: we do NOT rebuild any HTML. We only rewrite
    asbuilt_state.json; the vendored IDEA base + overlay.js pick it up on the
    next load (or via window.AsBuilt.refresh() on the client)."""
    try:
        params = request.get_json(force=True) or {}
        demo_path = params.get("demo_path")
        cloud = params.get("cloud", "azure")

        # Run SDK-based discovery (runtime token; no CLI profile needed).
        manifest = _sdk_discover(demo_path=demo_path)

        manifest_path = "/tmp/asbuilt_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, default=str)

        # Import and run the overlay generator (co-located in the app dir).
        import importlib.util
        gen_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generate_overlay.py")
        if not os.path.exists(gen_path):
            return jsonify({"ok": False, "error": "generate_overlay.py not found. Re-run the AsBuilt skill to set up the app."})

        spec = importlib.util.spec_from_file_location("generate_overlay", gen_path)
        gen_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gen_mod)

        out_path = _state_out_path()
        state = gen_mod.generate(manifest_path, out_path, cloud=cloud)

        return jsonify({
            "ok": True,
            "active": state["meta"]["active_count"],
            "core_edges": state["meta"]["core_edge_count"],
            "feature_edges": state["meta"]["feature_edge_count"],
            "idea_release": state["meta"].get("idea_release", ""),
            "state_path": out_path,
            "errors": manifest.get("errors", []),
        })

    except ImportError as e:
        return jsonify({"ok": False, "error": f"Databricks SDK not installed: {e}. Add 'databricks-sdk' to requirements.txt."})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)})

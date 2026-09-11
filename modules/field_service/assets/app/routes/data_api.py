"""
Lakebase Data API demo blueprint.

Proxies the **Lakebase Data API** — a PostgREST-compatible REST front end over the
Lakebase Postgres instance (https://docs.databricks.com/aws/en/oltp/projects/data-api)
— so the "Data API" page can demonstrate, live, the operations a customer asked
about: filtering + pagination, multi-table joins via resource embedding,
aggregations & nested queries via RPC, large result sets, and bulk read/write.

Every response carries the exact REST request (method + URL) it made, so the UI's
"under the hood" console shows what actually happened against the real Data API
host (not the workspace API, not our SQL warehouse).

Auth: ``_auth_headers()`` yields self-refreshing Bearer headers. The app's *own*
service principal has a control-plane-provisioned Postgres role that
``databricks_create_role`` can't register, so its token is rejected (PGRST301).
When a **dedicated** SP's credentials are present (``DATA_API_SP_CLIENT_ID`` /
``DATA_API_SP_SECRET``), we authenticate as that SP — whose role is registered
fresh (with ADMIN OPTION) by ``notebooks/setup_data_api_sp.py`` — otherwise we
fall back to the app SP. See CLAUDE.md for the one-time owner/admin setup.

Config: ``LAKEBASE_DATA_API_URL`` (base URL from the project's Data API tab). When
unset, endpoints return ``{"configured": false}`` so the page degrades gracefully
instead of erroring.

Routes
------
POST /api/data-api/filter-paginate   Filter + order + paginate + column-select (GET work_orders)
POST /api/data-api/embed             Multi-table join / hierarchical (resource embedding)
POST /api/data-api/rpc-aggregate     Aggregation via RPC (/rpc/sla_compliance_by_region)
POST /api/data-api/rpc-nested        Nested subquery + JOIN via RPC (/rpc/top_technicians_by_completions)
POST /api/data-api/large-result      Large result set: max-rows cap + exact count + pagination
POST /api/data-api/bulk-write        Bulk insert/update/delete against the public.data_api_demo sandbox
GET  /api/data-api/config            Whether the Data API base URL is configured

Dependencies from ``shared``: get_workspace_client, log_error
"""
import json
import os
import time
from urllib.parse import unquote

import requests
from flask import Blueprint, jsonify, request

from shared import get_workspace_client, log_error

data_api_bp = Blueprint("data_api", __name__)

# `public` is exposed by the Data API by default; `field_service` must be exposed
# explicitly in the project's Data API Advanced settings (see CLAUDE.md).
_FIELD_SCHEMA = "field_service"
_PUBLIC_SCHEMA = "public"
_SANDBOX = "/data_api_demo"
_TIMEOUT = 30


def _base_url():
    return (os.environ.get("LAKEBASE_DATA_API_URL") or "").rstrip("/")


# The app's own service principal has a control-plane-provisioned Postgres role that
# `databricks_create_role` can't register, so its token is rejected (PGRST301). When a
# dedicated SP's credentials are present (DATA_API_SP_CLIENT_ID / DATA_API_SP_SECRET),
# authenticate as that SP instead — its role is registered fresh (see
# notebooks/setup_data_api_sp.py). Falls back to the app SP otherwise.
_dedicated_cfg = None


def _auth_headers():
    """Bearer headers for the Data API — dedicated SP when configured, else app SP."""
    global _dedicated_cfg
    client_id = os.environ.get("DATA_API_SP_CLIENT_ID")
    client_secret = os.environ.get("DATA_API_SP_SECRET")
    if client_id and client_secret:
        if _dedicated_cfg is None:
            from databricks.sdk.core import Config
            host = get_workspace_client().config.host
            _dedicated_cfg = Config(host=host, client_id=client_id,
                                    client_secret=client_secret, auth_type="oauth-m2m")
        return _dedicated_cfg.authenticate() or {}
    return get_workspace_client().config.authenticate() or {}


def _sanitize_headers(headers):
    """Redact the bearer token before returning headers for display."""
    out = {}
    for k, v in (headers or {}).items():
        out[k] = "Bearer ••••••" if k.lower() == "authorization" else v
    return out


def _rowcount(body):
    if isinstance(body, list):
        return len(body)
    if isinstance(body, dict):
        return 1
    return None


def _data_api_request(method, schema, path, params=None, body=None, prefer=None):
    """Make one Lakebase Data API REST call.

    Returns a display-friendly record: {ok, configured, request:{method,url,headers},
    status, elapsed_ms, content_range, body|error}. `path` is appended after the
    schema, e.g. schema='field_service', path='/work_orders'.
    """
    base = _base_url()
    if not base:
        return {
            "ok": False, "configured": False,
            "error": ("Lakebase Data API is not configured. Enable it on the "
                      "dba-lakebase-1 project's Data API tab, then set "
                      "LAKEBASE_DATA_API_URL in the app config."),
        }
    auth = _auth_headers()
    # Databricks Data API URL layout is <base>/<schema>/<table> (schema is the first
    # path segment; the gateway exposes it dynamically), per the docs example
    # $REST_ENDPOINT/public/clients.
    url = f"{base}/{schema}{path}"
    headers = {**auth, "Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if prefer:
        headers["Prefer"] = prefer
    t0 = time.time()
    try:
        resp = requests.request(method, url, params=params, json=body,
                                headers=headers, timeout=_TIMEOUT)
        elapsed = round((time.time() - t0) * 1000)
        rec = {
            "ok": resp.ok, "configured": True,
            "request": {"method": method, "url": resp.url,
                        "headers": _sanitize_headers(headers),
                        "prefer": prefer, "body": body},
            "status": resp.status_code, "elapsed_ms": elapsed,
            "content_range": resp.headers.get("Content-Range"),
        }
        try:
            rec["body"] = resp.json()
        except Exception:
            rec["body"] = (resp.text or "")[:2000]
        if not resp.ok:
            rec["error"] = f"HTTP {resp.status_code}"
        return rec
    except Exception as e:  # noqa: BLE001
        log_error("data_api_request", e)
        return {"ok": False, "configured": True,
                "request": {"method": method, "url": url,
                            "prefer": prefer, "body": body},
                "error": str(e), "elapsed_ms": round((time.time() - t0) * 1000)}


def _curl(req):
    """Reproducible curl for the ACTUAL request issued (real method/URL/headers/body,
    auth redacted) — so the console proves it's a live call, not a canned string."""
    method = req.get("method", "GET")
    # Show the DECODED URL: the wire form percent-encodes commas/parens (%2C/%28/%29),
    # which reads like a mangled, truncated string. The Data API accepts the literal
    # form, so the decoded curl is both readable and runnable.
    url = unquote(req.get("url", ""))
    lines = [f"curl -X {method} '{url}'",
             "  -H 'Authorization: Bearer <databricks-oauth-token>'",
             "  -H 'Accept: application/json'"]
    if req.get("prefer"):
        lines.append(f"  -H 'Prefer: {req['prefer']}'")
    if req.get("body") is not None:
        lines.append("  -H 'Content-Type: application/json'")
        lines.append("  -d '" + json.dumps(req["body"]) + "'")
    return " \\\n".join(lines)


def _console_steps(rec, label):
    """Build 'under the hood' console steps (api request + result) from a record."""
    req = rec.get("request", {})
    steps = [{
        "kind": "api",
        "label": f"{req.get('method', '?')}  {label}",
        "code": _curl(req) if req.get("url") else req.get("url", ""),
    }]
    if rec.get("configured") is False:
        steps.append({"kind": "result", "label": rec.get("error", "Not configured")})
        return steps
    if rec.get("error") and not rec.get("status"):
        steps.append({"kind": "result", "label": f"Error: {rec['error']}"})
        return steps
    n = _rowcount(rec.get("body"))
    detail = f"HTTP {rec.get('status')} · {rec.get('elapsed_ms')} ms"
    if n is not None:
        detail += f" · {n} row(s)"
    if rec.get("content_range"):
        detail += f" · Content-Range: {rec['content_range']}"
    steps.append({"kind": "result", "label": detail})
    return steps


def _respond(rec, label):
    """Standard single-call response: console steps + parsed rows + meta."""
    return jsonify({
        "configured": rec.get("configured", True),
        "ok": rec.get("ok", False),
        "steps": _console_steps(rec, label),
        "rows": rec.get("body"),
        "error": rec.get("error"),
        "meta": {"status": rec.get("status"), "elapsed_ms": rec.get("elapsed_ms"),
                 "url": rec.get("request", {}).get("url"),
                 "content_range": rec.get("content_range")},
    })


# ── Filtering + ordering + pagination + column (vertical) selection ──────────
@data_api_bp.route("/api/data-api/filter-paginate", methods=["POST"])
def filter_paginate():
    data = request.get_json(silent=True) or {}
    try:
        limit = max(1, min(int(data.get("limit", 10)), 100))
        offset = max(0, int(data.get("offset", 0)))
    except (TypeError, ValueError):
        limit, offset = 10, 0
    params = {
        "select": "work_order_id,status,priority,created_at",
        "priority": "eq.critical",
        "order": "created_at.desc",
        "limit": str(limit),
        "offset": str(offset),
    }
    rec = _data_api_request("GET", _FIELD_SCHEMA, "/work_orders", params=params,
                            prefer="count=exact")
    return _respond(rec, "/field_service/work_orders  (filter + order + paginate + select)")


# ── Multi-table join / hierarchical retrieval (resource embedding) ───────────
@data_api_bp.route("/api/data-api/embed", methods=["POST"])
def embed():
    params = {
        "select": ("work_order_id,status,priority,"
                   "customers(first_name,last_name,region_id),"
                   "appointments(scheduled_start,status)"),
        "order": "work_order_id.asc",
        "limit": "5",
    }
    rec = _data_api_request("GET", _FIELD_SCHEMA, "/work_orders", params=params)
    return _respond(rec, "/field_service/work_orders  (embed customers + appointments via FK)")


# ── Aggregation via RPC ──────────────────────────────────────────────────────
@data_api_bp.route("/api/data-api/rpc-aggregate", methods=["POST"])
def rpc_aggregate():
    rec = _data_api_request("POST", _PUBLIC_SCHEMA, "/rpc/sla_compliance_by_region", body={})
    return _respond(rec, "/public/rpc/sla_compliance_by_region  (GROUP BY aggregation)")


# ── Nested subquery + JOIN via RPC ──────────────────────────────────────────
@data_api_bp.route("/api/data-api/rpc-nested", methods=["POST"])
def rpc_nested():
    data = request.get_json(silent=True) or {}
    try:
        n = max(1, min(int(data.get("n", 5)), 20))
    except (TypeError, ValueError):
        n = 5
    rec = _data_api_request("POST", _PUBLIC_SCHEMA, "/rpc/top_technicians_by_completions",
                            body={"n": n})
    return _respond(rec, "/public/rpc/top_technicians_by_completions  (CTE + JOIN + window)")


# ── Large result sets: max-rows cap + exact count + pagination ───────────────
@data_api_bp.route("/api/data-api/large-result", methods=["POST"])
def large_result():
    if not _base_url():
        rec = _data_api_request("GET", _FIELD_SCHEMA, "/work_orders")  # yields not-configured
        return jsonify({"configured": False, "ok": False,
                        "steps": _console_steps(rec, "/field_service/work_orders"),
                        "error": rec.get("error")})
    # Three-step story for large result sets — every call is cheap even on a cold
    # endpoint (an exact count(*) over 5M rows is a full scan that exceeds the Data
    # API's ~8s statement timeout when the buffer cache is cold):
    #  1) A bounded page (limit 1000, no server count) — reads a handful of pages.
    #  2) The TOTAL from the planner's estimate via /rpc/work_orders_estimate
    #     (pg_class.reltuples) — instant at any table size, the correct way to size a
    #     huge table over REST.
    #  3) The guardrail — an unbounded pull is rejected once the response exceeds the
    #     ~10 MB response-size cap (HTTP 400), so pagination is required.
    page = _data_api_request("GET", _FIELD_SCHEMA, "/work_orders",
                             params={"select": "work_order_id", "limit": "1000"})
    est = _data_api_request("POST", _PUBLIC_SCHEMA, "/rpc/work_orders_estimate", body={})
    over = _data_api_request("GET", _FIELD_SCHEMA, "/work_orders",
                             params={"select": "work_order_id", "limit": "1000000"})
    steps = _console_steps(page, "/field_service/work_orders?limit=1000  (bounded page — cheap)")
    steps += _console_steps(est, "/public/rpc/work_orders_estimate  (planner row estimate — instant)")
    steps += _console_steps(over, "/field_service/work_orders?limit=1000000  (unbounded — exceeds ~10 MB cap → 400)")
    estimated_total = est.get("body") if isinstance(est.get("body"), int) else None
    guardrail_msg = None
    if isinstance(over.get("body"), dict):
        guardrail_msg = over["body"].get("message")
    ok = bool(page.get("ok")) and bool(est.get("ok")) and over.get("status") == 400
    return jsonify({
        "configured": True, "ok": ok, "steps": steps,
        "rows": page.get("body"),
        "meta": {"status": page.get("status"), "elapsed_ms": page.get("elapsed_ms"),
                 "content_range": page.get("content_range"), "estimated_total": estimated_total,
                 "guardrail_status": over.get("status"), "guardrail_message": guardrail_msg},
        "error": None if ok else "Large-result demo did not behave as expected — see console",
    })


# ── Bulk read/write against the sandbox table (never touches production) ─────
@data_api_bp.route("/api/data-api/bulk-write", methods=["POST"])
def bulk_write():
    steps = []
    results = {}
    base_ok = bool(_base_url())
    if not base_ok:
        rec = _data_api_request("POST", _PUBLIC_SCHEMA, _SANDBOX, body=[])  # yields not-configured
        return jsonify({"configured": False, "ok": False,
                        "steps": _console_steps(rec, _SANDBOX), "error": rec.get("error")})

    # 1) Bulk INSERT (array payload) — return the inserted rows
    payload = [{"label": f"bulk-{i}", "value": i * 100} for i in range(1, 4)]
    ins = _data_api_request("POST", _PUBLIC_SCHEMA, _SANDBOX, body=payload,
                            prefer="return=representation")
    steps += _console_steps(ins, "/public/data_api_demo  (bulk INSERT — 3 rows)")
    results["insert"] = ins.get("body")

    # 2) Bulk UPDATE with a filter — bump value on all bulk-* rows
    upd = _data_api_request("PATCH", _PUBLIC_SCHEMA, _SANDBOX,
                            params={"label": "like.bulk-*"}, body={"value": 999},
                            prefer="return=representation")
    steps += _console_steps(upd, "/public/data_api_demo?label=like.bulk-*  (bulk UPDATE)")
    results["update"] = upd.get("body")

    # 3) Bulk DELETE with a filter — remove the bulk-* rows (resets the sandbox)
    dele = _data_api_request("DELETE", _PUBLIC_SCHEMA, _SANDBOX,
                             params={"label": "like.bulk-*"},
                             prefer="return=representation")
    steps += _console_steps(dele, "/public/data_api_demo?label=like.bulk-*  (bulk DELETE — cleanup)")
    results["delete"] = dele.get("body")

    ok = all(r.get("ok") for r in (ins, upd, dele))
    return jsonify({"configured": True, "ok": ok, "steps": steps, "results": results,
                    "error": None if ok else "One or more bulk operations failed — see console"})


# ── Config probe (page degrades gracefully when not enabled) ─────────────────
@data_api_bp.route("/api/data-api/config", methods=["GET"])
def config():
    return jsonify({
        "configured": bool(_base_url()),
        "field_schema": _FIELD_SCHEMA,
        "public_schema": _PUBLIC_SCHEMA,
    })

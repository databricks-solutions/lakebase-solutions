#!/usr/bin/env python3
"""
AsBuilt Overlay Generator (fork-and-overlay model)

Reads a workspace discovery manifest (from discover_resources.py) and the IDEA
box map (references/idea_box_map.json), then emits `asbuilt_state.json` — the
overlay state the vendored IDEA base consumes via overlay.js:

    {
      "meta":   {profile, host, generated_at, idea_release, cloud},
      "active": ["a8", "a30", ...],              # data-ids to illuminate
      "evidence": {"a8": ["endpoint: foo (READY)", ...], ...},
      "coreEdges":    [{"from":"a1","to":"a30","label":"ingest","kind":"core"}, ...],
      "featureEdges": [{"from":"a28","to":"a30","label":"governed","kind":"feature"}, ...]
    }

Two line tiers (see references/flow_taxonomy.md):
  • core     — the data/AI spine of the reviewed solution (solid, animated)
  • feature  — a capability a flow *relies on* but isn't the spine, e.g. Unity
               Catalog governance/lineage, AI Gateway, Genie Ontology (dashed)

DAG discipline (standing rule): every illuminated box gets >=1 edge; sources must
show; the box map is authoritative (no box is lit that isn't in the map).

Usage:
    python3 generate_overlay.py --manifest /tmp/asbuilt_manifest.json \
        --out <APP>/static/asbuilt_state.json [--cloud azure]
"""

import argparse
import json
import os
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SKILL_DIR = os.path.dirname(SCRIPT_DIR)


def _find_box_map():
    """Locate idea_box_map.json in the skill (references/) or beside the script
    (when copied flat into an app dir for live refresh). ASBUILT_BOX_MAP env
    overrides."""
    env = os.environ.get("ASBUILT_BOX_MAP")
    candidates = [env] if env else []
    candidates += [
        os.path.join(SKILL_DIR, "references", "idea_box_map.json"),
        os.path.join(SCRIPT_DIR, "idea_box_map.json"),
        os.path.join(SCRIPT_DIR, "references", "idea_box_map.json"),
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return candidates[0] if candidates else "idea_box_map.json"


BOX_MAP_PATH = _find_box_map()


# ---------------------------------------------------------------------------
# Resource -> data-id detection
#
# Each detector inspects one manifest list and lights one or more data-ids when
# a predicate holds, attaching human-readable evidence for the detail panel.
# data-ids come from references/idea_box_map.json (the authoritative box map).
# ---------------------------------------------------------------------------

def _nonempty(v):
    return isinstance(v, list) and len(v) > 0


def _name(item, *keys):
    for k in keys:
        v = item.get(k)
        if v:
            return v
    return item.get("name", "") or item.get("full_name", "") or "?"


def _fmt(item, *keys):
    return str(item.get("data_source_format") or item.get("dataSourceFormat") or "").upper()


def _table_format(t):
    """Canonical, mutually-exclusive table format. Critically distinguishes
    Managed/UniForm Iceberg from plain Delta: UC reports data_source_format=DELTA
    even for Iceberg tables, so we inspect the delta.*Iceberg* / universalFormat
    properties. Also surfaces Postgres (Lakebase-synced foreign tables), MVs/views."""
    tt = (t.get("table_type") or "").upper()
    if "MATERIALIZED_VIEW" in tt:
        return "MV"
    if tt == "VIEW":
        return "View"
    fmt = (t.get("data_source_format") or "").upper()
    props = t.get("properties", {}) or {}

    def pv(k):
        return str(props.get(k, "")).lower()

    if "ICEBERG" in fmt:
        return "Iceberg"
    if (pv("delta.enableIcebergCompatV2") == "true"
            or props.get("delta.universalFormat.enabledFormats", "") == "iceberg"
            or pv("delta.enableIcebergWriterCompatV1") == "true"):
        return "Iceberg"                      # UniForm / Managed Iceberg (Delta-backed)
    if "POSTGRESQL" in fmt:
        return "Postgres"                     # Lakebase-synced foreign table
    if "MYSQL" in fmt:
        return "MySQL"
    if "DELTA" in fmt:
        return "Delta"
    return fmt.title() if fmt else (tt.title() if tt else None)


def qualify_manifest(manifest):
    """Scope metastore-global resources down to what's actually in THIS workspace.

    Ported from the pre-overlay generator: UC catalogs, connections, models and
    serving endpoints are metastore-global, so lighting boxes off the raw manifest
    over-reports what this workspace really runs. Schema-discovery success is the
    hinge signal for which catalogs are genuinely accessible here. Guarded so a
    lite/repo-scoped manifest (no schema discovery) is left untouched."""
    qm = dict(manifest)
    accessible = {s.get("_catalog", "") for s in (manifest.get("schemas") or []) if s.get("_catalog")}
    qm["_accessible_catalogs"] = accessible

    if accessible:  # full discovery ran — scope to accessible catalogs
        qm["catalogs"] = [c for c in (manifest.get("catalogs") or []) if c.get("name", "") in accessible]

    # Models: drop Foundation Model / internal registry entries.
    qm["models"] = [r for r in (manifest.get("models") or [])
                    if not (r.get("full_name") or "").startswith("system.ai.")
                    and not (r.get("full_name") or "").startswith("__databricks_internal")]
    # Serving endpoints: drop Foundation Model API defaults (databricks-* prefix).
    qm["serving_endpoints"] = [ep for ep in (manifest.get("serving_endpoints") or [])
                               if not (ep.get("name") or "").startswith("databricks-")]
    return qm


def _ext_model(ep):
    """True if a serving endpoint fronts an external model provider (AI Gateway)."""
    try:
        ents = (ep.get("config") or {}).get("served_entities") or []
        if any(e.get("external_model") for e in ents):
            return True
    except Exception:
        pass
    return "external_model" in json.dumps(ep, default=str).lower()


def evaluate_active(manifest):
    """Return {data_id: [evidence, ...]} for every illuminated box.

    Detection nuance is ported from the pre-overlay generator so the overlay is
    workspace/solution-specific: it only lights what is actually deployed and
    guards against false positives (notably: Lakebase lights ONLY from real
    instances/projects, never from UC Postgres federation)."""
    active = {}

    def light(data_id, evidence):
        active.setdefault(data_id, [])
        if evidence and evidence not in active[data_id]:
            active[data_id].append(evidence)

    catalogs = manifest.get("catalogs", []) or []
    schemas = manifest.get("schemas", []) or []
    tables = manifest.get("tables", []) or []
    volumes = manifest.get("volumes", []) or []
    pipelines = manifest.get("pipelines", []) or []
    jobs = manifest.get("jobs", []) or []
    warehouses = manifest.get("warehouses", []) or []
    clusters = manifest.get("clusters", []) or []
    apps = manifest.get("apps", []) or []
    endpoints = manifest.get("serving_endpoints", []) or []
    vs = manifest.get("vector_search", []) or []
    models = manifest.get("models", []) or []
    genie = manifest.get("genie_spaces", []) or []
    dashboards = manifest.get("dashboards", []) or []
    lakebase = manifest.get("lakebase_instances", []) or []
    connections = manifest.get("connections", []) or []
    monitors = manifest.get("monitors", []) or []

    def _tname(t):
        return (t.get("full_name") or t.get("name") or "").lower() + " " + (t.get("_schema", "").lower())

    # --- Unity Catalog governance (a28) — lit when any accessible catalog exists ---
    if _nonempty(catalogs):
        for c in catalogs[:8]:
            light("a28", f"catalog: {_name(c)}")
    for mon in monitors[:6]:
        light("a28", f"quality monitor: {mon.get('_table', '?')}")   # lakehouse monitoring = governance evidence

    # --- Lakehouse (a30) + storage formats ---
    if _nonempty(tables) or _nonempty(warehouses):
        light("a30", f"{len(tables)} table(s) / {len(schemas)} schema(s)")
    delta = [t for t in tables if _table_format(t) == "Delta"]
    iceberg = [t for t in tables if _table_format(t) == "Iceberg"]   # incl. UniForm/Managed Iceberg
    if delta:
        light("a37", f"{len(delta)} Delta table(s)")
    if iceberg:
        light("a38", f"{len(iceberg)} Iceberg table(s)")
    # Medallion by name (raw/landing, cleaned/enriched, curated/analytics)
    def _has(*kws):
        return [t for t in tables if any(k in _tname(t) for k in kws)]
    if _has("bronze", "raw", "landing"):
        light("a40", "bronze / raw tables")
    if _has("silver", "clean", "enrich", "conform"):
        light("a41", "silver / cleaned tables")
    if _has("gold", "curated", "analytics", "report", "agg"):
        light("a42", "gold / curated tables")

    # --- Predictive Optimization (a18 ZeroOps, feature): AUTO_STATS or managed Iceberg ---
    po = []
    for t in tables:
        props = t.get("properties", {}) or {}
        auto_stats = "AUTO_STATS" in str(props.get("spark.sql.statistics.auxiliaryInfo", ""))
        managed_ice = props.get("delta.universalFormat.enabledFormats", "") == "iceberg"
        if (auto_stats or managed_ice) and t.get("_catalog", "") not in ("system", "samples", ""):
            po.append(t)
    if po:
        light("a18", f"predictive optimization on {len(po)} table(s)")

    # NOTE: the cloud infra band (a121-a132) is NOT lit — those data-id slots are
    # provider-specific and REORDER across Azure/AWS/GCP (e.g. a121 is ADLS Gen2 in
    # Azure but CloudWatch in AWS), so the Azure-derived box map can't map them
    # safely across clouds. overlay.js sets the correct provider view; the band
    # stays as dimmed context.

    # --- Sources ---
    if _nonempty(volumes):
        light("a61", f"{len(volumes)} UC volume(s)")                 # Files & Object Stores
    iot = _has("iot", "sensor", "telemetry", "device", "network")
    if iot:
        light("a70", f"{len(iot)} IoT/sensor table(s)")              # Device & Sensor Data

    # --- Ingest ---
    for p in pipelines:
        blob = json.dumps(p, default=str).lower()
        if "ingestion_definition" in blob:
            light("a1", f"ingestion pipeline: {_name(p, 'name')}")   # Lakeflow Connect
        if "cloudfiles" in blob or "autoloader" in blob:
            light("a2", f"pipeline: {_name(p, 'name')}")            # Auto Loader
        light("a32", f"pipeline: {_name(p, 'name')}")               # Lakeflow (transform/orchestrate)
    if _nonempty(volumes) and not any("cloudfiles" in json.dumps(p, default=str).lower() for p in pipelines):
        light("a2", f"{len(volumes)} volume(s) available to Auto Loader")  # widen: volumes = autoload source
    if _nonempty(jobs):
        light("a32", f"{len(jobs)} job(s)")                         # Lakeflow orchestrate

    # --- Compute: Spark / Photon (a34) ---
    if _nonempty(clusters) or _nonempty(warehouses):
        light("a34", (f"{len(clusters)} cluster(s)" if clusters else "") +
              (f" {len(warehouses)} warehouse(s)" if warehouses else "").strip())

    # --- Serve ---
    if _nonempty(warehouses):
        for w in warehouses[:6]:
            light("a5", f"warehouse: {_name(w)}")                   # SQL Warehouses
    for e in endpoints[:8]:
        nm = _name(e)
        light("a8", f"endpoint: {nm}")                              # Model Serving
        if _ext_model(e):
            light("a29", f"external model: {nm}")                   # Unity AI Gateway (feature)
            light("a130", f"external provider: {nm}")               # Anthropic/OpenAI integration
        if "agent" in nm.lower():
            light("a21", f"agent endpoint: {nm}")                   # Agent Bricks
            light("a16", f"agent: {nm}")                            # AGENTS
    if _nonempty(vs):
        for v in vs[:6]:
            light("a9", f"vector endpoint: {_name(v)}")             # AI Search
    if _nonempty(models):
        light("a35", f"{len(models)} registered model(s)")          # MLflow (registry)
        if _nonempty(endpoints):
            light("a8", f"{len(models)} model(s) available to serve")

    # --- Operational DB: Lakebase (a7 serve + a31 data) — instances/projects ONLY.
    #     UC Postgres federation must NOT light Lakebase (false-positive guard). ---
    if _nonempty(lakebase):
        for lb in lakebase[:6]:
            ev = f"lakebase: {_name(lb)} ({lb.get('_lakebase_kind', 'instance')})"
            light("a7", ev)
            light("a31", ev)

    # --- Apps ---
    for a in apps[:8]:
        light("a11", f"app: {_name(a)}")

    # --- Analytics / BI / Genie ---
    for d in dashboards[:8]:
        light("a10", f"dashboard: {_name(d, 'display_name')}")      # AI/BI
    if _nonempty(genie):
        light("a10", f"{len(genie)} Genie space(s)")
        light("a14", f"{len(genie)} Genie space(s)")                # Genie
        light("a15", "Genie ONE")

    # --- Delta Sharing (a33 OpenSharing + a105 Sharing Recipients) ---
    shares = [c for c in catalogs if (c.get("catalog_type") or "").upper() == "DELTASHARING_CATALOG"]
    if shares:
        for c in shares[:6]:
            light("a33", f"share: {_name(c)}")
            light("a105", f"share: {_name(c)}")

    # --- Federation / connections (workspace-scoped by qualify_manifest) ---
    for c in connections[:10]:
        ctype = str(c.get("connection_type") or c.get("connectionType") or "").upper()
        if ctype == "MANAGED_POSTGRESQL":
            light("a28", f"Lakebase federation: {_name(c)}")        # UC federation, NOT Lakebase itself
            continue
        light("a1", f"connection: {_name(c)} ({ctype})")            # Lakeflow Connect
        if ctype in ("REDSHIFT", "SNOWFLAKE", "BIGQUERY"):
            light("a59", f"{_name(c)} ({ctype})")                   # Data Warehouses
        elif ctype in ("MYSQL", "POSTGRESQL", "SQLSERVER", "ORACLE"):
            light("a58", f"{_name(c)} ({ctype})")                   # Operational Databases
        elif ctype in ("SALESFORCE", "WORKDAY", "SERVICENOW", "NETSUITE"):
            light("a57", f"{_name(c)} ({ctype})")                   # Business Applications
        elif "HIVE" in ctype:
            light("a77", f"{_name(c)} ({ctype})")                   # Hive Metastore
    # Hive Metastore catalog (federation) even without an explicit connection
    if any(c.get("name") == "hive_metastore" or (c.get("catalog_type") or "").upper() == "EXTERNAL" for c in catalogs):
        light("a77", "hive_metastore catalog")

    return active


# ---------------------------------------------------------------------------
# Flow graph (two tiers). Edges are data-id pairs. See references/flow_taxonomy.md.
# ---------------------------------------------------------------------------

# Boxes that represent a capability a flow RELIES ON but isn't the spine.
FEATURE_BOXES = {"a28", "a29", "a26", "a18"}   # Unity Catalog, Unity AI Gateway, Genie Ontology, ZeroOps (predictive optimization)

# Governance features that are "always on" when the platform is active — they may
# originate a feature edge even if not independently detected.
ALWAYS_ON_FEATURES = {"a28"}            # Unity Catalog governs everything

# Core spine (ordered data/AI flow). Only drawn when BOTH endpoints are active.
CORE_EDGES = [
    # ingest -> lakehouse
    ("a1", "a32", "connect"), ("a2", "a32", "autoload"), ("a3", "a32", "stream"),
    ("a4", "a30", "migrate"), ("a32", "a30", "transform"),
    # lakehouse medallion
    ("a40", "a41", "refine"), ("a41", "a42", "curate"),
    ("a42", "a30", "serve"),
    # storage formats under lakehouse
    ("a37", "a30", "delta"), ("a38", "a30", "iceberg"),
    # serve layer
    ("a30", "a5", "SQL"), ("a30", "a8", "models"), ("a30", "a9", "retrieval"),
    # Lakebase spans two IDEA boxes (a31 Agentic Data = the Postgres foundation,
    # a7 Serve = operational reads). Chain them into ONE flow instead of drawing
    # parallel duplicates: Lakehouse -> Lakebase(data) -> Lakebase(serve) -> Apps.
    ("a30", "a31", "operational store"),
    ("a31", "a7", "operational reads"),
    ("a9", "a8", "RAG"),
    # analytics / apps
    ("a5", "a10", "BI"), ("a30", "a14", "Genie"), ("a10", "a14", "AI/BI"),
    ("a8", "a11", "AI features"), ("a7", "a11", "app reads"),
    # agentic work (agents/agent-bricks are served via Model Serving, not the hub)
    ("a8", "a16", "agents"), ("a8", "a21", "agent bricks"),
]

# Source tiles -> their natural ingest box (dynamic; only when source is active).
SOURCE_TO_INGEST = {
    "a57": "a1", "a58": "a1", "a59": "a1",          # apps/db/dwh -> Lakeflow Connect
    "a61": "a2", "a62": "a2",                        # files/logs -> Auto Loader
    "a69": "a3", "a70": "a3", "a71": "a1",           # events/sensors -> Zerobus; CDC -> Connect
    "a77": "a1",                                     # Hive Metastore -> Connect
}

# Feature-dependency edges: (feature_box -> core_box, label). Drawn when the core
# box is active and the feature box is active OR in ALWAYS_ON_FEATURES.
FEATURE_EDGES = [
    ("a28", "a30", "governed"), ("a28", "a5", "governed"), ("a28", "a8", "governed"),
    ("a28", "a11", "governed"), ("a28", "a10", "governed"), ("a28", "a14", "governed"),
    ("a29", "a8", "AI gateway"),
    ("a26", "a14", "ontology"),
    ("a18", "a30", "predictive opt"),   # ZeroOps auto-optimizes lakehouse tables
]

# Consumer region tiles fed from serve/apps boxes (dynamic; when both active).
SERVE_BOXES = {"a5", "a8", "a10", "a11", "a14"}


def build_edges(active_ids):
    """Return (coreEdges, featureEdges) upholding the DAG rule."""
    active = set(active_ids)
    core, feat = [], []
    seen = set()

    def add_core(a, b, label):
        key = (a, b)
        if a in active and b in active and a != b and key not in seen:
            seen.add(key)
            core.append({"from": a, "to": b, "label": label, "kind": "core"})

    for a, b, label in CORE_EDGES:
        add_core(a, b, label)
    # dynamic source -> ingest
    for src, ing in SOURCE_TO_INGEST.items():
        add_core(src, ing, "ingest")
    # dynamic serve/apps -> active consumer tiles (a94-a120)
    consumer_ids = [i for i in active if i[0] == "a" and 94 <= int(i[1:]) <= 120]
    for c in consumer_ids:
        for s in SERVE_BOXES:
            if s in active:
                add_core(s, c, "consume")
                break

    # feature edges
    fseen = set()
    for f, c, label in FEATURE_EDGES:
        if c in active and (f in active or f in ALWAYS_ON_FEATURES) and (f, c) not in fseen:
            fseen.add((f, c))
            feat.append({"from": f, "to": c, "label": label, "kind": "feature"})

    # DAG rule: every active box needs >=1 edge. Attach orphans to the hub (a30)
    # if it's active, else to the first core box present.
    edged = set()
    for e in core + feat:
        edged.add(e["from"]); edged.add(e["to"])
    hub = "a30" if "a30" in active else (sorted(active)[0] if active else None)
    for box in active:
        if box in edged or box == hub or box in FEATURE_BOXES:
            continue
        if hub and box != hub:
            # direction: sources/ingest flow INTO hub; everything else flows FROM hub
            n = int(box[1:])
            if 57 <= n <= 85:       # source / external-ingestion tiles
                add_core(box, hub, "ingest")
            else:
                add_core(hub, box, "flow")
    return core, feat


def build_details(manifest, active_ids):
    """Per-box drill-down trees for the drawer. A node is {label, meta?, children?}.
    Data/governance boxes get a catalog -> schema -> table hierarchy; product boxes
    get a flat list with per-item metadata. Sizes are capped to bound state size."""
    active = set(active_ids)
    tables = manifest.get("tables", []) or []
    volumes = manifest.get("volumes", []) or []
    det = {}

    def _cst(subset, leaf_meta=_table_format):
        """catalog -> schema -> table tree."""
        grp = {}
        for t in subset:
            cat = t.get("_catalog") or (t.get("full_name", "").split(".")[0] if t.get("full_name") else "?")
            sch = t.get("_schema") or "?"
            grp.setdefault(cat, {}).setdefault(sch, []).append({"label": t.get("name") or "?", "meta": leaf_meta(t)})
        nodes = []
        for cat in sorted(grp):
            schs = []
            for sch in sorted(grp[cat]):
                items = sorted(grp[cat][sch], key=lambda x: x["label"])[:300]
                schs.append({"label": sch, "meta": f"{len(grp[cat][sch])} tables", "children": items})
            nodes.append({"label": cat, "meta": "catalog", "children": schs})
        return nodes

    def _name(x, *ks):
        for k in ks:
            if x.get(k):
                return x.get(k)
        return "?"

    def _list(items, labelf, metaf=None, cap=150):
        out = []
        for it in items[:cap]:
            node = {"label": labelf(it)}
            if metaf:
                m = metaf(it)
                if m:
                    node["meta"] = m
            out.append(node)
        return out

    if tables:
        if "a28" in active: det["a28"] = _cst(tables)     # Unity Catalog — all governed assets
        if "a30" in active: det["a30"] = _cst(tables)     # Lakehouse
        d = [t for t in tables if _table_format(t) == "Delta"]
        if d and "a37" in active: det["a37"] = _cst(d)
        ice = [t for t in tables if _table_format(t) == "Iceberg"]
        if ice and "a38" in active: det["a38"] = _cst(ice)
        def _med(kws): return [t for t in tables if any(k in (t.get("name", "") + " " + t.get("_schema", "")).lower() for k in kws)]
        if "a40" in active: det["a40"] = _cst(_med(["bronze", "raw", "landing"]))
        if "a41" in active: det["a41"] = _cst(_med(["silver", "clean", "enrich"]))
        if "a42" in active: det["a42"] = _cst(_med(["gold", "curated", "analytics", "report", "agg"]))
    if volumes and "a61" in active:
        det["a61"] = _cst([{"_catalog": v.get("_catalog"), "_schema": v.get("_schema"), "name": v.get("name")} for v in volumes],
                          leaf_meta=lambda t: "volume")

    eps = manifest.get("serving_endpoints", []) or []
    if eps:
        def _epmeta(e):
            s = e.get("state")
            return (s.get("ready") if isinstance(s, dict) else None) or None
        if "a8" in active: det["a8"] = _list(eps, lambda e: _name(e, "name"), _epmeta)
        agents = [e for e in eps if "agent" in _name(e, "name").lower()]
        for aid in ("a21", "a16"):
            if agents and aid in active: det[aid] = _list(agents, lambda e: _name(e, "name"))
    wh = manifest.get("warehouses", []) or []
    if wh and "a5" in active:
        det["a5"] = _list(wh, lambda w: _name(w, "name"),
                          lambda w: (str(w.get("cluster_size") or "") + (" · serverless" if w.get("enable_serverless_compute") else "")).strip(" ·") or None)
    apps = manifest.get("apps", []) or []
    if apps and "a11" in active: det["a11"] = _list(apps, lambda a: _name(a, "name"), lambda a: _name(a, "url") if a.get("url") else None)
    genie = manifest.get("genie_spaces", []) or []
    if genie and "a14" in active: det["a14"] = _list(genie, lambda g: _name(g, "title", "name", "space_id"))
    vs = manifest.get("vector_search", []) or []
    if vs and "a9" in active: det["a9"] = _list(vs, lambda v: _name(v, "name"))
    lb = manifest.get("lakebase_instances", []) or []
    if lb:
        lbmeta = lambda i: ((i.get("_lakebase_kind") or "") + ((" · " + i.get("state")) if i.get("state") else "")).strip(" ·") or None
        for aid in ("a31", "a7"):
            if aid in active: det[aid] = _list(lb, lambda i: _name(i, "name"), lbmeta)
    pipes = manifest.get("pipelines", []) or []
    jobs = manifest.get("jobs", []) or []
    if "a32" in active and (pipes or jobs):
        nodes = []
        if pipes: nodes.append({"label": "Pipelines", "meta": str(len(pipes)), "children": _list(pipes, lambda p: _name(p, "name"))})
        if jobs: nodes.append({"label": "Jobs", "meta": str(len(jobs)), "children": _list(jobs, lambda j: (j.get("settings", {}) or {}).get("name") or _name(j, "name"))})
        det["a32"] = nodes
    models = manifest.get("models", []) or []
    if models and "a35" in active: det["a35"] = _list(models, lambda m: _name(m, "name", "full_name"))
    conns = manifest.get("connections", []) or []
    if conns and "a1" in active: det["a1"] = _list(conns, lambda c: _name(c, "name"), lambda c: _name(c, "connection_type") if c.get("connection_type") else None)

    return det


def generate(manifest_path, out_path, cloud="azure"):
    with open(manifest_path) as f:
        manifest = json.load(f)
    with open(BOX_MAP_PATH) as f:
        box_map = json.load(f)
    valid_ids = set(box_map["boxes"].keys())

    manifest = qualify_manifest(manifest)   # scope metastore-global resources to this workspace
    active = evaluate_active(manifest)
    # Enforce the authoritative box map: never light a box that isn't in it.
    active = {k: v for k, v in active.items() if k in valid_ids}

    core, feat = build_edges(active.keys())

    ws = manifest.get("workspace", {})
    state = {
        "meta": {
            "profile": ws.get("profile", ""),
            "host": ws.get("host", ""),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "idea_release": box_map.get("idea_release", ""),
            "cloud": cloud,
            "active_count": len(active),
            "core_edge_count": len(core),
            "feature_edge_count": len(feat),
        },
        "active": sorted(active.keys(), key=lambda x: int(x[1:])),
        "evidence": active,
        "details": build_details(manifest, active.keys()),
        "coreEdges": core,
        "featureEdges": feat,
    }

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(state, f, indent=2)
    return state


def main():
    ap = argparse.ArgumentParser(description="Generate AsBuilt overlay state from a manifest.")
    ap.add_argument("--manifest", required=True, help="Path to discovery manifest JSON.")
    ap.add_argument("--out", required=True, help="Output path for asbuilt_state.json.")
    ap.add_argument("--cloud", default="azure", choices=["azure", "aws", "gcp"],
                    help="Cloud view the base is rendered in (affects cloud-tile labels).")
    args = ap.parse_args()

    state = generate(args.manifest, args.out, cloud=args.cloud)
    m = state["meta"]
    print(f"Overlay written to {args.out}")
    print(f"  active boxes : {m['active_count']}")
    print(f"  core edges   : {m['core_edge_count']}")
    print(f"  feature edges: {m['feature_edge_count']}")
    print(f"  active       : {', '.join(state['active'])}")


if __name__ == "__main__":
    main()

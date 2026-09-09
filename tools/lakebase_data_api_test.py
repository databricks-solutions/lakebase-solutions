# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Data API — Connectivity & Query Test
# MAGIC
# MAGIC ## Important — the instance-owner constraint (handled for you)
# MAGIC The Data API works by having a gateway role (`authenticator`) **assume your Postgres
# MAGIC identity**. So **the instance owner cannot use the Data API as themselves** (HTTP 403
# MAGIC `permission denied to set role`) — a **non-owner** identity is required.
# MAGIC
# MAGIC This notebook handles that for you:
# MAGIC - The **Endpoint** dropdown (built by Step 1) lists every Data-API-enabled instance you can
# MAGIC   see — including ones you **own** (tagged *"(you own)"*).
# MAGIC - Pick one you can already use → it runs as **you**.
# MAGIC - Pick one you **own** (or that needs setup) → set **`provision = yes`** and the notebook
# MAGIC   **auto-creates a service principal**, wires it, and runs the test as that SP. No code edits.
# MAGIC
# MAGIC > Provisioning is **gated** behind `provision = yes` (default off) so a plain Run All never
# MAGIC > creates identities by accident. It requires you to be a **workspace admin** and
# MAGIC > **owner/admin of the instance**. The final cell reports every asset it created/changed.
# MAGIC
# MAGIC **How to run:** run the `%pip` cell → **Step 1** (it adds the `Endpoint` dropdown) → pick your
# MAGIC Endpoint → run the rest. Re-pick the Endpoint after any Step 1 re-run.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-sdk psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ### Configuration — declarative parameters
# MAGIC Set these in the widget bar. The **`Endpoint`** dropdown is added when you run **Step 1** —
# MAGIC pick it there (it's the only required choice). Fields marked **(optional)** can be left as-is:
# MAGIC blank `schema`/`table` = auto-discover; `SP name` and `Expose schema` apply **only when
# MAGIC `provision = yes`**. `Provision SP?` is the gate (default `no`).

# COMMAND ----------

dbutils.widgets.dropdown("provision", "no", ["no", "yes"], "Provision SP? (no / yes)")
dbutils.widgets.text("sp_name", "lakebase-data-api-tester", "(optional) SP name — provisioning")
dbutils.widgets.text("expose_schema", "", "(optional) Expose schema — provisioning")
dbutils.widgets.text("schema", "", "(optional) Query schema — blank=auto")
dbutils.widgets.text("table", "", "(optional) Query table — blank=auto")
dbutils.widgets.text("row_limit", "5", "(optional) Row limit — default 5")

# COMMAND ----------

# MAGIC %md ### Step 1 — discover Data-API instances (fills the Endpoint dropdown)
# MAGIC Scans every Data-API-enabled instance you can see and fills the **Endpoint** dropdown at the
# MAGIC top (instances you own are included, tagged *"(you own)"* — pick one and set `provision = yes`
# MAGIC to auto-create a service principal for it).

# COMMAND ----------

import base64
import json
from urllib.parse import urlparse
from databricks.sdk import WorkspaceClient


def _jwt_sub(jwt):
    try:
        payload = jwt.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload)).get("sub")
    except Exception:
        raise SystemExit("Could not parse the database credential token.")


w_self = WorkspaceClient()          # your notebook identity (also the admin used for provisioning)
CREATED_ASSETS = []                  # inventory shown in the final cell
try:
    me = w_self.current_user.me().user_name
except Exception:
    me = None
    print("WARNING: couldn't resolve your identity — owned-instance detection is off; an instance "
          "you own may look usable and then return 403.")

TARGETS = {}
for proj in w_self.postgres.list_projects():
    owner = getattr(getattr(proj, "status", None), "owner", None)
    instance_name = (
        getattr(getattr(proj, "status", None), "display_name", None)
        or getattr(proj, "project_id", None) or proj.name.split("/")[-1]
    ).replace(",", " ")
    for br in w_self.postgres.list_branches(proj.name):
        host2ep = {}
        try:
            for e in w_self.postgres.list_endpoints(br.name):
                h = getattr(e.status, "hosts", None)
                hh = getattr(h, "host", None) or (h.get("host") if isinstance(h, dict) else None)
                if hh:
                    host2ep[hh] = e.name
        except Exception:
            pass
        try:
            databases = w_self.api_client.do("GET", f"/api/2.0/postgres/{br.name}/databases").get("databases", [])
        except Exception:
            databases = []
        for db in databases:
            dbid = db["status"]["database_id"]
            pg_db = db["status"]["postgres_database"]
            try:
                da = w_self.api_client.do("GET", f"/api/2.0/postgres/{br.name}/databases/{dbid}/data-api")
            except Exception:
                continue  # Data API not enabled on this database — skip
            owned = (owner is not None and owner == me)
            st = da.get("status", {})
            epname = host2ep.get(urlparse(st.get("url", "")).netloc) or next(iter(host2ep.values()), None)
            if not epname:
                continue
            label = f"{instance_name}|{epname.split('/')[-1]}" + (" (you own)" if owned else "")
            TARGETS[label] = {
                "endpoint": epname, "database": pg_db,
                "host": urlparse(st.get("url", "")).netloc,
                "schemas": st.get("db_schemas", []), "owned": owned,
                "branch": br.name, "dbid": dbid,
            }

choices = sorted(TARGETS.keys())
_prev = None
try:
    _prev = dbutils.widgets.get("endpoint")
except Exception:
    pass
try:
    dbutils.widgets.remove("endpoint")
except Exception:
    pass
if choices:
    # Preserve the prior pick across re-runs; on first run prefer a directly-usable (non-owned)
    # endpoint so a plain Run All doesn't dead-end on one you own.
    if _prev in choices:
        _default = _prev
    elif dbutils.widgets.get("provision") != "yes":
        _default = next((c for c in choices if not TARGETS[c]["owned"]), choices[0])
    else:
        _default = choices[0]
    dbutils.widgets.dropdown("endpoint", _default, choices, "(required) Endpoint — pick one")
    print(f"Found {len(choices)} Data-API instance(s). Selected '{_default}'. Change it in the "
          f"'Endpoint' dropdown above, then run the rest:")
    for c in choices:
        t = TARGETS[c]
        note = "  ← you own this: set provision=yes to auto-create an SP" if t["owned"] else ""
        print(f"  - {c}   →   {t['endpoint']}  (db={t['database']}, schemas={t['schemas'] or 'all'}){note}")
else:
    dbutils.widgets.text("endpoint", "", "(required) Endpoint — none found, type one")
    print("No Data-API-enabled instances found that you can see.")

# COMMAND ----------

# MAGIC %md ### Step 2 — resolve the pick (auto-provisioning an SP if it needs one)
# MAGIC If you picked an instance you own (or one needing setup), this creates/wires a service
# MAGIC principal — but only when `provision = yes`. Otherwise it runs as your own identity.

# COMMAND ----------

import requests

sel = dbutils.widgets.get("endpoint").strip()
if not sel:
    raise SystemExit("Run Step 1 and pick an endpoint from the 'Endpoint' dropdown.")
target = TARGETS.get(sel)
if not target:
    raise SystemExit(f"'{sel}' is not a discovered target — re-run Step 1.")

import re
provision = dbutils.widgets.get("provision") == "yes"
expose_schema = dbutils.widgets.get("expose_schema").strip()
sp_name = dbutils.widgets.get("sp_name").strip() or "lakebase-data-api-tester"
if not re.fullmatch(r"[A-Za-z0-9 _.\-]{1,255}", sp_name):
    raise SystemExit("SP name may contain only letters, numbers, spaces, '_', '.', '-'.")
if expose_schema and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]{0,62}", expose_schema):
    raise SystemExit("Expose schema is not a valid Postgres identifier name.")

endpoint_name = target["endpoint"]
database = target["database"]
exposed_schemas = target["schemas"]

if not target["owned"]:
    # Usable directly as your own identity.
    w = w_self
    print("Running as your own identity against:", endpoint_name)
else:
    # You own it → the Data API can't assume your role. Need a service principal.
    if not provision:
        raise SystemExit(
            f"You OWN '{sel}', so the Data API can't use your identity. Set provision = yes to "
            f"auto-create a service principal ('{sp_name}') for it, then re-run.")
    import psycopg2

    prior_schemas = list(target["schemas"])
    exposed_schemas = sorted(set(prior_schemas) | ({expose_schema} if expose_schema else set()))
    if not exposed_schemas:
        raise SystemExit("This Data API exposes no schemas yet — set 'Expose schema' to the schema "
                         "you want to query, then re-run.")
    # 1. create or reuse the service principal + mint an OAuth secret (held in memory only)
    existing = list(w_self.service_principals.list(filter=f'displayName eq "{sp_name}"'))
    if existing:
        sp = existing[0]
        print("Reusing service principal:", sp.application_id)
        CREATED_ASSETS.append(("Service principal", f"{sp_name} · app_id={sp.application_id} · id={sp.id}",
                               "reused", f"databricks service-principals delete {sp.id}"))
    else:
        sp = w_self.service_principals.create(display_name=sp_name)
        print("Created service principal:", sp.application_id)
        CREATED_ASSETS.append(("Service principal", f"{sp_name} · app_id={sp.application_id} · id={sp.id}",
                               "CREATED", f"databricks service-principals delete {sp.id}"))
    try:
        _sec = w_self.service_principal_secrets_proxy.create(service_principal_id=sp.id, lifetime="3600s")
    except Exception:
        # SP secret cap reached — remove the oldest proxy secret and retry once
        _old = list(w_self.service_principal_secrets_proxy.list(service_principal_id=sp.id))
        if _old:
            w_self.service_principal_secrets_proxy.delete(service_principal_id=sp.id, secret_id=_old[0].id)
        _sec = w_self.service_principal_secrets_proxy.create(service_principal_id=sp.id, lifetime="3600s")
    CREATED_ASSETS.append(("SP OAuth secret", f"secret_id={_sec.id} (in-memory only; ~1h lifetime)",
                           "CREATED", f"databricks service-principal-secrets-proxy delete {sp.id} {_sec.id}"))
    app_id = sp.application_id

    # 2. wire the SP in Postgres (as you, the owner): role + grants + grant to authenticator
    tok = w_self.postgres.generate_database_credential(endpoint=endpoint_name).token
    conn = psycopg2.connect(host=target["host"], port=5432, dbname=database, user=_jwt_sub(tok),
                            password=tok, sslmode="require", connect_timeout=30)
    conn.autocommit = True
    from psycopg2 import sql
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth;")
        try:
            cur.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL');", (app_id,))
        except Exception as e:
            print("create_role note (may already exist):", str(e)[:120])
        role = sql.Identifier(app_id)
        for sch in exposed_schemas:
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(sch), role))
            cur.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(sql.Identifier(sch), role))
        cur.execute(sql.SQL("GRANT {} TO authenticator").format(role))
    conn.close()
    CREATED_ASSETS.append(("Postgres role + grants", f'{app_id}: SELECT on {exposed_schemas}; member of authenticator',
                           "applied", f'REVOKE "{app_id}" FROM authenticator;'))

    # 3. refresh PostgREST's schema cache so it recognizes the new role (else HTTP 500)
    w_self.api_client.do("PATCH", f'/api/2.0/postgres/{target["branch"]}/databases/{target["dbid"]}/data-api',
                         query={"update_mask": "spec"}, body={"spec": {"db_schemas": exposed_schemas}})
    print("Wired SP and refreshed the Data API schema cache.")
    if set(exposed_schemas) != set(prior_schemas):
        CREATED_ASSETS.append(
            ("Data API schema exposure",
             f"db_schemas: {prior_schemas or '(none)'} → {exposed_schemas}", "CHANGED (persistent)",
             f"PATCH .../data-api db_schemas back to {prior_schemas or '[]'}"))

    # 4. run the test AS the new service principal
    w = WorkspaceClient(host=w_self.config.host, client_id=app_id, client_secret=_sec.secret)
    print(f"Running as provisioned service principal {app_id} against:", endpoint_name)

# Mint the credential + assemble the base URL with whichever identity we settled on. A just-created
# service principal's OAuth secret can take a few seconds to propagate, so retry briefly.
import time
_last = None
for _attempt in range(4):
    try:
        hosts = w.postgres.get_endpoint(name=endpoint_name).status.hosts
        host = getattr(hosts, "host", None) or (hosts.get("host") if isinstance(hosts, dict) else None)
        workspace_id = w.get_workspace_id()
        token = w.postgres.generate_database_credential(endpoint=endpoint_name).token
        break
    except Exception as e:
        _last = e
        time.sleep(3)
else:
    raise SystemExit(f"Could not authenticate / mint a credential for {endpoint_name}: {_last}")
pg_user = _jwt_sub(token)
base_url = f"https://{host}/api/2.0/workspace/{workspace_id}/rest/{database}"
auth_headers = {"Authorization": f"Bearer {token}"}
print("PG identity  :", pg_user, "| database:", database, "| exposed:", exposed_schemas or "(all)")

# COMMAND ----------

# MAGIC %md ### Step 3 — auto-discover a table (skip by setting the schema/table widgets)

# COMMAND ----------

schema = dbutils.widgets.get("schema").strip()
table = dbutils.widgets.get("table").strip()

if not (schema and table):
    try:
        import psycopg2
        conn = psycopg2.connect(host=host, port=5432, dbname=database, user=pg_user,
                                password=token, sslmode="require", connect_timeout=30)
        with conn, conn.cursor() as cur:
            if exposed_schemas:
                cur.execute(
                    """SELECT n.nspname, c.relname, c.reltuples::bigint
                       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE c.relkind='r' AND n.nspname = ANY(%s) AND has_table_privilege(c.oid,'SELECT')
                       ORDER BY c.reltuples DESC, 1, 2""", (exposed_schemas,))
            else:
                cur.execute(
                    """SELECT n.nspname, c.relname, c.reltuples::bigint
                       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema')
                         AND has_table_privilege(c.oid,'SELECT')
                       ORDER BY c.reltuples DESC, 1, 2""")
            accessible = cur.fetchall()
        conn.close()
        if not accessible:
            raise SystemExit("No readable tables for this identity. Check grants / expose_schema, "
                             "or set the schema/table widgets.")
        candidates = [r for r in accessible if (not schema or r[0] == schema)]
        if not candidates:
            raise SystemExit(f"No accessible tables in schema '{schema}'.")
        if not schema:
            schema = candidates[0][0]
            candidates = [r for r in accessible if r[0] == schema]
        print(f"Tables in '{schema}' (est. rows, largest first):",
              ", ".join(f"{t} (~{max(n, 0):,})" for _, t, n in candidates[:20]))
        if not table:
            table = candidates[0][1]
    except ImportError:
        raise SystemExit("psycopg2 unavailable — set the schema/table widgets and re-run.")
    except Exception as e:
        raise SystemExit(f"Auto-discovery failed ({e}). Set schema/table widgets and re-run.")

print("Using schema :", schema)
print("Using table  :", table)

# COMMAND ----------

# MAGIC %md ### Step 4 — query the table via the Data API (REST)

# COMMAND ----------

import json
import time
import requests
from urllib.parse import quote

try:
    row_limit = int(dbutils.widgets.get("row_limit").strip() or "5")
except ValueError:
    print("Row limit is not a number — using 5.")
    row_limit = 5

query_url = f"{base_url}/{quote(schema, safe='')}/{quote(table, safe='')}?limit={row_limit}"
_headers = {**auth_headers, "Accept": "application/json", "Prefer": "count=exact"}
resp = requests.get(query_url, headers=_headers, timeout=30)
if resp.status_code == 500:
    # PostgREST schema-cache reload after a fresh grant is async — wait briefly and retry once.
    time.sleep(5)
    resp = requests.get(query_url, headers=_headers, timeout=30)

print("GET", query_url)
print("HTTP status  :", resp.status_code)
print("Content-Range:", resp.headers.get("Content-Range"), "(start-end/total)")

if resp.status_code in (200, 206):  # 206 = Partial Content (limit + exact count); both are success
    rows = resp.json()
    print(f"Returned {len(rows)} row(s).")
    if rows:
        print("Columns:", ", ".join(rows[0].keys()))
        try:
            import pandas as pd
            display(pd.DataFrame(rows))
        except Exception:
            for r_ in rows:
                print(r_)
    print("\n✅ Data API test PASSED — endpoint reachable, auth works, data returned.")
else:
    try:
        body = resp.json()
    except Exception:
        body = {}
    msg = body.get("message", "")
    print("\n❌ Data API test FAILED.")
    if resp.status_code == 403 and "set role" in msg.lower():
        print("HTTP 403: identity is the instance owner, or its role isn't granted to 'authenticator'. "
              "Pick this same instance with provision = yes to auto-create a service principal.")
    elif resp.status_code == 500:
        print("HTTP 500: often a stale PostgREST schema cache after a fresh grant. Re-run Step 2 "
              "(it PATCHes the Data API to refresh the cache), then retry.")
    elif body.get("code") == "PGRST205":
        print(f"Table not found: {msg}. Check schema/table, or that the schema is Data-API-exposed.")
    else:
        print(f"HTTP {resp.status_code}: {json.dumps(body) if body else resp.text[:300]}")

# COMMAND ----------

# MAGIC %md ### Summary — what this run created / changed

# COMMAND ----------

_assets = globals().get("CREATED_ASSETS", [])
if not _assets:
    print("No assets were created or changed — this run only READ the Data API.")
else:
    print(f"This run created / changed {len(_assets)} asset(s):\n")
    try:
        import pandas as pd
        display(pd.DataFrame(_assets, columns=["Asset", "Detail", "Action", "Teardown / cleanup"]))
    except Exception:
        for a in _assets:
            print(f"- {a[0]}: {a[1]}\n    action: {a[2]}\n    cleanup: {a[3]}\n")
    print("\nNotes: the SP OAuth secret is in memory only (never printed) — re-run to mint a fresh "
          "one. Teardown commands assume `--profile <yours>` (CLI) or running as owner/admin (SQL).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Troubleshooting
# MAGIC | Symptom | Cause / Fix |
# MAGIC |---|---|
# MAGIC | Endpoint dropdown empty | No Data-API-enabled instance is visible to you. Enable the Data API on one first. |
# MAGIC | `You OWN '…'` SystemExit | Expected — pick it again with `provision = yes` to auto-create a service principal. |
# MAGIC | `403 permission denied to set role` | Owner identity, or role not granted to `authenticator`. Use `provision = yes`. |
# MAGIC | `500` after provisioning | Stale PostgREST schema cache — re-run Step 2 (it PATCHes a refresh) and retry. |
# MAGIC | Auto-discovery finds no tables | Role lacks `SELECT` on an exposed schema — check `expose_schema`, or set schema/table widgets. |
# MAGIC | `AttributeError: ...'postgres'` | `databricks-sdk` too old — re-run the first `%pip` cell. |
# MAGIC
# MAGIC > **206 is success** (`limit` + `Prefer: count=exact` → `206 Partial Content`). Provisioning
# MAGIC > needs workspace-admin + instance owner/admin rights.

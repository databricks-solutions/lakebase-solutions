# Spec: lakebase-solutions
_Basis: forked from `lakebase_fsm` (deploy harness + infra patterns) and `lakebase_admin` (admin app) · Author: Chase · Date: 2026-09-09_

> A good spec is **testable, bounded, and decision-recording**. If a sentence can't be verified or doesn't constrain a decision, it's decoration — cut it.

---

## 1. Problem & Goal
Databricks customers want to run workshops across a suite of Databricks tools and services. We are building a **reusable foundation for Databricks Lakebase workshops** that Solutions Architects run with their customers. The project is **modular**: a set of always-on **core** components (Lakebase and everything it depends on) plus **optional modules** selected per engagement.

Because it's Lakebase-oriented, the solution **always deploys Lakebase**. Beyond that, workshop **modules** — chosen by problem area or persona — bring their own datasets/tables/catalogs, prebuilt Genie/agent resources, ML models, apps, etc. **Core** (always installed): Lakebase, service principals, key management (Databricks secrets), the **admin app** (Lakebase DBA console), user management, and the Data API.

This initial spec designs the **repo architecture** so modules can be added later **without editing the deploy notebook**. Deployment is **immutable/repeatable** (like `lakebase_fsm` today) and driven by a **single, human-friendly deploy notebook**. The repo ships **instructions for authoring new modules**.

## 2. Success Criteria (acceptance tests)
- [ ] Deploys end-to-end via a single parameterized notebook
- [ ] Foundational components are always deployed
- [ ] Modules are deployed on an as-needed basis
- [ ] Multiple modules can be deployed easily at a time
- [ ] New-module integration works and the authoring instructions are functional (proven by the `_canary` module)
- [ ] Lakebase Data API works and is functional (authenticated REST call returns RLS-scoped rows via a dedicated non-owner role)
- [ ] Data API flow is two-phase: notebook loudly instructs the manual UI enable, then a re-runnable step configures the SP/role/RLS
- [ ] Teardown removes all deployed assets via the same notebook

## 3. Scope
**In:**
- New Lakebase instance, its deployment and configuration
- Install/deploy any modules discovered in the project via one easy notebook
- Strong security posture (this is a **public-facing repo**) — assessed, no secrets in git
- All major personas addressed by future modules: DBAs, App Developers, AI Engineers, Technical Leadership, ETL Developers
- Data API for the created Lakebase instance
- Teardown of all assets via the same deploy notebook, to clean up after a workshop

**Out (parking lot — capture later, don't build now):**
- New module creation (beyond a single `_canary` module for testing the contract)
- Control-plane-in-app (triggering module deploy/teardown from the admin app UI) — the **notebook is the control plane** for now
- Persona workshop content (dispatch/map/whatif/etc. from FSM) — future modules

## 4. Constraints & Non-Negotiables
- **Databricks-native, DABs-first across the ENTIRE project** — express every resource DABs can manage as a bundle resource: `app`, jobs, pipelines, `model_serving_endpoint`, `secret_scope`, `genie_spaces`, and the Lakebase **`database_instance`** (GA). SDK/REST only for what DABs cannot do at PP/GA.
- **Feature-maturity gate:** only **Public Preview or GA** features. No Private Preview / Beta. Confirmed maturity (verified via CLI v1.13.0 + docs, 2026-09-09):
  - `database` / Database Instances surface = **Public Preview** ✅ · `database_instance` bundle resource = **GA** ✅ — **this is the Lakebase surface we build on.**
  - `postgres` / Autoscaling API (projects/branches/**roles**) = **Beta** ❌ · `postgres_role` bundle resource = **Beta** ❌ — **excluded.** (FSM is built on this Beta surface; we do NOT copy its Lakebase provisioning.)
  - Lakebase + Data API = **GA** ✅ · Lakebase CDC = Public Preview ✅ · Lakebase Search = Beta ❌ (excluded).
- **PG roles/grants via `CREATE ROLE` SQL** over psycopg (plain Postgres, gate-agnostic) — sidesteps the Beta `postgres_role` resource. FSM's SQL role/grant patterns port cleanly.
- **Data API is two-phase (manual enable):** the notebook prints a loud, explicit "**you MUST enable the Data API manually**" instruction with steps; a **re-runnable** step (same or second notebook) then configures the dedicated SP, `databricks_auth`, role registration, and RLS/schema. (Programmatic `UpdateDataApi`/`db_schemas` may later remove the manual step — not v1.)
- Deploy runs **inside Databricks** — no laptop CLI execution. `databricks bundle deploy` is invoked from within the workspace (commit → push → pull → run).
- Never execute code locally against Databricks/Lakebase.
- **Standalone assets:** own PG roles, secret scope, credentials — nothing reused from another app. Each module's app is a **separate Databricks App**.
- **Immutable / repeatable** deploy; no hardcoded secrets. Every ad-hoc fix lands back in the deploy scripts.
- Single parameterized notebook deploys (and tears down) everything; **no notebook edits when adding a module** (module discovery via manifest).

## 5. Inputs & Parameters (proposed — best-practice inference)
Global params (deploy notebook widgets), namespaced by a single `deployment_id`:

| Param | Example | Notes |
|---|---|---|
| `deployment_id` / `prefix` | `acme-ws` | Namespaces ALL resources (catalogs, scopes, SP names, PG roles, app names) so multiple deployments coexist |
| `mode` | `deploy` \| `teardown` | Single notebook, two modes |
| `cloud` / `region` | `aws` / `us-west-2` | Host/endpoint patterns |
| `lakebase_instance` | `${prefix}-lakebase` | Derived from prefix |
| `database` | `databricks_postgres` | PG database name |
| `admin_group` | `${prefix}-admins` | Databricks group gating the admin app |
| `workshop_group` | `${prefix}-participants` | Workshop users |
| `modules_enabled` | multiselect | Discovered from `modules/*/module.yaml` |
| `enable_data_api` | `true` | Runs SP/role/RLS setup; flags the manual UI step |
| `secret_scope` | `${prefix}-secrets` | Standalone per deployment |

## 6. Architecture Decisions (locked)
1. **Spine = fork of `lakebase_fsm`** for the deploy harness + Lakebase/security/data-api patterns; **`core/admin_app` = fork of `lakebase_admin`** (already generic, multi-instance OAuth-OBO auth). FSM's field-service blueprints are *future module* reference, not core.
2. **DABs-first across the whole project** (see §4). The deploy notebook is a thin human layer: collect widget params → set bundle variables → `bundle deploy` → run the few non-bundle SDK/REST steps (Data API SP/role, data seeding) → run health checks.
3. **Module discovery via manifest** (`module.yaml`): declares `depends_on`, params, provided resources (for teardown/as-built), and the bundle resource files + any SDK steps it contributes. Orchestrator scans `core/` (always) + `modules/` (selected), topologically orders by `depends_on`, deploys. **Adding a module = drop a folder; no notebook edits.**
4. **Separate Databricks App per module**; `core/admin_app` is the only always-on app. Honors standalone-assets.
5. **Notebook is the control plane** for MVP; admin app stays a lean DBA console.
6. **Data API** harvested from FSM (`data_api.py`, `setup_data_api_sp.py`, `data_api_demo.sql`) — dedicated-SP + `databricks_auth` + `databricks_create_role`; the PGRST301 "owner-can't-use-it" pattern is the core of the setup. **Two-phase:** loud manual UI-enable instruction + re-runnable configure step (§4).
7. **State/idempotency:** per-deployment config (prefix-derived names + captured IDs), DABs state for bundle-managed resources; teardown = `bundle destroy` for the bundle half + SDK teardown for the rest.
8. **Lakebase surface = Database Instances (GA/PP), NOT Autoscaling (Beta).** We build the instance via the `database_instance` DABs resource and manage roles/grants with `CREATE ROLE` SQL. FSM's Autoscaling-based provisioning code is a *reference*, not a copy source, for the Lakebase layer; its SQL (schema, roles, features, Data API) ports directly.

## 7. Open items (validate in P0/P1)
- **Resolved:** `database_instance` = GA (use); Autoscaling `postgres`/`postgres_role` = Beta (exclude); Data API = GA with manual UI enable (two-phase per §4).
- **Resolved:** `synced-database-table` is Public Preview on the `database` surface (via `databricks database create-synced-database-table`) → usable under the gate.
- **Resolved:** Lakebase is **autoscaling-only** — "Provisioned" was the legacy type and has been fully replaced; there is no provisioned option on the GA surface, so "always autoscaling" is satisfied by construction. `--capacity` = SKU/size (not a provisioned toggle).
- **Only remaining unknown:** stability of `databricks bundle deploy` invoked from inside a workspace notebook (validate empirically in P0/P1).

## 8. Phased plan → MVP (architect review at P4)
- **P0 — Scaffold:** repo skeleton, `bootstrap/` engine, `module.yaml` schema, `databricks.yml` base, CI green.
- **P1 — Core infra:** `core/lakebase` + `core/security` deploy + teardown via the notebook (idempotent).
- **P2 — Core app + Data API:** `core/admin_app` (DABs) + `core/data_api` with the §2 acceptance test passing.
- **P3 — Canary module:** discovery → deps → deploy → health → teardown, zero notebook edits.
- **P4 — `MODULE_AUTHORING.md` + finalize spec → architect review.**

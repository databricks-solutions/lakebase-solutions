# Architecture

`lakebase-solutions` is a reusable, modular foundation for **Databricks Lakebase
workshops** that Solutions Architects run with customers. It always deploys
Lakebase plus its supporting **core** components, and layers optional **modules**
selected per engagement. See [`../SPEC_lakebase-solutions.md`](../SPEC_lakebase-solutions.md)
for the authoritative spec.

## Core vs. module

| | **core** | **module** |
|---|---|---|
| When | Always deployed | Selected per engagement |
| Examples | `lakebase`, `security`, `user_management`, `data_api`, `admin_app` | `_canary` (+ future workshop modules) |
| Location | `core/<name>/` | `modules/<name>/` |
| App | Only `admin_app` (the single always-on app) | Each module ships its **own** separate Databricks App |
| Selection | Implicit (all core) | `modules` multiselect in `deploy.py` |

Every deployable unit ships a `module.yaml` manifest. **Adding a module is a
matter of dropping a folder with a manifest — the deploy notebook never
changes** (see [`MODULE_AUTHORING.md`](MODULE_AUTHORING.md)).

## The orchestrator engine (`bootstrap/`)

```
deploy.py  (notebook / control plane)
    │  collects widget params → DeployContext
    ▼
bootstrap.orchestrator.run(mode, selected_modules, ctx)
    │
    ├─ discovery.discover(root)      scan core/*/module.yaml + modules/*/module.yaml
    ├─ manifest.validate_manifest    schema + semantic checks
    ├─ orchestrator.select_components all core + selected/default modules (+ transitive module deps)
    ├─ dag.topological_order         dependency order; core-before-modules; cycle detection
    └─ iterate                       deploy (+ health) forward; teardown in reverse
```

| Module | Responsibility |
|---|---|
| `manifest.py` | `module.yaml` schema (pydantic v2): `name, version, kind, personas, enabled_by_default, depends_on{core,modules}, parameters, provides, entrypoint, teardown, health_check`, plus the `data_api` `two_phase` note. Each `parameter` carries `type, default, required, advanced, label, help` — the three tiers (required / optional / advanced) the notebook renders. `load_manifest` + `validate_manifest` raise on bad input. |
| `discovery.py` | `discover(root)` scans `core/` (always) + `modules/`. No hard-coded lists. |
| `dag.py` | Topological sort (Kahn's, deterministic). Enforces dependency order, core-before-modules, cycle + missing-dependency detection, and "a core may not depend on a module". |
| `context.py` | `DeployContext`: `deployment_id`/prefix, mode, cloud/region, params, resolved names, logger, and a **lazy** `get_workspace_client()` (no live calls until asked). |
| `orchestrator.py` | `run(...)`: the full control flow. Per-step execution dispatches to each component's `deploy`/`teardown`/`health_check` function. |

## DABs / SDK split

**DABs-first across the whole project** (SPEC section 4). Every resource DABs can
manage at PP/GA is a bundle resource in `databricks.yml`; SDK/REST is reserved
for what DABs cannot do.

| Concern | Mechanism |
|---|---|
| Lakebase instance | `database_instance` DABs resource (**GA**), autoscaling-only (`capacity` = SKU) |
| Secret scope | `secret_scope` DABs resource |
| Admin app | `app` DABs resource (`source_code_path: ./core/admin_app`) |
| **PG roles / grants** | **`CREATE ROLE` SQL over psycopg** — deliberately NOT the Beta `postgres_role` resource |
| Data API SP / role / RLS | SDK/REST + SQL (no PP/GA bundle resource) |
| Service principals, groups | SDK |

**Feature-maturity gate:** only Public Preview or GA. The Beta
`postgres`/`postgres_role`/autoscaling API and Lakebase Search are excluded.

## Data API: two-phase (manual enable)

The Lakebase Data API (managed PostgREST) has no PP/GA programmatic enable, so
`core/data_api` is **two-phase**:

1. **Manual (phase 1):** a human enables the Data API in the Lakebase UI and
   exposes the target schema. `deploy.py` prints a loud, explicit instruction.
2. **Re-runnable (phase 2):** configure the dedicated non-owner SP, register the
   `databricks_auth` role, and apply RLS. Safe to re-run after the manual enable.

Key gotcha (from FSM): the instance **owner cannot use the Data API** (PGRST301);
a dedicated non-owner role is mandatory.

## Deploy order (current)

```
lakebase → security → data_api → user_management → admin_app → [_canary]
```

(`data_api` and `user_management` are independent; ties break deterministically
by name. `admin_app` follows `user_management`; modules always follow core.)
Teardown runs this order in reverse.

## Refactor mapping: FSM / admin → lakebase-solutions

| Source | lakebase-solutions | Notes |
|---|---|---|
| `lakebase_fsm` deploy harness (`notebooks/deploy_all.py`, 15+ ordered steps) | `deploy.py` + `bootstrap/` engine | Ordered, hard-coded steps → manifest-driven discovery + DAG. Notebook is a thin human layer. |
| FSM Lakebase provisioning (Autoscaling `postgres` API, **Beta**) | `core/lakebase` via `database_instance` (**GA**) | FSM's Beta provisioning is *reference only*; its schema/roles/features SQL ports directly. |
| FSM security/permissions (`03_setup_permissions.py`, `10b_setup_secrets.py`) | `core/security` | PG roles via `CREATE ROLE` SQL; standalone secret scope per deployment. |
| FSM Data API (`data_api.py`, `setup_data_api_sp.py`, `data_api_demo.sql`) | `core/data_api` | Dedicated-SP + `databricks_auth` + RLS; two-phase manual enable. |
| `lakebase_admin` (standalone Flask DBA console) | `core/admin_app` | Fork harvested in P2; already generic + multi-instance OBO auth. |
| FSM persona blueprints (dispatch/map/whatif/…) | future `modules/*` | Not core; become optional workshop modules. |

## Phased plan

- **P0 (this scaffold):** engine + manifest schema + `databricks.yml` base + CI green. Per-step logic stubbed.
- **P1:** `core/lakebase` + `core/security` deploy/teardown (idempotent).
- **P2:** `core/admin_app` (DABs) + `core/data_api` (acceptance test passing).
- **P3:** `_canary` module: discovery → deps → deploy → health → teardown, zero notebook edits.
- **P4:** finalize `MODULE_AUTHORING.md` + spec → architect review.

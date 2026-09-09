# CLAUDE.md — lakebase-solutions

Guidance for AI agents and contributors in this repo. Read this first, then the linked docs.

## What this is
A modular, **DABs-first** foundation for Databricks **Lakebase workshops**: always-on `core/` components + optional `modules/`, deployed by a single parameterized notebook. Full detail in [`SPEC_lakebase-solutions.md`](SPEC_lakebase-solutions.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Non-negotiables (do not violate)
- **PP/GA only** — no Private Preview / Beta features. Lakebase uses the **GA `database_instance`** surface, NOT the Beta `postgres`/Autoscaling API. Create PG databases/roles via **`CREATE ROLE` / SQL**, not the Beta `postgres_role` bundle resource.
- **DABs-first** — every resource DABs can manage lives in `databricks.yml`; SDK/REST only for what DABs can't do at PP/GA.
- **No local execution against Databricks** — deploy is workspace-run: commit → push → `databricks repos update <ID> --branch main` → run the deploy notebook. See [`DEPLOYMENT.md`](DEPLOYMENT.md).
- **Standalone assets** — each deployment/module gets its own secret scope, PG roles, and credentials; never reuse across apps. **No secrets in git.**
- **Autoscaling-only** Lakebase — `capacity` is the SKU/size; there is no "provisioned" option.
- **Data API is two-phase** — a manual UI enable step, then a re-runnable configure step (SP + `databricks_auth` role + RLS).

## Adding a module
Drop a folder under `modules/` with a `module.yaml`; discovery + the dependency DAG pick it up — **no deploy-notebook edits**. See [`docs/MODULE_AUTHORING.md`](docs/MODULE_AUTHORING.md).

## Before you push
GitHub Actions is **disabled org-wide** on this repo, so tests gate **locally**: run **`make check`** (see [`CONTRIBUTING.md`](CONTRIBUTING.md)). The Databricks secret-scan git hooks run on every commit/push.

## Layout
- `bootstrap/` — orchestrator: manifest schema, discovery, dependency DAG
- `core/` — always-on components (`lakebase`, `security`, `user_management`, `data_api`, `admin_app`)
- `modules/_canary/` — reference module that proves the contract
- `deploy.py` — the single deploy/teardown notebook (widget-driven)
- `tools/` — standalone utilities (e.g., Data API connectivity test); separate from the harness core
- `docs/`, `SPEC_lakebase-solutions.md`, `DEPLOYMENT.md`, `CONTRIBUTING.md`

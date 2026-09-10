# CLAUDE.md — lakebase-solutions

Guidance for AI agents and contributors in this repo. Read this first, then the linked docs.

## What this is
A modular, **DABs-first** foundation for Databricks **Lakebase workshops**: always-on `core/` components + optional `modules/`, deployed by a single parameterized notebook. Full detail in [`SPEC_lakebase-solutions.md`](SPEC_lakebase-solutions.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Non-negotiables (do not violate)
- **Autoscaling-only Lakebase** — provision the **autoscaling `postgres` projects → branches → endpoints** surface (min/max CU, scale-to-zero), NOT the legacy provisioned `database_instance` tier. This is the GA product's primary surface; its CLI group wears a `*Beta*` label (tooling maturity, not a preview *feature*) and is the only path to autoscaling, so it is accepted. Create PG databases/roles via **`CREATE ROLE` / SQL**, not a `postgres_role` bundle resource.
- **PP/GA only** — no Private Preview / Beta *features*.
- **SDK/REST provisioning in-workspace, DABs for local/CI validate** — the `databricks` CLI (incl. `bundle deploy`) cannot run on notebook/job compute, so the deploy notebook provisions via the Databricks Python SDK / REST (`w.api_client.do` for the `postgres` + `apps` surfaces, which are not typed on every runtime SDK). `databricks.yml` is kept for local/CI `bundle validate` only.
- **No local execution against Databricks** — deploy is workspace-run: commit → push → `databricks repos update <ID> --branch main` → run the deploy notebook. See [`DEPLOYMENT.md`](DEPLOYMENT.md).
- **Standalone assets** — each deployment/module gets its own secret scope, PG roles, and credentials; never reuse across apps. **No secrets in git.**
- **Data API is two-phase** — a manual UI enable step, then a re-runnable configure step (dedicated service principal + `databricks_auth` role + grants + schema-cache refresh). Configure detects whether the API is enabled and stops short with loud instructions until it is.

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

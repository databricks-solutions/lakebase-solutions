# lakebase-solutions

A reusable, **modular foundation for Databricks Lakebase workshops** that
Solutions Architects run with their customers. It always deploys Lakebase and
its supporting **core** components, then layers optional **modules** chosen per
engagement (by problem area or persona). Deployment is immutable/repeatable and
driven by a **single parameterized notebook** — and **adding a module never
requires editing that notebook**.

> Status: **P0 scaffold.** The orchestrator engine, manifest schema, DABs base,
> and CI are real and tested. Per-component deploy logic is stubbed (logs intent)
> and lands in later phases. See [`SPEC_lakebase-solutions.md`](SPEC_lakebase-solutions.md).

## What it is

- **Core (always deployed):** `lakebase`, `security`, `user_management`,
  `data_api`, `admin_app` (the single always-on app — a Lakebase DBA console).
- **Modules (optional):** each in `modules/<name>/`, with its **own** Databricks
  App and resources. `modules/_canary/` is the reference module.
- **Control plane:** `deploy.py` (a Databricks notebook) collects parameters and
  calls the `bootstrap/` engine, which discovers components/modules from
  `module.yaml` manifests, orders them by dependency, and deploys or tears down.

## Quickstart (shape)

Deployment runs **inside Databricks** (commit → push → pull → run); there is no
laptop CLI execution.

1. `cp config.template.yaml config.yaml` and set advanced params (optional).
2. Open `deploy.py` in the workspace. Set widgets:
   - **required:** `deployment_id` (prefix that namespaces everything)
   - **optional:** `mode` (`deploy`/`teardown`), `cloud`, `region`,
     `autoscaling_min_cu`, `autoscaling_max_cu`, `admin_group`,
     `workshop_group`, `enable_data_api`, `modules`
3. Run. The notebook discovers core + selected modules, orders them, and
   deploys. `mode: teardown` removes everything in reverse.
4. **Data API is two-phase:** the notebook prints a loud manual UI-enable
   instruction; re-run afterward to configure the SP/role/RLS.

## Repository structure

```
bootstrap/            orchestrator engine (discovery, manifest schema, DAG, context, run)
core/                 always-on components, one dir each (module.yaml + deploy/teardown/health)
  lakebase/  security/  user_management/  data_api/  admin_app/
modules/              optional workshop modules
  _canary/            reference module proving the authoring contract
deploy.py             single control-plane notebook (dbutils-guarded; importable off-Databricks)
databricks.yml        DABs bundle (autoscaling postgres_project/endpoint, secret_scope, app)
config.template.yaml  copy to config.yaml for advanced params
tests/                pytest suite (manifests, DAG, orchestrator, notebook import) — no workspace
docs/                 ARCHITECTURE.md, MODULE_AUTHORING.md
.github/workflows/    CI (gitleaks secret scan + pytest)
```

## Key design decisions

- **DABs-first.** Lakebase via the **autoscaling** `postgres_project` /
  `postgres_endpoint` bundle resources (min/max CU + scale-to-zero), NOT the
  provisioned `database_instance` tier. **No `postgres_role` resource** — PG
  roles/grants are created via `CREATE ROLE` SQL. See
  [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
- **Manifest-driven discovery.** The notebook never changes when a module is
  added; modules are found by scanning for `module.yaml`.
- **Standalone assets.** Every module gets its own app, PG roles, and secret
  keys — nothing reused across apps.

## Develop

```bash
pip install -r requirements-dev.txt
pytest -q          # pure-python; no Databricks workspace needed
```

To author a module, see [`docs/MODULE_AUTHORING.md`](docs/MODULE_AUTHORING.md)
and copy `modules/_canary/`.

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open
a GitHub issue and the team will help on a best effort basis.

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is
provided subject to the Databricks License [https://databricks.com/db-license-source].
All included or referenced third party libraries are subject to the licenses set
forth below.

| library | description | license | source |
|---------|-------------|---------|--------|
| pydantic | Data validation | MIT | https://github.com/pydantic/pydantic |
| PyYAML | YAML parser | MIT | https://github.com/yaml/pyyaml |
| pytest | Test framework (dev) | MIT | https://github.com/pytest-dev/pytest |

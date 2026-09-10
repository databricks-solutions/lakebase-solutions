# Authoring a module

Adding a workshop module is **drop a folder + a manifest**. You never edit
`deploy.py` or `databricks.yml`'s include logic — the orchestrator discovers your
module from its `module.yaml` and orders it by its declared dependencies.

The reference implementation is [`../modules/_canary/`](../modules/_canary/) —
copy it as your starting point.

## 1. Create the folder

```
modules/<your_module>/
├── module.yaml     # the manifest (contract)
├── deploy.py       # def deploy(ctx) -> dict
├── teardown.py     # def teardown(ctx) -> dict
├── health.py       # def health_check(ctx) -> dict
└── README.md       # responsibility, deps, provides
```

## 2. Write `module.yaml`

```yaml
name: my_module            # slug: [a-z_][a-z0-9_-]*  (unique across the repo)
version: 0.1.0
kind: module               # 'module' (optional) vs 'core' (always-on)
personas: [app_developer]  # who the module serves
enabled_by_default: false  # true pre-selects it in the modules multiselect
depends_on:
  core: [lakebase, security]   # core components you need (deploy before you)
  modules: []                  # other modules you build on (pulled in automatically)
parameters:
  - name: my_setting
    type: string             # string | int | float | bool | multiselect
    default: some_value
    required: false          # see "Parameter tiers" below
    advanced: false
    label: My setting        # friendly widget name (falls back to `name`)
    help: What this controls and when to change it.
provides:                    # resources you create (for teardown / as-built)
  pg_schemas: ["my_schema"]
  app: ["${prefix}-my-module-app"]   # each module ships its OWN app
entrypoint: deploy.py
teardown: teardown.py
health_check: health.py
```

### Parameter tiers (`required` / `advanced` / `label` / `help`)

Parameters render in the deploy notebook in three tiers:

| Flags | Tier | Notebook behavior |
|---|---|---|
| `required: true` | **required** | Prominent widget with **no default**; run fails fast if left blank. |
| neither flag | **optional** | Widget carrying `default`, labelled `"<label> (optional · default: <default>)"`. |
| `advanced: true` | **advanced** | **No widget.** Read only from `config.yaml` (document it in `config.template.yaml`). |

- `label` is the human display string (keep it short; the widget name is
  generated). `help` is the longer explanation shown in the notebook's parameter
  summary table.
- Use **required** for values with no safe default (rare). Use **optional** for
  the common knobs. Use **advanced** for rarely-changed or sensitive settings.

## 3. Implement the steps

Each step is a plain function taking the shared `DeployContext` (`ctx`) and
returning a small dict. Use `ctx.logger`, `ctx.name("suffix")` for
prefix-namespaced resource names, `ctx.params[...]` for parameter values, and
`ctx.get_workspace_client()` for the Databricks SDK (lazy; no live call until
used).

```python
# deploy.py
def deploy(ctx):
    schema = ctx.params.get("my_setting", "my_schema")
    ctx.logger.info("creating %s for deployment %s", schema, ctx.deployment_id)
    # ... create resources (DABs vars + bundle deploy, SDK, or CREATE ... SQL) ...
    return {"schema": schema, "status": "ok"}
```

```python
# teardown.py
def teardown(ctx):
    # remove exactly what deploy created (runs in REVERSE dependency order)
    return {"status": "ok"}
```

```python
# health.py
def health_check(ctx):
    # cheap check the deploy actually worked (e.g. SELECT from your table)
    return {"healthy": True, "status": "ok"}
```

### Conventions

- **DABs-first.** Express every resource DABs can manage at PP/GA as a bundle
  resource. Use the SDK/REST only for what DABs cannot do.
- **PG roles/grants via `CREATE ROLE` SQL** — do **not** use the Beta
  `postgres_role` resource.
- **Standalone assets.** Your module gets its **own** app, PG roles, and secret
  keys — never reuse another module's. Namespace everything with the prefix.
- **PP/GA only.** No Private Preview / Beta features.
- **Idempotent.** Deploy and teardown must be safe to re-run.

## 4. Verify

The manifest and DAG tests pick your module up automatically:

```bash
make check
```

`test_manifests.py` validates your `module.yaml` against the schema and confirms
the entrypoint/teardown/health files exist; `test_dag.py` confirms your module
orders correctly behind its dependencies. No workspace is required.

## 5. Deploy

In `deploy.py`'s **modules** multiselect (populated from discovery), select your
module and run. It deploys after its `depends_on` are satisfied — with **no
notebook edits**.

# Deployment

`lakebase-solutions` follows an **immutable, workspace-run** deployment model.
You never execute against Databricks/Lakebase from your laptop. You commit,
push, pull into the workspace, and run the single deploy notebook there.

## Model

```
local edit → git commit → git push (main)
                               │
                               ▼
workspace: databricks repos update <REPO_ID> --branch main      (pull)
                               │
                               ▼
workspace: run deploy.py   (mode = deploy | teardown)           (run)
     └─ orchestrates:  databricks bundle deploy  +  SDK/SQL steps
```

Nothing is deployed from a local machine. Every ad-hoc fix goes back into the
repo scripts and through this same flow — never hand-patched in the workspace.

## One-time setup (per workspace / deployment)

1. Add this repo as a workspace **Git folder** and note its `<REPO_ID>`
   (`databricks repos list --profile <PROFILE>`).
2. Ensure the deploying identity can create Lakebase instances, secret scopes,
   service principals, and apps, and can grant UC/PG permissions.

## Deploy

1. **Locally:** commit + push your changes to `main`.
2. **In the workspace, sync:**
   ```
   databricks repos update <REPO_ID> --branch main --profile <PROFILE>
   ```
3. Open **`deploy.py`** in the workspace and **Run All**. Widgets:
   - `deployment_id` *(required)* — namespaces every resource
   - `mode` = `deploy`
   - module multiselect — pick the modules for this engagement
   - optional widgets (`autoscaling_min_cu`/`autoscaling_max_cu`, groups, …)
     carry sensible defaults; advanced values live in `config.yaml`
4. The notebook runs `databricks bundle deploy` for bundle-managed resources
   (Lakebase autoscaling `postgres_project`/`postgres_endpoint`, `secret_scope`,
   admin `app`) and SDK/SQL steps
   for the rest (PG roles via `CREATE ROLE`, grants, Genie, …), in dependency
   order (**core → modules**).

## Data API (two-phase — manual enable required)

The Lakebase Data API is **enabled from the UI only**. The notebook will:

1. Print a prominent instruction: **"You MUST enable the Data API manually"**
   (Lakebase project → Data API → expose the schema → refresh cache).
2. After you enable it, **re-run the notebook** (or just the `data_api` step) to
   configure the dedicated service principal, `databricks_auth` role
   registration, RLS, and secrets.

## Teardown

Run `deploy.py` with `mode = teardown`. It tears down in **reverse** dependency
order (modules → core): `databricks bundle destroy` for bundle-managed resources
plus SDK/SQL teardown for the rest.

## Operational notes

- **`config.yaml` is gitignored** (copied from `config.template.yaml`); it holds
  per-deployment names/IDs and is never committed.
- **App runtime config / secrets are not in git.** Credentials live in the
  deployment's own secret scope (referenced by `app.yaml` via `valueFrom`),
  never hard-coded.
- **Re-running is safe:** steps are idempotent (create-if-not-exists / bundle
  convergence). A clean rebuild is teardown then deploy.


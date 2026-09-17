# AGENTS.md — lakebase-solutions

**Canonical guidance for AI agents (Claude Code, Cursor, others) and human
contributors.** Read this first, then the linked docs. `CLAUDE.md` points here.

## What this is
A modular foundation for Databricks **Lakebase workshops**: always-on `core/`
components + optional `modules/`, deployed by a **single parameterized notebook**
(`deploy.py`). Provisioning is **in-workspace via the Databricks SDK / REST + SQL**
— not DABs. Full detail in [`docs/SPEC_lakebase-solutions.md`](docs/SPEC_lakebase-solutions.md)
and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Branching & pull requests — DO NOT COMMIT TO `main`
**`main` is protected. Work on your own branch and open a Pull Request to merge.**
Direct commits/pushes to `main` are restricted to specific maintainers — currently:
- `chase.marler@databricks.com`

If you are **not** on that allowlist (this includes every AI coding agent unless it
is acting as an allowlisted maintainer), you must **never** commit or push to `main`.

**Required flow for everyone else (and their agents):**
1. Branch off `main`: `git switch -c <your-name>/<short-topic>`.
2. Commit your work on that branch; push the branch (`git push -u origin <branch>`).
3. Open a **Pull Request** into `main` and let it be reviewed + merged.

**Agent checklist before ANY commit:** run `git config user.email`. If it is not on
the maintainer allowlist above, create/checkout a feature branch first — do **not**
`git commit` on `main`, and **never** `git push origin main`. Branch protection also
rejects direct pushes server-side, so a direct push will fail; branching is the
supported path. As always, **never `--no-verify`** (the secret-scan hooks must run).

Unreviewed commits landing straight on `main` cause unstable deploys — the whole
point of this rule.

## Non-negotiables (do not violate)
- **Autoscaling-only Lakebase** — provision the **autoscaling `postgres` projects →
  branches → endpoints** surface (min/max CU, scale-to-zero), NOT the legacy
  provisioned `database_instance` tier. Its CLI group wears a `*Beta*` label
  (tooling maturity, not a preview *feature*) and is the only path to autoscaling,
  so it is accepted. Create PG databases/roles via **`CREATE ROLE` / SQL**, not a
  `postgres_role` bundle resource.
- **PP/GA features only** — no Private Preview / Beta *features* ship. Preview-or-
  better is allowed **only if** its maturity is declared (see Maturity gate below).
- **SDK/REST provisioning in-workspace; DABs = validate only** — the `databricks`
  CLI (incl. `bundle deploy`) **cannot run on notebook/job compute**. The deploy
  notebook provisions via the Python SDK / REST. `databricks.yml` is kept for
  local/CI `bundle validate` only.
- **No local execution against Databricks** — deploy is workspace-run:
  commit → push → `databricks repos update <ID> --branch main` → run `deploy.py`.
  See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
- **Per-app credentials — no shared `pguser`/`pgpassword`** — each app gets its
  **own** native-password Postgres role and its **own** secret keys; never a shared
  role. Admin console → role `<id>_app`, keys `admin_app-pguser`/`admin_app-pgpassword`.
  Field-service app → role `<id>_fs_app`, keys `field_service-pguser`/`field_service-pgpassword`,
  provisioned **first** (in the module's DATA step) so its jobs/notebooks have creds.
  Only the **connection-info** keys `pghost`/`pgdatabase`/`pgschema` are shared
  (they are not credentials). The shared role helper lives at **`bootstrap/roles.py`**
  — `core/` is loaded flat by file path (not an importable package), so shared role
  code goes in `bootstrap/`, not `core/`. Each deployment/module still gets its own
  secret scope; **no secrets in git.**
- **Data API is two-phase** — a manual UI enable step, then a re-runnable configure
  step (dedicated service principal + `databricks_auth` role + grants + schema-cache
  refresh). Configure detects whether the API is enabled and stops with loud
  instructions until it is.

## Landmines (hard-won — read before you touch provisioning)
- **The runtime job SDK lacks typed services** for `postgres`, `apps`, and
  `service_principal_secrets_proxy`. Drive those via **`w.api_client.do(method, path,
  body=, query=)`** (REST), not `w.postgres` / `w.apps`. `w.service_principals`,
  `w.secrets`, `w.current_user` DO exist.
- **`postgres create-project` is a long-running operation** — POST returns an op to
  poll; the project/endpoint materializes ~20s later. Reusing a **just-deleted**
  `deployment_id` hits a **post-deletion cooldown** — use a FRESH id.
- **Managed online catalogs register Lakebase tables as foreign tables LAZILY** (on
  first query). Trigger + poll a `SELECT` per table before anything validates them.
- **Identity** = the workspace email (`w.current_user.me().user_name`), not a token
  `sub` claim. `ALTER ROLE ... PASSWORD` is DDL and can't bind params — inline a
  quoted literal (redact in logs).

## Lakebase admin & DBA governance
The **admin console** (`core/admin_app`) is a **fleet DBA tool**, not a per-deploy UI.
It connects to **every** Lakebase instance in the workspace **on-behalf-of the
signed-in admin** (their forwarded token) — introspecting **all** user schemas with a
database selector; the ASH sampler only writes on its home instance. This requires the
`postgres` `user_api_scope`, which the app declares. **Adding an OBO scope to an
existing app does NOT re-prompt consent** — a viewer must force a fresh consent
(incognito / new browser) to pick up the new `postgres` scope.

**Two permission planes — keep them separate.** Databricks project ACLs
(`CAN_USE`/`CAN_MANAGE`) are NOT Postgres privileges: a workspace/project admin has
**no** Postgres grants automatically. Postgres access comes from role membership.
- The instance **creator/owner** is auto-added to `DATABRICKS_SUPERUSER` for that
  project, so on instances you created your OBO identity already has read/write/monitor
  and the console sees everything with no extra grant.
- Raw SQL `GRANT databricks_superuser` **does not work** (needs in-DB ADMIN OPTION;
  only the control-plane `cloud_admin` holds it). The **sanctioned** path is the
  **Lakebase Roles API**, run control-plane-side for a **workspace admin / `CAN_MANAGE`**
  holder (workspace admins get `CAN_MANAGE` on all workspace projects by default):
  ```
  databricks postgres create-role projects/<id>/branches/production \
    --role-id <slug> --replace-existing \
    --json '{"spec":{"identity_type":"USER"|"SERVICE_PRINCIPAL","postgres_role":"<email|sp-id>","auth_method":"LAKEBASE_OAUTH_V1","membership_roles":["DATABRICKS_SUPERUSER"]}}'
  ```
  `role_id` must match `^[a-z][a-z0-9-]{0,61}[a-z0-9]?$` — a slug, **not** the email.
- The console's **"Elevate me here"** action (`POST /api/admin/elevate` → Roles API)
  runs exactly this for the operator on the selected instance, gated by the admin group
  **and** the Roles API's own `CAN_MANAGE` check.

**Governance framing:** you may not be able to *prevent* instance creation (a separate
workspace entitlement), but a workspace admin can **supersede** any instance after
creation via the Roles API — automatable as a fleet sweep, with a central DBA
**service principal** (itself a workspace admin) as the persistent identity. Caveat:
`DATABRICKS_SUPERUSER` is broad but **not** a full unmanaged-cluster superuser — a few
control-plane cleanup ops still require Databricks `cloud_admin`/support.

## Deploy / teardown workflow
1. Land your change on the target branch: for `main`, via a **merged PR** (see
   Branching above); for iterating on your own branch, push that branch.
2. `databricks repos update <REPO_ID> --branch <branch> -p <PROFILE>` (syncs the
   workspace Git folder to the branch you're deploying)
3. `databricks jobs submit --json '{... notebook_task on .../deploy ...}' --no-wait`
   with base params `1_deployment_id`, `modules`, and `2_mode` (`deploy`/`teardown`).
4. The notebook returns **per-step results** via `dbutils.notebook.exit(json)` —
   inspect with `jobs get-run-output <task_run_id>` → `notebook_output.result`
   (status + any deferred error per component; **key names only, no secret values**).

## Before you push
- GitHub **Actions is disabled org-wide** on this repo → tests gate **locally**:
  run **`make check`** (see [`CONTRIBUTING.md`](CONTRIBUTING.md)).
- The Databricks **secret-scan git hooks** run on every commit/push (global
  `core.hooksPath`) — **do not `--no-verify`**; fix the finding.
- **Offline test runner:** the suite runs with no workspace — pure Python. Live
  steps guard behind `ctx.is_live()` and return a `stub` result off-Databricks;
  tests inject fakes (`tests/_fakes.py`) for every REST surface.

## Adding a module
Drop a folder under `modules/` with a `module.yaml`; discovery + the dependency DAG
pick it up — **no deploy-notebook edits**. Each module owns its app + assets. Then
**add a row to [`modules/README.md`](modules/README.md)** (the inventory the top-level
README links to). See [`docs/MODULE_AUTHORING.md`](docs/MODULE_AUTHORING.md) and copy
`modules/_canary/` (the minimal reference). Large real example + its own guide:
[`modules/field_service/AGENTS.md`](modules/field_service/AGENTS.md).

## Maturity gate (transparency, not GA-only)
Each `core/`+`module/` `module.yaml` declares `features: [{name, maturity:
GA|PUBLIC_PREVIEW|BETA, note}]`; `bootstrap/features.py` aggregates a customer-facing
**feature matrix** so it's always clear what isn't GA.

## Layout
- `bootstrap/` — orchestrator: manifest schema, discovery, dependency DAG, context
- `core/` — always-on components (`lakebase`, `security`, `user_management`,
  `data_api`, `admin_app`)
- `modules/_canary/` — reference module that proves the contract
- `modules/field_service/` — full field-service solution (see its `AGENTS.md`)
- `deploy.py` — the single deploy/teardown notebook (widget-driven)
- `tools/` — standalone utilities (separate from the harness core)
- `docs/` — `ARCHITECTURE.md`, `MODULE_AUTHORING.md`, `DEPLOYMENT.md`, `SPEC_lakebase-solutions.md`
- `CONTRIBUTING.md`

# core/admin_app

**Responsibility:** The always-on **admin app** -- a Lakebase DBA console. This is
the **only core app**; every module ships its own separate Databricks App (SPEC
section 4).

- Deployed via the GA `app` DABs resource; `databricks.yml` points its
  `source_code_path` at this directory.
- Runtime config in `app.yaml`; entry point `app.py`.
- The real console is a fork of **`lakebase_admin`** (Flask + psycopg v3:
  instance introspection, schema explorer, live ASH dashboard, VACUUM/REINDEX,
  backup/restore). Harvested in **P2** -- P0 ships a placeholder `app.py`.

**Depends on:** `lakebase`, `security`, `user_management`.

**Provides:** `app` (`${prefix}-admin-app`).

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup. All P0 stubs.

> Note: `deploy.py` / `teardown.py` / `health.py` here are orchestrator step
> stubs (control-plane), distinct from the app's own `app.py` runtime.

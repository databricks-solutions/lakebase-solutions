# core/admin_app

**Responsibility:** The always-on **admin app** — a Lakebase DBA console. This is
the **only core app**; every module ships its own separate Databricks App.

- Deployed via the GA `app` DABs resource; `databricks.yml` points its
  `source_code_path` at this directory.
- Runtime config in `app.yaml`; entry point `app.py`.
- A Flask + psycopg v3 console covering instance introspection, schema explorer,
  live ASH dashboard, VACUUM/REINDEX, and backup/restore.

**Depends on:** `lakebase`, `security`, `user_management`.

**Provides:** `app` (`${prefix}-admin-app`).

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup.

> Note: `deploy.py` / `teardown.py` / `health.py` here are orchestrator step
> scripts (control-plane), distinct from the app's own `app.py` runtime.

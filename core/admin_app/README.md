# core/admin_app

**Responsibility:** The always-on **admin app** — a Lakebase DBA console. This is
the **only core app**; every module ships its own separate Databricks App.

A server-rendered Flask + vanilla-JS console (psycopg v3) for operating the
deployment's Lakebase Postgres:

- **Instance introspection** — connection info, roles, triggers, materialized
  views, database and table sizes.
- **Schema explorer** — tables, columns, foreign keys, and row estimates for the
  administered schema.
- **Live query dashboard** — real-time session monitoring with ASH sampling,
  blocking-tree/lock summary, and `pg_stat_statements` query analysis.
- **Maintenance** — `VACUUM ANALYZE` with before/after bloat, `REINDEX
  CONCURRENTLY`, table-bloat estimates, and slow-query analysis.
- **Backup & restore** — SQL backup of the administered schema (optionally
  persisted to a UC Volume), download, list, and restore into a new schema.
- **Cluster status** — PG settings plus autoscaling endpoint state and compute
  limits.

## How it connects

The default connection uses native PG auth. `app.yaml` reads `pghost`,
`pgdatabase`, `pguser`, and `pgpassword` from this deployment's standalone secret
scope via `valueFrom` (written by `core/lakebase`); the host is the autoscaling
`primary` endpoint and the driver requires TLS (`sslmode=require`). The console
also supports selecting other Lakebase instances in the workspace, connecting to
those with short-lived OAuth database credentials minted on behalf of the
signed-in user.

- Deployed via the GA `app` DABs resource; `databricks.yml` points its
  `source_code_path` at this directory.
- Runtime config in `app.yaml`; entry point `app.py`. Access is gated on
  Databricks group membership (`ADMIN_GROUP`, plus the workspace `admins` group).

**Depends on:** `lakebase`, `security`, `user_management`.

**Provides:** `app` (`${prefix}-admin-app`).

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup.

> Note: `deploy.py` / `teardown.py` / `health.py` here are orchestrator step
> scripts (control-plane), distinct from the app's own `app.py` runtime.

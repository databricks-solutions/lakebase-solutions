# core/lakebase

**Responsibility:** Provision and manage the Lakebase Postgres instance -- the
foundation every other component and module depends on.

- Creates the instance via the GA **`database_instance`** DABs resource
  (autoscaling-only; `capacity` is the SKU/size, there is no provisioned option).
- Owns the base database (`databricks_postgres`) bootstrap.
- Does **not** use the Beta `postgres`/`postgres_role` surface (SPEC section 4).

**Depends on:** nothing (deploys first; tears down last).

**Provides:** `database_instance`, base `database`.

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup. All P0 stubs.

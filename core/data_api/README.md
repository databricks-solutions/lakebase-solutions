# core/data_api

**Responsibility:** Configure the Lakebase **Data API** (managed PostgREST) so an
authenticated REST call returns RLS-scoped rows via a dedicated non-owner role.

## Two-phase setup (SPEC section 4)

1. **Manual (phase 1):** a human enables the Data API in the Lakebase UI and
   exposes the target schema. `deploy.py` prints a loud, explicit instruction --
   there is no PP/GA programmatic enable yet.
2. **Re-runnable (phase 2):** configure the dedicated service principal, register
   the `databricks_auth` role (`GRANT ... TO authenticator`), and apply RLS. Safe
   to re-run after the manual enable.

**Key gotcha (from FSM):** the instance **owner cannot use the Data API**
(PGRST301). A dedicated non-owner role is mandatory.

**Depends on:** `lakebase`, `security`.

**Provides:** Data API `service_principals`, the `databricks_auth` `pg_role`.

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup. All P0 stubs.

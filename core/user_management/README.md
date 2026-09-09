# core/user_management

**Responsibility:** Workshop identity plumbing -- the admin and participant
Databricks groups, and the Postgres roles that scope each attendee's DB access.

- Ensures `${prefix}-admins` and `${prefix}-participants` Databricks groups.
- Maps participants to Postgres roles via **`CREATE ROLE` SQL** (not the Beta
  `postgres_role` resource).

**Depends on:** `lakebase`, `security`.

**Provides:** `databricks_groups`, participant `pg_roles`.

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup. All P0 stubs.

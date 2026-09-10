# core/security

**Responsibility:** The deployment's security posture -- standalone secret scope
and credentials, service principal(s), and Postgres roles/grants.

- Secret scope + keys (PG user/password, Data API SP client id/secret) — **own
  scope per deployment, never reused**.
- App/Data API service principal(s).
- PG roles and grants created via **`CREATE ROLE` SQL over psycopg**, deliberately
  avoiding the Beta `postgres_role` bundle resource.

**Depends on:** `lakebase`.

**Provides:** `secret_scope`, `pg_roles`, `service_principals`.

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup.

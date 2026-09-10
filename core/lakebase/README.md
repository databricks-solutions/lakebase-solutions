# core/lakebase

**Responsibility:** Provision and manage the autoscaling Lakebase `postgres`
project -- the foundation every other component and module depends on.

- Provisions the **autoscaling `postgres` project** (+ its default `production`
  branch and `primary` read-write endpoint) via the `postgres_project` /
  `postgres_endpoint` bundle resources; the endpoint carries the
  `autoscaling_min_cu` / `autoscaling_max_cu` range + scale-to-zero suspend
  timeout. NOT the legacy provisioned `database_instance` tier.
- Creates the workshop **database** explicitly (the endpoint's default
  `postgres` db has a restricted `public` schema), then the workshop **schema**.
- Connects as admin with `(workspace email, OAuth token from
  generate-database-credential)`, `sslmode=require`.
- PG roles/grants are `CREATE ROLE` SQL (in `core/security`), not a bundle
  `postgres_role` resource.

**Depends on:** nothing (deploys first; tears down last).

**Provides:** `postgres_project`, `postgres_endpoint`, workshop `database`.

**Steps:** `deploy.py` -> `health.py`; `teardown.py` for cleanup.

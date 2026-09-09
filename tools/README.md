# tools/

Standalone utilities for Lakebase workshops. These run directly (import into a Databricks
workspace or run with the CLI) and are **independent of the deploy harness** — they don't
participate in manifest discovery or `deploy.py`.

## `lakebase_data_api_test.py`

A Databricks notebook that tests connectivity to the Lakebase **Data API** (the managed
PostgREST layer). Import it into a workspace and run top-to-bottom:

1. Installs dependencies (`databricks-sdk`, `psycopg2-binary`).
2. **Step 1** discovers every Data-API-enabled Lakebase instance you can see and fills an
   `Endpoint` dropdown (instances you own are included, tagged *"(you own)"*).
3. Pick one. If you can use it directly it runs as you; if you **own** it (owners can't use the
   Data API), set `provision = yes` and the notebook **auto-creates a service principal**, wires
   it (`databricks_auth` role + grants + `authenticator` membership), refreshes the PostgREST
   schema cache, and runs the test as that SP — gated so a plain Run All never creates identities.
4. **Steps 2–4** auto-discover a table and query it over REST; a final cell inventories every
   asset created, each with a teardown command.

See the notebook's own **"Important"** cell for the owner-cannot-use-the-Data-API constraint and
the one-time setup it automates.

> **API-surface note.** This tool discovers endpoints and mints credentials via the Lakebase
> **`postgres`** API (Autoscaling *projects/branches/endpoints*). The deploy harness standardizes
> on the GA **`database_instance`** surface (see `ARCHITECTURE.md` / `SPEC_lakebase-solutions.md`).
> Before relying on this in a workshop, confirm it lists instances the harness deploys — the two
> surfaces may enumerate Lakebase differently.

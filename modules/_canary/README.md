# modules/_canary

**Responsibility:** The reference module. It proves the module-authoring contract
end to end: **discovery -> dependency ordering -> deploy -> health -> teardown**,
with **zero edits to the deploy notebook**.

Use it as the copy-paste starting point for a real module (see
`docs/MODULE_AUTHORING.md`).

- Declares `kind: module` and `depends_on.core: [lakebase, security]`, so the
  orchestrator always orders it **after** those core components.
- `deploy.py` creates a `canary` schema + `canary.heartbeat` table via SQL;
  `health.py` runs a `SELECT count(*)`; `teardown.py` runs `DROP SCHEMA ... CASCADE`.
- Not selected by default (`enabled_by_default: false`) — opt in via the modules
  multiselect in `deploy.py`.

**Depends on:** core `lakebase`, `security`.

**Provides:** `pg_schemas` (`canary`), `pg_tables` (`canary.heartbeat`).

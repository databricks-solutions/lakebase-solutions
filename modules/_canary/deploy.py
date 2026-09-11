"""modules/_canary -- the REFERENCE MODULE (real, minimal, heavily annotated).

This is the copy-paste TEMPLATE for authoring a new module. It exercises the
ENTIRE module contract with one tiny real resource, so you can see every
required piece in a single place. Read this top-to-bottom, then read
``module.yaml``, ``teardown.py``, and ``health.py`` -- together they are the
whole contract.

────────────────────────────────────────────────────────────────────────────
Every module is a folder under ``modules/<name>/`` with exactly these files
(their names come from ``module.yaml``'s entrypoint / teardown / health_check):

    module.yaml    the manifest: name, kind, personas, depends_on, parameters,
                   features (maturity), provides, and the 3 step filenames.
    deploy.py      def deploy(ctx)        -> provisions the module's resources
    teardown.py    def teardown(ctx)      -> removes them (runs in reverse order)
    health.py      def health_check(ctx)  -> asserts they actually work

Dropping this folder in is all it takes -- discovery + the dependency DAG pick
it up automatically. **You never edit the deploy notebook to add a module.**

────────────────────────────────────────────────────────────────────────────
The DeployContext (``ctx``) is what every step is handed. It gives you:

    ctx.deployment_id            the prefix that namespaces ALL resources
    ctx.name("suffix")           -> "<deployment_id>-suffix" (namespaced name)
    ctx.params                   merged widget + config.yaml parameter values
    ctx.resolved_names           derived names (secret_scope, pg roles, ...)
    ctx.is_live()                True in-workspace; False in tests/offline
    ctx.has_workspace_client()   True if a Databricks SDK client is available
    ctx.pg_connection(role=,     a psycopg connection (role "admin" = workspace
                      database=) identity; "app"/"readonly" = native PG roles)
    ctx.workspace_client()       the Databricks SDK WorkspaceClient (SDK/REST)
    ctx.logger                   the shared logger

────────────────────────────────────────────────────────────────────────────
Contract rules every step MUST follow:

  1. Guard live work behind ``ctx.is_live()`` (or ``has_workspace_client()``)
     and return a ``{"status": "stub"}`` dict otherwise -- so the offline tests
     and the orchestrator smoke test run with NO network and NO workspace.
  2. Be IDEMPOTENT (create-if-not-exists) -- deploys are immutable/repeatable,
     so a re-run must be safe.
  3. Return a small result dict (status + what happened). Put only names/counts
     in it -- NEVER secret values (results are surfaced in run output).
  4. In-workspace, provision via the Databricks SDK / REST or SQL -- the CLI
     cannot run in job compute.
"""

from __future__ import annotations

from typing import Any, Dict

# Idempotent DDL: safe to run on every deploy (immutable/repeatable contract).
_DDL = [
    'CREATE SCHEMA IF NOT EXISTS "{schema}"',
    'CREATE TABLE IF NOT EXISTS "{schema}".heartbeat ('
    " id bigserial PRIMARY KEY,"
    " beat_at timestamptz NOT NULL DEFAULT now(),"
    " deployment text NOT NULL)",
]


def deploy(ctx: Any) -> Dict[str, Any]:
    # Read this module's parameter (declared in module.yaml). ctx.params merges
    # widget values (in-workspace) or config/defaults (offline).
    schema = ctx.params.get("canary_schema", "canary")

    # RULE 1 -- off-Databricks (unit tests / orchestrator smoke): log intent and
    # return a stub. No connection is attempted, so nothing needs a network.
    if not ctx.is_live():
        ctx.logger.info(
            "[stub] _canary.deploy: would CREATE SCHEMA %s + %s.heartbeat and "
            "insert one row for deployment %r.",
            schema,
            schema,
            ctx.deployment_id,
        )
        return {"schema": schema, "table": f"{schema}.heartbeat", "status": "stub"}

    # LIVE -- open a psycopg connection as the admin (workshop) identity and run
    # the idempotent DDL, then record one heartbeat row proving connectivity.
    conn = ctx.pg_connection(
        role="admin", database=ctx.params.get("database") or "databricks_postgres"
    )
    try:  # DDL runs cleanly on autocommit.
        conn.autocommit = True
    except Exception:  # pragma: no cover - fake/driver without the attribute
        pass
    cur = conn.cursor()
    for stmt in _DDL:
        cur.execute(stmt.format(schema=schema))
    cur.execute(
        f'INSERT INTO "{schema}".heartbeat (deployment) VALUES (%s)', (ctx.deployment_id,)
    )
    ctx.logger.info(
        "_canary.deploy: ensured %s.heartbeat and inserted a heartbeat row.", schema
    )
    return {"schema": schema, "table": f"{schema}.heartbeat", "status": "deployed"}

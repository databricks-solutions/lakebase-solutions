"""core/lakebase deploy step (P1).

The Lakebase Postgres *instance* itself is provisioned by the GA
``database_instance`` DABs resource (``databricks.yml``); the secret *scope* is
the GA ``secret_scope`` resource. This step owns the part DABs cannot express at
PP/GA:

1. obtain a connection credential from the GA ``generate-database-credential``
   surface (guarded/mockable via ``ctx.workspace_client()``),
2. connect as admin and run **idempotent** SQL to create the workshop schema,
3. write the instance connection info (host/db/schema/user/password) to the
   deployment's standalone secret scope.

When no live clients are injected (e.g. the orchestrator smoke tests), it logs
intent and returns a ``stub`` result -- the live run happens in-workspace.
"""

from __future__ import annotations

from typing import Any, Dict, List

from bootstrap.adapters import username_from_token

# Connection-info secret keys this step writes (and teardown removes).
CONN_SECRET_KEYS: List[str] = ["pghost", "pgdatabase", "pgschema", "pguser", "pgpassword"]


def workshop_schema_sql(schema: str) -> List[str]:
    """Idempotent DDL that bootstraps the workshop schema.

    ``CREATE SCHEMA IF NOT EXISTS`` is Postgres-native idempotency, so the step
    is safe to re-run (immutable/repeatable deploy, SPEC section 4).
    """

    return [f'CREATE SCHEMA IF NOT EXISTS "{schema}"']


def deploy(ctx: Any) -> Dict[str, Any]:
    instance = ctx.resolved_names.get("lakebase_instance", ctx.name("lakebase"))
    database = ctx.params.get("database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")
    scope = ctx.params.get("secret_scope") or ctx.resolved_names.get("secret_scope")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/lakebase.deploy: no live clients injected; would ensure "
            "schema %r in %s.%s and write connection secrets to %r. "
            "P1 live run happens in-workspace.",
            schema,
            instance,
            database,
            scope,
        )
        return {"instance": instance, "database": database, "schema": schema, "status": "stub"}

    w = ctx.workspace_client()

    # (1) Obtain a connection credential (GA generate-database-credential) and
    #     resolve the instance host. Both are guarded/mockable.
    cred = w.database.generate_database_credential(instance_names=[instance])
    token = getattr(cred, "token", None)
    inst = w.database.get_database_instance(name=instance)
    host = getattr(inst, "read_write_dns", None)

    # (2) Connect as admin and run idempotent schema DDL.
    conn = ctx.pg_connection(role="admin", database=database)
    executed: List[str] = []
    cur = conn.cursor()
    for stmt in workshop_schema_sql(schema):
        cur.execute(stmt)
        executed.append(stmt)
    conn.commit()

    # (3) Persist connection info to the standalone secret scope.
    secrets_written: Dict[str, str] = {
        "pghost": host or "",
        "pgdatabase": database,
        "pgschema": schema,
        "pguser": username_from_token(token),
        "pgpassword": token or "",
    }
    for key, value in secrets_written.items():
        w.secrets.put_secret(scope=scope, key=key, string_value=value)

    ctx.logger.info(
        "core/lakebase.deploy: ensured schema %r in %s.%s; wrote %d connection "
        "secret(s) to %r.",
        schema,
        instance,
        database,
        len(secrets_written),
        scope,
    )
    return {
        "instance": instance,
        "database": database,
        "schema": schema,
        "host": host,
        "secret_scope": scope,
        "sql": executed,
        "secrets_written": sorted(secrets_written),
        "status": "deployed",
    }

"""modules/_canary deploy step (P0 stub).

The canary is the reference module that proves the authoring contract: it is
discovered from ``modules/_canary/module.yaml``, ordered AFTER its core
dependencies (lakebase, security), and deployed with zero notebook edits.

What the real step will do (P1+): create a tiny schema + table over psycopg, e.g.

    CREATE SCHEMA IF NOT EXISTS canary;
    CREATE TABLE IF NOT EXISTS canary.heartbeat (
        id          bigserial PRIMARY KEY,
        beat_at     timestamptz NOT NULL DEFAULT now(),
        deployment  text NOT NULL
    );
    INSERT INTO canary.heartbeat (deployment) VALUES (%(deployment_id)s);

P0: logs intent only -- no live SQL is executed.
"""

from __future__ import annotations

from typing import Any, Dict


def deploy(ctx: Any) -> Dict[str, Any]:
    schema = ctx.params.get("canary_schema", "canary")
    ctx.logger.info(
        "[stub] would CREATE SCHEMA %s + table %s.heartbeat and insert one row "
        "for deployment %r (via psycopg SQL).",
        schema,
        schema,
        ctx.deployment_id,
    )
    # TODO(P3): open a psycopg connection using the app role and run the DDL above.
    return {"schema": schema, "table": f"{schema}.heartbeat", "status": "stub"}

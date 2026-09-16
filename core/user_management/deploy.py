"""core/user_management deploy step (P1 -- real).

Workshop identity plumbing:

* ensure the admin and workshop **Databricks workspace groups** exist (SCIM),
* add the **deploying identity** to the admin group so the always-on admin
  console (which is fail-closed on admin-group membership) is usable by the SA
  who ran the deploy -- without this, ``STRICT_ADMIN=true`` locks everyone out,
* create a prefix-namespaced **participant PG role** (NOLOGIN group role) with
  read access to the workshop schema, to grant attendee logins later.

Groups are managed via SCIM REST (``w.api_client.do``) -- the job-runtime SDK
does not reliably type the membership surface, consistent with the rest of the
harness. When no live clients are injected it logs intent and returns ``stub``.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scim  # noqa: E402


def participant_role_sql(role: str, database: str, schema: str) -> List[str]:
    """Idempotent NOLOGIN group role + read grants on the workshop schema."""

    return [
        (
            "DO $$ BEGIN "
            f"IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN "
            f'CREATE ROLE "{role}" WITH NOLOGIN; '
            "END IF; END $$;"
        ),
        f'GRANT CONNECT ON DATABASE "{database}" TO "{role}"',
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT SELECT ON TABLES TO "{role}"',
    ]


def deploy(ctx: Any) -> Dict[str, Any]:
    admin_group = ctx.params.get("admin_group") or ctx.resolved_names.get("admin_group")
    workshop_group = ctx.params.get("workshop_group") or ctx.resolved_names.get("workshop_group")
    database = ctx.params.get("database") or ctx.resolved_names.get("workshop_database") or "databricks_postgres"
    schema = ctx.resolved_names.get("workshop_schema", "workshop")
    participant_role = ctx.resolved_names.get("pg_participant_role", f"{ctx.deployment_id}_participant")

    if not ctx.is_live():
        ctx.logger.info(
            "[stub] core/user_management.deploy: would ensure groups %r (admins) "
            "and %r (participants), add the deploying identity to %r, and create "
            "participant PG role %r on %s.%s.",
            admin_group,
            workshop_group,
            admin_group,
            participant_role,
            database,
            schema,
        )
        return {
            "admin_group": admin_group,
            "workshop_group": workshop_group,
            "pg_roles": [participant_role],
            "status": "stub",
        }

    result: Dict[str, Any] = {
        "admin_group": admin_group,
        "workshop_group": workshop_group,
        "pg_roles": [participant_role],
    }

    # (1) Ensure both workspace groups exist + add the deployer to the admin group.
    try:
        w = ctx.workspace_client()
        deployer = w.current_user.me().user_name
        admin_grp = scim.ensure_group(w, admin_group)
        scim.ensure_group(w, workshop_group)

        admin_id = admin_grp.get("id")
        uid = scim.find_user_id(w, deployer)
        if admin_id and uid and uid not in scim.group_member_ids(admin_grp):
            scim.add_member(w, admin_id, uid)
            result["admin_member_added"] = deployer
        elif admin_id and uid:
            result["admin_member_added"] = f"{deployer} (already a member)"
        else:
            ctx.logger.warning(
                "core/user_management.deploy: could not resolve SCIM id for group "
                "%r (id=%r) or user %r; admin membership NOT changed.",
                admin_group,
                admin_id,
                deployer,
            )
            result["admin_member_added"] = None
        result["deployer"] = deployer
        ctx.logger.info(
            "core/user_management.deploy: ensured groups %r + %r; admin membership for %r = %s.",
            admin_group,
            workshop_group,
            deployer,
            result.get("admin_member_added"),
        )
    except Exception as exc:  # defer, don't abort the whole run
        ctx.logger.error("[user_management] group/membership deferred: %s", exc)
        result["groups_error"] = str(exc)

    # (2) Create the participant PG role (best-effort; independent of the group work).
    try:
        conn = ctx.pg_connection(role="admin", database=database)
        cur = conn.cursor()
        executed: List[str] = []
        for stmt in participant_role_sql(participant_role, database, schema):
            cur.execute(stmt)
            executed.append(stmt)
        conn.commit()
        result["sql"] = executed
    except Exception as exc:
        ctx.logger.error("[user_management] participant role deferred: %s", exc)
        result["pg_role_error"] = str(exc)

    # (3) Grant the DEPLOYER's Databricks identity the built-in databricks_superuser
    #     OAuth role. Lakebase separates Databricks admin from Postgres admin -- a
    #     workspace admin has NO Postgres privileges by default. The admin console
    #     connects on-behalf-of the signed-in admin, so without this it can't
    #     read/maintain the schemas. Default ON; opt out with grant_deployer_superuser.
    #     Mirrors the SP variant in core/data_api (databricks_auth + create_role + grant).
    result["superuser_identity"] = None
    result["superuser_granted"] = False
    grant_superuser = str(ctx.params.get("grant_deployer_superuser", True)).lower() != "false"
    if not grant_superuser:
        result["superuser_skipped"] = True
        ctx.logger.info("core/user_management.deploy: grant_deployer_superuser is off; skipping.")
    else:
        try:
            email = result.get("deployer") or ctx.workspace_client().current_user.me().user_name
            conn = ctx.pg_connection(role="admin", database=database)
            try:  # extension + role creation run cleanly on autocommit.
                conn.autocommit = True
            except Exception:  # pragma: no cover - fake/driver without the attribute
                pass
            cur = conn.cursor()
            su_sql: List[str] = []
            su_errors: List[str] = []  # per-statement failures, surfaced for diagnosis
            # (label, callable) -- best-effort per statement (role may already exist).
            statements = [
                ("CREATE EXTENSION IF NOT EXISTS databricks_auth",
                 lambda: cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")),
                (f"SELECT databricks_create_role('{email}', 'USER')",
                 lambda: cur.execute("SELECT databricks_create_role(%s, 'USER')", (email,))),
                (f'GRANT databricks_superuser TO "{email}"',
                 lambda: cur.execute(f'GRANT databricks_superuser TO "{email}"')),
            ]
            for label, run in statements:
                try:
                    run()
                    conn.commit()
                    su_sql.append(label)
                except Exception as exc:  # idempotent re-run / role may exist -- best effort
                    conn.rollback()
                    su_errors.append(f"{label} -> {str(exc)[:220]}")
                    ctx.logger.info(
                        "core/user_management.deploy: superuser stmt deferred (%s): %s",
                        label, str(exc)[:120],
                    )
            if su_errors:
                result["superuser_errors"] = su_errors
            result["superuser_identity"] = email
            # Report granted only when the GRANT itself succeeded (not merely the
            # extension/role-create), so the deploy result reflects real privilege.
            result["superuser_granted"] = any(
                s.startswith("GRANT databricks_superuser") for s in su_sql
            )
            result.setdefault("sql", []).extend(su_sql)
            ctx.logger.info(
                "core/user_management.deploy: databricks_superuser grant for %r = %s (%d stmt).",
                email, result["superuser_granted"], len(su_sql),
            )
        except Exception as exc:
            ctx.logger.error("[user_management] superuser grant deferred: %s", exc)
            result["superuser_error"] = str(exc)

    result["status"] = "deferred" if ("groups_error" in result or "pg_role_error" in result) else "deployed"
    return result

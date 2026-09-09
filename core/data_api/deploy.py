"""core/data_api deploy step (P0 stub).

The Lakebase Data API is a managed PostgREST layer. Setup is **two-phase**
(SPEC section 4):

* Phase 1 -- MANUAL: a human enables the Data API in the Lakebase UI and exposes
  the target schema. There is no PP/GA programmatic enable yet, so this step
  prints a loud, explicit instruction and stops short of assuming it is done.
* Phase 2 -- RE-RUNNABLE: configure the dedicated service principal, register
  the `databricks_auth` role (`GRANT ... TO authenticator`), and apply RLS. This
  is safe to re-run after the manual enable.

The core gotcha (ported from FSM): the instance owner CANNOT use the Data API
(PGRST301) -- a dedicated non-owner role must be registered.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict

_MANUAL_ENABLE_BANNER = """
================================================================================
  ACTION REQUIRED -- Data API must be enabled MANUALLY (two-phase, phase 1)
--------------------------------------------------------------------------------
  1. Open the Lakebase instance in the Databricks UI.
  2. Go to the Data API tab and ENABLE it.
  3. Expose the target schema.
  4. Re-run this deploy to configure the SP, databricks_auth role, and RLS.
================================================================================
"""


def deploy(ctx: Any) -> Dict[str, Any]:
    enabled = str(ctx.params.get("enable_data_api", "true")).lower() == "true"
    if not enabled:
        ctx.logger.info("[stub] Data API disabled (enable_data_api=false); skipping configure.")
        return {"status": "skipped"}

    # Phase 1 -- loud manual instruction.
    ctx.logger.warning(_MANUAL_ENABLE_BANNER)

    # Phase 2 -- re-runnable configure (stubbed).
    ctx.logger.info(
        "[stub] would configure Data API: dedicated SP %r, register `databricks_auth` "
        "role (GRANT ... TO authenticator), and apply RLS. Owner cannot use the Data "
        "API (PGRST301) -- a non-owner role is required.",
        ctx.name("data-api-sp"),
    )
    # TODO(P1): mint dedicated SP + OAuth secret; `CREATE ROLE databricks_auth`;
    #   register role; apply RLS policies; store creds in the secret scope.
    return {"sp": ctx.name("data-api-sp"), "phase": "configure", "status": "stub"}

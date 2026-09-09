"""core/data_api health check (P0 stub).

Responsibility: the SPEC section 2 acceptance test -- an authenticated REST call
against the Data API returns RLS-scoped rows via the dedicated non-owner role.

P0: logs intent only -- no live calls.
"""

from __future__ import annotations

from typing import Any, Dict


def health_check(ctx: Any) -> Dict[str, Any]:
    ctx.logger.info(
        "[stub] would mint an OAuth token for SP %r and issue an authenticated Data API "
        "request, asserting RLS-scoped rows return (owner would get PGRST301).",
        ctx.name("data-api-sp"),
    )
    # TODO(P2): OAuth m2m token -> GET <data-api>/<table>; assert 200 + scoped rows.
    return {"sp": ctx.name("data-api-sp"), "healthy": None, "status": "stub"}

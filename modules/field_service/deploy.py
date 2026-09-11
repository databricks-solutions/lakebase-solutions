"""field_service module — deploy entrypoint.

Runs the module's ordered internal sub-pipeline (see ``fs_steps.ORDERED_STEPS``)
forward. Each step is best-effort: a failure is caught and recorded as
``deferred`` so one component cannot abort the module (which runs mid-DAG), and
the notebook still surfaces every step's status via the run results. Gated steps
(pipeline/ml/agent/ops) are skipped when their module param is off.

The orchestrator loads this file flat by path, so we add the module directory to
``sys.path`` to import the sibling ``fs_steps`` module (same pattern the admin
app uses for its package).
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fs_steps import steps_for  # noqa: E402


def deploy(ctx: Any) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for step in steps_for(ctx):
        try:
            res = step.deploy(ctx)
        except Exception as exc:  # best-effort: defer, don't abort the module
            ctx.logger.error("[field_service] step %r deferred: %s", step.name, exc)
            res = {"step": step.name, "status": "deferred", "error": str(exc)}
        results.append(res)

    statuses = {r.get("status") for r in results}
    overall = "deployed"
    if "deferred" in statuses:
        overall = "partial"
    elif statuses == {"stub"}:
        overall = "stub"
    ctx.logger.info(
        "field_service.deploy: ran %d step(s); statuses=%s.",
        len(results),
        sorted(s for s in statuses if s),
    )
    return {"module": "field_service", "steps": results, "status": overall}

"""field_service module — teardown entrypoint.

Runs the internal sub-pipeline in REVERSE order (app → … → data) so dependents
are removed before their dependencies. Every step is best-effort: teardown never
aborts on a resource that is already gone.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fs_steps import steps_for  # noqa: E402


def teardown(ctx: Any) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for step in reversed(steps_for(ctx)):
        try:
            res = step.teardown(ctx)
        except Exception as exc:  # best-effort: a missing resource must not abort
            ctx.logger.warning("[field_service] teardown %r skipped: %s", step.name, exc)
            res = {"step": step.name, "status": "skipped", "error": str(exc)}
        results.append(res)
    ctx.logger.info("field_service.teardown: ran %d step(s) in reverse.", len(results))
    return {"module": "field_service", "steps": results, "status": "torn_down"}

"""field_service module — health entrypoint.

Aggregates the per-step health checks. Overall healthy only when no enabled step
reports unhealthy; steps that are stubs or deferred are reported but do not fail
the aggregate (they are surfaced for visibility).
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fs_steps import steps_for  # noqa: E402


def health_check(ctx: Any) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for step in steps_for(ctx):
        try:
            res = step.health(ctx)
        except Exception as exc:
            res = {"step": step.name, "status": "deferred", "error": str(exc)}
        results.append(res)

    unhealthy = [r["step"] for r in results if r.get("status") == "unhealthy"]
    healthy = None if any(r.get("status") == "stub" for r in results) else not unhealthy
    status = "unhealthy" if unhealthy else ("stub" if healthy is None else "ok")
    return {
        "module": "field_service",
        "steps": results,
        "unhealthy": unhealthy,
        "healthy": healthy,
        "status": status,
    }

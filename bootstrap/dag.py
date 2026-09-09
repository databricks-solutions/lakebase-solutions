"""Dependency DAG: topological ordering of components and modules.

Given a set of manifests, produce a deploy order that:

1. Honors every ``depends_on`` edge (a dependency deploys before its dependent).
2. Places all ``core`` components before any ``module`` (the core-before-modules
   invariant -- core is always-on foundation that modules build on).
3. Is deterministic (ties broken by ``(kind, name)``) so re-runs are stable.

Teardown uses the reverse of this order.

Raises:
* :class:`MissingDependencyError` if a ``depends_on`` name has no manifest.
* :class:`CycleError` if the dependency graph contains a cycle.
* :class:`DagError` for duplicate names or a core-depends-on-module edge.
"""

from __future__ import annotations

import heapq
from typing import Dict, Iterable, List

from .manifest import Manifest

__all__ = ["DagError", "CycleError", "MissingDependencyError", "topological_order"]

# Lower rank deploys first when there is no dependency edge forcing an order.
# Core (0) always sorts ahead of modules (1).
_KIND_RANK: Dict[str, int] = {"core": 0, "module": 1}


class DagError(Exception):
    """Base error for dependency-graph problems."""


class CycleError(DagError):
    """Raised when the dependency graph contains a cycle."""


class MissingDependencyError(DagError):
    """Raised when a ``depends_on`` entry names an unknown component/module."""


def _dependencies(manifest: Manifest) -> List[str]:
    return list(manifest.depends_on.core) + list(manifest.depends_on.modules)


def topological_order(manifests: Iterable[Manifest]) -> List[Manifest]:
    """Return ``manifests`` in a valid deploy order (see module docstring)."""

    items = list(manifests)

    by_name: Dict[str, Manifest] = {}
    for manifest in items:
        if manifest.name in by_name:
            raise DagError(f"duplicate component name: {manifest.name!r}")
        by_name[manifest.name] = manifest

    adjacency: Dict[str, List[str]] = {name: [] for name in by_name}
    indegree: Dict[str, int] = {name: 0 for name in by_name}

    for manifest in items:
        # A core component may not depend on a module -- doing so could force a
        # module ahead of a core in the order, breaking the invariant.
        if manifest.kind == "core" and manifest.depends_on.modules:
            raise DagError(
                f"core component {manifest.name!r} may not depend on modules "
                f"{manifest.depends_on.modules!r}"
            )
        for dep in _dependencies(manifest):
            if dep not in by_name:
                raise MissingDependencyError(
                    f"{manifest.name!r} depends on unknown component {dep!r}"
                )
            adjacency[dep].append(manifest.name)
            indegree[manifest.name] += 1

    # Kahn's algorithm with a priority queue keyed by (kind_rank, name) so the
    # output is deterministic and core-before-modules holds among ready nodes.
    ready: List[tuple[int, str]] = [
        (_KIND_RANK.get(by_name[name].kind, 9), name)
        for name, deg in indegree.items()
        if deg == 0
    ]
    heapq.heapify(ready)

    order: List[Manifest] = []
    while ready:
        _, name = heapq.heappop(ready)
        order.append(by_name[name])
        for dependent in adjacency[name]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                heapq.heappush(
                    ready, (_KIND_RANK.get(by_name[dependent].kind, 9), dependent)
                )

    if len(order) != len(by_name):
        ordered_names = {manifest.name for manifest in order}
        remaining = sorted(name for name in by_name if name not in ordered_names)
        raise CycleError(f"dependency cycle detected among: {remaining}")

    return order

"""Topological ordering: dependency order, core-before-modules, cycle detection.

Pure-python; needs NO workspace.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bootstrap.dag import (
    CycleError,
    DagError,
    MissingDependencyError,
    topological_order,
)
from bootstrap.discovery import discover
from bootstrap.manifest import DependsOn, Manifest

ROOT = Path(__file__).resolve().parents[1]


def _mk(name, kind, core=None, modules=None, enabled=False):
    return Manifest(
        name=name,
        kind=kind,
        enabled_by_default=enabled,
        depends_on=DependsOn(core=core or [], modules=modules or []),
    )


# -- real repo manifests --------------------------------------------------


def test_dependencies_precede_dependents():
    manifests = discover(ROOT)
    order = topological_order(manifests)
    position = {m.name: i for i, m in enumerate(order)}

    for manifest in manifests:
        for dep in list(manifest.depends_on.core) + list(manifest.depends_on.modules):
            assert position[dep] < position[manifest.name], (
                f"{dep} must be ordered before {manifest.name}"
            )


def test_core_precedes_all_modules():
    manifests = discover(ROOT)
    order = topological_order(manifests)
    kinds = [m.kind for m in order]

    core_positions = [i for i, k in enumerate(kinds) if k == "core"]
    module_positions = [i for i, k in enumerate(kinds) if k == "module"]

    if core_positions and module_positions:
        assert max(core_positions) < min(module_positions)


def test_order_is_deterministic():
    manifests = discover(ROOT)
    first = [m.name for m in topological_order(manifests)]
    second = [m.name for m in topological_order(list(reversed(manifests)))]
    assert first == second


def test_lakebase_is_first():
    order = topological_order(discover(ROOT))
    assert order[0].name == "lakebase"


# -- synthetic graphs -----------------------------------------------------


def test_cycle_detection():
    a = _mk("a", "module", modules=["b"])
    b = _mk("b", "module", modules=["a"])
    with pytest.raises(CycleError):
        topological_order([a, b])


def test_missing_dependency():
    orphan = _mk("orphan", "module", core=["ghost"])
    with pytest.raises(MissingDependencyError):
        topological_order([orphan])


def test_duplicate_name_rejected():
    a1 = _mk("dup", "module")
    a2 = _mk("dup", "module")
    with pytest.raises(DagError):
        topological_order([a1, a2])


def test_core_cannot_depend_on_module():
    bad_core = _mk("bad_core", "core", modules=["m"])
    module = _mk("m", "module")
    with pytest.raises(DagError):
        topological_order([bad_core, module])


def test_simple_chain_orders_correctly():
    lakebase = _mk("lakebase", "core")
    security = _mk("security", "core", core=["lakebase"])
    canary = _mk("_canary", "module", core=["lakebase", "security"])
    order = [m.name for m in topological_order([canary, security, lakebase])]
    assert order == ["lakebase", "security", "_canary"]

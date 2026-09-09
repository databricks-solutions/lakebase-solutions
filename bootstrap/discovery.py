"""Manifest discovery.

The orchestrator NEVER hard-codes the list of components/modules. Instead it
scans the repo for ``module.yaml`` files:

* ``core/*/module.yaml``  -- always-on foundation (loaded every deploy)
* ``modules/*/module.yaml`` -- optional workshop modules (selected per run)

This is what makes "adding a module = drop a folder; no notebook edits" true:
a new ``modules/<name>/module.yaml`` is picked up automatically on the next run.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from .manifest import Manifest, load_manifest

__all__ = ["CORE_DIR", "MODULES_DIR", "MANIFEST_FILENAME", "discover", "discover_core", "discover_modules"]

CORE_DIR = "core"
MODULES_DIR = "modules"
MANIFEST_FILENAME = "module.yaml"


def _discover_dir(base: Path) -> List[Manifest]:
    if not base.is_dir():
        return []
    manifests: List[Manifest] = []
    for child in sorted(base.iterdir()):
        manifest_path = child / MANIFEST_FILENAME
        if manifest_path.is_file():
            manifests.append(load_manifest(manifest_path))
    return manifests


def discover_core(root: str | Path) -> List[Manifest]:
    """Discover all ``core/*/module.yaml`` manifests under ``root``."""

    return _discover_dir(Path(root) / CORE_DIR)


def discover_modules(root: str | Path) -> List[Manifest]:
    """Discover all ``modules/*/module.yaml`` manifests under ``root``."""

    return _discover_dir(Path(root) / MODULES_DIR)


def discover(root: str | Path) -> List[Manifest]:
    """Discover every core component and module manifest under ``root``.

    Core manifests are returned first, then modules; both groups are sorted by
    directory name for deterministic ordering. Each returned manifest has its
    ``source_dir`` populated.
    """

    return discover_core(root) + discover_modules(root)

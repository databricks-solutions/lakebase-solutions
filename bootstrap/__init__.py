"""lakebase-solutions orchestrator engine.

The ``bootstrap`` package is the deploy notebook's control plane. It discovers
core components and modules by scanning for ``module.yaml`` manifests, validates
them, orders them by their declared dependencies, and drives per-step
deploy/teardown/health execution.

Public surface:

* :func:`bootstrap.orchestrator.run` -- the entrypoint the notebook calls.
* :class:`bootstrap.manifest.Manifest` + :func:`bootstrap.manifest.load_manifest`.
* :func:`bootstrap.discovery.discover`.
* :func:`bootstrap.dag.topological_order`.
* :class:`bootstrap.context.DeployContext`.
"""

from __future__ import annotations

from .context import DeployContext, get_logger
from .dag import CycleError, DagError, MissingDependencyError, topological_order
from .discovery import discover, discover_core, discover_modules
from .manifest import (
    DependsOn,
    Manifest,
    ManifestError,
    Parameter,
    load_manifest,
    validate_manifest,
)
from .orchestrator import run, select_components

__all__ = [
    "DeployContext",
    "get_logger",
    "topological_order",
    "DagError",
    "CycleError",
    "MissingDependencyError",
    "discover",
    "discover_core",
    "discover_modules",
    "Manifest",
    "ManifestError",
    "Parameter",
    "DependsOn",
    "load_manifest",
    "validate_manifest",
    "run",
    "select_components",
]

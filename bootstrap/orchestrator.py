"""Orchestrator: the control plane the deploy notebook drives.

``run(mode, selected_modules, ctx)`` performs the full flow:

    discover  ->  validate  ->  select  ->  build DAG  ->  iterate steps

For each step it invokes the component's declared entrypoint (deploy),
``health_check``, or ``teardown`` function. **In P0 every per-step execution is
a stub that logs its intent** ("[would deploy] ...") and makes no live calls;
the real logic lands in P1+. The *control flow* here is real and is what the
manifest/DAG tests exercise.

The notebook never changes when a module is added: modules are discovered from
``modules/*/module.yaml`` and ordered by their declared dependencies.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from .context import DeployContext
from .dag import topological_order
from .discovery import discover
from .manifest import Manifest, validate_manifest

__all__ = ["run", "select_components", "DEFAULT_ROOT"]

# Repo root (the parent of the ``bootstrap`` package).
DEFAULT_ROOT = Path(__file__).resolve().parents[1]

# Maps an orchestrator action to (manifest field naming the file, function name
# expected inside that file).
_ACTION_MAP: Dict[str, tuple[str, str]] = {
    "deploy": ("entrypoint", "deploy"),
    "teardown": ("teardown", "teardown"),
    "health": ("health_check", "health_check"),
}

# Signature every step executor must satisfy.
Executor = Callable[[Manifest, str, DeployContext], Any]


def _load_step_callable(manifest: Manifest, filename: str, func_name: str) -> Optional[Callable]:
    """Dynamically import ``func_name`` from ``<source_dir>/<filename>``.

    Returns ``None`` if the file or function is absent. Each file is imported
    under a unique module name to avoid ``deploy.py`` collisions across dirs.
    """

    if manifest.source_dir is None:
        return None
    path = Path(manifest.source_dir) / filename
    if not path.is_file():
        return None

    mod_name = f"lakebase_solutions_step_{manifest.source_dir.name}_{path.stem}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, func_name, None)


def _default_executor(manifest: Manifest, action: str, ctx: DeployContext) -> Dict[str, Any]:
    """Invoke a component's step function, or log intent if none is present.

    This is a real dispatcher over stubbed steps: it logs ``[would <action>]``
    and then calls the component's ``deploy``/``teardown``/``health_check``
    function (themselves P0 stubs that only log). No live calls are made.
    """

    field_name, func_name = _ACTION_MAP[action]
    filename = getattr(manifest, field_name)
    ctx.logger.info("[would %s] %s (%s)", action, manifest.name, manifest.kind)

    step_fn = _load_step_callable(manifest, filename, func_name)
    if step_fn is None:
        ctx.logger.warning(
            "no %s() found for %s (%s); logged intent only",
            func_name,
            manifest.name,
            filename,
        )
        return {"component": manifest.name, "action": action, "status": "logged"}

    result = step_fn(ctx)
    return {"component": manifest.name, "action": action, "status": "ok", "result": result}


def select_components(
    manifests: Sequence[Manifest], selected_modules: Optional[Sequence[str]] = None
) -> List[Manifest]:
    """Choose which manifests to run: all core + selected/default modules.

    Core is always included. Modules are included if named in
    ``selected_modules`` or flagged ``enabled_by_default``; any module those
    depend on (transitively) is pulled in as well.

    Raises ``KeyError`` if a selected module name is unknown.
    """

    by_name = {m.name: m for m in manifests}
    cores = [m for m in manifests if m.kind == "core"]

    wanted: set[str] = set(selected_modules or [])
    wanted.update(m.name for m in manifests if m.kind == "module" and m.enabled_by_default)

    resolved: set[str] = set()
    stack = list(wanted)
    while stack:
        name = stack.pop()
        if name in resolved:
            continue
        if name not in by_name:
            raise KeyError(f"selected module {name!r} not found among discovered manifests")
        resolved.add(name)
        stack.extend(by_name[name].depends_on.modules)

    modules = [by_name[name] for name in resolved if by_name[name].kind == "module"]
    return cores + modules


def run(
    mode: str,
    selected_modules: Optional[Sequence[str]] = None,
    ctx: Optional[DeployContext] = None,
    root: Optional[str | Path] = None,
    executor: Optional[Executor] = None,
    run_health: bool = True,
) -> List[Dict[str, Any]]:
    """Discover, validate, order, and execute the deploy/teardown flow.

    Args:
        mode: ``"deploy"`` or ``"teardown"``.
        selected_modules: module names to include (core is always included).
        ctx: shared :class:`DeployContext`; a scaffold context is built if omitted.
        root: repo root to scan; defaults to the repo containing ``bootstrap``.
        executor: per-step executor (injectable for testing); defaults to the
            logging stub dispatcher.
        run_health: after each deploy, also invoke the component health check.

    Returns:
        A list of per-step result dicts, in execution order.
    """

    if mode not in ("deploy", "teardown"):
        raise ValueError(f"unknown mode {mode!r}; expected 'deploy' or 'teardown'")

    scan_root = Path(root) if root is not None else DEFAULT_ROOT
    ctx = ctx or DeployContext(deployment_id="scaffold", mode=mode)
    executor = executor or _default_executor

    manifests = discover(scan_root)
    for manifest in manifests:
        validate_manifest(manifest)

    selected = select_components(manifests, selected_modules)
    ordered = topological_order(selected)

    ctx.logger.info(
        "%s: %d components in order: %s",
        mode,
        len(ordered),
        ", ".join(m.name for m in ordered),
    )

    results: List[Dict[str, Any]] = []
    if mode == "deploy":
        for manifest in ordered:
            results.append(executor(manifest, "deploy", ctx))
            if run_health:
                executor(manifest, "health", ctx)
    else:  # teardown runs in reverse dependency order
        for manifest in reversed(ordered):
            results.append(executor(manifest, "teardown", ctx))

    ctx.logger.info("%s complete: %d components", mode, len(ordered))
    return results

# Databricks notebook source
# MAGIC %md
# MAGIC # lakebase-solutions - deploy / teardown
# MAGIC
# MAGIC Single, parameterized control-plane notebook for the whole project.
# MAGIC It collects parameters via widgets, builds a `DeployContext`, and hands
# MAGIC off to `bootstrap.orchestrator.run(...)`, which discovers core components
# MAGIC and the selected modules, orders them by dependency, and deploys (or
# MAGIC tears down) each.
# MAGIC
# MAGIC **Adding a module never requires editing this notebook** - modules are
# MAGIC discovered from `modules/*/module.yaml` and appear in the modules
# MAGIC multiselect automatically.
# MAGIC
# MAGIC > Off-Databricks (pytest) per-step execution is stubbed (logs intent
# MAGIC > only). In-workspace, each step provisions live via the Databricks
# MAGIC > Python SDK (`WorkspaceClient`) -- the CLI cannot run on notebook/job
# MAGIC > compute, so there is no `databricks bundle` shell-out.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import guard
# MAGIC `dbutils` only exists inside Databricks. Guarding it keeps this file
# MAGIC importable off-Databricks (the pytest suite imports this module).

# COMMAND ----------

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Make the repo root importable both in a Databricks Repo and in local pytest.
_REPO_ROOT = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from bootstrap.context import DeployContext  # noqa: E402
from bootstrap.discovery import discover, discover_core, discover_modules  # noqa: E402
from bootstrap.manifest import Manifest, Parameter  # noqa: E402
from bootstrap.orchestrator import run  # noqa: E402

try:  # pragma: no cover - only truthy inside Databricks
    dbutils  # type: ignore[name-defined]  # noqa: B018
    _IN_DATABRICKS = True
except NameError:
    _IN_DATABRICKS = False


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters: Required vs Optional vs Advanced (SPEC section 5)
# MAGIC All resources are namespaced by `deployment_id` so multiple deployments
# MAGIC coexist in one workspace. Parameters come from the discovered manifests
# MAGIC and render in three tiers:
# MAGIC
# MAGIC | Tier | Widget? | Where it comes from |
# MAGIC |---|---|---|
# MAGIC | **required** | Yes, prominent, no default (must be non-empty) | widget |
# MAGIC | **optional** | Yes, carries the default (labelled `optional · default: …`) | widget |
# MAGIC | **advanced** | No widget | `config.yaml` (see `config.template.yaml`) |
# MAGIC
# MAGIC Run the next cell to print the live table generated from the manifests.

# COMMAND ----------

# Notebook-level orchestration controls (not component parameters). These always
# render. ``deployment_id`` is required (no default).
_CONTROL_SPECS: List[Dict[str, Any]] = [
    {"name": "deployment_id", "kind": "text", "default": "", "label": "Deployment ID / prefix", "required": True},
    {"name": "mode", "kind": "dropdown", "default": "deploy", "choices": ["deploy", "teardown"], "label": "Mode", "required": False},
    {"name": "cloud", "kind": "dropdown", "default": "aws", "choices": ["aws", "azure", "gcp"], "label": "Cloud", "required": False},
    {"name": "region", "kind": "text", "default": "us-west-2", "label": "Region (optional · default: us-west-2)", "required": False},
]

# Off-Databricks / test defaults (mirror the widgets). Component defaults are
# merged in from the manifests by ``_default_params()``.
_CONTROL_DEFAULTS: Dict[str, Any] = {
    "deployment_id": "acme-ws",
    "mode": "deploy",
    "cloud": "aws",
    "region": "us-west-2",
    "modules": "",
}


def _module_choices() -> List[str]:
    """Discover selectable module names from ``modules/*/module.yaml``."""

    try:
        return [m.name for m in discover_modules(_REPO_ROOT)]
    except Exception:  # pragma: no cover - discovery is best-effort for widgets
        return []


def _collect_core_parameters() -> List[Parameter]:
    """Dedup core-component parameters by name (first occurrence wins)."""

    seen: Dict[str, Parameter] = {}
    try:
        core = discover_core(_REPO_ROOT)
    except Exception:  # pragma: no cover - best-effort for widget rendering
        return []
    for manifest in core:
        for param in manifest.parameters:
            seen.setdefault(param.name, param)
    return list(seen.values())


def _param_tier(param: Parameter) -> str:
    if param.required:
        return "required"
    if param.advanced:
        return "advanced"
    return "optional"


def _optional_label(param: Parameter) -> str:
    base = param.label or param.name
    shown = param.default if param.default not in (None, "") else "auto"
    return f"{base} (optional · default: {shown})"


def _default_params() -> Dict[str, Any]:
    """Full default parameter map (controls + component defaults + advanced)."""

    params: Dict[str, Any] = dict(_CONTROL_DEFAULTS)
    for param in _collect_core_parameters():
        params.setdefault(param.name, "" if param.default is None else str(param.default))
    return params


# Public constant kept for readability / external callers.
DEFAULT_PARAMS: Dict[str, Any] = _default_params()


def _load_config_yaml() -> Dict[str, Any]:
    """Read advanced params from ``config.yaml`` if present (else empty)."""

    import yaml  # local import keeps top-level import surface small

    config_path = _REPO_ROOT / "config.yaml"
    if not config_path.is_file():
        return {}
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


def parameter_summary() -> str:
    """Return a Markdown table of parameters and their tier (from manifests)."""

    lines = [
        "| Parameter | Tier | Default | Help |",
        "|---|---|---|---|",
        "| deployment_id | required | (none) | Deployment ID / prefix -- namespaces all resources |",
    ]
    for param in _collect_core_parameters():
        tier = _param_tier(param)
        default = param.default if param.default not in (None, "") else "auto"
        lines.append(f"| {param.name} | {tier} | {default} | {param.help} |")
    return "\n".join(lines)


def _build_widgets() -> None:
    """Create dbutils widgets in three tiers (Databricks only).

    Widget NAMES are prefixed with a numeric sort key (``1_``, ``2_``, ...) so
    required widgets render before optional ones; the prefix is stripped when
    reading values. Human ``label`` strings stay clean. Advanced params get NO
    widget (they come from ``config.yaml``).
    """

    dbutils.widgets.removeAll()  # type: ignore[name-defined]

    order = 0

    def _add(spec: Dict[str, Any]) -> None:
        nonlocal order
        order += 1
        wname = f"{order}_{spec['name']}"
        label = spec["label"]
        if spec["kind"] == "dropdown":
            dbutils.widgets.dropdown(wname, spec["default"], spec["choices"], label)  # type: ignore[name-defined]
        else:
            dbutils.widgets.text(wname, spec["default"], label)  # type: ignore[name-defined]

    # Tier 1: required controls first (deployment_id has no default).
    for spec in _CONTROL_SPECS:
        if spec["required"]:
            _add(spec)

    # Tier 1 (cont.): required component params (none today; engine supports it).
    for param in _collect_core_parameters():
        if _param_tier(param) == "required":
            _add({"name": param.name, "kind": "text", "default": "", "label": param.label or param.name, "required": True})

    # Tier 2: optional controls, then optional component params.
    for spec in _CONTROL_SPECS:
        if not spec["required"]:
            _add(spec)

    for param in _collect_core_parameters():
        if _param_tier(param) == "optional":
            if param.type == "bool":
                _add({"name": param.name, "kind": "dropdown", "default": str(param.default).lower(),
                      "choices": ["true", "false"], "label": _optional_label(param)})
            elif param.name in ("autoscaling_min_cu", "autoscaling_max_cu"):
                # Autoscaling compute-unit range (0.5-32); scale-to-zero via suspend timeout.
                _add({"name": param.name, "kind": "dropdown", "default": str(param.default),
                      "choices": ["0.5", "1", "2", "4", "8", "16", "32"], "label": _optional_label(param)})
            else:
                _add({"name": param.name, "kind": "text", "default": "" if param.default is None else str(param.default),
                      "label": _optional_label(param)})

    # modules multiselect (control) -- populated from discovery, never hard-coded.
    order += 1
    choices = _module_choices()
    if choices:
        dbutils.widgets.multiselect(f"{order}_modules", choices[0], choices, "Modules to deploy")  # type: ignore[name-defined]
    else:
        dbutils.widgets.text(f"{order}_modules", "", "Modules (comma-separated)")  # type: ignore[name-defined]

    # Advanced params are intentionally NOT rendered; they live in config.yaml.


def _read_params() -> Dict[str, Any]:
    """Read parameter values from widgets (Databricks) or defaults (elsewhere).

    On Databricks, widget names carry a numeric ``<n>_`` prefix that is stripped
    here so the returned keys are the clean parameter names.
    """

    if not _IN_DATABRICKS:
        return _default_params()

    values: Dict[str, Any] = {}
    for wname in dbutils.widgets.getAll():  # type: ignore[name-defined]
        clean = wname.split("_", 1)[1] if wname[:1].isdigit() and "_" in wname else wname
        values[clean] = dbutils.widgets.get(wname)  # type: ignore[name-defined]
    return values


def _parse_modules(raw: str) -> List[str]:
    """Parse a comma- or multiselect- delimited module string into a list."""

    if not raw:
        return []
    return [name.strip() for name in raw.split(",") if name.strip()]


def _validate_required(params: Dict[str, Any]) -> None:
    """Fail fast if any required parameter is missing/blank."""

    required = [s["name"] for s in _CONTROL_SPECS if s["required"]]
    required += [p.name for p in _collect_core_parameters() if p.required]
    missing = [name for name in required if not str(params.get(name, "")).strip()]
    if missing:
        raise ValueError(
            f"Missing required parameter(s): {', '.join(missing)}. "
            "Fill the required widget(s) before running."
        )


def build_context(params: Dict[str, Any]) -> DeployContext:
    """Construct a :class:`DeployContext` from raw parameter values.

    In-workspace (``_IN_DATABRICKS``) the real live-access factories from
    ``bootstrap.adapters`` are injected so ``ctx.is_live()`` is True and every
    step executes live SDK/SQL. Off-Databricks (the pytest suite) they are left
    unset, so ``ctx.is_live()`` stays False and each step logs intent and stubs.
    """

    deployment_id = params["deployment_id"]
    factory_kwargs: Dict[str, Any] = {}
    if _IN_DATABRICKS:  # pragma: no cover - only truthy inside Databricks
        from bootstrap import adapters  # deferred: keeps import surface offline-safe

        factory_kwargs = {
            "workspace_client_factory": adapters.default_workspace_client_factory,
            "pg_connection_factory": adapters.default_pg_connection_factory,
        }
    ctx = DeployContext(
        deployment_id=deployment_id,
        mode=params.get("mode", "deploy"),
        cloud=params.get("cloud", "aws"),
        region=params.get("region", "us-west-2"),
        params=dict(params),
        **factory_kwargs,
    )
    # Honor explicit group overrides; otherwise the prefix-derived defaults stand.
    if params.get("admin_group"):
        ctx.resolved_names["admin_group"] = params["admin_group"]
    if params.get("workshop_group"):
        ctx.resolved_names["workshop_group"] = params["workshop_group"]
    return ctx


def main(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Entry point: build context from params and run the orchestrator.

    Advanced params (no widget) are overlaid from ``config.yaml``, then required
    params are validated before anything runs.

    All provisioning is done by the SDK-backed component steps themselves (a
    ``WorkspaceClient`` from ``bootstrap.adapters``, injected in-workspace):
    ``deploy`` runs the orchestrator forward; ``teardown`` runs it in teardown
    mode. There is NO CLI shell-out -- the ``databricks bundle`` CLI cannot run on
    notebook/job compute ("only supported for interactive use from the web
    terminal ... use the Databricks Python SDK"), so the in-workspace notebook
    provisions the project, endpoint, secret scope, and app via the SDK.
    """

    params = params or _read_params()

    # Overlay advanced params (database, secret_scope, ...) from config.yaml,
    # falling back to their manifest defaults.
    config = _load_config_yaml()
    for param in _collect_core_parameters():
        if param.advanced:
            params.setdefault(
                param.name,
                config.get(param.name, "" if param.default is None else str(param.default)),
            )

    _validate_required(params)
    ctx = build_context(params)
    modules = _parse_modules(params.get("modules", ""))
    return run(mode=ctx.mode, selected_modules=modules, ctx=ctx, root=_REPO_ROOT)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter summary (generated from discovered manifests)

# COMMAND ----------

print(parameter_summary())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run
# MAGIC Guarded so importing this module (in tests) never triggers a deploy.

# COMMAND ----------

if _IN_DATABRICKS:
    _build_widgets()
    _results = main()
    print(f"Orchestrator finished: {len(_results)} component step(s).")

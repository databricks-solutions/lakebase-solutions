"""The deploy notebook must be importable off-Databricks (dbutils guarded).

The pytest suite runs with no `dbutils` in scope, so importing `deploy.py` must
NOT create widgets or trigger the orchestrator. It should expose the callable
entry points instead.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _import_deploy():
    spec = importlib.util.spec_from_file_location("deploy_notebook", ROOT / "deploy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_deploy_imports_off_databricks():
    module = _import_deploy()
    # guard correctly detects we are NOT inside Databricks
    assert module._IN_DATABRICKS is False
    # entry points are present
    assert callable(module.main)
    assert callable(module.build_context)


def test_module_choices_include_canary():
    module = _import_deploy()
    assert "_canary" in module._module_choices()


def test_parse_modules_handles_whitespace_and_blanks():
    module = _import_deploy()
    assert module._parse_modules("a, b ,,c") == ["a", "b", "c"]
    assert module._parse_modules("") == []


def test_build_context_applies_group_overrides():
    module = _import_deploy()
    ctx = module.build_context(
        {
            "deployment_id": "acme-ws",
            "mode": "deploy",
            "cloud": "aws",
            "region": "us-west-2",
            "admin_group": "custom-admins",
            "workshop_group": "",
        }
    )
    assert ctx.deployment_id == "acme-ws"
    assert ctx.resolved_names["admin_group"] == "custom-admins"
    # blank workshop_group falls back to the prefix-derived default
    assert ctx.resolved_names["workshop_group"] == "acme-ws-participants"

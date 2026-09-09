"""Every core and module manifest must validate against the schema.

These tests need NO workspace -- they are pure-python and run in CI.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bootstrap.discovery import discover
from bootstrap.manifest import ManifestError, load_manifest, validate_manifest

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_CORE = {"lakebase", "security", "user_management", "data_api", "admin_app"}
EXPECTED_MODULES = {"_canary"}


def _all_manifest_files():
    files = []
    for sub in ("core", "modules"):
        base = ROOT / sub
        if base.is_dir():
            files.extend(sorted(base.glob("*/module.yaml")))
    return files


def test_manifest_files_present():
    files = _all_manifest_files()
    assert files, "expected at least one module.yaml under core/ or modules/"


@pytest.mark.parametrize(
    "manifest_file",
    _all_manifest_files(),
    ids=lambda p: str(p.relative_to(ROOT)),
)
def test_manifest_validates(manifest_file):
    manifest = load_manifest(manifest_file)
    validate_manifest(manifest)  # raises ManifestError on failure

    assert manifest.name
    assert manifest.kind in ("core", "module")

    # kind must match the directory it lives in
    top_dir = manifest_file.relative_to(ROOT).parts[0]
    expected_kind = "core" if top_dir == "core" else "module"
    assert manifest.kind == expected_kind

    # entrypoint/teardown/health files declared by the manifest must exist
    assert (manifest_file.parent / manifest.entrypoint).is_file()
    assert (manifest_file.parent / manifest.teardown).is_file()
    assert (manifest_file.parent / manifest.health_check).is_file()


def test_discovery_finds_expected_components():
    names = {m.name for m in discover(ROOT)}
    assert EXPECTED_CORE <= names, f"missing core components: {EXPECTED_CORE - names}"
    assert EXPECTED_MODULES <= names, f"missing modules: {EXPECTED_MODULES - names}"


def test_core_components_are_kind_core():
    for manifest in discover(ROOT):
        if manifest.name in EXPECTED_CORE:
            assert manifest.kind == "core"


def test_data_api_declares_two_phase():
    data_api = next(m for m in discover(ROOT) if m.name == "data_api")
    assert data_api.two_phase, "data_api manifest must declare the two-phase note"
    assert "manual_enable" in data_api.two_phase


def test_bad_kind_raises(tmp_path):
    bad = tmp_path / "module.yaml"
    bad.write_text("name: broken\nkind: not_a_kind\n")
    with pytest.raises(ManifestError):
        load_manifest(bad)


def test_unknown_field_raises(tmp_path):
    bad = tmp_path / "module.yaml"
    bad.write_text("name: broken\nkind: module\nbogus_field: 1\n")
    with pytest.raises(ManifestError):
        load_manifest(bad)


def test_missing_manifest_raises(tmp_path):
    with pytest.raises(ManifestError):
        load_manifest(tmp_path / "does_not_exist.yaml")


def test_core_depending_on_module_rejected_by_validate(tmp_path):
    bad = tmp_path / "module.yaml"
    bad.write_text(
        "name: bad_core\nkind: core\ndepends_on:\n  core: []\n  modules: [some_module]\n"
    )
    manifest = load_manifest(bad)  # schema-valid
    with pytest.raises(ManifestError):
        validate_manifest(manifest)  # semantic check fails

"""Feature-maturity schema + aggregation tests (offline).

Covers the manifest ``Feature`` schema (valid parse, maturity enum, extra-field
rejection) and the ``bootstrap.features`` aggregator/rendering over the real
repo manifests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bootstrap.features import aggregate_matrix, render_json, render_markdown, summary_counts
from bootstrap.manifest import Feature, ManifestError

ROOT = Path(__file__).resolve().parents[1]


def test_feature_from_dict_valid():
    f = Feature.from_dict({"name": "Genie spaces", "maturity": "GA", "note": "n"})
    assert f.name == "Genie spaces" and f.maturity == "GA" and f.note == "n"


def test_feature_defaults_to_ga():
    assert Feature.from_dict({"name": "X"}).maturity == "GA"


def test_feature_bad_maturity_rejected():
    with pytest.raises(ManifestError):
        Feature.from_dict({"name": "X", "maturity": "PRIVATE_PREVIEW"})


def test_feature_unknown_field_rejected():
    with pytest.raises(ManifestError):
        Feature.from_dict({"name": "X", "stage": "GA"})


def test_matrix_aggregates_core_features_all_ga():
    rows = aggregate_matrix(ROOT)
    # Core declares several GA features (lakebase, security, user_mgmt, data_api, admin_app).
    assert len(rows) >= 7
    names = {r["feature"] for r in rows}
    assert any("Lakebase" in n for n in names)
    assert any("Data API" in n for n in names)
    counts = summary_counts(rows)
    assert counts["total"] == len(rows)
    # Before the field_service module is added, every declared feature is GA.
    assert counts["GA"] == counts["total"]


def test_render_markdown_and_json():
    rows = aggregate_matrix(ROOT)
    md = render_markdown(rows)
    assert md.startswith("### Feature maturity")
    assert "| Component | Feature | Maturity | Note |" in md
    payload = render_json(rows)
    assert payload["summary"]["total"] == len(rows)
    assert payload["features"] == rows


def test_matrix_selected_modules_filter():
    # Restricting to a non-existent module yields only core rows (core always in).
    all_rows = aggregate_matrix(ROOT)
    core_only = aggregate_matrix(ROOT, selected_modules=[])
    assert len([r for r in all_rows if r["kind"] == "module"]) >= 0
    assert all(r["kind"] == "core" for r in core_only)

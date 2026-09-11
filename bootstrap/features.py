"""Feature-maturity aggregation for the customer-facing feature matrix.

The maturity gate allows Public-Preview-or-better features, on the condition
that customers can always see what is not GA. Each ``core/`` component and
``modules/`` module declares the Databricks features it provisions, with a
maturity, in its ``module.yaml`` (see :class:`bootstrap.manifest.Feature`).
This module scans those manifests and aggregates them into a single matrix that
the deploy notebook prints, the admin console renders as a page, and the
standalone feature-matrix app renders as its whole UI -- one source of truth.

Pure-stdlib and import-safe (only depends on discovery + manifest), so it runs
on any runtime and in the offline tests with no network.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from bootstrap.discovery import discover_core, discover_modules

__all__ = ["aggregate_matrix", "summary_counts", "render_markdown", "render_json"]

# Display order + labels for maturities (worst-to-best is intentional: the
# not-GA rows are what a customer most needs to see).
_MATURITY_LABEL = {
    "GA": "GA",
    "PUBLIC_PREVIEW": "Public Preview",
    "BETA": "Beta",
}
_MATURITY_ORDER = {"BETA": 0, "PUBLIC_PREVIEW": 1, "GA": 2}


def aggregate_matrix(
    root: str | Path, selected_modules: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Return the flattened feature matrix across core + (selected) modules.

    Each row is ``{component, kind, feature, maturity, maturity_label, note}``.
    ``selected_modules=None`` includes every discovered module; a list restricts
    module rows to those names (core is always included). Rows are sorted
    not-GA-first, then by component/feature so the matrix reads consistently.
    """

    root = Path(root)
    rows: List[Dict[str, Any]] = []

    def _add(manifest: Any) -> None:
        for feat in manifest.features:
            rows.append(
                {
                    "component": manifest.name,
                    "kind": manifest.kind,
                    "feature": feat.name,
                    "maturity": feat.maturity,
                    "maturity_label": _MATURITY_LABEL.get(feat.maturity, feat.maturity),
                    "note": feat.note,
                }
            )

    for manifest in discover_core(root):
        _add(manifest)
    for manifest in discover_modules(root):
        if selected_modules is not None and manifest.name not in selected_modules:
            continue
        _add(manifest)

    rows.sort(
        key=lambda r: (_MATURITY_ORDER.get(r["maturity"], 9), r["component"], r["feature"])
    )
    return rows


def summary_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count features per maturity (keys: GA, PUBLIC_PREVIEW, BETA, total)."""

    counts = {"GA": 0, "PUBLIC_PREVIEW": 0, "BETA": 0, "total": 0}
    for r in rows:
        counts[r["maturity"]] = counts.get(r["maturity"], 0) + 1
        counts["total"] += 1
    return counts


def render_markdown(rows: List[Dict[str, Any]]) -> str:
    """Render the matrix as a Markdown table (used in the notebook + docs)."""

    c = summary_counts(rows)
    lines = [
        f"### Feature maturity — {c['total']} features "
        f"({c['GA']} GA · {c['PUBLIC_PREVIEW']} Public Preview · {c['BETA']} Beta)",
        "",
        "| Component | Feature | Maturity | Note |",
        "|---|---|---|---|",
    ]
    for r in rows:
        note = r["note"].replace("|", "\\|")
        lines.append(
            f"| {r['component']} | {r['feature']} | {r['maturity_label']} | {note} |"
        )
    return "\n".join(lines)


def render_json(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Return a JSON-serializable matrix payload (used by the apps/artifact)."""

    return {"summary": summary_counts(rows), "features": rows}

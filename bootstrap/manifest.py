"""Manifest schema for lakebase-solutions core components and modules.

Every deployable unit (a ``core/<name>/`` component or a ``modules/<name>/``
module) ships a ``module.yaml`` manifest that this module parses and validates.
The manifest is the contract the orchestrator relies on for discovery,
dependency ordering, parameter collection, and teardown -- which is why
**adding a module is a matter of dropping a folder + manifest, with no edits
to the deploy notebook**.

Schema is built on stdlib :mod:`dataclasses` with hand-written validation so
the tool runs on **any** Databricks runtime with no pip installs and no
third-party validation library. ``load_manifest`` raises
:class:`ManifestError` on any malformed manifest; ``validate_manifest`` layers
a few semantic checks on top of the schema.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml

__all__ = [
    "ManifestError",
    "Parameter",
    "Feature",
    "DependsOn",
    "Manifest",
    "load_manifest",
    "validate_manifest",
    "MATURITIES",
]

# Component/module names namespace resources, PG roles, app names, secret keys,
# etc., so keep them to a conservative slug (leading underscore allowed for
# reference modules like ``_canary``).
_NAME_RE = re.compile(r"^[a-z_][a-z0-9_-]*$")

# Supported parameter widget/coercion types (mirrors the deploy notebook's
# dbutils widget kinds).
ParameterType = Literal["string", "int", "float", "bool", "multiselect"]
_PARAMETER_TYPES = ("string", "int", "float", "bool", "multiselect")

# A component is either always-on infrastructure (``core``) or an optional,
# per-engagement workshop unit (``module``).
ComponentKind = Literal["core", "module"]
_COMPONENT_KINDS = ("core", "module")

# Databricks feature-maturity levels a component may declare. The gate allows
# Public-Preview-or-better; maturity is surfaced to customers via the feature
# matrix (see bootstrap/features.py) -- so nothing is hidden, but nothing below
# Public Preview should be relied on.
Maturity = Literal["GA", "PUBLIC_PREVIEW", "BETA"]
MATURITIES = ("GA", "PUBLIC_PREVIEW", "BETA")


class ManifestError(Exception):
    """Raised when a ``module.yaml`` is missing, unreadable, or invalid."""


def _reject_unknown_keys(data: Dict[str, Any], allowed: set, context: str) -> None:
    """Enforce ``extra="forbid"`` semantics for a mapping.

    Raises :class:`ManifestError` if ``data`` carries any key not in ``allowed``.
    """

    extra = set(data) - allowed
    if extra:
        raise ManifestError(
            f"unknown {context} field(s): {sorted(extra)!r} "
            f"(allowed: {sorted(allowed)!r})"
        )


@dataclass
class Parameter:
    """A single deploy-time parameter contributed by a component/module.

    Parameters render in the deploy notebook in three tiers (see ``deploy.py``):

    * ``required=True``  -- a prominent widget with NO default; must be non-empty.
    * default (neither flag) -- a widget carrying ``default``, labelled optional.
    * ``advanced=True``  -- no widget; read only from ``config.yaml``.
    """

    name: str
    type: ParameterType = "string"
    default: Any = None
    required: bool = False
    advanced: bool = False
    # Human-friendly display name for the widget (falls back to ``name``).
    label: Optional[str] = None
    # Longer explanation of the parameter (shown in the param summary table).
    help: str = ""
    # Only meaningful for ``multiselect`` (and optionally ``string``) params.
    choices: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Any) -> "Parameter":
        """Build a :class:`Parameter` from a YAML mapping (``extra="forbid"``)."""

        if not isinstance(data, dict):
            raise ManifestError(
                f"parameter must be a mapping, got {type(data).__name__}"
            )
        allowed = {f.name for f in fields(cls)}
        _reject_unknown_keys(data, allowed, "parameter")
        try:
            return cls(**data)
        except TypeError as exc:  # missing required 'name', etc.
            raise ManifestError(f"invalid parameter {data!r}: {exc}") from exc


@dataclass
class Feature:
    """A Databricks feature a component provisions, with its maturity.

    Aggregated across all manifests into a customer-facing feature matrix so it
    is always clear what is GA vs. Public Preview vs. Beta. ``name`` is the
    human-facing capability (e.g. "Lakebase autoscaling", "Genie spaces"); an
    optional ``note`` carries a caveat or doc pointer.
    """

    name: str
    maturity: Maturity = "GA"
    note: str = ""

    @classmethod
    def from_dict(cls, data: Any) -> "Feature":
        """Build a :class:`Feature` from a YAML mapping (``extra="forbid"``)."""

        if not isinstance(data, dict):
            raise ManifestError(f"feature must be a mapping, got {type(data).__name__}")
        allowed = {f.name for f in fields(cls)}
        _reject_unknown_keys(data, allowed, "feature")
        try:
            feature = cls(**data)
        except TypeError as exc:  # missing required 'name', etc.
            raise ManifestError(f"invalid feature {data!r}: {exc}") from exc
        if feature.maturity not in MATURITIES:
            raise ManifestError(
                f"invalid feature maturity {feature.maturity!r} for {feature.name!r}: "
                f"must be one of {list(MATURITIES)!r}"
            )
        return feature


@dataclass
class DependsOn:
    """Declared dependencies, split by the kind of thing depended upon.

    ``core`` names must resolve to ``core/<name>`` components; ``modules`` names
    to ``modules/<name>`` modules. A ``core`` component may only depend on other
    ``core`` components (enforced by the DAG builder) so that core always
    deploys before any module.
    """

    core: List[str] = field(default_factory=list)
    modules: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Any) -> "DependsOn":
        """Build a :class:`DependsOn` from a YAML mapping (``extra="forbid"``)."""

        if not isinstance(data, dict):
            raise ManifestError(
                f"depends_on must be a mapping, got {type(data).__name__}"
            )
        allowed = {f.name for f in fields(cls)}
        _reject_unknown_keys(data, allowed, "depends_on")
        return cls(**data)


@dataclass
class Manifest:
    """Parsed ``module.yaml`` for one core component or module."""

    # ``name`` and ``kind`` are required; the sentinel defaults keep the
    # dataclass importable on any Python (a required field cannot follow a
    # defaulted one) and are rejected as invalid in ``__post_init__``.
    name: str = ""
    version: str = "0.1.0"
    kind: ComponentKind = ""  # type: ignore[assignment]
    personas: List[str] = field(default_factory=list)
    # ``core`` components are always deployed regardless of this flag; for
    # ``module`` units this controls whether the deploy notebook pre-selects it.
    enabled_by_default: bool = False
    depends_on: DependsOn = field(default_factory=DependsOn)
    parameters: List[Parameter] = field(default_factory=list)
    # Free-form declaration of resources this unit provides (for teardown and
    # as-built reporting), e.g. ``{"database_instance": ["${prefix}-lakebase"]}``.
    provides: Dict[str, Any] = field(default_factory=dict)
    # Databricks features this unit provisions, with maturity, for the
    # customer-facing feature matrix (see bootstrap/features.py).
    features: List[Feature] = field(default_factory=list)
    entrypoint: str = "deploy.py"
    teardown: str = "teardown.py"
    health_check: str = "health.py"
    # Optional two-phase note (used by data_api): a manual UI-enable step plus a
    # re-runnable configure step. See SPEC section 4.
    two_phase: Optional[Dict[str, str]] = None

    # Populated by ``load_manifest`` with the directory the manifest was read
    # from (so the orchestrator can locate entrypoint/teardown/health files).
    # Excluded from serialization; never present in the YAML itself.
    source_dir: Optional[Path] = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _NAME_RE.match(self.name):
            raise ManifestError(
                f"invalid component name {self.name!r}: must match {_NAME_RE.pattern}"
            )
        if self.kind not in _COMPONENT_KINDS:
            raise ManifestError(
                f"invalid kind {self.kind!r}: must be one of {list(_COMPONENT_KINDS)!r}"
            )


# Top-level keys accepted from a ``module.yaml``. ``source_dir`` is intentionally
# excluded -- it is set by ``load_manifest`` and never present in the YAML.
_MANIFEST_INPUT_KEYS = {
    "name",
    "version",
    "kind",
    "personas",
    "enabled_by_default",
    "depends_on",
    "parameters",
    "provides",
    "features",
    "entrypoint",
    "teardown",
    "health_check",
    "two_phase",
}


def load_manifest(path: str | Path) -> Manifest:
    """Read and validate a ``module.yaml`` file.

    Raises :class:`ManifestError` if the file is missing, not valid YAML, not a
    mapping, or fails schema validation. On success, ``source_dir`` is set to
    the manifest's parent directory.
    """

    manifest_path = Path(path)
    if not manifest_path.is_file():
        raise ManifestError(f"manifest not found: {manifest_path}")

    try:
        raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:  # pragma: no cover - defensive
        raise ManifestError(f"invalid YAML in {manifest_path}: {exc}") from exc

    if raw is None:
        raise ManifestError(f"empty manifest: {manifest_path}")
    if not isinstance(raw, dict):
        raise ManifestError(
            f"manifest {manifest_path} must be a mapping, got {type(raw).__name__}"
        )

    # ``extra="forbid"`` at the top level.
    _reject_unknown_keys(raw, _MANIFEST_INPUT_KEYS, "manifest")

    # Build nested typed fields explicitly from the raw mapping.
    data = dict(raw)
    if "depends_on" in data:
        data["depends_on"] = DependsOn.from_dict(data["depends_on"])
    if "parameters" in data:
        params = data["parameters"]
        if not isinstance(params, list):
            raise ManifestError(
                f"parameters must be a list, got {type(params).__name__}"
            )
        data["parameters"] = [Parameter.from_dict(p) for p in params]
    if "features" in data:
        feats = data["features"]
        if not isinstance(feats, list):
            raise ManifestError(f"features must be a list, got {type(feats).__name__}")
        data["features"] = [Feature.from_dict(f) for f in feats]

    try:
        manifest = Manifest(**data)
    except TypeError as exc:
        raise ManifestError(f"invalid manifest {manifest_path}: {exc}") from exc

    manifest.source_dir = manifest_path.parent
    return manifest


def validate_manifest(manifest: Manifest) -> None:
    """Semantic validation layered on top of the dataclass schema.

    Schema-level constraints (required fields, allowed ``kind`` values, the
    name pattern) are already enforced at construction time; this adds
    cross-field checks that the schema cannot express. Raises
    :class:`ManifestError` on failure.
    """

    if not manifest.entrypoint.strip():
        raise ManifestError(f"{manifest.name}: entrypoint must be non-empty")
    if not manifest.teardown.strip():
        raise ManifestError(f"{manifest.name}: teardown must be non-empty")
    if not manifest.health_check.strip():
        raise ManifestError(f"{manifest.name}: health_check must be non-empty")

    # A core component must not depend on a module -- that would violate the
    # core-before-modules invariant the whole architecture rests on.
    if manifest.kind == "core" and manifest.depends_on.modules:
        raise ManifestError(
            f"core component {manifest.name!r} may not depend on modules "
            f"{manifest.depends_on.modules!r}"
        )

    # A unit may not declare itself as a dependency.
    all_deps = list(manifest.depends_on.core) + list(manifest.depends_on.modules)
    if manifest.name in all_deps:
        raise ManifestError(f"{manifest.name!r} declares a dependency on itself")

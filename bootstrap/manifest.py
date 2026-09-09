"""Manifest schema for lakebase-solutions core components and modules.

Every deployable unit (a ``core/<name>/`` component or a ``modules/<name>/``
module) ships a ``module.yaml`` manifest that this module parses and validates.
The manifest is the contract the orchestrator relies on for discovery,
dependency ordering, parameter collection, and teardown -- which is why
**adding a module is a matter of dropping a folder + manifest, with no edits
to the deploy notebook**.

Schema is a pydantic v2 model so validation is declarative and errors are
precise. ``load_manifest`` raises :class:`ManifestError` on any malformed
manifest; ``validate_manifest`` layers a few semantic checks on top of the
schema.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

__all__ = [
    "ManifestError",
    "Parameter",
    "DependsOn",
    "Manifest",
    "load_manifest",
    "validate_manifest",
]

# Component/module names namespace resources, PG roles, app names, secret keys,
# etc., so keep them to a conservative slug (leading underscore allowed for
# reference modules like ``_canary``).
_NAME_RE = re.compile(r"^[a-z_][a-z0-9_-]*$")

# Supported parameter widget/coercion types (mirrors the deploy notebook's
# dbutils widget kinds).
ParameterType = Literal["string", "int", "float", "bool", "multiselect"]

# A component is either always-on infrastructure (``core``) or an optional,
# per-engagement workshop unit (``module``).
ComponentKind = Literal["core", "module"]


class ManifestError(Exception):
    """Raised when a ``module.yaml`` is missing, unreadable, or invalid."""


class Parameter(BaseModel):
    """A single deploy-time parameter contributed by a component/module.

    Parameters render in the deploy notebook in three tiers (see ``deploy.py``):

    * ``required=True``  -- a prominent widget with NO default; must be non-empty.
    * default (neither flag) -- a widget carrying ``default``, labelled optional.
    * ``advanced=True``  -- no widget; read only from ``config.yaml``.
    """

    model_config = ConfigDict(extra="forbid")

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
    choices: List[str] = Field(default_factory=list)


class DependsOn(BaseModel):
    """Declared dependencies, split by the kind of thing depended upon.

    ``core`` names must resolve to ``core/<name>`` components; ``modules`` names
    to ``modules/<name>`` modules. A ``core`` component may only depend on other
    ``core`` components (enforced by the DAG builder) so that core always
    deploys before any module.
    """

    model_config = ConfigDict(extra="forbid")

    core: List[str] = Field(default_factory=list)
    modules: List[str] = Field(default_factory=list)


class Manifest(BaseModel):
    """Parsed ``module.yaml`` for one core component or module."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str = "0.1.0"
    kind: ComponentKind
    personas: List[str] = Field(default_factory=list)
    # ``core`` components are always deployed regardless of this flag; for
    # ``module`` units this controls whether the deploy notebook pre-selects it.
    enabled_by_default: bool = False
    depends_on: DependsOn = Field(default_factory=DependsOn)
    parameters: List[Parameter] = Field(default_factory=list)
    # Free-form declaration of resources this unit provides (for teardown and
    # as-built reporting), e.g. ``{"database_instance": ["${prefix}-lakebase"]}``.
    provides: Dict[str, Any] = Field(default_factory=dict)
    entrypoint: str = "deploy.py"
    teardown: str = "teardown.py"
    health_check: str = "health.py"
    # Optional two-phase note (used by data_api): a manual UI-enable step plus a
    # re-runnable configure step. See SPEC section 4.
    two_phase: Optional[Dict[str, str]] = None

    # Populated by ``load_manifest`` with the directory the manifest was read
    # from (so the orchestrator can locate entrypoint/teardown/health files).
    # Excluded from serialization; never present in the YAML itself.
    source_dir: Optional[Path] = Field(default=None, exclude=True)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if not _NAME_RE.match(value):
            raise ValueError(
                f"invalid component name {value!r}: must match {_NAME_RE.pattern}"
            )
        return value


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

    try:
        manifest = Manifest(**raw)
    except ValidationError as exc:
        raise ManifestError(f"invalid manifest {manifest_path}:\n{exc}") from exc

    manifest.source_dir = manifest_path.parent
    return manifest


def validate_manifest(manifest: Manifest) -> None:
    """Semantic validation layered on top of the pydantic schema.

    Schema-level constraints (types, required fields, allowed ``kind`` values)
    are already enforced at construction time; this adds cross-field checks that
    the schema cannot express. Raises :class:`ManifestError` on failure.
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

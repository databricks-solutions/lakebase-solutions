"""Deploy context: the shared state threaded through every deploy/teardown step.

``DeployContext`` carries the per-deployment identity (``deployment_id`` /
prefix), the run ``mode``, cloud/region, the collected parameter values, a map
of resolved resource names, and a logger. It also lazily exposes a Databricks
``WorkspaceClient`` for the components that need it -- but constructs nothing and
makes no API calls until asked (P0 is a scaffold; live calls land in P1+).

All resource names are namespaced by ``deployment_id`` so multiple deployments
can coexist in one workspace (see SPEC section 5).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

__all__ = ["DeployContext", "get_logger"]

_LOGGER_NAME = "lakebase_solutions"


def get_logger() -> logging.Logger:
    """Return the shared lakebase-solutions logger (configured once)."""

    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


@dataclass
class DeployContext:
    """Shared, per-deployment state passed to every component step."""

    deployment_id: str
    mode: str = "deploy"
    cloud: str = "aws"
    region: str = "us-west-2"
    params: Dict[str, Any] = field(default_factory=dict)
    resolved_names: Dict[str, str] = field(default_factory=dict)
    logger: logging.Logger = field(default_factory=get_logger)

    # Cached WorkspaceClient; built lazily, never at construction time.
    _workspace_client: Optional[Any] = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        # ``prefix`` is an alias for ``deployment_id`` used throughout the spec.
        self.resolved_names.setdefault("prefix", self.deployment_id)
        self.resolve_names()

    # -- name resolution ---------------------------------------------------

    def name(self, suffix: str) -> str:
        """Return a prefix-namespaced resource name: ``<deployment_id>-<suffix>``."""

        return f"{self.deployment_id}-{suffix}"

    def resolve_names(self) -> Dict[str, str]:
        """Populate the standard prefix-derived resource names (idempotent).

        These mirror SPEC section 5. Components read from ``resolved_names`` so
        naming stays consistent across the notebook, DABs variables, and SQL.
        """

        derived = {
            "prefix": self.deployment_id,
            "lakebase_instance": self.name("lakebase"),
            "secret_scope": self.name("secrets"),
            "admin_group": self.name("admins"),
            "workshop_group": self.name("participants"),
            "admin_app": self.name("admin-app"),
        }
        for key, value in derived.items():
            self.resolved_names.setdefault(key, value)
        return self.resolved_names

    # -- lazy Databricks client -------------------------------------------

    def get_workspace_client(self) -> Any:
        """Lazily import and construct a Databricks ``WorkspaceClient``.

        The import is deferred so this file stays importable off-Databricks and
        in CI (where ``databricks-sdk`` may be absent and no auth is configured).
        Constructing the client performs no API call; live calls are made by the
        individual component steps in later phases.

        TODO(P1): thread real auth/config through here (profile, host, SP).
        """

        if self._workspace_client is None:
            from databricks.sdk import WorkspaceClient  # lazy import

            self._workspace_client = WorkspaceClient()
        return self._workspace_client

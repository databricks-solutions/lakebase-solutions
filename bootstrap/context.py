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
from typing import Any, Callable, Dict, Optional

__all__ = ["DeployContext", "LiveClientUnavailable", "get_logger"]

_LOGGER_NAME = "lakebase_solutions"


class LiveClientUnavailable(RuntimeError):
    """Raised when a step needs a live workspace client / PG connection but none
    was injected.

    Guards against accidental live calls off-Databricks: the P1 live run happens
    *in-workspace*, where the deploy notebook injects real factories (see
    ``bootstrap/adapters.py``); unit tests inject fakes. When neither is present,
    asking for a client raises this rather than silently building one and
    reaching out to a real workspace.
    """


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

    # -- injectable live-access adapters (P1) -----------------------------
    # Live access is INJECTABLE so steps stay unit-testable off-Databricks.
    # Both default to ``None``: a live call then raises ``LiveClientUnavailable``
    # instead of touching a real workspace. The in-workspace deploy notebook
    # wires real factories (``bootstrap/adapters.py``); tests inject fakes.
    #   * ``workspace_client_factory() -> WorkspaceClient``
    #   * ``pg_connection_factory(ctx, role=..., **kw) -> psycopg connection``
    workspace_client_factory: Optional[Callable[[], Any]] = field(
        default=None, repr=False, compare=False
    )
    pg_connection_factory: Optional[Callable[..., Any]] = field(
        default=None, repr=False, compare=False
    )

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
            # Autoscaling Lakebase surface: the `postgres` PROJECT id is the bare
            # prefix (SPEC section 5). Creating the project auto-provisions a
            # `production` branch + `primary` read-write endpoint.
            "lakebase_project": self.deployment_id,
            "secret_scope": self.name("secrets"),
            "admin_group": self.name("admins"),
            "workshop_group": self.name("participants"),
            "admin_app": self.name("admin-app"),
            # Workshop Postgres namespace + prefix-namespaced PG roles. Roles use
            # ``_`` (not ``-``) but the prefix may still contain hyphens, so all
            # identifiers are double-quoted where they appear in SQL.
            "workshop_schema": "workshop",
            "pg_app_role": f"{self.deployment_id}_app",
            "pg_readonly_role": f"{self.deployment_id}_readonly",
            "pg_participant_role": f"{self.deployment_id}_participant",
        }
        for key, value in derived.items():
            self.resolved_names.setdefault(key, value)
        return self.resolved_names

    # -- injectable live access (P1) --------------------------------------

    def has_workspace_client(self) -> bool:
        """True if a workspace client (or its factory) has been injected."""

        return self._workspace_client is not None or self.workspace_client_factory is not None

    def has_pg_connection(self) -> bool:
        """True if a PG-connection factory has been injected."""

        return self.pg_connection_factory is not None

    def is_live(self) -> bool:
        """True when both live adapters are available (a real or fake run).

        Steps consult this to decide whether to execute live logic or to log
        intent and return a ``stub`` result (e.g. the P0 orchestrator smoke
        tests, which inject nothing).
        """

        return self.has_workspace_client() and self.has_pg_connection()

    def workspace_client(self) -> Any:
        """Return the injected Databricks ``WorkspaceClient`` (guarded, lazy).

        Injection: pass ``workspace_client_factory=...`` to ``DeployContext``
        (the in-workspace notebook passes
        ``adapters.default_workspace_client_factory``; tests pass a fake).
        Raises :class:`LiveClientUnavailable` when nothing is injected -- no
        accidental live calls.
        """

        if self._workspace_client is None:
            if self.workspace_client_factory is None:
                raise LiveClientUnavailable(
                    "no workspace client configured -- P1 live run happens "
                    "in-workspace; inject workspace_client_factory (tests inject a fake)"
                )
            self._workspace_client = self.workspace_client_factory()
        return self._workspace_client

    def pg_connection(self, role: Optional[str] = None, **kwargs: Any) -> Any:
        """Return a psycopg connection for ``role`` (guarded, injectable).

        The real factory (``adapters.default_pg_connection_factory``) resolves
        the autoscaling ``postgres`` endpoint, obtains a connection credential
        from ``generate-database-credential``, and connects over psycopg; tests
        inject a fake connection that records executed SQL. Raises
        :class:`LiveClientUnavailable` when no factory is injected.
        """

        if self.pg_connection_factory is None:
            raise LiveClientUnavailable(
                "no pg connection configured -- P1 live run happens in-workspace; "
                "inject pg_connection_factory (tests inject a fake)"
            )
        return self.pg_connection_factory(self, role=role, **kwargs)

    # Backward-compatible alias for the pre-P1 accessor name. Unlike
    # ``workspace_client()``, this one falls back to building a *real* client
    # in-workspace, preserving the P0 behaviour for any legacy caller.
    def get_workspace_client(self) -> Any:
        if self.workspace_client_factory is None and self._workspace_client is None:
            from .adapters import default_workspace_client_factory

            self.workspace_client_factory = default_workspace_client_factory
        return self.workspace_client()

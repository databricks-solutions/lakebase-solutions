"""Offline test doubles for the P1 live-access adapters.

These fakes let the ``core/lakebase`` and ``core/security`` steps run with NO
network and NO Databricks workspace:

* :class:`FakeConnection` / :class:`FakeCursor` record every executed SQL
  statement (and bound params) and serve canned ``fetch`` results.
* :class:`FakeWorkspaceClient` records secret puts/deletes and serves a fake
  ``database`` surface (credential + instance host).

``load_step`` imports a step file exactly the way the orchestrator does (by file
path, flat) so tests exercise the same module objects without needing ``core``
to be an importable package. ``live_context`` wires the fakes into a
:class:`~bootstrap.context.DeployContext`.

Not a test module itself (no ``test_`` prefix), so pytest never collects it.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple

from bootstrap.context import DeployContext

ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Step loader (mirrors bootstrap.orchestrator._load_step_callable)
# --------------------------------------------------------------------------- #
def load_step(component: str, filename: str) -> ModuleType:
    """Import ``core/<component>/<filename>`` flat, by path (as the orchestrator does)."""

    path = ROOT / "core" / component / filename
    mod_name = f"lakebase_solutions_test_{component}_{path.stem}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec and spec.loader, f"could not load {path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------- #
# Fake psycopg connection / cursor
# --------------------------------------------------------------------------- #
class FakeCursor:
    """Records executed SQL; serves queued ``fetchone``/``fetchall`` results."""

    def __init__(
        self,
        fetchone_results: Optional[List[Any]] = None,
        fetchall_results: Optional[List[Any]] = None,
    ) -> None:
        self.executed: List[Tuple[str, Any]] = []
        self._one: List[Any] = list(fetchone_results or [])
        self._all: List[Any] = list(fetchall_results or [])

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> Any:
        return self._one.pop(0) if self._one else (1,)

    def fetchall(self) -> Any:
        return self._all.pop(0) if self._all else []

    def close(self) -> None:  # pragma: no cover - trivial
        pass

    def __enter__(self) -> "FakeCursor":  # pragma: no cover - convenience
        return self

    def __exit__(self, *exc: Any) -> bool:  # pragma: no cover - convenience
        return False


class FakeConnection:
    def __init__(self, cursor: Optional[FakeCursor] = None) -> None:
        self._cursor = cursor or FakeCursor()
        self.committed = 0
        self.rolled_back = 0

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1

    def close(self) -> None:  # pragma: no cover - trivial
        pass

    @property
    def executed(self) -> List[Tuple[str, Any]]:
        return self._cursor.executed

    def executed_sql(self) -> List[str]:
        return [sql for sql, _ in self._cursor.executed]


# --------------------------------------------------------------------------- #
# Fake workspace client (secrets + database surfaces)
# --------------------------------------------------------------------------- #
class FakeSecrets:
    def __init__(self) -> None:
        self.put: List[Tuple[str, str, str]] = []
        self.deleted: List[Tuple[str, str]] = []
        self.scopes_created: List[str] = []
        self.scopes_deleted: List[str] = []

    def create_scope(self, scope: str, **_kw: Any) -> None:
        self.scopes_created.append(scope)

    def delete_scope(self, scope: str, **_kw: Any) -> None:
        self.scopes_deleted.append(scope)

    def put_secret(self, scope: str, key: str, string_value: str) -> None:
        self.put.append((scope, key, string_value))

    def delete_secret(self, scope: str, key: str) -> None:
        self.deleted.append((scope, key))

    def keys_written(self) -> List[str]:
        return [key for _, key, _ in self.put]

    def value_for(self, key: str) -> Optional[str]:
        for _, k, v in self.put:
            if k == key:
                return v
        return None


class FakeNotFound(Exception):
    """Fake 404 raised by :class:`FakeApiClient` for a missing postgres project.

    Mirrors ``databricks.sdk.errors.NotFound`` enough for the deploy step's
    offline-safe 404 detection: carries ``error_code`` + ``status_code``.
    """

    def __init__(self, message: str = "RESOURCE_DOES_NOT_EXIST") -> None:
        super().__init__(message)
        self.error_code = "RESOURCE_DOES_NOT_EXIST"
        self.status_code = 404


class FakeApiClient:
    """Fake ``w.api_client`` for the autoscaling Postgres REST API.

    Records every ``do(method, path, body=..., query=..., headers=...)`` call as
    ``(method, path, body)`` in ``.calls`` and returns canned parsed-dict
    responses keyed by path (matching the real REST shapes):

    * ``GET  /api/2.0/postgres/projects/<id>`` -> a project dict when
      ``project_exists`` is True, else raises :class:`FakeNotFound` (so the deploy
      step exercises its create path);
    * ``POST /api/2.0/postgres/projects`` -> ``{}``;
    * ``GET  .../branches/production/endpoints`` -> one endpoint with
      ``status.hosts.host`` + ``status.current_state``;
    * ``PATCH .../endpoints/primary`` -> ``{}``;
    * ``POST /api/2.0/postgres/credentials`` -> ``{"token": <fake jwt>}``;
    * ``DELETE /api/2.0/postgres/projects/<id>`` -> ``{}``.
    """

    def __init__(
        self,
        host: str = "host.example",
        token: str = "oauth-token-xyz",
        endpoint: str = "primary",
        project_exists: bool = False,
    ) -> None:
        self._host = host
        self._token = token
        self._endpoint = endpoint
        self._project_exists = project_exists
        self.calls: List[Tuple[str, str, Any]] = []

    def do(
        self,
        method: str,
        path: str,
        body: Any = None,
        query: Any = None,
        headers: Any = None,
    ) -> Dict[str, Any]:
        self.calls.append((method, path, body))
        m = method.upper()
        p = path.rstrip("/")

        if m == "POST" and p.endswith("/postgres/credentials"):
            return {"token": self._token}
        if m == "POST" and p.endswith("/postgres/projects"):
            return {}
        if m == "GET" and p.endswith("/endpoints"):
            return {
                "endpoints": [
                    {"status": {"hosts": {"host": self._host}, "current_state": "AVAILABLE"}}
                ]
            }
        if m == "PATCH" and "/endpoints/" in p:
            return {}
        if m == "DELETE" and "/postgres/projects/" in p:
            return {}
        if m == "GET" and "/postgres/projects/" in p:
            if self._project_exists:
                return {"project_id": p.rsplit("/", 1)[-1], "spec": {}}
            raise FakeNotFound()
        return {}

    def calls_for(self, method: str) -> List[Tuple[str, str, Any]]:
        """Recorded ``(method, path, body)`` tuples for a given HTTP method."""

        return [c for c in self.calls if c[0].upper() == method.upper()]


class _Me:
    def __init__(self, user_name: str) -> None:
        self.user_name = user_name


class FakeCurrentUser:
    """Fake ``w.current_user``: the connecting identity is the workspace email."""

    def __init__(self, user_name: str = "admin@example.com") -> None:
        self._user_name = user_name
        self.me_calls = 0

    def me(self) -> _Me:
        self.me_calls += 1
        return _Me(self._user_name)


class _App:
    def __init__(self, name: str, url: str = "https://app.example") -> None:
        self.name = name
        self.url = url
        self.app_status = "RUNNING"


class FakeApps:
    """Minimal ``w.apps`` surface: create / get / deploy / delete with recording.

    ``get`` raises ``KeyError`` until the app has been created (so the admin_app
    step's create-if-missing path is exercised); ``create`` registers it.
    """

    def __init__(self) -> None:
        self.created: List[str] = []
        self.deployed: List[Tuple[str, Optional[str]]] = []
        self.deleted: List[str] = []
        self._apps: Dict[str, _App] = {}

    def get(self, name: str, **_kw: Any) -> _App:
        if name not in self._apps:
            raise KeyError(f"app not found: {name}")
        return self._apps[name]

    def create(self, name: str, **_kw: Any) -> _App:
        self.created.append(name)
        app = _App(name)
        self._apps[name] = app
        return app

    def deploy(self, app_name: str, source_code_path: Optional[str] = None, **_kw: Any) -> None:
        self.deployed.append((app_name, source_code_path))

    def delete(self, name: str, **_kw: Any) -> None:
        self.deleted.append(name)
        self._apps.pop(name, None)


class FakeWorkspaceClient:
    def __init__(self, email: str = "admin@example.com", host: str = "host.example") -> None:
        self.secrets = FakeSecrets()
        self.api_client = FakeApiClient(host=host)
        self.current_user = FakeCurrentUser(email)
        self.apps = FakeApps()


# --------------------------------------------------------------------------- #
# Context wiring
# --------------------------------------------------------------------------- #
def live_context(
    deployment_id: str = "acme-ws",
    conn: Optional[FakeConnection] = None,
    ws: Optional[FakeWorkspaceClient] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[DeployContext, FakeConnection, FakeWorkspaceClient]:
    """Build a ``DeployContext`` with fake live adapters injected."""

    conn = conn if conn is not None else FakeConnection()
    ws = ws if ws is not None else FakeWorkspaceClient()
    merged = {"database": "databricks_postgres"}
    merged.update(params or {})
    ctx = DeployContext(
        deployment_id=deployment_id,
        params=merged,
        workspace_client_factory=lambda: ws,
        pg_connection_factory=lambda c, role=None, **kw: conn,
    )
    return ctx, conn, ws

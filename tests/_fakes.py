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


class _Credential:
    def __init__(self, token: str) -> None:
        self.token = token


class _Host:
    def __init__(self, host: str) -> None:
        self.host = host


class _EndpointStatus:
    def __init__(self, hosts: List[_Host]) -> None:
        self.hosts = hosts


class _Endpoint:
    def __init__(self, name: str, host: str) -> None:
        self.name = name
        self.status = _EndpointStatus([_Host(host)])


class _ListEndpointsResponse:
    def __init__(self, endpoints: List[_Endpoint]) -> None:
        self.endpoints = endpoints


class _Project:
    def __init__(self, name: str) -> None:
        self.name = name


class FakePostgres:
    """Fake ``w.postgres`` surface (autoscaling): provisioning + lookup + credential.

    Provisioning: ``create_project`` (records the project id; auto-creates the
    branch/endpoint in the real API), ``create_endpoint`` / ``update_endpoint``
    (records the endpoint resource + autoscaling CU range), ``get_project``
    (records the poll). Lookup: ``list_endpoints`` returns a response exposing
    ``.endpoints`` (each with a hierarchical ``name`` and ``status.hosts[*].host``).
    ``generate_database_credential`` mints an OAuth token for an endpoint
    resource path. ``delete_project`` records teardown.
    """

    def __init__(
        self,
        host: str = "host.example",
        token: str = "oauth-token-xyz",
        branch: str = "production",
        endpoint: str = "primary",
    ) -> None:
        self._host = host
        self._token = token
        self._branch = branch
        self._endpoint = endpoint
        self.list_calls: List[str] = []
        self.cred_calls: List[str] = []
        self.create_project_calls: List[str] = []
        self.get_project_calls: List[str] = []
        self.delete_project_calls: List[str] = []
        self.create_endpoint_calls: List[Tuple[str, Any, Any]] = []
        self.update_endpoint_calls: List[Tuple[str, Any, Any]] = []

    def create_project(self, name: str, **_kw: Any) -> _Project:
        self.create_project_calls.append(name)
        return _Project(name)

    def get_project(self, name: str, **_kw: Any) -> _Project:
        self.get_project_calls.append(name)
        return _Project(name)

    def delete_project(self, name: str, **_kw: Any) -> None:
        self.delete_project_calls.append(name)

    def create_endpoint(
        self,
        name: str,
        autoscaling_limit_min_cu: Any = None,
        autoscaling_limit_max_cu: Any = None,
        **_kw: Any,
    ) -> _Endpoint:
        self.create_endpoint_calls.append((name, autoscaling_limit_min_cu, autoscaling_limit_max_cu))
        return _Endpoint(name, self._host)

    def update_endpoint(
        self,
        name: str,
        autoscaling_limit_min_cu: Any = None,
        autoscaling_limit_max_cu: Any = None,
        **_kw: Any,
    ) -> _Endpoint:
        self.update_endpoint_calls.append((name, autoscaling_limit_min_cu, autoscaling_limit_max_cu))
        return _Endpoint(name, self._host)

    def list_endpoints(self, parent: str) -> _ListEndpointsResponse:
        self.list_calls.append(parent)
        name = f"{parent}/endpoints/{self._endpoint}"
        return _ListEndpointsResponse([_Endpoint(name, self._host)])

    def generate_database_credential(self, name: str) -> _Credential:
        self.cred_calls.append(name)
        return _Credential(self._token)


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
        self.postgres = FakePostgres(host=host)
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

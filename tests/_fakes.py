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

    def get_secret(self, scope: str, key: str) -> Any:
        """Return the most-recent put for ``key`` as a base64-``.value`` object.

        Mirrors the real ``w.secrets.get_secret`` shape (a ``.value`` that is a
        base64-encoded string), so step code that base64-decodes works offline.
        Raises ``KeyError`` when the key was never written (never/deleted).
        """

        import base64 as _b64

        latest = None
        for s, k, v in self.put:
            if s == scope and k == key:
                latest = v
        if latest is None:
            raise KeyError(f"secret not found: {scope}/{key}")

        class _Secret:
            value = _b64.b64encode(latest.encode("utf-8")).decode("ascii")

        return _Secret()

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
        app_exists: bool = False,
        data_api_enabled: bool = False,
        db_mgmt_id: str = "db-mgmt-1",
        pg_database: str = "databricks_postgres",
    ) -> None:
        self._host = host
        self._token = token
        self._endpoint = endpoint
        self._project_exists = project_exists
        self._app_exists = app_exists
        self._data_api_enabled = data_api_enabled
        self._db_mgmt_id = db_mgmt_id
        self._pg_database = pg_database
        # Autoscaling CU adopted by the last PATCH (echoed on subsequent GETs so
        # the deploy step's read-back verification passes offline).
        self._cu_min: Any = None
        self._cu_max: Any = None
        # Exposed schemas last set via the Data API PATCH.
        self._exposed_schemas: List[str] = []
        # Stateful stores so create -> list/health is consistent (genie/lakeview).
        self._genie: Dict[str, str] = {}
        self._dash: Dict[str, str] = {}
        self._seq = 0
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

        # --- Service principal OAuth (M2M) secrets REST ---
        if "/credentials/secrets/" in p and m == "DELETE":
            return {}
        if p.endswith("/credentials/secrets") and m == "POST":
            return {"id": "sec-1", "secret": "sp-oauth-secret-xyz", "status": "ACTIVE"}
        if p.endswith("/credentials/secrets") and m == "GET":
            return {"secrets": []}

        # --- Databricks Apps REST (/api/2.0/apps) ---
        if p.endswith("/deployments") and m == "POST":
            return {"deployment_id": "dep-1", "status": {"state": "SUCCEEDED"}}
        if "/deployments/" in p and m == "GET":
            return {"status": {"state": "SUCCEEDED"}}
        if p.endswith("/apps") and m == "POST":
            self._app_exists = True
            return {"name": (body or {}).get("name")}
        if "/apps/" in p and m == "DELETE":
            self._app_exists = False
            return {}
        if "/apps/" in p and m == "GET":
            if not self._app_exists:
                raise FakeNotFound()
            return {
                "name": p.rsplit("/", 1)[-1],
                "url": "https://app.example",
                "compute_status": {"state": "ACTIVE"},
                "app_status": {"state": "RUNNING"},
                "active_deployment": {"status": {"state": "SUCCEEDED"}},
            }

        # --- Data API management REST ---
        if p.endswith("/data-api") and m == "PATCH":
            spec = (body or {}).get("spec", {})
            self._exposed_schemas = list(spec.get("db_schemas", []))
            return {}
        if p.endswith("/data-api") and m == "GET":
            if not self._data_api_enabled:
                raise FakeNotFound()
            return {"status": {"url": f"https://{self._host}", "db_schemas": self._exposed_schemas}}
        if p.endswith("/databases") and m == "GET":
            return {
                "databases": [
                    {"status": {"database_id": self._db_mgmt_id, "postgres_database": self._pg_database}}
                ]
            }

        # --- Unity Catalog managed online catalogs ---
        if p.endswith("/database/catalogs") and m == "POST":
            return {"name": (body or {}).get("name")}
        if "/database/catalogs/" in p and m == "DELETE":
            return {}
        if "/database/catalogs/" in p and m == "GET":
            return {"name": p.rsplit("/", 1)[-1], "catalog_type": "MANAGED_ONLINE_CATALOG"}
        if p.endswith("/database/catalogs") and m == "GET":
            return {"catalogs": []}

        # --- SQL warehouses ---
        if p.endswith("/sql/warehouses") and m == "POST":
            return {"id": "wh-fake-1", "name": (body or {}).get("name"), "state": "STARTING"}
        if "/sql/warehouses/" in p and m == "DELETE":
            return {}
        if "/sql/warehouses/" in p and m == "GET":
            return {"id": p.rsplit("/", 1)[-1], "name": "fs-wh", "state": "RUNNING"}
        if p.endswith("/sql/warehouses") and m == "GET":
            return {"warehouses": []}

        # --- SQL statements ---
        if p.endswith("/sql/statements") and m == "POST":
            return {"status": {"state": "SUCCEEDED"}}

        # --- Genie spaces (stateful) ---
        if p.endswith("/genie/spaces") and m == "POST":
            self._seq += 1
            sid = f"genie-{self._seq}"
            self._genie[(body or {}).get("title")] = sid
            return {"space_id": sid}
        if "/genie/spaces/" in p and m == "DELETE":
            sid = p.rsplit("/", 1)[-1]
            self._genie = {k: v for k, v in self._genie.items() if v != sid}
            return {}
        if p.endswith("/genie/spaces") and m == "GET":
            return {"spaces": [{"title": t, "space_id": i} for t, i in self._genie.items()]}

        # --- Lakeview dashboards (stateful) ---
        if p.endswith("/published") and m == "POST":
            return {}
        if p.endswith("/lakeview/dashboards") and m == "POST":
            self._seq += 1
            did = f"dash-{self._seq}"
            self._dash[(body or {}).get("display_name")] = did
            return {"dashboard_id": did}
        if "/lakeview/dashboards/" in p and m == "DELETE":
            did = p.rsplit("/", 1)[-1]
            self._dash = {k: v for k, v in self._dash.items() if v != did}
            return {}
        if p.endswith("/lakeview/dashboards") and m == "GET":
            return {"dashboards": [{"display_name": n, "dashboard_id": i} for n, i in self._dash.items()]}

        # --- Autoscaling Postgres REST ---
        if m == "POST" and p.endswith("/postgres/credentials"):
            return {"token": self._token}
        if m == "GET" and p.endswith("/branches/production"):
            return {"uid": "branch-uid-1", "status": {"current_state": "READY"}}
        if m == "POST" and p.endswith("/postgres/projects"):
            return {}
        if m == "GET" and p.endswith("/endpoints"):
            status: Dict[str, Any] = {"hosts": {"host": self._host}, "current_state": "AVAILABLE"}
            if self._cu_min is not None:
                status["autoscaling_limit_min_cu"] = self._cu_min
                status["autoscaling_limit_max_cu"] = self._cu_max
            return {"endpoints": [{"status": status}]}
        if m == "PATCH" and "/endpoints/" in p:
            spec = (body or {}).get("spec", {})
            self._cu_min = spec.get("autoscaling_limit_min_cu", self._cu_min)
            self._cu_max = spec.get("autoscaling_limit_max_cu", self._cu_max)
            return {}
        if m == "DELETE" and "/postgres/projects/" in p:
            return {}
        if m == "GET" and "/postgres/projects/" in p:
            if self._project_exists:
                return {"project_id": p.rsplit("/", 1)[-1], "uid": "project-uid-1", "spec": {}}
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


class _SP:
    def __init__(self, sp_id: str, application_id: str) -> None:
        self.id = sp_id
        self.application_id = application_id


class _SPSecret:
    def __init__(self, secret_id: str, secret: str) -> None:
        self.id = secret_id
        self.secret = secret


class FakeServicePrincipals:
    """Minimal ``w.service_principals``: list / create / delete with recording."""

    def __init__(self) -> None:
        self.created: List[str] = []
        self.deleted: List[str] = []
        self._by_name: Dict[str, _SP] = {}
        self._seq = 0

    def list(self, filter: Optional[str] = None) -> List[_SP]:  # noqa: A002 - SDK arg name
        # Emulate `displayName eq "<name>"` filtering used by the data_api step.
        if filter and 'eq "' in filter:
            name = filter.split('eq "', 1)[1].rstrip('"')
            sp = self._by_name.get(name)
            return [sp] if sp else []
        return list(self._by_name.values())

    def create(self, display_name: str, **_kw: Any) -> _SP:
        self._seq += 1
        sp = _SP(sp_id=str(1000 + self._seq), application_id=f"app-{self._seq:04d}")
        self._by_name[display_name] = sp
        self.created.append(display_name)
        return sp

    def delete(self, id: str, **_kw: Any) -> None:  # noqa: A002 - SDK arg name
        self.deleted.append(id)
        for name, sp in list(self._by_name.items()):
            if sp.id == id:
                self._by_name.pop(name, None)


class FakeSecretsProxy:
    """Minimal ``w.service_principal_secrets_proxy``: create / list / delete."""

    def __init__(self) -> None:
        self._by_sp: Dict[Any, List[_SPSecret]] = {}
        self._seq = 0

    def create(self, service_principal_id: Any, lifetime: str = "3600s", **_kw: Any) -> _SPSecret:
        self._seq += 1
        sec = _SPSecret(secret_id=f"sec-{self._seq}", secret=f"oauth-secret-{self._seq}")
        self._by_sp.setdefault(service_principal_id, []).append(sec)
        return sec

    def list(self, service_principal_id: Any, **_kw: Any) -> List[_SPSecret]:
        return list(self._by_sp.get(service_principal_id, []))

    def delete(self, service_principal_id: Any, secret_id: str, **_kw: Any) -> None:
        self._by_sp[service_principal_id] = [
            s for s in self._by_sp.get(service_principal_id, []) if s.id != secret_id
        ]


class _Config:
    def __init__(self, host: str, token: str) -> None:
        self.host = host
        self.token = token


class FakeWorkspaceClient:
    def __init__(
        self,
        email: str = "admin@example.com",
        host: str = "host.example",
        api_client: Optional["FakeApiClient"] = None,
    ) -> None:
        self.secrets = FakeSecrets()
        self.api_client = api_client if api_client is not None else FakeApiClient(host=host)
        self.current_user = FakeCurrentUser(email)
        self.apps = FakeApps()
        self.service_principals = FakeServicePrincipals()
        self.service_principal_secrets_proxy = FakeSecretsProxy()
        self.config = _Config(host=f"https://{host}", token="fake-workspace-token")


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

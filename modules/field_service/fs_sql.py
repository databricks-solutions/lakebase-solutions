"""SQL asset utilities for the field_service module (ported from lakebase_fsm).

The module seeds its Postgres schema from the vendored ``assets/sql/*.sql``
files. This module provides:

* ``split_sql_statements`` — a dollar-quote-aware statement splitter (so ``DO
  $$ … $$`` blocks and ``CREATE FUNCTION … $$ LANGUAGE`` bodies stay intact),
* ``substitute_scale`` — replace ``{token}`` placeholders with row counts from a
  scale profile (the seed_volume param picks demo vs scale),
* ``SCALE_PROFILES`` — demo/scale row-count profiles,
* ``DATA_SQL_FILES`` — the schema/data files the ``data`` step applies, in
  dependency order (``lakebase_features.sql`` is applied by the ``features``
  step, not here),
* ``apply_sql`` — execute a list of statements best-effort (autocommit),
  returning the ones that failed so the caller can retry (a second pass resolves
  most cross-file ordering dependencies).

Pure stdlib; import-safe with no third-party deps.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

ASSETS_SQL_DIR = Path(__file__).resolve().parent / "assets" / "sql"

# Postgres schemas the data step creates (dropped CASCADE on teardown).
DATA_SCHEMAS = ["field_service", "ai_memory", "monitoring"]

# Schema/data files applied by the `data` step, in dependency order. The base
# schema first (its tables are referenced by the rest); lakebase_features.sql is
# intentionally excluded — the `features` step owns the SLA engine / MVs.
DATA_SQL_FILES: List[str] = [
    "field_service_schema.sql",
    "service_orders_and_appointments.sql",
    "fleet_management.sql",
    "fleet_fuel_and_costs.sql",
    "dispatch_optimization.sql",
    "gps_breadcrumbs.sql",
    "tier2_enhancements.sql",
    "tier3_enhancements.sql",
    "agent_memory.sql",
    "monitoring_views.sql",
    "data_api_demo.sql",
]

# Row-count profiles for the {token} placeholders in the seed SQL. `demo` is
# sized for a fast workshop deploy; `scale` for a realistic large dataset.
SCALE_PROFILES: Dict[str, Dict[str, int]] = {
    "demo": {
        "work_orders": 2000,
        "work_order_parts": 4000,
        "technicians": 120,
        "techs_per_region": 20,
        "customers": 1500,
        "equipment": 400,
        "appointments": 1500,
        "notes_creation": 2000,
        "notes_tech": 1000,
        "notes_resolution": 1000,
    },
    "scale": {
        "work_orders": 200000,
        "work_order_parts": 400000,
        "technicians": 2500,
        "techs_per_region": 420,
        "customers": 50000,
        "equipment": 20000,
        "appointments": 50000,
        "notes_creation": 200000,
        "notes_tech": 100000,
        "notes_resolution": 100000,
    },
}


def scale_profile(name: str) -> Dict[str, int]:
    """Return the row-count profile for ``name`` (falls back to demo)."""

    return SCALE_PROFILES.get((name or "demo").lower(), SCALE_PROFILES["demo"])


def split_sql_statements(sql: str) -> List[str]:
    """Split a SQL file into statements, respecting ``$$`` blocks and comments.

    Ported from lakebase_fsm: tracks ``$$`` dollar-quoting so function/DO bodies
    are not split on their internal semicolons; skips pure-comment fragments.
    """

    statements: List[str] = []
    current: List[str] = []
    in_dollar = False

    for line in sql.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--") and not in_dollar:
            current.append(line)
            continue
        if line.count("$$") % 2 == 1:
            in_dollar = not in_dollar
        current.append(line)
        if stripped.endswith(";") and not in_dollar:
            stmt = "\n".join(current).strip()
            if [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]:
                statements.append(stmt)
            current = []

    if current:
        stmt = "\n".join(current).strip()
        if [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]:
            statements.append(stmt)
    return statements


def substitute_scale(sql: str, scale: Dict[str, int]) -> str:
    """Replace ``{token}`` placeholders with values from a scale profile."""

    for key, value in scale.items():
        sql = sql.replace("{" + key + "}", str(value))
    return sql


def read_asset_sql(filename: str) -> str:
    """Read one vendored SQL asset by filename."""

    return (ASSETS_SQL_DIR / filename).read_text(encoding="utf-8")


def apply_sql(cur: Any, statements: List[str], logger: Any, label: str) -> List[str]:
    """Execute ``statements`` best-effort on an autocommit cursor.

    Returns the statements that raised (so the caller can retry a second pass —
    cross-file ordering means an early statement may reference an object created
    later). Never raises for a single failed statement.
    """

    failed: List[str] = []
    for stmt in statements:
        try:
            cur.execute(stmt)
        except Exception as exc:  # best-effort; collect for retry
            failed.append(stmt)
            logger.info("field_service.data: [%s] deferred statement: %s", label, str(exc)[:120])
    return failed


def apply_sql_files(cur: Any, filenames: List[str], scale: Dict[str, int], logger: Any) -> Tuple[int, int]:
    """Apply an ordered list of SQL asset files (two passes for ordering).

    Returns ``(applied, still_failing)``. Statements that fail on the first pass
    are retried once after all files are applied, which resolves most cross-file
    forward references.
    """

    all_statements: List[str] = []
    for fname in filenames:
        sql = substitute_scale(read_asset_sql(fname), scale)
        all_statements.extend(split_sql_statements(sql))

    failed = apply_sql(cur, all_statements, logger, "pass1")
    if failed:
        failed = apply_sql(cur, failed, logger, "pass2")
    applied = len(all_statements) - len(failed)
    return applied, len(failed)

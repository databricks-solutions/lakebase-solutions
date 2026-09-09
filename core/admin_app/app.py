"""Admin app entry point (P0 placeholder).

The real console is a fork of ``lakebase_admin`` (Flask + psycopg v3, a Lakebase
DBA console with instance introspection, schema explorer, live ASH dashboard,
VACUUM/REINDEX, backup/restore). It is harvested in P2.

For P0 this is a minimal placeholder so the DABs `app` source path
(``./core/admin_app``) resolves to something coherent. It is intentionally not
wired to any live resource.
"""

from __future__ import annotations

# TODO(P2): replace with the harvested lakebase_admin Flask app (app.py, shared.py,
# routes/admin.py, templates/, static/) and its requirements.txt.

if __name__ == "__main__":  # pragma: no cover - placeholder
    print("lakebase-solutions admin app placeholder (P0). Real app lands in P2.")

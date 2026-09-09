"""pytest bootstrap: ensure the repo root is importable.

Placing this at the repo root puts the root on ``sys.path`` (pytest prepends the
rootdir of the topmost ``conftest.py``), so ``import bootstrap`` works without an
install step -- matching the CI job (`pip install -r requirements-dev.txt` then
`pytest -q`).
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

"""Make ``dags/`` and ``plugins/`` importable from tests."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for sub in ("dags", "plugins", "include"):
    p = ROOT / sub
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

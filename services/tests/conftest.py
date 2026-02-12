"""
This file configures pytest.

This file is in the root since it can be used for tests in any place in this
project, including tests under resources/.

First deactivate current python environment
cd services
uv sync --project services --group dev
source .venv/bin/activate
uv run pytest -q tests
"""

from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

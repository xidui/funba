"""Make the repo root importable so `import web.app`, `import db.models`,
etc. work whether pytest is invoked from CI (Linux, Python 3.12) or
locally (macOS, Python 3.14). Without this, `pytest tests/` fails on
Python 3.14 with ModuleNotFoundError for top-level packages.
"""
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = REPO_ROOT / "tests"
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from test_auth import _make_app  # noqa: E402


def test_health_endpoint_is_lightweight_and_public():
    app, _, _ = _make_app()
    app.config["TESTING"] = True
    resp = app.test_client().get("/api/health", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True, "service": "funba"}

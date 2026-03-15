"""Tests for admin access control and visitor cookie tracking."""
import unittest
from unittest.mock import patch, MagicMock


def _make_app():
    """Import the Flask app with DB operations patched out."""
    import sys, types

    # Stub out DB-heavy modules so we don't need a live MySQL connection.
    fake_engine = MagicMock()

    fake_models = types.ModuleType("db.models")
    for name in (
        "Game", "GamePlayByPlay", "MetricJobClaim", "MetricDefinition",
        "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "ShotRecord", "Team", "TeamGameStats",
    ):
        setattr(fake_models, name, MagicMock())
    fake_models.engine = fake_engine
    sys.modules["db.models"] = fake_models

    fake_db = types.ModuleType("db")
    sys.modules["db"] = fake_db

    fake_backfill = types.ModuleType("db.backfill_nba_player_shot_detail")
    fake_backfill.back_fill_game_shot_record_from_api = MagicMock()
    sys.modules["db.backfill_nba_player_shot_detail"] = fake_backfill

    # Remove cached web.app so imports are re-evaluated with stubs
    for key in list(sys.modules):
        if key.startswith("web.app") or key == "web.app":
            del sys.modules[key]

    from web.app import app, is_admin, _VISITOR_COOKIE
    return app, is_admin, _VISITOR_COOKIE


class TestIsAdmin(unittest.TestCase):
    """is_admin() must distinguish direct-localhost from Cloudflare-tunnel traffic."""

    def setUp(self):
        self.app, self.is_admin_fn, self.cookie_name = _make_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    # ------------------------------------------------------------------ helpers
    def _ctx(self, environ_overrides=None):
        """Return a pushed request context for '/'."""
        return self.app.test_request_context("/", environ_overrides=environ_overrides)

    # ------------------------------------------------------------------ tests: is_admin
    def test_direct_localhost_ipv4_is_admin(self):
        with self._ctx({"REMOTE_ADDR": "127.0.0.1"}):
            self.assertTrue(self.is_admin_fn())

    def test_direct_localhost_ipv6_is_admin(self):
        with self._ctx({"REMOTE_ADDR": "::1"}):
            self.assertTrue(self.is_admin_fn())

    def test_external_ip_is_not_admin(self):
        with self._ctx({"REMOTE_ADDR": "1.2.3.4"}):
            self.assertFalse(self.is_admin_fn())

    def test_cloudflare_tunnel_with_cf_header_is_not_admin(self):
        """cloudflared sends from 127.0.0.1 but adds CF-Connecting-IP."""
        with self._ctx({
            "REMOTE_ADDR": "127.0.0.1",
            "HTTP_CF_CONNECTING_IP": "203.0.113.5",
        }):
            self.assertFalse(self.is_admin_fn())

    def test_cloudflare_tunnel_with_x_forwarded_for_is_not_admin(self):
        """Proxy adds X-Forwarded-For with a non-local client IP."""
        with self._ctx({
            "REMOTE_ADDR": "127.0.0.1",
            "HTTP_X_FORWARDED_FOR": "203.0.113.5",
        }):
            self.assertFalse(self.is_admin_fn())

    def test_localhost_x_forwarded_for_still_admin(self):
        """X-Forwarded-For of 127.0.0.1 should not block admin (edge case)."""
        with self._ctx({
            "REMOTE_ADDR": "127.0.0.1",
            "HTTP_X_FORWARDED_FOR": "127.0.0.1",
        }):
            self.assertTrue(self.is_admin_fn())

    # ------------------------------------------------------------------ tests: admin gates
    def test_admin_gate_allows_localhost(self):
        """_require_admin_page() returns None (passes) for direct localhost."""
        from web.app import _require_admin_page
        with self._ctx({"REMOTE_ADDR": "127.0.0.1"}):
            result = _require_admin_page()
        self.assertIsNone(result)

    def test_admin_route_blocks_cloudflare_traffic(self):
        """/admin returns 403 when CF-Connecting-IP is present."""
        resp = self.client.get(
            "/admin",
            environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            headers={"CF-Connecting-IP": "203.0.113.5"},
        )
        self.assertEqual(resp.status_code, 403)

    def test_admin_route_blocks_external_ip(self):
        """/admin returns 403 for a public IP."""
        resp = self.client.get("/admin", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
        self.assertEqual(resp.status_code, 403)

    # ------------------------------------------------------------------ tests: visitor cookie
    def test_new_visitor_receives_cookie(self):
        """First GET to a tracked page should set the funba_visitor cookie."""
        with patch("web.app.SessionLocal") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session_cls.return_value = mock_session
            # Patch render_template to avoid template loading
            with patch("web.app.render_template", return_value="<html></html>"):
                resp = self.client.get("/", environ_overrides={"REMOTE_ADDR": "127.0.0.1"})
        self.assertIn(self.cookie_name, resp.headers.get("Set-Cookie", ""))

    def test_returning_visitor_no_new_cookie(self):
        """Returning visitor (cookie already set) should not get a new Set-Cookie."""
        with patch("web.app.SessionLocal") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session_cls.return_value = mock_session
            with patch("web.app.render_template", return_value="<html></html>"):
                # Simulate pre-existing cookie
                self.client.set_cookie(self.cookie_name, "existing-visitor-uuid")
                resp = self.client.get("/", environ_overrides={"REMOTE_ADDR": "127.0.0.1"})
        self.assertNotIn(self.cookie_name, resp.headers.get("Set-Cookie", ""))

    def test_new_visitor_no_500_on_first_get(self):
        """after_this_request cookie set must not cause a 500."""
        with patch("web.app.SessionLocal") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session_cls.return_value = mock_session
            with patch("web.app.render_template", return_value="<html></html>"):
                resp = self.client.get("/", environ_overrides={"REMOTE_ADDR": "127.0.0.1"})
        self.assertNotEqual(resp.status_code, 500)


if __name__ == "__main__":
    unittest.main()

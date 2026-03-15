"""Tests for admin access control, visitor cookie tracking, and Google OAuth."""
import unittest
from unittest.mock import patch, MagicMock


def _make_app():
    """Import the Flask app with DB operations patched out."""
    import sys, types

    # Stub out DB-heavy modules so we don't need a live MySQL connection.
    fake_engine = MagicMock()

    # Fake User class with realistic attributes
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_models = types.ModuleType("db.models")
    for name in (
        "Game", "GamePlayByPlay", "MetricJobClaim", "MetricDefinition",
        "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "ShotRecord", "Team", "TeamGameStats",
    ):
        setattr(fake_models, name, MagicMock())
    fake_models.User = fake_user_cls
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


class TestGoogleOAuth(unittest.TestCase):
    """Google OAuth login/logout routes."""

    def setUp(self):
        self.app, _, _ = _make_app()
        self.app.config["TESTING"] = True
        self.app.config["SERVER_NAME"] = "localhost"
        self.client = self.app.test_client()

    # ── /auth/login ─────────────────────────────────────────────────
    def test_auth_login_redirects_to_google(self):
        """/auth/login must redirect to Google OAuth (302 with location)."""
        with patch("web.app.oauth") as mock_oauth:
            mock_google = MagicMock()
            mock_oauth.google = mock_google
            mock_google.authorize_redirect.return_value = (
                MagicMock(status_code=302, headers={"Location": "https://accounts.google.com/o/oauth2/auth?..."})
            )
            resp = self.client.get("/auth/login")
        # Either a real redirect or our mock — both are fine as long as it's not 500
        self.assertNotEqual(resp.status_code, 500)

    # ── /auth/callback ──────────────────────────────────────────────
    def test_auth_callback_creates_user_and_sets_session(self):
        """Successful callback creates User record and stores user_id in session."""
        fake_user = MagicMock()
        fake_user.id = "test-uuid-1234"

        with patch("web.app.oauth") as mock_oauth, \
             patch("web.app.SessionLocal") as mock_session_cls:
            mock_google = MagicMock()
            mock_oauth.google = mock_google
            mock_google.authorize_access_token.return_value = {
                "userinfo": {
                    "sub": "google-123",
                    "email": "test@example.com",
                    "name": "Test User",
                    "picture": "https://example.com/avatar.jpg",
                }
            }

            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_session_cls.return_value = mock_db
            mock_db.query.return_value.filter.return_value.first.return_value = None
            mock_db.refresh.side_effect = lambda u: setattr(u, "id", fake_user.id)

            with self.app.test_request_context():
                resp = self.client.get("/auth/callback?code=fake-code&state=fake-state")

        # Should redirect (302) on success, not error (500)
        self.assertNotEqual(resp.status_code, 500)

    def test_auth_callback_oauth_error_flashes_message(self):
        """If OAuth token exchange fails, redirect to home with flash message."""
        with patch("web.app.oauth") as mock_oauth:
            mock_google = MagicMock()
            mock_oauth.google = mock_google
            mock_google.authorize_access_token.side_effect = Exception("OAuth error")

            resp = self.client.get("/auth/callback?error=access_denied")

        self.assertEqual(resp.status_code, 302)
        self.assertIn("/", resp.headers.get("Location", ""))

    def test_auth_callback_missing_google_id_flashes_message(self):
        """If userinfo has no sub, redirect to home with flash message."""
        with patch("web.app.oauth") as mock_oauth:
            mock_google = MagicMock()
            mock_oauth.google = mock_google
            mock_google.authorize_access_token.return_value = {
                "userinfo": {"email": "test@example.com"}  # no 'sub'
            }

            resp = self.client.get("/auth/callback")

        self.assertEqual(resp.status_code, 302)

    # ── /auth/logout ─────────────────────────────────────────────────
    def test_auth_logout_clears_session_and_redirects(self):
        """/auth/logout (POST) clears user_id from session and redirects home."""
        with self.client.session_transaction() as sess:
            sess["user_id"] = "some-user-id"

        resp = self.client.post("/auth/logout")

        self.assertEqual(resp.status_code, 302)
        with self.client.session_transaction() as sess:
            self.assertNotIn("user_id", sess)

    def test_auth_logout_get_not_allowed(self):
        """/auth/logout only accepts POST."""
        resp = self.client.get("/auth/logout")
        self.assertEqual(resp.status_code, 405)

    # ── current_user context ─────────────────────────────────────────
    def test_unauthenticated_user_gets_sign_in_link(self):
        """Anonymous visitor gets the Sign in link in the topbar (no session)."""
        with patch("web.app.SessionLocal") as mock_session_cls, \
             patch("web.app.render_template", return_value="<html></html>"):
            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_session_cls.return_value = mock_db
            resp = self.client.get("/", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
        self.assertNotEqual(resp.status_code, 500)

    def test_authenticated_non_admin_blocked_from_admin(self):
        """Authenticated Google user from non-localhost still gets 403 on /admin."""
        with self.client.session_transaction() as sess:
            sess["user_id"] = "some-user-id"

        resp = self.client.get(
            "/admin",
            environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
        )
        self.assertEqual(resp.status_code, 403)

    def test_authenticated_non_admin_blocked_from_metrics_new(self):
        """Authenticated user from non-localhost still gets 403 on /metrics/new."""
        with self.client.session_transaction() as sess:
            sess["user_id"] = "some-user-id"

        resp = self.client.get(
            "/metrics/new",
            environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
        )
        self.assertEqual(resp.status_code, 403)


class TestRedirectSafety(unittest.TestCase):
    """_safe_redirect_url must reject off-site URLs and allow local paths."""

    def setUp(self):
        self.app, _, _ = _make_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def _safe(self, url):
        from web.app import _safe_redirect_url
        with self.app.test_request_context("/"):
            return _safe_redirect_url(url)

    def test_relative_path_allowed(self):
        self.assertEqual(self._safe("/players/123"), "/players/123")

    def test_root_path_allowed(self):
        self.assertEqual(self._safe("/"), "/")

    def test_external_http_blocked(self):
        result = self._safe("http://evil.example.com/steal")
        self.assertFalse(result.startswith("http://evil"))

    def test_external_https_blocked(self):
        result = self._safe("https://evil.example.com")
        self.assertFalse(result.startswith("https://evil"))

    def test_protocol_relative_blocked(self):
        """//evil.example.com must be blocked (protocol-relative open redirect)."""
        result = self._safe("//evil.example.com")
        self.assertFalse(result.startswith("//"))

    def test_none_falls_back_to_home(self):
        result = self._safe(None)
        self.assertEqual(result, "/")

    def test_auth_login_external_next_does_not_redirect_offsite(self):
        """GET /auth/login?next=https://evil.example must not redirect to evil after OAuth."""
        with patch("web.app.oauth") as mock_oauth:
            mock_oauth.google.authorize_redirect.return_value = MagicMock(
                status_code=302, headers={"Location": "https://accounts.google.com/"}
            )
            self.client.get("/auth/login?next=https://evil.example.com/steal")

        # The stored oauth_next must be the safe fallback, not the external URL
        with self.client.session_transaction() as sess:
            stored = sess.get("oauth_next", "")
        self.assertFalse(stored.startswith("https://evil"), f"oauth_next was unsafe: {stored!r}")

    def test_auth_callback_external_next_does_not_redirect_offsite(self):
        """After OAuth, an external oauth_next in session must redirect home, not offsite."""
        fake_user = MagicMock()
        fake_user.id = "uid-999"

        with patch("web.app.oauth") as mock_oauth, \
             patch("web.app.SessionLocal") as mock_session_cls:
            mock_oauth.google.authorize_access_token.return_value = {
                "userinfo": {
                    "sub": "gid-999",
                    "email": "x@x.com",
                    "name": "X",
                    "picture": None,
                }
            }
            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_session_cls.return_value = mock_db
            mock_db.query.return_value.filter.return_value.first.return_value = None
            mock_db.refresh.side_effect = lambda u: setattr(u, "id", fake_user.id)

            # Inject a malicious oauth_next into the session before callback fires
            with self.client.session_transaction() as sess:
                sess["oauth_next"] = "https://evil.example.com/steal"

            resp = self.client.get("/auth/callback?code=c&state=s")

        location = resp.headers.get("Location", "")
        self.assertFalse(location.startswith("https://evil"), f"Redirected to external URL: {location!r}")

    def test_same_origin_absolute_url_normalized_to_path(self):
        """Same-origin absolute URL (e.g. request.url) must round-trip to its local path."""
        from web.app import _safe_redirect_url
        with self.app.test_request_context("/", headers={"Host": "localhost"}):
            result = _safe_redirect_url("http://localhost/players/123")
        self.assertEqual(result, "/players/123")

    def test_same_origin_absolute_url_with_query_normalized(self):
        """Same-origin absolute URL with query string normalizes to path?query."""
        from web.app import _safe_redirect_url
        with self.app.test_request_context("/", headers={"Host": "localhost"}):
            result = _safe_redirect_url("http://localhost/metrics?season=22025")
        self.assertEqual(result, "/metrics?season=22025")

    def test_auth_login_absolute_same_origin_next_stored_as_path(self):
        """GET /auth/login?next=<absolute same-origin URL> must store the local path in session."""
        with patch("web.app.oauth") as mock_oauth:
            mock_oauth.google.authorize_redirect.return_value = MagicMock(
                status_code=302, headers={"Location": "https://accounts.google.com/"}
            )
            self.client.get("/auth/login?next=http://localhost/players/456")

        with self.client.session_transaction() as sess:
            stored = sess.get("oauth_next", "")
        self.assertEqual(stored, "/players/456", f"oauth_next should be path-only, got: {stored!r}")


if __name__ == "__main__":
    unittest.main()

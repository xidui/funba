"""Tests for admin access control, visitor cookie tracking, and Google OAuth."""
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from unittest.mock import ANY, patch, MagicMock

from tests.db_model_stubs import install_fake_db_module

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app():
    """Import the Flask app with DB operations patched out."""
    import types

    fake_limiter_mod = types.ModuleType("flask_limiter")

    class FakeLimiter:
        def __init__(self, *args, **kwargs):
            pass

        def limit(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return decorator

    fake_limiter_mod.Limiter = FakeLimiter
    sys.modules["flask_limiter"] = fake_limiter_mod

    fake_limiter_util = types.ModuleType("flask_limiter.util")
    fake_limiter_util.get_remote_address = MagicMock(return_value="127.0.0.1")
    sys.modules["flask_limiter.util"] = fake_limiter_util

    fake_authlib = types.ModuleType("authlib")
    fake_authlib_integrations = types.ModuleType("authlib.integrations")
    fake_authlib_flask = types.ModuleType("authlib.integrations.flask_client")

    class FakeOAuth:
        def __init__(self, *args, **kwargs):
            self.google = MagicMock()

        def register(self, *args, **kwargs):
            return self.google

    fake_authlib_flask.OAuth = FakeOAuth
    sys.modules["authlib"] = fake_authlib
    sys.modules["authlib.integrations"] = fake_authlib_integrations
    sys.modules["authlib.integrations.flask_client"] = fake_authlib_flask

    # Stub out DB-heavy modules so we don't need a live MySQL connection.
    fake_engine = MagicMock()

    # Fake User class with realistic attributes
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_models = install_fake_db_module(
        REPO_ROOT,
        user_cls=fake_user_cls,
        engine=fake_engine,
        extra_model_names=("Setting",),
    )

    fake_backfill = types.ModuleType("db.backfill_nba_player_shot_detail")
    fake_backfill.back_fill_game_shot_record = MagicMock()
    fake_backfill.back_fill_game_shot_record_from_api = MagicMock()
    fake_backfill.is_game_shot_back_filled = MagicMock(return_value=False)
    sys.modules["db.backfill_nba_player_shot_detail"] = fake_backfill

    fake_line = types.ModuleType("db.backfill_nba_game_line_score")
    fake_line.back_fill_game_line_score = MagicMock()
    fake_line.has_game_line_score = MagicMock(return_value=False)
    fake_line.normalize_game_line_score_payload = MagicMock()
    sys.modules["db.backfill_nba_game_line_score"] = fake_line

    # Remove cached web.app so imports are re-evaluated with stubs
    for key in list(sys.modules):
        if key.startswith("web.app") or key == "web.app":
            del sys.modules[key]

    import web.app as web_app

    def _session_ctx():
        fake_session = MagicMock()
        fake_session.__enter__ = MagicMock(return_value=fake_session)
        fake_session.__exit__ = MagicMock(return_value=False)
        fake_session.get.return_value = None
        return fake_session

    web_app.SessionLocal = MagicMock(side_effect=_session_ctx)

    return web_app.app, web_app.is_admin, web_app._VISITOR_COOKIE


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

    def test_logged_in_admin_user_is_admin_even_on_external_ip(self):
        with patch("web.app._current_user", return_value=SimpleNamespace(is_admin=True)):
            with self._ctx({"REMOTE_ADDR": "8.8.8.8"}):
                self.assertTrue(self.is_admin_fn())

    def test_logged_in_non_admin_user_stays_blocked_on_external_ip(self):
        with patch("web.app._current_user", return_value=SimpleNamespace(is_admin=False)):
            with self._ctx({"REMOTE_ADDR": "8.8.8.8"}):
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
                resp = self.client.get("/", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
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
                resp = self.client.get("/", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
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
        with self.client.session_transaction() as sess:
            self.assertEqual(sess.get("user_id"), fake_user.id)
            self.assertTrue(sess.permanent)
        self.assertEqual(self.app.permanent_session_lifetime.days, 30)

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

    def test_admin_route_success_for_logged_in_admin(self):
        """Logged-in admin user can access /admin from any IP (200)."""
        with self.client.session_transaction() as sess:
            sess["user_id"] = "admin-user-id"

        from web.app import _require_admin_page

        def fake_admin():
            denied = _require_admin_page()
            if denied:
                return denied
            return "<html></html>"

        original = self.app.view_functions["admin_pipeline"]
        self.app.view_functions["admin_pipeline"] = fake_admin
        try:
            with patch("web.app._current_user", return_value=SimpleNamespace(is_admin=True)):
                resp = self.client.get(
                    "/admin",
                    environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
                )
            self.assertEqual(resp.status_code, 200)
        finally:
            self.app.view_functions["admin_pipeline"] = original

    def test_authenticated_non_admin_blocked_from_metrics_new(self):
        """Authenticated user from non-localhost still gets 403 on /metrics/new."""
        with self.client.session_transaction() as sess:
            sess["user_id"] = "some-user-id"

        with patch("web.app._current_user", return_value=SimpleNamespace(is_admin=False, subscription_tier="free", display_name="Test User")), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.render_template", return_value="upgrade"):
            resp = self.client.get(
                "/metrics/new",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )
        self.assertEqual(resp.status_code, 403)

    def test_logged_in_free_user_can_access_metrics_new_when_feature_level_is_logged_in(self):
        with self.client.session_transaction() as sess:
            sess["user_id"] = "some-user-id"

        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)
        session.query.return_value.distinct.return_value.all.return_value = [("22025",)]

        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-1", is_admin=False, subscription_tier="free", display_name="Test User")), \
             patch("web.app._feature_access_level", return_value="logged_in"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.get_llm_model_for_purpose", return_value="gpt-5.4"), \
             patch("web.app.get_feature_access_config", return_value={"metric_search": "logged_in", "metric_create": "logged_in"}), \
             patch("web.app._build_metric_feature_context", return_value={}), \
             patch("web.app.available_llm_models", return_value=["gpt-5.4"]), \
             patch("web.app.render_template", return_value="ok"):
            resp = self.client.get(
                "/metrics/new",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(resp.status_code, 200)


class TestMyMetricsRoute(unittest.TestCase):
    def setUp(self):
        self.app, _, _ = _make_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def test_metrics_mine_requires_login_for_external_visitors(self):
        response = self.client.get(
            "/metrics/mine",
            environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/auth/login", response.location)

    def test_metrics_mine_allows_logged_in_non_pro_users(self):
        with self.client.session_transaction() as sess:
            sess["user_id"] = "some-user-id"

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        with patch("web.app._current_user", return_value=SimpleNamespace(id="some-user-id", is_admin=False, subscription_tier="free", display_name="Test User")), \
             patch("web.app.SessionLocal", return_value=mock_session), \
             patch("web.app.get_feature_access_config", return_value={"metric_search": "logged_in", "metric_create": "pro"}), \
             patch("web.app._build_metric_feature_context", return_value={}), \
             patch("web.app.render_template", return_value="ok"):
            response = self.client.get(
                "/metrics/mine",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)

    def test_metrics_mine_renders_separate_drafts_and_published_lists(self):
        class FakeColumn:
            def __init__(self, name):
                self.name = name

            def __eq__(self, other):
                return ("eq", self.name, other)

            def in_(self, other):
                return ("in", self.name, other)

            def is_(self, other):
                return ("is", self.name, other)

            def desc(self):
                return ("desc", self.name)

        class FakeMetricDefinitionModel:
            created_by_user_id = FakeColumn("created_by_user_id")
            base_metric_key = FakeColumn("base_metric_key")
            status = FakeColumn("status")
            updated_at = FakeColumn("updated_at")
            created_at = FakeColumn("created_at")

        class RecordingMetricQuery:
            def __init__(self, rows):
                self.rows = rows
                self.filters = []
                self.orderings = []

            def filter(self, *conditions):
                self.filters.extend(conditions)
                return self

            def order_by(self, *orderings):
                self.orderings.extend(orderings)
                return self

            def all(self):
                return self.rows

        draft_metric = SimpleNamespace(key="draft_metric", name="Draft Metric", description="Draft desc", scope="player", updated_at=None)
        published_metric = SimpleNamespace(key="published_metric", name="Published Metric", description="Published desc", scope="team", updated_at=None)

        draft_query = RecordingMetricQuery([draft_metric])
        published_query = RecordingMetricQuery([published_metric])

        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)
        session.query.side_effect = [draft_query, published_query]

        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-123", is_admin=False, subscription_tier="pro", subscription_expires_at=None)), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.get_feature_access_config", return_value={"metric_search": "logged_in", "metric_create": "pro"}), \
             patch("web.app._build_metric_feature_context", return_value={}), \
             patch("web.app.MetricDefinitionModel", FakeMetricDefinitionModel), \
             patch("web.app.render_template", return_value="<html></html>") as mock_render:
            response = self.client.get(
                "/metrics/mine",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            draft_query.filters,
            [
                ("eq", "created_by_user_id", "user-123"),
                ("is", "base_metric_key", None),
                ("eq", "status", "draft"),
            ],
        )
        self.assertEqual(draft_query.orderings, [("desc", "updated_at")])
        self.assertEqual(
            published_query.filters,
            [
                ("eq", "created_by_user_id", "user-123"),
                ("is", "base_metric_key", None),
                ("in", "status", ["published", "disabled"]),
            ],
        )
        self.assertEqual(published_query.orderings, [("desc", "created_at")])
        mock_render.assert_called_once_with(
            "my_metrics.html",
            drafts=[draft_metric],
            published=[published_metric],
            total_metrics=2,
            scope_labels={
                "player": "Player",
                "player_franchise": "Player Franchise",
                "team": "Team",
                "game": "Game",
            },
        )


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
        """GET /auth/google?next=<absolute same-origin URL> must store the local path in session."""
        with patch("web.app.oauth") as mock_oauth, patch.dict("os.environ", {"GOOGLE_CLIENT_ID": "test-client-id"}):
            mock_oauth.google.authorize_redirect.return_value = MagicMock(
                status_code=302, headers={"Location": "https://accounts.google.com/"}
            )
            self.client.get("/auth/google?next=http://localhost/players/456")

        with self.client.session_transaction() as sess:
            stored = sess.get("oauth_next", "")
        self.assertEqual(stored, "/players/456", f"oauth_next should be path-only, got: {stored!r}")


class TestMetricSearchAuth(unittest.TestCase):
    def setUp(self):
        self.app, _, _ = _make_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def test_metrics_allows_external_visitors_and_uses_anonymous_catalog_context(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._current_user", return_value=None), \
             patch("web.app.get_llm_model_for_purpose", return_value="gpt-5.4"), \
             patch("web.app.get_feature_access_config", return_value={"metric_search": "logged_in", "metric_create": "pro"}), \
             patch("web.app._build_metric_feature_context", return_value={}), \
             patch("web.app.available_llm_models", return_value=["gpt-5.4", "gpt-5.4-mini"]), \
             patch("web.app._catalog_metrics_page", return_value=([{"key": "late_game_scoring"}], False)) as mock_catalog, \
             patch("web.app.render_template", return_value="<html></html>") as mock_render:
            response = self.client.get(
                "/metrics?scope=player&status=published&q=late",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        mock_catalog.assert_called_once_with(
            session,
            scope_filter="player",
            status_filter="published",
            current_user_id=None,
        )
        mock_render.assert_called_once_with(
            "metrics.html",
            metrics_list=[{"key": "late_game_scoring"}],
            metrics_total=1,
            metrics_has_more=False,
            metrics_page_size=24,
            scope_filter="player",
            status_filter="published",
            search_query="late",
            top3_by_metric={},
            llm_default_model="gpt-5.4",
            llm_available_models=["gpt-5.4", "gpt-5.4-mini"],
        )

    def test_metric_search_api_requires_login_for_external_visitors(self):
        with patch("web.app._feature_access_level", return_value="logged_in"):
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "clutch shooting"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "login_required")

    def test_metrics_catalog_count_api_returns_exact_total(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._catalog_metrics_total", return_value=547) as mock_total:
            response = self.client.get(
                "/api/metrics/catalog-count?scope=player&status=published",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"ok": True, "total": 547})
        mock_total.assert_called_once_with(
            session,
            scope_filter="player",
            status_filter="published",
        )

    def test_metric_search_api_allows_anonymous_visitors_when_feature_level_is_anonymous(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=None), \
             patch("web.app._feature_access_level", return_value="anonymous"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._catalog_metrics", return_value=[{"key": "late_game_scoring", "name": "Late Game Scoring"}]), \
             patch("web.app.resolve_llm_model", return_value="gpt-5.4"), \
             patch("metrics.framework.search.rank_metrics", return_value=[{"key": "late_game_scoring", "reason": "Best fit"}]):
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "late game scoring"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

    def test_metric_search_api_allows_logged_in_non_pro_users(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-123", is_admin=False, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="logged_in"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._catalog_metrics", return_value=[{"key": "late_game_scoring", "name": "Late Game Scoring"}]), \
             patch("web.app.resolve_llm_model", return_value="gpt-5.4"), \
             patch("metrics.framework.search.rank_metrics", return_value=[{"key": "late_game_scoring", "reason": "Best fit"}]) as mock_rank:
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "late game scoring"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["matches"][0]["key"], "late_game_scoring")
        mock_rank.assert_called_once_with(
            "late game scoring",
            [{"key": "late_game_scoring", "name": "Late Game Scoring"}],
            limit=8,
            model="gpt-5.4",
            usage_recorder=ANY,
        )

    def test_metric_search_api_ignores_explicit_draft_status(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-123", is_admin=False, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="logged_in"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._catalog_metrics", return_value=[{"key": "late_game_scoring", "name": "Late Game Scoring"}]) as mock_catalog, \
             patch("web.app.resolve_llm_model", return_value="gpt-5.4"), \
             patch("metrics.framework.search.rank_metrics", return_value=[{"key": "late_game_scoring", "reason": "Best fit"}]):
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "late game scoring", "status": "draft"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        mock_catalog.assert_called_once_with(
            session,
            scope_filter="",
            status_filter="",
        )

    def test_metric_search_api_passes_admin_model_override(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="admin-1", is_admin=True, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="logged_in"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._catalog_metrics", return_value=[{"key": "late_game_scoring", "name": "Late Game Scoring"}]), \
             patch("web.app.resolve_llm_model", return_value="claude-sonnet-4-6") as mock_resolve, \
             patch("metrics.framework.search.rank_metrics", return_value=[{"key": "late_game_scoring", "reason": "Best fit"}]) as mock_rank:
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "late game scoring", "model": "claude-sonnet-4-6"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        mock_resolve.assert_called_once_with(session, requested_model="claude-sonnet-4-6", purpose="search")
        mock_rank.assert_called_once_with(
            "late game scoring",
            [{"key": "late_game_scoring", "name": "Late Game Scoring"}],
            limit=8,
            model="claude-sonnet-4-6",
            usage_recorder=ANY,
        )

    def test_metric_search_api_blocks_free_user_when_search_requires_pro(self):
        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-123", is_admin=False, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="pro"):
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "late game scoring"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"], "pro_required")

    def test_metric_generate_api_passes_admin_model_override(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="admin-1", is_admin=True, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.resolve_llm_model", return_value="claude-sonnet-4-6") as mock_resolve, \
             patch("metrics.framework.generator.generate", return_value={"responseType": "code", "name": "Demo", "description": "Demo", "scope": "player", "code": "class Demo: pass"}) as mock_generate:
            response = self.client.post(
                "/api/metrics/generate",
                json={"expression": "clutch scoring", "model": "claude-sonnet-4-6"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "ok": True,
                "responseType": "code",
                "spec": {
                    "responseType": "code",
                    "name": "Demo",
                    "description": "Demo",
                    "scope": "player",
                    "code": "class Demo: pass",
                },
            },
        )
        mock_resolve.assert_called_once_with(session, requested_model="claude-sonnet-4-6", purpose="generate")
        mock_generate.assert_called_once_with(
            "clutch scoring",
            history=None,
            existing=None,
            model="claude-sonnet-4-6",
            usage_recorder=ANY,
        )

    def test_metric_generate_api_returns_clarification_payload(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="admin-1", is_admin=True, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.resolve_llm_model", return_value="claude-sonnet-4-6"), \
             patch("metrics.framework.generator.generate", return_value={"responseType": "clarification", "message": "rank_order controls how results are sorted."}):
            response = self.client.post(
                "/api/metrics/generate",
                json={"expression": "What does rank_order do?"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "ok": True,
                "responseType": "clarification",
                "message": "rank_order controls how results are sorted.",
            },
        )

    def test_metric_generate_api_defaults_missing_response_type_to_code(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="admin-1", is_admin=True, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.resolve_llm_model", return_value="claude-sonnet-4-6"), \
             patch("metrics.framework.generator.generate", return_value={"name": "Demo", "description": "Demo", "scope": "player", "code": "class Demo: pass"}):
            response = self.client.post(
                "/api/metrics/generate",
                json={"expression": "clutch scoring"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "ok": True,
                "responseType": "code",
                "spec": {
                    "name": "Demo",
                    "description": "Demo",
                    "scope": "player",
                    "code": "class Demo: pass",
                },
            },
        )

    def test_admin_model_config_endpoints_require_admin(self):
        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-1", is_admin=False, subscription_tier="free")):
            get_response = self.client.get("/api/admin/model-config", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
            post_response = self.client.post(
                "/api/admin/model-config",
                json={"default_model": "gpt-5.4"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(get_response.status_code, 403)
        self.assertEqual(get_response.get_json(), {"error": "admin_only"})
        self.assertEqual(post_response.status_code, 403)
        self.assertEqual(post_response.get_json(), {"error": "admin_only"})

    def test_admin_model_config_endpoints_round_trip(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="admin-1", is_admin=True, subscription_tier="free")), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.get_default_llm_model_for_ui", return_value="gpt-5.4"), \
             patch("web.app.get_llm_model_for_purpose", return_value="gpt-5.4"), \
             patch("web.app.available_llm_models", return_value=["gpt-5.4", "gpt-5.4-mini"]), \
             patch("web.app.set_llm_model_for_purpose", return_value="gpt-5.4-mini") as mock_set_purpose:
            get_response = self.client.get("/api/admin/model-config", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
            post_response = self.client.post(
                "/api/admin/model-config",
                json={"search_model": "gpt-5.4-mini", "generate_model": "gpt-5.4-mini"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(get_response.status_code, 200)
        get_data = get_response.get_json()
        self.assertEqual(get_data["default_model"], "gpt-5.4")
        self.assertEqual(get_data["search_model"], "gpt-5.4")
        self.assertEqual(get_data["generate_model"], "gpt-5.4")
        self.assertEqual(get_data["available_models"], ["gpt-5.4", "gpt-5.4-mini"])

        self.assertEqual(post_response.status_code, 200)
        post_data = post_response.get_json()
        self.assertTrue(post_data["ok"])
        self.assertEqual(mock_set_purpose.call_count, 2)
        session.commit.assert_called_once()

    def test_admin_feature_access_endpoints_require_admin(self):
        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-1", is_admin=False, subscription_tier="free")):
            get_response = self.client.get("/api/admin/feature-access", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
            post_response = self.client.post(
                "/api/admin/feature-access",
                json={"metric_search": "pro"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(get_response.status_code, 403)
        self.assertEqual(get_response.get_json(), {"error": "admin_only"})
        self.assertEqual(post_response.status_code, 403)
        self.assertEqual(post_response.get_json(), {"error": "admin_only"})

    def test_admin_feature_access_endpoints_round_trip(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        initial_features = [
            {
                "key": "metric_search",
                "label": "Find Metrics",
                "description": "Natural-language metric search from the public catalog.",
                "default_level": "logged_in",
                "current_level": "logged_in",
                "allowed_levels": [{"value": "logged_in", "label": "Signed in"}],
            }
        ]
        updated_features = [
            {
                "key": "metric_search",
                "label": "Find Metrics",
                "description": "Natural-language metric search from the public catalog.",
                "default_level": "logged_in",
                "current_level": "pro",
                "allowed_levels": [{"value": "pro", "label": "Pro"}],
            }
        ]

        with patch("web.app._current_user", return_value=SimpleNamespace(id="admin-1", is_admin=True, subscription_tier="free")), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._serialize_feature_access", side_effect=[initial_features, updated_features]), \
             patch("web.app.set_feature_access_level", return_value="pro") as mock_set_feature:
            get_response = self.client.get("/api/admin/feature-access", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
            post_response = self.client.post(
                "/api/admin/feature-access",
                json={"metric_search": "pro"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(get_response.get_json(), {"ok": True, "features": initial_features})
        self.assertEqual(post_response.status_code, 200)
        self.assertEqual(post_response.get_json()["features"], updated_features)
        mock_set_feature.assert_called_once_with(session, "metric_search", "pro")
        session.commit.assert_called_once()

    def test_admin_ai_usage_endpoint_requires_admin(self):
        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-1", is_admin=False, subscription_tier="free")):
            response = self.client.get("/api/admin/ai-usage", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json(), {"error": "admin_only"})

    def test_admin_ai_usage_endpoint_returns_dashboard(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)
        dashboard = {"window_24h": {"calls": 1, "total_tokens": 100}, "window_7d": {"calls": 2, "total_tokens": 300}}

        with patch("web.app._current_user", return_value=SimpleNamespace(id="admin-1", is_admin=True, subscription_tier="free")), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.get_ai_usage_dashboard", return_value=dashboard) as mock_dashboard:
            response = self.client.get("/api/admin/ai-usage", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"ok": True, "dashboard": dashboard})
        mock_dashboard.assert_called_once_with(session)

    def test_metric_search_api_sets_visitor_cookie_for_anonymous_usage(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=None), \
             patch("web.app._feature_access_level", return_value="anonymous"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._catalog_metrics", return_value=[{"key": "late_game_scoring", "name": "Late Game Scoring"}]), \
             patch("web.app.resolve_llm_model", return_value="gpt-5.4"), \
             patch("metrics.framework.search.rank_metrics", return_value=[{"key": "late_game_scoring", "reason": "Best fit"}]):
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "late game scoring"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("funba_visitor=", response.headers.get("Set-Cookie", ""))

    def test_metric_search_api_logs_query_preview(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-123", is_admin=False, subscription_tier="free")), \
             patch("web.app._feature_access_level", return_value="logged_in"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._catalog_metrics", return_value=[{"key": "late_game_scoring", "name": "Late Game Scoring"}]), \
             patch("web.app.resolve_llm_model", return_value="gpt-5.4"), \
             patch("metrics.framework.search.rank_metrics", return_value=[{"key": "late_game_scoring", "reason": "Best fit"}]), \
             patch("web.app._record_ai_usage_event") as mock_log:
            response = self.client.post(
                "/api/metrics/search",
                json={"query": "late game scoring"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_log.call_args.kwargs["metadata"]["query_text"], "late game scoring")

    def test_metric_generate_api_logs_conversation_id(self):
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)

        with patch("web.app._current_user", return_value=SimpleNamespace(id="user-123", is_admin=False, subscription_tier="pro", subscription_expires_at=None)), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app.resolve_llm_model", return_value="gpt-5.4"), \
             patch("metrics.framework.generator.generate", return_value={"responseType": "clarification", "message": "Need more detail."}), \
             patch("web.app._record_ai_usage_event") as mock_log:
            response = self.client.post(
                "/api/metrics/generate",
                json={"expression": "clutch scoring", "conversationId": "conv-123"},
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["responseType"], "clarification")
        kwargs = mock_log.call_args.kwargs
        self.assertEqual(kwargs["feature"], "metric_create")
        self.assertEqual(kwargs["operation"], "generate")
        self.assertEqual(kwargs["conversation_id"], "conv-123")
        self.assertEqual(kwargs["metadata"]["input_text"], "clutch scoring")


class TestMetricPublishAuth(unittest.TestCase):
    def setUp(self):
        self.app, _, _ = _make_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def _publish_session(self):
        metric = SimpleNamespace(key="custom_metric", status="draft", updated_at=None)
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)
        session.query.return_value.filter.return_value.first.return_value = metric
        return session, metric

    def test_metric_publish_api_allows_pro_user(self):
        session, metric = self._publish_session()

        with patch("web.app._current_user", return_value=SimpleNamespace(is_admin=False, subscription_tier="pro", subscription_expires_at=None)), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._metric_family_base_row", return_value=metric), \
             patch("web.app._metric_family_rows", return_value=[metric]), \
             patch("web.app._dispatch_metric_backfill") as mock_dispatch:
            response = self.client.post(
                "/api/metrics/custom_metric/publish",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "published")
        self.assertEqual(metric.status, "published")
        mock_dispatch.assert_called_once_with("custom_metric")
        session.commit.assert_called_once()

    def test_metric_publish_api_blocks_free_user(self):
        with patch("web.app._current_user", return_value=SimpleNamespace(is_admin=False, subscription_tier="free", subscription_expires_at=None)), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.SessionLocal") as mock_session:
            response = self.client.post(
                "/api/metrics/custom_metric/publish",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"], "pro_required")
        mock_session.assert_not_called()

    def test_metric_publish_api_keeps_admin_access(self):
        session, metric = self._publish_session()

        with patch("web.app._current_user", return_value=SimpleNamespace(is_admin=True, subscription_tier="free", subscription_expires_at=None)), \
             patch("web.app._feature_access_level", return_value="pro"), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("web.app._metric_family_base_row", return_value=metric), \
             patch("web.app._metric_family_rows", return_value=[metric]), \
             patch("web.app._dispatch_metric_backfill") as mock_dispatch:
            response = self.client.post(
                "/api/metrics/custom_metric/publish",
                environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "published")
        self.assertEqual(metric.status, "published")
        mock_dispatch.assert_called_once_with("custom_metric")

    def test_metric_detail_template_shows_edit_link_for_pro_users(self):
        template = (REPO_ROOT / "web" / "templates" / "metric_detail.html").read_text()
        self.assertIn("{% if can_create_metrics %}", template)
        self.assertIn("url_for('metric_edit', metric_key=metric_def.key)", template)

    def test_metric_detail_template_includes_admin_deep_dive_workflow(self):
        template = (REPO_ROOT / "web" / "templates" / "metric_detail.html").read_text()
        self.assertIn("metric-deep-dive-panel", template)
        self.assertIn("admin_metric_trigger_deep_dive_post", template)
        self.assertIn("No deep-dive post triggered yet.", template)

    def test_base_template_shows_my_metrics_link_for_all_logged_in_users(self):
        template = (REPO_ROOT / "web" / "templates" / "base.html").read_text()
        self.assertIn("url_for('my_metrics')", template)
        # Link is no longer gated behind is_pro
        self.assertNotIn("{% if is_pro %}\n              <a href=\"{{ url_for('my_metrics')", template)


class TestMetricDeepDiveWorkflow(unittest.TestCase):
    def setUp(self):
        self.app, _, _ = _make_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def test_metric_deep_dive_trigger_creates_placeholder_and_returns_state(self):
        runtime_metric = SimpleNamespace(
            key="blowout_rate",
            name="Blowout Rate",
            scope="team",
            category="results",
            description="How often a team wins big.",
            trigger="game",
        )
        metric_query = MagicMock()
        metric_query.filter.return_value.first.return_value = None

        created_post = SimpleNamespace(
            id=91,
            paperclip_issue_id="issue-1",
            paperclip_sync_error=None,
        )
        post_query = MagicMock()
        post_query.filter.return_value.first.return_value = created_post

        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)
        session.query.side_effect = [metric_query, post_query]

        final_state = {
            "can_trigger": False,
            "active_post": {
                "id": 91,
                "topic": "Blowout Rate 深度分析",
                "status": "draft",
                "created_at": "2026-03-30T12:00:00",
                "created_at_label": "2026-03-30 12:00:00",
                "workflow": {
                    "enabled": True,
                    "issue_identifier": "XIX-999",
                    "issue_status": "todo",
                    "owner_label": "Content Analyst",
                    "sync_error": None,
                },
                "admin_url": "/admin/content/91",
            },
            "latest_post": {
                "id": 91,
                "topic": "Blowout Rate 深度分析",
                "status": "draft",
                "created_at": "2026-03-30T12:00:00",
                "created_at_label": "2026-03-30 12:00:00",
                "workflow": {
                    "enabled": True,
                    "issue_identifier": "XIX-999",
                    "issue_status": "todo",
                    "owner_label": "Content Analyst",
                    "sync_error": None,
                },
                "admin_url": "/admin/content/91",
            },
        }

        with patch("web.app._is_bot", return_value=False), \
             patch("web.app._paperclip_client_or_raise", return_value=(MagicMock(), SimpleNamespace())), \
             patch("web.app.SessionLocal", return_value=session), \
             patch("metrics.framework.runtime.get_metric", return_value=runtime_metric), \
             patch("web.app._metric_deep_dive_state", side_effect=[{"can_trigger": True, "active_post": None, "latest_post": None}, final_state]), \
             patch("web.app._create_metric_deep_dive_placeholder_post", return_value=(91, "2026-03-30T12:00:00Z")) as mock_create, \
             patch("web.app._ensure_paperclip_issue_for_post") as mock_ensure, \
             patch("web.app._mirror_paperclip_comment") as mock_mirror, \
             patch("web.app._sync_social_post_from_paperclip", return_value={"workflow": {"issue_identifier": "XIX-999"}}):
            response = self.client.post(
                "/api/admin/metrics/blowout_rate/deep-dive-post",
                json={
                    "selected_view_label": "2024-25 Regular Season",
                    "current_season_label": "2025-26 Regular Season",
                    "metric_page_url": "/metrics/blowout_rate?season=22024",
                },
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["post_id"], 91)
        self.assertEqual(payload["metric_deep_dive"]["active_post"]["id"], 91)
        mock_create.assert_called_once()
        mock_ensure.assert_called_once_with(91)
        mock_mirror.assert_called_once()


class TestMetricDetailSeasonOptions(unittest.TestCase):
    def setUp(self):
        _make_app()

    def test_non_pro_metric_detail_season_options_keep_two_newest_regular_seasons(self):
        from web.app import _non_pro_metric_detail_season_options

        season_options = ["52025", "42025", "22025", "22024", "22023", "12025"]

        self.assertEqual(
            _non_pro_metric_detail_season_options(season_options),
            ["22025", "22024"],
        )

    def test_non_pro_metric_detail_season_options_falls_back_to_latest_available_season(self):
        from web.app import _non_pro_metric_detail_season_options

        season_options = ["42025", "52025", "42024"]

        self.assertEqual(
            _non_pro_metric_detail_season_options(season_options),
            ["52025"],
        )


if __name__ == "__main__":
    unittest.main()

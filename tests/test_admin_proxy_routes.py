import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import requests
from flask import Flask, Response

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from web.admin_proxy_routes import register_admin_proxy_routes


def _upstream_response(body: bytes = b"ok", *, status: int = 200, headers: dict[str, str] | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = body
    response.headers.update(headers or {"Content-Type": "text/plain"})
    response.encoding = "utf-8"
    return response


def _make_app(*, denied=None, monitor_url="http://127.0.0.1:19999", tickets_url="https://paperclip.test/FUN/issues"):
    app = Flask(__name__)
    register_admin_proxy_routes(
        app,
        SimpleNamespace(
            require_admin_page=lambda: (lambda: denied),
            monitor_url=lambda: monitor_url,
            tickets_url=lambda: tickets_url,
            timeout_seconds=lambda: 3,
        ),
    )
    return app


def test_admin_proxy_exempts_registered_views_from_limiter():
    class FakeLimiter:
        def __init__(self):
            self.exempted = []

        def exempt(self, view_func):
            self.exempted.append(view_func.__name__)
            return view_func

    limiter = FakeLimiter()
    app = Flask(__name__)
    register_admin_proxy_routes(
        app,
        SimpleNamespace(
            require_admin_page=lambda: (lambda: None),
            monitor_url=lambda: "http://127.0.0.1:19999",
            tickets_url=lambda: "https://paperclip.test/FUN/issues",
            timeout_seconds=lambda: 3,
            limiter=lambda: limiter,
        ),
    )

    assert limiter.exempted == ["admin_monitor", "admin_tickets"]


def test_admin_proxy_blocks_non_admin_before_upstream_request():
    app = _make_app(denied=Response("blocked", status=403))

    with patch("web.admin_proxy_routes.requests.request") as request_mock:
        response = app.test_client().get("/admin/monitor")

    assert response.status_code == 403
    assert response.get_data(as_text=True) == "blocked"
    request_mock.assert_not_called()


def test_admin_monitor_proxies_subpath_query_and_filters_sensitive_headers():
    app = _make_app()

    with patch("web.admin_proxy_routes.requests.request", return_value=_upstream_response()) as request_mock:
        response = app.test_client().post(
            "/admin/monitor/api/status?verbose=1",
            data=b"payload",
            headers={"Authorization": "Bearer secret", "X-Test": "keep"},
        )

    assert response.status_code == 200
    _, kwargs = request_mock.call_args
    assert request_mock.call_args.args[:2] == ("POST", "http://127.0.0.1:19999/api/status?verbose=1")
    assert kwargs["data"] == b"payload"
    assert kwargs["headers"]["X-Test"] == "keep"
    assert kwargs["headers"]["X-Funba-Admin-Proxy"] == "1"
    assert "Authorization" not in kwargs["headers"]
    assert "Cookie" not in kwargs["headers"]


def test_admin_proxy_disables_browser_caching_of_rewritten_responses():
    app = _make_app()
    upstream = _upstream_response(
        b"ok",
        headers={
            "Content-Type": "text/html",
            "Cache-Control": "public",
            "Expires": "Wed, 22 Apr 2026 08:22:36 GMT",
            "ETag": "upstream",
        },
    )

    with patch("web.admin_proxy_routes.requests.request", return_value=upstream):
        response = app.test_client().get("/admin/monitor")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store, no-cache, must-revalidate, max-age=0"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["Expires"] == "0"
    assert "ETag" not in response.headers


def test_admin_tickets_root_uses_configured_entry_path():
    app = _make_app(tickets_url="https://paperclip.test/FUN/issues")

    with patch("web.admin_proxy_routes.requests.request", return_value=_upstream_response()) as request_mock:
        response = app.test_client().get("/admin/tickets")

    assert response.status_code == 302
    assert response.headers["Location"] == "/admin/tickets/FUN/issues"
    request_mock.assert_not_called()


def test_admin_tickets_full_subpaths_are_relative_to_upstream_root():
    app = _make_app(tickets_url="https://paperclip.test/FUN/issues")

    with patch("web.admin_proxy_routes.requests.request", return_value=_upstream_response()) as request_mock:
        response = app.test_client().get("/admin/tickets/FUN/issues/FUN-208")

    assert response.status_code == 200
    assert request_mock.call_args.args[:2] == ("GET", "https://paperclip.test/FUN/issues/FUN-208")


def test_admin_tickets_rewrites_root_relative_html_links_and_redirects():
    app = _make_app(tickets_url="https://paperclip.test/FUN/issues")
    upstream = _upstream_response(
        (
            b"<html><head>"
            b'<script type="module">import x from "/@react-refresh";</script>'
            b"</head><body>"
            b'<a href="/FUN/issues/FUN-209">next</a>'
            b'<a href="https://paperclip.test/FUN/issues/FUN-211">absolute</a>'
            b'<a href="https://external.test/path">external</a>'
            b'<script src="/assets/app.js"></script>'
            b"</body></html>"
        ),
        headers={
            "Content-Type": "text/html; charset=utf-8",
            "Location": "/FUN/issues/FUN-210",
            "Set-Cookie": "upstream=secret",
        },
    )

    with patch("web.admin_proxy_routes.requests.request", return_value=upstream):
        response = app.test_client().get("/admin/tickets/FUN/issues")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'href="/admin/tickets/FUN/issues/FUN-209"' in body
    assert 'href="/admin/tickets/FUN/issues/FUN-211"' in body
    assert 'href="https://external.test/path"' in body
    assert 'from "/admin/tickets/@react-refresh"' in body
    assert 'src="/admin/tickets/assets/app.js"' in body
    assert 'window.__PAPERCLIP_BASENAME__="/admin/tickets"' in body
    assert response.headers["Location"] == "/admin/tickets/FUN/issues/FUN-210"
    assert "Set-Cookie" not in response.headers


def test_admin_proxy_rewrites_javascript_root_imports_and_fetches():
    app = _make_app(tickets_url="https://paperclip.test/FUN/issues")
    upstream = _upstream_response(
        (
            b'import "/@vite/client";\n'
            b'import mod from "/src/main.tsx";\n'
            b'fetch("/api/issues");\n'
            b'const route = "/agents/all";\n'
            b'jsxDEV(BrowserRouter, { children: app });\n'
            b'const ws = `${protocol}://${window.location.host}/api/companies/id/events/ws`;'
        ),
        headers={"Content-Type": "text/javascript"},
    )

    with patch("web.admin_proxy_routes.requests.request", return_value=upstream):
        response = app.test_client().get("/admin/tickets/src/main.tsx")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'import "/admin/tickets/@vite/client";' in body
    assert 'from "/admin/tickets/src/main.tsx";' in body
    assert 'fetch("/admin/tickets/api/issues");' in body
    assert 'const route = "/agents/all";' in body
    assert 'jsxDEV(BrowserRouter, { basename: "/admin/tickets", children: app });' in body
    assert "`${protocol}://${window.location.host}/admin/tickets/api/companies/id/events/ws`" in body


def test_admin_monitor_keeps_agent_api_url_concatenated_paths_single_mounted():
    app = _make_app()
    upstream = _upstream_response(
        (
            b'return fetch(window.envSettings.agentApiUrl + "/api/v1/registry?action=hello");\n'
            b'return fetch(`${window.envSettings.agentApiUrl}/api/v3/info`);\n'
            b'return cloudRequest("/api/v3/spaces");'
        ),
        headers={"Content-Type": "text/javascript"},
    )

    with patch("web.admin_proxy_routes.requests.request", return_value=upstream):
        response = app.test_client().get("/admin/monitor")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'agentApiUrl + "/api/v1/registry?action=hello"' in body
    assert '`${window.envSettings.agentApiUrl}/api/v3/info`' in body
    assert 'cloudRequest("/admin/monitor/api/v3/spaces")' in body
    assert "/admin/monitor/admin/monitor/api" not in body


def test_admin_proxy_reports_missing_target_config():
    app = _make_app(tickets_url=None)

    response = app.test_client().get("/admin/tickets")

    assert response.status_code == 503
    assert "not configured" in response.get_data(as_text=True)

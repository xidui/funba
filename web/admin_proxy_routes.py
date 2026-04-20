from __future__ import annotations

import re
from types import SimpleNamespace
from urllib.parse import quote, urlsplit, urlunsplit

import requests
from flask import Response, request


_PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
_ROOT_MARKER = "__root"

_REQUEST_HEADER_BLOCKLIST = {
    "accept-encoding",
    "authorization",
    "connection",
    "content-length",
    "cookie",
    "host",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

_RESPONSE_HEADER_BLOCKLIST = {
    "connection",
    "content-encoding",
    "content-length",
    "content-security-policy",
    "proxy-authenticate",
    "set-cookie",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

_URL_ATTR_RE = re.compile(
    r"(?P<prefix>\b(?:href|src|action|poster|data-url)=['\"])(?P<url>/(?!/)[^'\"]*)",
    re.IGNORECASE,
)
_CSS_URL_RE = re.compile(r"(?P<prefix>url\(\s*['\"]?)(?P<url>/(?!/)[^'\")\s]*)", re.IGNORECASE)
_ABS_URL_ATTR_RE = re.compile(
    r"(?P<prefix>\b(?:href|src|action|poster|data-url)=['\"])(?P<url>https?://[^'\"]*)",
    re.IGNORECASE,
)
_ABS_CSS_URL_RE = re.compile(r"(?P<prefix>url\(\s*['\"]?)(?P<url>https?://[^'\")\s]*)", re.IGNORECASE)


def _validated_entry_url(raw_url: str | None) -> str | None:
    value = str(raw_url or "").strip()
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Admin proxy target must be an absolute http(s) URL")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/") or "/", parsed.query, ""))


def _entry_parts(entry_url: str):
    parsed = urlsplit(entry_url)
    entry_path = parsed.path.rstrip("/")
    if entry_path == "":
        entry_path = "/"
    return parsed, entry_path


def _join_query(base_query: str, request_query: bytes) -> str:
    current_query = request_query.decode("utf-8", errors="ignore")
    if base_query and current_query:
        return f"{base_query}&{current_query}"
    return base_query or current_query


def _target_url(entry_url: str, route_path: str) -> str:
    parsed, entry_path = _entry_parts(entry_url)
    clean_route_path = (route_path or "").lstrip("/")

    if not clean_route_path:
        target_path = entry_path
        target_query = _join_query(parsed.query, request.query_string)
    elif clean_route_path == _ROOT_MARKER or clean_route_path.startswith(f"{_ROOT_MARKER}/"):
        root_path = clean_route_path[len(_ROOT_MARKER) :].lstrip("/")
        target_path = f"/{quote(root_path, safe='/:@!$&()*+,;=-._~')}" if root_path else "/"
        target_query = request.query_string.decode("utf-8", errors="ignore")
    else:
        quoted_path = quote(clean_route_path, safe="/:@!$&()*+,;=-._~")
        target_path = f"{entry_path.rstrip('/')}/{quoted_path}" if entry_path != "/" else f"/{quoted_path}"
        target_query = request.query_string.decode("utf-8", errors="ignore")

    return urlunsplit((parsed.scheme, parsed.netloc, target_path or "/", target_query, ""))


def _request_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, value in request.headers.items():
        if key.lower() in _REQUEST_HEADER_BLOCKLIST:
            continue
        headers[key] = value
    headers["X-Funba-Admin-Proxy"] = "1"
    return headers


def _mounted_url_for_upstream_path(entry_url: str, mount_prefix: str, upstream_url: str) -> str:
    parsed_url = urlsplit(upstream_url)
    _, entry_path = _entry_parts(entry_url)
    upstream_path = parsed_url.path or "/"

    if entry_path != "/" and (upstream_path == entry_path or upstream_path.startswith(f"{entry_path}/")):
        mounted_path = upstream_path[len(entry_path) :].lstrip("/")
        mounted = f"{mount_prefix}/{mounted_path}" if mounted_path else mount_prefix
    else:
        root_path = upstream_path.lstrip("/")
        if entry_path == "/":
            mounted = f"{mount_prefix}/{root_path}" if root_path else mount_prefix
        else:
            mounted = f"{mount_prefix}/{_ROOT_MARKER}/{root_path}" if root_path else f"{mount_prefix}/{_ROOT_MARKER}"

    if parsed_url.query:
        mounted = f"{mounted}?{parsed_url.query}"
    if parsed_url.fragment:
        mounted = f"{mounted}#{parsed_url.fragment}"
    return mounted


def _rewrite_location(entry_url: str, mount_prefix: str, location: str) -> str:
    raw_location = str(location or "")
    if not raw_location:
        return raw_location

    entry = urlsplit(entry_url)
    loc = urlsplit(raw_location)

    if loc.scheme or loc.netloc:
        if loc.scheme == entry.scheme and loc.netloc == entry.netloc:
            return _mounted_url_for_upstream_path(entry_url, mount_prefix, raw_location)
        return raw_location

    if raw_location.startswith("/"):
        absolute = urlunsplit((entry.scheme, entry.netloc, loc.path, loc.query, loc.fragment))
        return _mounted_url_for_upstream_path(entry_url, mount_prefix, absolute)

    return raw_location


def _rewrite_body(entry_url: str, mount_prefix: str, content: bytes, encoding: str | None) -> bytes:
    charset = encoding or "utf-8"
    try:
        text = content.decode(charset)
    except UnicodeDecodeError:
        text = content.decode("utf-8", errors="replace")

    def replace_root_attr(match: re.Match[str]) -> str:
        return f"{match.group('prefix')}{_rewrite_location(entry_url, mount_prefix, match.group('url'))}"

    text = _URL_ATTR_RE.sub(replace_root_attr, text)
    text = _CSS_URL_RE.sub(replace_root_attr, text)
    text = _ABS_URL_ATTR_RE.sub(replace_root_attr, text)
    text = _ABS_CSS_URL_RE.sub(replace_root_attr, text)

    return text.encode(charset, errors="replace")


def _response_headers(upstream_response: requests.Response, entry_url: str, mount_prefix: str) -> list[tuple[str, str]]:
    headers: list[tuple[str, str]] = []
    for key, value in upstream_response.headers.items():
        lower_key = key.lower()
        if lower_key in _RESPONSE_HEADER_BLOCKLIST:
            continue
        if lower_key == "location":
            value = _rewrite_location(entry_url, mount_prefix, value)
        headers.append((key, value))
    return headers


def _proxy_target(entry_url: str | None, mount_prefix: str, route_path: str, timeout_seconds: float) -> Response:
    try:
        validated_url = _validated_entry_url(entry_url)
    except ValueError as exc:
        return Response(str(exc), status=503, content_type="text/plain; charset=utf-8")

    if not validated_url:
        return Response("Admin proxy target is not configured.", status=503, content_type="text/plain; charset=utf-8")

    target_url = _target_url(validated_url, route_path)
    try:
        upstream = requests.request(
            request.method,
            target_url,
            headers=_request_headers(),
            data=request.get_data(),
            allow_redirects=False,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        return Response(f"Admin proxy upstream request failed: {exc}", status=502, content_type="text/plain; charset=utf-8")

    content = upstream.content
    content_type = upstream.headers.get("Content-Type", "")
    if any(kind in content_type.lower() for kind in ("text/html", "text/css")):
        content = _rewrite_body(validated_url, mount_prefix, content, upstream.encoding)

    return Response(
        content,
        status=upstream.status_code,
        headers=_response_headers(upstream, validated_url, mount_prefix),
    )


def register_admin_proxy_routes(app, deps: SimpleNamespace):
    timeout_seconds = float(getattr(deps, "timeout_seconds", lambda: 30.0)())

    def admin_monitor(path: str = ""):
        denied = deps.require_admin_page()()
        if denied:
            return denied
        return _proxy_target(deps.monitor_url(), "/admin/monitor", path, timeout_seconds)

    def admin_tickets(path: str = ""):
        denied = deps.require_admin_page()()
        if denied:
            return denied
        return _proxy_target(deps.tickets_url(), "/admin/tickets", path, timeout_seconds)

    app.add_url_rule("/admin/monitor", endpoint="admin_monitor", view_func=admin_monitor, defaults={"path": ""}, methods=_PROXY_METHODS)
    app.add_url_rule("/admin/monitor/", endpoint="admin_monitor_slash", view_func=admin_monitor, defaults={"path": ""}, methods=_PROXY_METHODS)
    app.add_url_rule("/admin/monitor/<path:path>", endpoint="admin_monitor_path", view_func=admin_monitor, methods=_PROXY_METHODS)
    app.add_url_rule("/admin/tickets", endpoint="admin_tickets", view_func=admin_tickets, defaults={"path": ""}, methods=_PROXY_METHODS)
    app.add_url_rule("/admin/tickets/", endpoint="admin_tickets_slash", view_func=admin_tickets, defaults={"path": ""}, methods=_PROXY_METHODS)
    app.add_url_rule("/admin/tickets/<path:path>", endpoint="admin_tickets_path", view_func=admin_tickets, methods=_PROXY_METHODS)

    return SimpleNamespace(
        admin_monitor=admin_monitor,
        admin_tickets=admin_tickets,
    )

#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, time as dt_time, timedelta, timezone
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


REAL_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 "
    "Safari/537.36 Edg/145.0.0.0"
)
_IMAGE_PLACEHOLDER_RE = re.compile(r"^\s*\[\[IMAGE:(.+?)\]\]\s*$")
_TAGS_PLACEHOLDER_RE = re.compile(r"^\s*\[\[TAGS:(.+?)\]\]\s*$")
_URL_RE = re.compile(r"https?://\S+")
_TITLE_LIMIT = 20
_BODY_LIMIT = 1000
_MAX_POST_AGE_HOURS = 24.0
_SOURCE_DATE_LOCAL_TZ = ZoneInfo("America/Los_Angeles")


def _default_funba_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _http_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> Any:
    body = None
    headers = {
        "User-Agent": REAL_BROWSER_UA,
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"{method} {url} -> HTTP {exc.code}: {detail[:400]}") from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {url} failed: {exc}") from exc
    return json.loads(raw) if raw else None


def _find_delivery_bundle(post: dict[str, Any], delivery_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    for variant in post.get("variants") or []:
        for delivery in variant.get("deliveries") or []:
            if int(delivery.get("id")) == delivery_id:
                return variant, delivery
    raise RuntimeError(f"Delivery {delivery_id} not found in post {post.get('id')}")


def _trim_output(text: str, limit: int = 800) -> str:
    flat = re.sub(r"\s+", " ", (text or "").strip())
    return flat[:limit]


def _extract_published_url(output: str) -> str | None:
    matches = re.findall(r"^Post submitted! URL:\s*(.+?)\s*$", output, flags=re.MULTILINE)
    if not matches:
        return None
    candidate = matches[-1].strip()
    if not candidate or candidate.lower() == "not detected":
        return None
    return candidate


def _update_delivery_status(base_url: str, delivery_id: int, payload: dict[str, Any]) -> None:
    _http_json(
        f"{base_url}/api/content/deliveries/{delivery_id}/status",
        method="POST",
        payload=payload,
    )


def _source_date_age_hours(source_date: str | None, *, now_utc: datetime | None = None) -> float | None:
    raw = str(source_date or "").strip()
    if not raw:
        return None
    if "T" not in raw and " " not in raw:
        try:
            source_day = datetime.fromisoformat(raw).date()
        except ValueError:
            return None
        source_dt_local = datetime.combine(source_day + timedelta(days=1), dt_time.min, tzinfo=_SOURCE_DATE_LOCAL_TZ)
        source_dt = source_dt_local.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        try:
            source_dt = datetime.fromisoformat(raw)
        except ValueError:
            try:
                source_dt = datetime.fromisoformat(f"{raw}T00:00:00")
            except ValueError:
                return None
        if source_dt.tzinfo is not None:
            source_dt = source_dt.astimezone(timezone.utc).replace(tzinfo=None)
    now = now_utc or datetime.utcnow()
    return (now - source_dt).total_seconds() / 3600.0


def _preflight_publish_guard_error(post: dict[str, Any], delivery: dict[str, Any]) -> str | None:
    post_status = str(post.get("status") or "").strip().lower()
    if post_status != "approved":
        return (
            f"Refusing to publish delivery {delivery.get('id')} because post {post.get('id')} "
            f"is not approved (current status: {post.get('status') or 'unknown'})"
        )
    age_hours = _source_date_age_hours(post.get("source_date"))
    if age_hours is not None and age_hours > _MAX_POST_AGE_HOURS:
        return (
            f"Refusing to publish delivery {delivery.get('id')} because post {post.get('id')} "
            f"is stale ({age_hours:.1f}h since source_date {post.get('source_date')})"
        )
    return None


def _body_text_for_xiaohongshu(content_raw: str) -> str:
    lines: list[str] = []
    for raw_line in str(content_raw or "").splitlines():
        if _IMAGE_PLACEHOLDER_RE.match(raw_line):
            continue
        tags = _parse_tags_placeholder(raw_line)
        if tags is not None:
            continue
        lines.append(raw_line.rstrip())
    tags = _extract_tags_from_content(content_raw)
    if tags:
        if lines and lines[-1] != "":
            lines.append("")
        lines.append(" ".join(tags))
    return "\n".join(lines).strip()


def _parse_tags_placeholder(line: str) -> list[str] | None:
    match = _TAGS_PLACEHOLDER_RE.match(line or "")
    if not match:
        return None
    payload = match.group(1)
    seen: set[str] = set()
    ordered: list[str] = []
    for tag in re.findall(r"#([^\s#]+)", payload):
        candidate = f"#{tag.strip()}"
        if not candidate or candidate == "#" or candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
    return ordered


def _extract_tags_from_content(content_raw: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw_line in str(content_raw or "").splitlines():
        tags = _parse_tags_placeholder(raw_line)
        if not tags:
            continue
        for tag in tags:
            if tag in seen:
                continue
            seen.add(tag)
            ordered.append(tag)
    return ordered


def _estimated_body_length_for_xiaohongshu(content_raw: str) -> int:
    body = _body_text_for_xiaohongshu(content_raw)
    tags = _extract_tags_from_content(content_raw)
    if not tags:
        return len(body)
    divider = 1 if body else 0
    return len(body) + divider + len(" ".join(tags))


def _enabled_image_count(post: dict[str, Any]) -> int:
    return sum(1 for image in (post.get("images") or []) if bool(image.get("is_enabled")))


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish one Funba Xiaohongshu delivery with a fixed timeout.")
    parser.add_argument("--post-id", type=int, required=True)
    parser.add_argument("--delivery-id", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--funba-base-url",
        "--funba-admin-base-url",
        dest="funba_base_url",
        default="http://127.0.0.1:5001",
        help="Funba admin API base URL for fetching delivery payloads and updating delivery status.",
    )
    parser.add_argument("--funba-repo-root", default=str(_default_funba_repo_root()))
    args = parser.parse_args()

    base_url = args.funba_base_url.rstrip("/")
    funba_repo_root = Path(args.funba_repo_root).expanduser().resolve()
    funba_python = funba_repo_root / ".venv" / "bin" / "python"
    if not funba_python.exists():
        raise RuntimeError(f"Funba Python not found: {funba_python}")

    post = _http_json(f"{base_url}/api/admin/content/{args.post_id}")
    variant, delivery = _find_delivery_bundle(post, args.delivery_id)
    if str(delivery.get("platform") or "").strip().lower() != "xiaohongshu":
        raise RuntimeError(f"Delivery {args.delivery_id} is not Xiaohongshu")
    if not bool(delivery.get("is_enabled", True)):
        raise RuntimeError(f"Delivery {args.delivery_id} is disabled")
    if str(delivery.get("status") or "").strip().lower() == "published":
        published_url = str(delivery.get("published_url") or "").strip()
        if published_url:
            print(published_url)
            return 0
        raise RuntimeError(f"Delivery {args.delivery_id} is already published")
    guard_error = _preflight_publish_guard_error(post, delivery)
    if guard_error:
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": guard_error})
        print(guard_error)
        return 1

    title = str(variant.get("title") or "").strip()
    content_raw = str(variant.get("content_raw") or "")
    body_text = _body_text_for_xiaohongshu(content_raw)
    estimated_body_length = _estimated_body_length_for_xiaohongshu(content_raw)
    image_count = _enabled_image_count(post)
    if not title or not body_text:
        err = f"Xiaohongshu delivery {args.delivery_id} missing title/body"
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
        print(err)
        return 1
    if len(title) > _TITLE_LIMIT:
        err = (
            f"Xiaohongshu title too long: {len(title)} characters "
            f"(current creator title limit appears to be {_TITLE_LIMIT})"
        )
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
        print(err)
        return 1
    if image_count <= 0:
        err = f"Xiaohongshu delivery {args.delivery_id} requires at least one enabled image"
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
        print(err)
        return 1
    if estimated_body_length > _BODY_LIMIT:
        err = (
            f"Xiaohongshu body too long: {estimated_body_length} characters "
            f"(current creator graph-note limit appears to be {_BODY_LIMIT})"
        )
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
        print(err)
        return 1
    if _URL_RE.search(body_text):
        err = "Xiaohongshu body contains external URL text; normal graph-note delivery should not rely on links"
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
        print(err)
        return 1

    _update_delivery_status(base_url, args.delivery_id, {"status": "publishing"})

    check_cmd = [str(funba_python), "-u", "-m", "social_media.xiaohongshu.post", "check"]
    check_proc = subprocess.run(
        check_cmd,
        cwd=str(funba_repo_root),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if check_proc.returncode != 0:
        err = _trim_output(check_proc.stdout + "\n" + check_proc.stderr)
        _update_delivery_status(
            base_url,
            args.delivery_id,
            {"status": "failed", "error_message": f"Xiaohongshu check failed: {err}"},
        )
        print(f"failed: Xiaohongshu check failed: {err}")
        return 1

    post_cmd = [
        str(funba_python),
        "-u",
        "-m",
        "social_media.xiaohongshu.post",
        "post",
        "--title",
        title,
        "--content",
        content_raw,
        "--post-id",
        str(args.post_id),
        "--submit",
    ]

    try:
        post_proc = subprocess.run(
            post_cmd,
            cwd=str(funba_repo_root),
            capture_output=True,
            text=True,
            timeout=int(args.timeout_seconds),
        )
    except subprocess.TimeoutExpired as exc:
        err = f"Xiaohongshu publish timed out after >{args.timeout_seconds}s"
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
        print(err)
        if exc.stdout:
            print(exc.stdout)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        return 1

    output = (post_proc.stdout or "") + ("\n" + post_proc.stderr if post_proc.stderr else "")
    if post_proc.returncode != 0:
        err = _trim_output(output)
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
        print(err)
        return 1

    published_url = _extract_published_url(output)
    payload: dict[str, Any] = {"status": "published"}
    if published_url:
        payload["published_url"] = published_url
    _update_delivery_status(base_url, args.delivery_id, payload)
    print(published_url or "published")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

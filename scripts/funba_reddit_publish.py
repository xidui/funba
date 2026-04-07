#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


REAL_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 "
    "Safari/537.36 Edg/145.0.0.0"
)
_MAX_POST_AGE_HOURS = 24.0


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


def _slugify_artifact_part(value: str | None) -> str:
    cleaned = re.sub(r"[^\w-]+", "-", str(value or "").strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "artifact"


def _attempt_artifact_dir(
    funba_repo_root: Path,
    *,
    post_id: int,
    delivery_id: int,
    subreddit: str,
    attempt: int,
) -> Path:
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = (
        funba_repo_root
        / "logs"
        / "reddit_publish"
        / f"post{post_id}-delivery{delivery_id}-{_slugify_artifact_part(subreddit)}-{stamp}"
        / f"attempt-{attempt}"
    )
    base.mkdir(parents=True, exist_ok=True)
    return base


def _write_text_artifact(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(content or ""), encoding="utf-8")


def _decorate_error(summary: str, *, artifact_dir: Path, wrapper_log_path: Path | None = None) -> str:
    extras = [f"artifacts: {artifact_dir}"]
    if wrapper_log_path is not None:
        extras.append(f"full_log: {wrapper_log_path}")
    return _trim_output(f"{summary} ({'; '.join(extras)})")


def _output_reached_submit_phase(output: str) -> bool:
    text = str(output or "")
    return any(
        marker in text
        for marker in (
            "Draft prepared.",
            "[DRY RUN] Draft filled but not submitted.",
            "Post submitted! URL:",
            "Submit completed but Reddit post URL was not detected",
        )
    )


def _is_retryable_reddit_publish_failure(output: str, *, timed_out: bool = False) -> bool:
    text = str(output or "")
    non_retryable_markers = (
        "ERROR: Not logged in.",
        "Delivery ",
        "Invalid subreddit:",
        "missing title/content/subreddit",
        "Post submitted! URL:",
        "Submit completed but Reddit post URL was not detected",
    )
    if any(marker in text for marker in non_retryable_markers):
        return False

    retryable_markers = (
        "Reddit title input not found",
        "Reddit content editor not found",
        "Reddit submit button not found",
        "net::ERR",
        "Target page, context or browser has been closed",
    )
    if any(marker in text for marker in retryable_markers):
        return True

    if timed_out:
        return not _output_reached_submit_phase(text)

    return not _output_reached_submit_phase(text)


def _final_attempt_error(attempt_errors: list[str]) -> str:
    if not attempt_errors:
        return "Reddit publish failed without any captured error output"
    if len(attempt_errors) == 1:
        return attempt_errors[0]
    return _trim_output(
        f"Reddit publish failed after {len(attempt_errors)} attempts. Last error: {attempt_errors[-1]}"
    )


def _extract_published_url(output: str) -> str | None:
    matches = re.findall(r"^Post submitted! URL:\s*(.+?)\s*$", output, flags=re.MULTILINE)
    if not matches:
        return None
    candidate = matches[-1].strip()
    return candidate or None


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
    try:
        source_dt = datetime.fromisoformat(raw)
    except ValueError:
        try:
            source_dt = datetime.fromisoformat(f"{raw}T00:00:00")
        except ValueError:
            return None
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish one Funba Reddit delivery with a fixed timeout.")
    parser.add_argument("--post-id", type=int, required=True)
    parser.add_argument("--delivery-id", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=float, default=5.0)
    parser.add_argument(
        "--funba-base-url",
        "--funba-admin-base-url",
        dest="funba_base_url",
        default="http://127.0.0.1:5001",
        help="Funba admin API base URL for fetching delivery payloads and updating delivery status.",
    )
    parser.add_argument("--funba-repo-root", default=str(_default_funba_repo_root()))
    parser.add_argument("--submit", action="store_true", help="Actually submit to Reddit (default: dry run)")
    args = parser.parse_args()

    base_url = args.funba_base_url.rstrip("/")
    funba_repo_root = Path(args.funba_repo_root).expanduser().resolve()
    funba_python = funba_repo_root / ".venv" / "bin" / "python"
    if not funba_python.exists():
        raise RuntimeError(f"Funba Python not found: {funba_python}")

    post = _http_json(f"{base_url}/api/admin/content/{args.post_id}")
    variant, delivery = _find_delivery_bundle(post, args.delivery_id)
    if str(delivery.get("platform") or "").strip().lower() != "reddit":
        raise RuntimeError(f"Delivery {args.delivery_id} is not Reddit")
    if not bool(delivery.get("is_enabled", True)):
        raise RuntimeError(f"Delivery {args.delivery_id} is disabled")
    if args.submit and str(delivery.get("status") or "").strip().lower() == "published":
        published_url = str(delivery.get("published_url") or "").strip()
        if published_url:
            print(published_url)
            return 0
        raise RuntimeError(f"Delivery {args.delivery_id} is already published")
    guard_error = _preflight_publish_guard_error(post, delivery)
    if guard_error:
        if args.submit:
            _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": guard_error})
        print(guard_error)
        return 1

    title = str(variant.get("title") or "").strip()
    content = str(variant.get("content_raw") or "")
    subreddit = str(delivery.get("forum") or "").strip()
    if not title or not content or not subreddit:
        raise RuntimeError(f"Delivery {args.delivery_id} missing title/content/subreddit")

    if args.submit:
        _update_delivery_status(base_url, args.delivery_id, {"status": "publishing"})

    check_cmd = [str(funba_python), "-u", "-m", "social_media.reddit.post", "check"]
    check_proc = subprocess.run(
        check_cmd,
        cwd=str(funba_repo_root),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if check_proc.returncode != 0:
        err = _trim_output(check_proc.stdout + "\n" + check_proc.stderr)
        if args.submit:
            _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": f"Reddit check failed: {err}"})
        print(f"failed: Reddit check failed: {err}")
        return 1

    max_attempts = max(int(args.max_attempts), 1)
    retry_delay_seconds = max(float(args.retry_delay_seconds), 0.0)
    attempt_errors: list[str] = []

    for attempt in range(1, max_attempts + 1):
        artifact_dir = _attempt_artifact_dir(
            funba_repo_root,
            post_id=args.post_id,
            delivery_id=args.delivery_id,
            subreddit=subreddit,
            attempt=attempt,
        )
        wrapper_log_path = artifact_dir / "wrapper_output.log"
        post_cmd = [
            str(funba_python),
            "-u",
            "-m",
            "social_media.reddit.post",
            "post",
            "--title",
            title,
            "--content",
            content,
            "--subreddit",
            subreddit,
            "--post-id",
            str(args.post_id),
            "--artifact-dir",
            str(artifact_dir),
        ]
        if args.submit:
            post_cmd.append("--submit")

        try:
            post_proc = subprocess.run(
                post_cmd,
                cwd=str(funba_repo_root),
                capture_output=True,
                text=True,
                timeout=int(args.timeout_seconds),
            )
        except subprocess.TimeoutExpired as exc:
            output = (exc.stdout or "") + ("\n" + exc.stderr if exc.stderr else "")
            _write_text_artifact(wrapper_log_path, output)
            err = f"Reddit publish timed out after >{args.timeout_seconds}s for subreddit {subreddit}"
            attempt_errors.append(err)
            retryable = attempt < max_attempts and _is_retryable_reddit_publish_failure(output, timed_out=True)
            if retryable:
                print(
                    f"Retryable Reddit timeout on attempt {attempt}/{max_attempts}: "
                    f"{_decorate_error(err, artifact_dir=artifact_dir, wrapper_log_path=wrapper_log_path)}"
                )
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
                continue
            final_err = _decorate_error(
                _final_attempt_error(attempt_errors),
                artifact_dir=artifact_dir,
                wrapper_log_path=wrapper_log_path,
            )
            if args.submit:
                _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": final_err})
            print(final_err)
            if exc.stdout:
                print(exc.stdout)
            if exc.stderr:
                print(exc.stderr, file=sys.stderr)
            return 1

        output = (post_proc.stdout or "") + ("\n" + post_proc.stderr if post_proc.stderr else "")
        _write_text_artifact(wrapper_log_path, output)
        if post_proc.returncode != 0:
            err = _trim_output(output)
            attempt_errors.append(err)
            retryable = attempt < max_attempts and _is_retryable_reddit_publish_failure(output)
            if retryable:
                print(
                    f"Retryable Reddit publish failure on attempt {attempt}/{max_attempts}: "
                    f"{_decorate_error(err, artifact_dir=artifact_dir, wrapper_log_path=wrapper_log_path)}"
                )
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
                continue
            final_err = _decorate_error(
                _final_attempt_error(attempt_errors),
                artifact_dir=artifact_dir,
                wrapper_log_path=wrapper_log_path,
            )
            if args.submit:
                _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": final_err})
            print(final_err)
            return 1

        if not args.submit:
            print(f"[DRY RUN] Reddit draft prepared for r/{subreddit}")
            print(f"Artifacts: {artifact_dir}")
            return 0

        published_url = _extract_published_url(output)
        if not published_url:
            err = _decorate_error(
                "Reddit post command succeeded but did not return a published URL",
                artifact_dir=artifact_dir,
                wrapper_log_path=wrapper_log_path,
            )
            _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": err})
            print(err)
            return 1

        _update_delivery_status(
            base_url,
            args.delivery_id,
            {"status": "published", "published_url": published_url},
        )
        print(published_url)
        return 0

    final_err = _decorate_error(
        _final_attempt_error(attempt_errors),
        artifact_dir=artifact_dir,
        wrapper_log_path=wrapper_log_path,
    )
    if args.submit:
        _update_delivery_status(base_url, args.delivery_id, {"status": "failed", "error_message": final_err})
    print(final_err)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

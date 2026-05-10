"""Prepare and optionally submit an X/Twitter reply.

This is intentionally a manual confirmation tool. The daily engagement
discovery task creates disabled reply drafts; it never calls this module.

Usage:
    python -m social_media.twitter.reply --tweet-url "https://x.com/user/status/..." --content "..."
    python -m social_media.twitter.reply --tweet-url "https://x.com/user/status/..." --content "..." --submit
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import re
import sys
import time

from .post import (
    DEFAULT_COMPOSER_READY_TIMEOUT_SECONDS,
    DEFAULT_TWEET_LIMIT,
    _click_post,
    _env_float,
    _first_selector_present,
    _normalize_status_url,
    _playwright,
    _resolve_artifact_dir,
    _safe_page_screenshot,
    _safe_page_url,
    _set_composer_text,
    _tweet_text_for_twitter,
    _estimated_tweet_length,
    _persist_twitter_failure_artifacts,
    _write_json_artifact,
    _write_text_artifact,
    _create_context,
)

_SHORT_SLEEP = 0.1
_MEDIUM_SLEEP = 0.4
_LONG_SLEEP = 1.0


def _status_id_from_url(value: str | None) -> str | None:
    normalized = _normalize_status_url(value)
    if not normalized:
        return None
    match = re.search(r"/status/(\d+)", normalized)
    return match.group(1) if match else None


def _reply_target_url(value: str | None) -> str:
    normalized = _normalize_status_url(value)
    if not normalized:
        raise ValueError("A valid X/Twitter status URL is required for replies.")
    return normalized


def _reply_button_selectors() -> list[str]:
    return [
        '[data-testid="reply"]',
        '[aria-label*="Reply" i][role="button"]',
        'button[aria-label*="Reply" i]',
    ]


def _composer_button_selectors() -> list[str]:
    return [
        '[data-testid="tweetButtonInline"]',
        '[data-testid="tweetButton"]',
        'button[data-testid*="tweetButton"]',
    ]


def record_successful_reply_in_db(
    session,
    *,
    inbound_message_id: int,
    delivery_id: int | None,
    final_url: str,
    content: str,
    account_handle: str,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    from db.models import (
        SocialPostDelivery,
        TwitterEngagementConversation,
        TwitterEngagementMessage,
    )

    tweet_id = _status_id_from_url(final_url)
    if not tweet_id:
        raise ValueError("A final X/Twitter status URL is required to record an outbound reply.")
    now_value = now_utc or datetime.utcnow()
    inbound = session.get(TwitterEngagementMessage, int(inbound_message_id))
    if inbound is None:
        raise ValueError(f"TwitterEngagementMessage not found: {inbound_message_id}")
    conversation = session.get(TwitterEngagementConversation, inbound.conversation_id)
    if conversation is None:
        raise ValueError(f"TwitterEngagementConversation not found: {inbound.conversation_id}")

    outbound = (
        session.query(TwitterEngagementMessage)
        .filter(TwitterEngagementMessage.tweet_id == tweet_id)
        .first()
    )
    normalized_url = _normalize_status_url(final_url) or final_url
    if outbound is None:
        outbound = TwitterEngagementMessage(
            conversation_id=conversation.id,
            tweet_id=tweet_id,
            x_conversation_id=conversation.x_conversation_id,
            parent_tweet_id=inbound.tweet_id,
            direction="outbound",
            status="sent",
            author_id=None,
            author_username=str(account_handle or "").strip().lstrip("@") or None,
            author_name=None,
            author_verified=False,
            author_followers_count=0,
            text=content,
            tweet_url=normalized_url,
            posted_at=now_value,
            discovered_at=now_value,
            discovered_query=None,
            public_metrics_json=json.dumps({}, ensure_ascii=False),
            raw_payload_json=json.dumps({}, ensure_ascii=False),
            score=None,
            score_reason=None,
            matched_game_ids=inbound.matched_game_ids,
            reply_post_id=inbound.reply_post_id,
            created_at=now_value,
            updated_at=now_value,
        )
        session.add(outbound)
        session.flush()
    else:
        outbound.conversation_id = conversation.id
        outbound.x_conversation_id = conversation.x_conversation_id
        outbound.parent_tweet_id = inbound.tweet_id
        outbound.direction = "outbound"
        outbound.status = "sent"
        outbound.author_username = str(account_handle or "").strip().lstrip("@") or outbound.author_username
        outbound.text = content
        outbound.tweet_url = normalized_url
        outbound.posted_at = now_value
        outbound.matched_game_ids = inbound.matched_game_ids
        outbound.reply_post_id = inbound.reply_post_id
        outbound.updated_at = now_value

    inbound.status = "replied"
    inbound.updated_at = now_value
    conversation.last_replied_at = now_value
    conversation.last_seen_tweet_id = tweet_id
    conversation.last_seen_at = now_value
    conversation.updated_at = now_value

    if delivery_id is not None:
        delivery = session.get(SocialPostDelivery, int(delivery_id))
        if delivery is None:
            raise ValueError(f"SocialPostDelivery not found: {delivery_id}")
        delivery.status = "published"
        delivery.content_final = content
        delivery.published_url = normalized_url
        delivery.published_at = now_value
        delivery.error_message = None
        delivery.updated_at = now_value

    return {
        "conversation_id": conversation.id,
        "inbound_message_id": inbound.id,
        "outbound_message_id": outbound.id,
        "tweet_id": tweet_id,
        "delivery_id": delivery_id,
    }


def _open_reply_composer(page, tweet_url: str, *, timeout_seconds: float) -> str | None:
    page.goto(tweet_url, wait_until="domcontentloaded", timeout=30000)
    time.sleep(_LONG_SLEEP)
    if "/login" in str(getattr(page, "url", "")):
        raise RuntimeError("Not logged in. X/Twitter redirected to login.")

    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        for selector in _reply_button_selectors():
            try:
                button = page.locator(selector).first
                button.wait_for(state="visible", timeout=1000)
                button.click(timeout=3000)
                time.sleep(_MEDIUM_SLEEP)
                return selector
            except Exception as exc:
                last_error = exc
                continue
        time.sleep(_MEDIUM_SLEEP)
    raise RuntimeError(f"X/Twitter reply button not found: {last_error}")


def _record_browser_events(page, browser_events: dict[str, list[dict[str, str]]]) -> None:
    page.on(
        "console",
        lambda msg: browser_events["console"].append(
            {"type": str(getattr(msg, "type", "unknown")), "text": str(getattr(msg, "text", ""))}
        ),
    )
    page.on("pageerror", lambda exc: browser_events["pageerror"].append({"text": str(exc)}))
    page.on(
        "requestfailed",
        lambda request: browser_events["requestfailed"].append(
            {
                "url": str(getattr(request, "url", "")),
                "method": str(getattr(request, "method", "")),
                "failure": str(request.failure) if getattr(request, "failure", None) else "",
            }
        ),
    )


def cmd_reply(args: argparse.Namespace) -> None:
    try:
        tweet_url = _reply_target_url(args.tweet_url)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    raw_content = str(args.content or "")
    content = _tweet_text_for_twitter(raw_content)
    submit = bool(args.submit)
    artifact_dir = _resolve_artifact_dir(getattr(args, "artifact_dir", None), post_id=None)
    keep_open_seconds = max(float(getattr(args, "keep_open_seconds", 0) or 0), 0.0)
    headed = bool(getattr(args, "headed", False)) or keep_open_seconds > 0
    tweet_limit = max(int(getattr(args, "tweet_limit", DEFAULT_TWEET_LIMIT) or DEFAULT_TWEET_LIMIT), 1)
    estimated_length = _estimated_tweet_length(content)

    print(f"Reply target: {tweet_url}")
    print(f"Estimated X length: {estimated_length}/{tweet_limit}")
    print(f"Submit: {'YES' if submit else 'NO (draft only)'}")
    print(f"Artifacts: {artifact_dir}")
    print()

    _write_json_artifact(
        artifact_dir / "request.json",
        {
            "tweet_url": tweet_url,
            "submit": submit,
            "raw_content": raw_content,
            "reply_text": content,
            "estimated_length": estimated_length,
            "tweet_limit": tweet_limit,
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )

    if not content.strip():
        print("ERROR: X/Twitter reply content is required.")
        sys.exit(1)
    if estimated_length > tweet_limit:
        print(f"ERROR: X/Twitter reply too long: estimated {estimated_length}/{tweet_limit}.")
        sys.exit(1)

    stage = "starting"
    context = None
    page = None
    browser_events: dict[str, list[dict[str, str]]] = {
        "console": [],
        "pageerror": [],
        "requestfailed": [],
    }
    try:
        with _playwright() as pw:
            context = _create_context(pw, headless=not headed)
            page = context.new_page()
            _record_browser_events(page, browser_events)

            stage = "open_reply_composer"
            composer_timeout = _env_float(
                "FUNBA_TWITTER_COMPOSER_READY_TIMEOUT_SECONDS",
                DEFAULT_COMPOSER_READY_TIMEOUT_SECONDS,
            )
            reply_selector = _open_reply_composer(page, tweet_url, timeout_seconds=composer_timeout)
            _safe_page_screenshot(page, artifact_dir / "reply_loaded.png")

            stage = "fill_reply"
            text_selector = _set_composer_text(page, content, timeout_seconds=composer_timeout)
            if not text_selector:
                raise RuntimeError("X/Twitter reply text box not found")
            time.sleep(_MEDIUM_SLEEP)
            _safe_page_screenshot(page, artifact_dir / "reply_filled.png")
            print("Reply draft prepared.")

            if not submit:
                _write_json_artifact(
                    artifact_dir / "result.json",
                    {
                        "status": "dry_run",
                        "target_url": tweet_url,
                        "page_url": _safe_page_url(page),
                        "reply_selector": reply_selector,
                        "text_selector": text_selector,
                        "post_button_selector": _first_selector_present(page, _composer_button_selectors()),
                        "captured_at": datetime.utcnow().isoformat() + "Z",
                    },
                )
                print("[DRY RUN] Reply filled but not submitted.")
                if keep_open_seconds > 0:
                    print(f"Keeping browser open for {keep_open_seconds:g}s for review.")
                    time.sleep(keep_open_seconds)
                print("Pass --submit to actually reply.")
                return

            stage = "submit_reply"
            expected_handle = os.getenv("FUNBA_TWITTER_ACCOUNT_HANDLE", "FUNBA_APP")
            final_url = _click_post(page, expected_handle=expected_handle)
            time.sleep(_LONG_SLEEP)
            _safe_page_screenshot(page, artifact_dir / "reply_submitted.png")
            _write_json_artifact(
                artifact_dir / "result.json",
                {
                    "status": "published",
                    "target_url": tweet_url,
                    "final_url": final_url,
                    "page_url": _safe_page_url(page),
                    "captured_at": datetime.utcnow().isoformat() + "Z",
                },
            )
            if final_url:
                print(f"Reply submitted! URL: {final_url}")
            else:
                print("Reply submitted! URL: not detected")
            if final_url and getattr(args, "message_id", None):
                from sqlalchemy.orm import Session

                from db.models import engine

                with Session(engine) as session:
                    db_result = record_successful_reply_in_db(
                        session,
                        inbound_message_id=int(args.message_id),
                        delivery_id=int(args.delivery_id) if getattr(args, "delivery_id", None) else None,
                        final_url=final_url,
                        content=content,
                        account_handle=expected_handle,
                    )
                    session.commit()
                print(f"Recorded outbound reply message {db_result['outbound_message_id']} in Funba DB.")
    except Exception as exc:
        if page is not None:
            _persist_twitter_failure_artifacts(
                artifact_dir,
                page=page,
                stage=stage,
                content=content,
                post_id=None,
                exception=exc,
                browser_events=browser_events,
            )
        else:
            _write_text_artifact(artifact_dir / "failure.txt", str(exc))
        print(f"ERROR: {exc}")
        print(f"Artifacts saved to: {artifact_dir}")
        sys.exit(1)
    finally:
        if context is not None:
            try:
                context.close()
            except Exception:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m social_media.twitter.reply",
        description="Prepare and optionally submit a manual X/Twitter reply.",
    )
    parser.add_argument("--tweet-url", required=True, help="Target X/Twitter status URL to reply to")
    parser.add_argument("--content", required=True, help="Reply body")
    parser.add_argument("--artifact-dir", help="Directory for debug screenshots/logs/artifacts")
    parser.add_argument("--message-id", type=int, help="Inbound TwitterEngagementMessage ID to mark replied")
    parser.add_argument("--delivery-id", type=int, help="SocialPostDelivery ID to mark published after submit")
    parser.add_argument("--tweet-limit", type=int, default=DEFAULT_TWEET_LIMIT)
    parser.add_argument(
        "--keep-open-seconds",
        type=float,
        default=0,
        help="In dry-run mode, keep the visible browser open for manual review.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Show the browser window for manual confirmation. Default is headless.",
    )
    parser.add_argument("--submit", action="store_true", help="Actually submit the reply")
    cmd_reply(parser.parse_args())


if __name__ == "__main__":
    main()

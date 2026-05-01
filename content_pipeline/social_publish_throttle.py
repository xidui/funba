"""DB-backed social publish throttles.

Admin approval moves a delivery into the publishable pool.  This module decides
whether one Twitter delivery may be reserved for publishing right now.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from db.models import Setting, SocialPost, SocialPostDelivery, SocialPostVariant

THROTTLED_PLATFORMS = ("twitter", "instagram")
PLATFORM_ALIASES = {"x": "twitter", "ig": "instagram"}
TWITTER_THROTTLE_ENABLED_KEY = "social.twitter.throttle.enabled"
TWITTER_THROTTLE_MIN_INTERVAL_KEY = "social.twitter.throttle.min_min"
TWITTER_THROTTLE_MAX_PER_DAY_KEY = "social.twitter.throttle.daily_max"
TWITTER_THROTTLE_MAX_PER_GAME_DAY_KEY = "social.twitter.throttle.game_daily_max"
TWITTER_THROTTLE_MAX_PENDING_AGE_KEY = "social.twitter.throttle.max_age_h"

THROTTLE_TIMEZONE = ZoneInfo("America/Los_Angeles")


@dataclass(frozen=True)
class SocialThrottleConfig:
    enabled: bool = True
    min_interval_minutes: int = 60
    max_posts_per_day: int = 3
    max_posts_per_game_per_day: int = 1
    max_pending_age_hours: int = 24


TwitterThrottleConfig = SocialThrottleConfig


def normalize_throttled_platform(platform: str) -> str | None:
    raw = str(platform or "").strip().lower()
    normalized = PLATFORM_ALIASES.get(raw, raw)
    return normalized if normalized in THROTTLED_PLATFORMS else None


def _platform_db_values(platform: str) -> tuple[str, ...]:
    if platform == "twitter":
        return ("twitter", "x")
    return (platform,)


def _setting_key(platform: str, suffix: str) -> str:
    return f"social.{platform}.throttle.{suffix}"


def _setting_value(session: Session, key: str) -> str | None:
    row = session.get(Setting, key)
    return str(row.value) if row is not None else None


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


def _parse_int(value: str | int | None, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def get_social_throttle_config(session: Session, platform: str) -> SocialThrottleConfig:
    normalized = normalize_throttled_platform(platform)
    if normalized is None:
        raise ValueError(f"unsupported throttled platform: {platform}")
    defaults = SocialThrottleConfig()
    return SocialThrottleConfig(
        enabled=_parse_bool(_setting_value(session, _setting_key(normalized, "enabled")), defaults.enabled),
        min_interval_minutes=_parse_int(
            _setting_value(session, _setting_key(normalized, "min_min")),
            defaults.min_interval_minutes,
            minimum=0,
            maximum=24 * 60,
        ),
        max_posts_per_day=_parse_int(
            _setting_value(session, _setting_key(normalized, "daily_max")),
            defaults.max_posts_per_day,
            minimum=0,
            maximum=50,
        ),
        max_posts_per_game_per_day=_parse_int(
            _setting_value(session, _setting_key(normalized, "game_daily_max")),
            defaults.max_posts_per_game_per_day,
            minimum=0,
            maximum=20,
        ),
        max_pending_age_hours=_parse_int(
            _setting_value(session, _setting_key(normalized, "max_age_h")),
            defaults.max_pending_age_hours,
            minimum=0,
            maximum=24 * 14,
        ),
    )


def get_twitter_throttle_config(session: Session) -> TwitterThrottleConfig:
    return get_social_throttle_config(session, "twitter")


def _write_setting(session: Session, key: str, value: str) -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    row = session.get(Setting, key)
    if row is None:
        session.add(Setting(key=key, value=value, updated_at=now))
    else:
        row.value = value
        row.updated_at = now


def update_social_throttle_config(session: Session, platform: str, payload: dict) -> SocialThrottleConfig:
    normalized = normalize_throttled_platform(platform)
    if normalized is None:
        raise ValueError(f"unsupported throttled platform: {platform}")
    current = get_social_throttle_config(session, normalized)
    values = {
        "enabled": _parse_bool(payload.get("enabled"), current.enabled),
        "min_interval_minutes": _parse_int(
            payload.get("min_interval_minutes"),
            current.min_interval_minutes,
            minimum=0,
            maximum=24 * 60,
        ),
        "max_posts_per_day": _parse_int(
            payload.get("max_posts_per_day"),
            current.max_posts_per_day,
            minimum=0,
            maximum=50,
        ),
        "max_posts_per_game_per_day": _parse_int(
            payload.get("max_posts_per_game_per_day"),
            current.max_posts_per_game_per_day,
            minimum=0,
            maximum=20,
        ),
        "max_pending_age_hours": _parse_int(
            payload.get("max_pending_age_hours"),
            current.max_pending_age_hours,
            minimum=0,
            maximum=24 * 14,
        ),
    }
    _write_setting(session, _setting_key(normalized, "enabled"), "true" if values["enabled"] else "false")
    _write_setting(session, _setting_key(normalized, "min_min"), str(values["min_interval_minutes"]))
    _write_setting(session, _setting_key(normalized, "daily_max"), str(values["max_posts_per_day"]))
    _write_setting(session, _setting_key(normalized, "game_daily_max"), str(values["max_posts_per_game_per_day"]))
    _write_setting(session, _setting_key(normalized, "max_age_h"), str(values["max_pending_age_hours"]))
    return SocialThrottleConfig(**values)


def update_twitter_throttle_config(session: Session, payload: dict) -> TwitterThrottleConfig:
    return update_social_throttle_config(session, "twitter", payload)


def _as_utc_naive(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _local_day_bounds_utc(target: datetime) -> tuple[datetime, datetime, date]:
    aware = target.replace(tzinfo=UTC) if target.tzinfo is None else target.astimezone(UTC)
    local_day = aware.astimezone(THROTTLE_TIMEZONE).date()
    start_local = datetime.combine(local_day, time.min, tzinfo=THROTTLE_TIMEZONE)
    end_local = start_local + timedelta(days=1)
    return (
        start_local.astimezone(UTC).replace(tzinfo=None),
        end_local.astimezone(UTC).replace(tzinfo=None),
        local_day,
    )


def _decode_game_ids(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    try:
        parsed = json.loads(value)
    except Exception:
        return ()
    if not isinstance(parsed, list):
        return ()
    out: list[str] = []
    for item in parsed:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return tuple(out)


def _activity_timestamp(delivery: SocialPostDelivery) -> datetime | None:
    if delivery.status == "published":
        return _as_utc_naive(delivery.published_at) or _as_utc_naive(delivery.updated_at)
    if delivery.status == "publishing":
        return _as_utc_naive(delivery.updated_at) or _as_utc_naive(delivery.created_at)
    return None


def _latest_social_activity_at(session: Session, platform: str) -> datetime | None:
    rows = (
        session.query(SocialPostDelivery)
        .filter(
            SocialPostDelivery.platform.in_(_platform_db_values(platform)),
            SocialPostDelivery.status.in_(("published", "publishing")),
        )
        .order_by(SocialPostDelivery.updated_at.desc(), SocialPostDelivery.id.desc())
        .limit(25)
        .all()
    )
    timestamps = [ts for ts in (_activity_timestamp(row) for row in rows) if ts is not None]
    return max(timestamps) if timestamps else None


def _today_activity_rows(session: Session, platform: str, start_utc: datetime, end_utc: datetime):
    return (
        session.query(SocialPostDelivery, SocialPost, SocialPostVariant)
        .join(SocialPostVariant, SocialPostVariant.id == SocialPostDelivery.variant_id)
        .join(SocialPost, SocialPost.id == SocialPostVariant.post_id)
        .filter(
            SocialPostDelivery.platform.in_(_platform_db_values(platform)),
            SocialPostDelivery.status.in_(("published", "publishing")),
            or_(
                and_(
                    SocialPostDelivery.status == "published",
                    SocialPostDelivery.published_at >= start_utc,
                    SocialPostDelivery.published_at < end_utc,
                ),
                and_(
                    SocialPostDelivery.status == "publishing",
                    SocialPostDelivery.updated_at >= start_utc,
                    SocialPostDelivery.updated_at < end_utc,
                ),
            ),
        )
        .all()
    )


def _candidate_rows(session: Session, platform: str, now_utc: datetime, config: SocialThrottleConfig):
    query = (
        session.query(SocialPostDelivery, SocialPost, SocialPostVariant)
        .join(SocialPostVariant, SocialPostVariant.id == SocialPostDelivery.variant_id)
        .join(SocialPost, SocialPost.id == SocialPostVariant.post_id)
        .filter(
            SocialPostDelivery.platform.in_(_platform_db_values(platform)),
            SocialPostDelivery.is_enabled.is_(True),
            SocialPostDelivery.status == "pending",
            SocialPostVariant.status == "approved",
            SocialPost.status != "archived",
        )
    )
    if config.max_pending_age_hours > 0:
        query = query.filter(SocialPostDelivery.created_at >= now_utc - timedelta(hours=config.max_pending_age_hours))
    return (
        query.order_by(
            SocialPost.source_date.desc(),
            SocialPost.priority.asc(),
            SocialPostDelivery.created_at.asc(),
            SocialPostDelivery.id.asc(),
        )
        .all()
    )


def social_throttle_status(session: Session, platform: str, *, now_utc: datetime | None = None) -> dict:
    normalized = normalize_throttled_platform(platform)
    if normalized is None:
        raise ValueError(f"unsupported throttled platform: {platform}")
    now = _as_utc_naive(now_utc) or datetime.now(UTC).replace(tzinfo=None)
    config = get_social_throttle_config(session, normalized)
    start_utc, end_utc, local_day = _local_day_bounds_utc(now)
    activity_rows = _today_activity_rows(session, normalized, start_utc, end_utc)
    pending_count = (
        session.query(SocialPostDelivery)
        .join(SocialPostVariant, SocialPostVariant.id == SocialPostDelivery.variant_id)
        .filter(
            SocialPostDelivery.platform.in_(_platform_db_values(normalized)),
            SocialPostDelivery.is_enabled.is_(True),
            SocialPostDelivery.status == "pending",
            SocialPostVariant.status == "approved",
        )
        .count()
    )
    latest_activity_at = _latest_social_activity_at(session, normalized)
    return {
        "platform": normalized,
        "config": asdict(config),
        "local_day": local_day.isoformat(),
        "published_or_reserved_today": len(activity_rows),
        "pending_approved": int(pending_count),
        "last_activity_at": latest_activity_at.isoformat() if latest_activity_at else None,
    }


def twitter_throttle_status(session: Session, *, now_utc: datetime | None = None) -> dict:
    return social_throttle_status(session, "twitter", now_utc=now_utc)


def dispatch_next_social_delivery(
    session: Session,
    *,
    platform: str,
    now_utc: datetime | None = None,
    enqueue_publish: Callable[[int, int], None] | None = None,
) -> dict:
    """Reserve and enqueue at most one approved pending delivery for a platform."""
    normalized = normalize_throttled_platform(platform)
    if normalized is None:
        raise ValueError(f"unsupported throttled platform: {platform}")
    now = _as_utc_naive(now_utc) or datetime.now(UTC).replace(tzinfo=None)
    config = get_social_throttle_config(session, normalized)
    start_utc, end_utc, local_day = _local_day_bounds_utc(now)

    if not config.enabled:
        return {"ok": True, "platform": normalized, "status": "disabled", "config": asdict(config)}

    if config.max_posts_per_day <= 0:
        return {"ok": True, "platform": normalized, "status": "daily_cap_zero", "config": asdict(config)}

    latest_activity_at = _latest_social_activity_at(session, normalized)
    if latest_activity_at is not None and config.min_interval_minutes > 0:
        next_allowed = latest_activity_at + timedelta(minutes=config.min_interval_minutes)
        if now < next_allowed:
            return {
                "ok": True,
                "platform": normalized,
                "status": "waiting_interval",
                "last_activity_at": latest_activity_at.isoformat(),
                "next_allowed_at": next_allowed.isoformat(),
                "config": asdict(config),
            }

    activity_rows = _today_activity_rows(session, normalized, start_utc, end_utc)
    if len(activity_rows) >= config.max_posts_per_day:
        return {
            "ok": True,
            "platform": normalized,
            "status": "daily_cap_reached",
            "local_day": local_day.isoformat(),
            "published_or_reserved_today": len(activity_rows),
            "config": asdict(config),
        }

    per_game_counts: dict[str, int] = {}
    for _delivery, post, _variant in activity_rows:
        for game_id in _decode_game_ids(post.source_game_ids):
            per_game_counts[game_id] = per_game_counts.get(game_id, 0) + 1

    candidates = _candidate_rows(session, normalized, now, config)
    skipped_game_cap: list[int] = []
    for delivery, post, _variant in candidates:
        game_ids = _decode_game_ids(post.source_game_ids)
        if config.max_posts_per_game_per_day <= 0 and game_ids:
            skipped_game_cap.append(int(delivery.id))
            continue
        if game_ids and any(
            per_game_counts.get(game_id, 0) >= config.max_posts_per_game_per_day
            for game_id in game_ids
        ):
            skipped_game_cap.append(int(delivery.id))
            continue

        delivery.status = "publishing"
        delivery.error_message = None
        delivery.updated_at = now
        session.flush()

        def _default_enqueue(post_id: int, delivery_id: int) -> None:
            from tasks.content import publish_social_delivery_task

            publish_social_delivery_task.apply_async(
                args=(post_id, delivery_id),
                kwargs={"platform": normalized},
                retry=False,
            )

        try:
            (enqueue_publish or _default_enqueue)(int(post.id), int(delivery.id))
        except Exception:
            delivery.status = "pending"
            delivery.error_message = "Twitter throttle failed to enqueue publisher"
            delivery.updated_at = now
            session.flush()
            raise

        return {
            "ok": True,
            "platform": normalized,
            "status": "enqueued",
            "post_id": int(post.id),
            "delivery_id": int(delivery.id),
            "local_day": local_day.isoformat(),
            "published_or_reserved_today": len(activity_rows) + 1,
            "config": asdict(config),
        }

    return {
        "ok": True,
        "platform": normalized,
        "status": "no_eligible_delivery",
        "candidate_count": len(candidates),
        "skipped_game_cap_delivery_ids": skipped_game_cap,
        "local_day": local_day.isoformat(),
        "published_or_reserved_today": len(activity_rows),
        "config": asdict(config),
    }


def dispatch_next_twitter_delivery(
    session: Session,
    *,
    now_utc: datetime | None = None,
    enqueue_publish: Callable[[int, int], None] | None = None,
) -> dict:
    return dispatch_next_social_delivery(
        session,
        platform="twitter",
        now_utc=now_utc,
        enqueue_publish=enqueue_publish,
    )

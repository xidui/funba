"""Deterministic social variants for curated game hero highlights.

This pipeline intentionally does not involve Paperclip or LLM drafting.  It
turns the already-curated hero card snapshots into short, review-ready social
posts, then relies on the existing human approval + delivery pipeline.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from typing import Callable
from urllib.parse import quote, urlencode

from sqlalchemy.orm import Session

from db.models import (
    Game,
    MetricDefinition,
    MetricResult,
    Player,
    SocialPost,
    SocialPostDelivery,
    SocialPostVariant,
    Team,
)

PUBLIC_BASE_URL = "https://funba.app"
DEFAULT_HERO_HIGHLIGHT_PLATFORMS = ("twitter",)
HERO_HIGHLIGHT_PLATFORMS_ENV = "FUNBA_HERO_HIGHLIGHT_PLATFORMS"
HERO_HIGHLIGHT_TOPIC_PREFIX = "Hero Highlight"
HERO_HIGHLIGHT_STATUS = "in_review"


@dataclass(frozen=True)
class HeroHighlightCard:
    game_id: str
    game_date: date | None
    season: str | None
    matchup: str
    scope: str
    metric_key: str
    entity_id: str | None
    entity_label: str | None
    metric_name: str
    metric_name_zh: str | None
    narrative_zh: str | None
    narrative_en: str | None
    value_text: str
    rank_text: str
    rank_window: str
    top_results: tuple[str, ...]
    metric_url: str
    game_url: str


def _json_dict(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_platform(value: str) -> str | None:
    platform = str(value or "").strip().lower()
    if platform == "x":
        return "twitter"
    return platform if platform in HERO_HIGHLIGHT_RENDERERS else None


def enabled_hero_highlight_platforms(environ: dict[str, str] | None = None) -> list[str]:
    env = environ if environ is not None else os.environ
    raw = env.get(HERO_HIGHLIGHT_PLATFORMS_ENV)
    if raw is None:
        candidates = list(DEFAULT_HERO_HIGHLIGHT_PLATFORMS)
    else:
        candidates = [part.strip() for part in raw.split(",") if part.strip()]

    out: list[str] = []
    for candidate in candidates:
        platform = _normalize_platform(candidate)
        if platform and platform not in out:
            out.append(platform)
    return out


def _public_game_url(game: Game) -> str:
    path = quote(str(game.slug or game.game_id))
    return f"{PUBLIC_BASE_URL}/games/{path}"


def _public_metric_url(metric_key: str, season: str | None = None) -> str:
    path = quote(str(metric_key))
    url = f"{PUBLIC_BASE_URL}/metrics/{path}"
    if season:
        url += "?" + urlencode({"season": season})
    return url


def _matchup_text(game: Game, teams: dict[str, str]) -> str:
    road = teams.get(str(game.road_team_id), str(game.road_team_id or "?"))
    home = teams.get(str(game.home_team_id), str(game.home_team_id or "?"))
    return f"{road} @ {home}"


def _metric_names(session: Session, metric_keys: set[str]) -> dict[str, tuple[str, str | None]]:
    if not metric_keys:
        return {}
    rows = (
        session.query(MetricDefinition.key, MetricDefinition.name, MetricDefinition.name_zh)
        .filter(MetricDefinition.key.in_(metric_keys))
        .all()
    )
    return {str(key): (name or str(key).replace("_", " ").title(), name_zh) for key, name, name_zh in rows}


def _best_rank_context(rank_snapshot: dict) -> tuple[str, str]:
    options = []
    labels = (
        ("alltime", "All-time"),
        ("season", "Season"),
        ("last5", "Last 5"),
        ("last3", "Last 3"),
    )
    for key, label in labels:
        rank = rank_snapshot.get(key)
        total = rank_snapshot.get(f"{key}_total") if key != "season" else rank_snapshot.get("season_total")
        if rank is None:
            continue
        try:
            rank_int = int(rank)
        except (TypeError, ValueError):
            continue
        try:
            total_int = int(total) if total is not None else None
        except (TypeError, ValueError):
            total_int = None
        ratio = (rank_int / total_int) if total_int else None
        options.append((ratio if ratio is not None else 999.0, rank_int, total_int, label))

    if not options:
        return "season", "Ranking: unavailable"

    _ratio, rank, total, label = sorted(options, key=lambda item: (item[0], item[1]))[0]
    window = {
        "All-time": "alltime",
        "Season": "season",
        "Last 5": "last5",
        "Last 3": "last3",
    }.get(label, "season")
    if total:
        return window, f"#{rank} / {total} ({label})"
    return window, f"#{rank} ({label})"


def _metric_rank_order(session: Session, metric_key: str) -> str:
    try:
        from metrics.framework.runtime import get_metric

        metric = get_metric(metric_key, session=session)
    except Exception:
        metric = None
    rank_order = getattr(metric, "rank_order", "desc") if metric is not None else "desc"
    return "asc" if str(rank_order).lower() == "asc" else "desc"


def _season_type_prefix(season: str | None) -> str | None:
    raw = str(season or "")
    return raw[:1] if raw[:1] in {"1", "2", "4", "5"} else None


def _metric_result_season(metric_key: str, season: str | None) -> str:
    raw = str(season or "")
    virtual_suffixes = {
        "all_regular": "regular",
        "all_playoffs": "playoffs",
        "all_playin": "playin",
    }
    suffix = virtual_suffixes.get(raw)
    if suffix:
        if metric_key.endswith("_last5"):
            return f"last5_{suffix}"
        if metric_key.endswith("_last3"):
            return f"last3_{suffix}"
    return raw


def _recent_seasons(session: Session, season: str | None, window: str) -> list[str]:
    prefix = _season_type_prefix(season)
    if prefix is None:
        return []
    limit = 3 if window == "last3" else 5
    rows = (
        session.query(Game.season)
        .filter(Game.season.like(f"{prefix}%"))
        .distinct()
        .order_by(Game.season.desc())
        .limit(limit)
        .all()
    )
    return [str(row[0]) for row in rows if row[0]]


def _season_label(entity_id: str) -> str:
    raw = str(entity_id or "")
    if len(raw) == 5 and raw.isdigit():
        year = raw[1:]
        try:
            return f"{year}-{str(int(year) + 1)[-2:]}"
        except ValueError:
            return raw
    return raw


def _game_entity_label(session: Session, entity_id: str) -> str:
    raw = str(entity_id or "")
    game_id, _sep, team_id = raw.partition(":")
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        return raw
    teams = {
        str(tid): abbr
        for tid, abbr in (
            session.query(Team.team_id, Team.abbr)
            .filter(Team.team_id.in_([game.road_team_id, game.home_team_id, team_id]))
            .all()
        )
        if tid
    }
    label = _matchup_text(game, teams)
    if team_id:
        label += f" ({teams.get(team_id, team_id)})"
    return label


def _result_entity_label(session: Session, row: MetricResult) -> str:
    entity_type = str(row.entity_type or "")
    entity_id = str(row.entity_id or "")
    if entity_type == "player":
        player = session.query(Player.full_name).filter(Player.player_id == entity_id).first()
        return str(player[0]) if player and player[0] else entity_id
    if entity_type == "team":
        team = session.query(Team.abbr, Team.full_name).filter(Team.team_id == entity_id).first()
        return str(team[0] or team[1]) if team else entity_id
    if entity_type == "game":
        return _game_entity_label(session, entity_id)
    if entity_type == "player_franchise":
        player_id, _sep, team_id = entity_id.partition(":")
        player = session.query(Player.full_name).filter(Player.player_id == player_id).first()
        team = session.query(Team.abbr).filter(Team.team_id == team_id).first() if team_id else None
        player_label = str(player[0]) if player and player[0] else player_id
        return f"{player_label} ({team[0]})" if team and team[0] else player_label
    if entity_type == "season":
        return _season_label(entity_id)
    return entity_id


def _format_result_value(row: MetricResult) -> str:
    if row.value_str:
        return str(row.value_str)
    value = row.value_num
    if value is None:
        return "unavailable"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _top_result_query(session: Session, card: "HeroHighlightCard", window: str):
    query = session.query(MetricResult).filter(
        MetricResult.metric_key == card.metric_key,
        MetricResult.value_num.isnot(None),
    )
    season = _metric_result_season(card.metric_key, card.season)
    if window == "season" and season:
        query = query.filter(MetricResult.season == season)
    elif window == "alltime" and _season_type_prefix(season):
        query = query.filter(MetricResult.season.like(f"{_season_type_prefix(season)}%"))
    elif window in {"last3", "last5"} and season.startswith(f"{window}_"):
        query = query.filter(MetricResult.season == season)
    elif window in {"last3", "last5"}:
        recent = _recent_seasons(session, season, window)
        if recent:
            query = query.filter(MetricResult.season.in_(recent))
        elif season:
            query = query.filter(MetricResult.season == season)
    elif season:
        query = query.filter(MetricResult.season == season)
    return query


def _top_three_results(session: Session, card: "HeroHighlightCard") -> tuple[str, ...]:
    rank_order = _metric_rank_order(session, card.metric_key)
    order_col = MetricResult.value_num.asc() if rank_order == "asc" else MetricResult.value_num.desc()

    rows = _top_result_query(session, card, card.rank_window).order_by(order_col, MetricResult.entity_id.asc()).limit(3).all()
    fallback_season = _metric_result_season(card.metric_key, card.season)
    if not rows and fallback_season:
        rows = (
            session.query(MetricResult)
            .filter(
                MetricResult.metric_key == card.metric_key,
                MetricResult.season == fallback_season,
                MetricResult.value_num.isnot(None),
            )
            .order_by(order_col, MetricResult.entity_id.asc())
            .limit(3)
            .all()
        )

    return tuple(
        f"{idx}. {_result_entity_label(session, row)} - {_format_result_value(row)}"
        for idx, row in enumerate(rows, start=1)
    )


def _value_text(entry: dict) -> str:
    raw = entry.get("value_str_snapshot")
    if raw:
        return str(raw)
    value = entry.get("value_snapshot")
    if value is None:
        return "unavailable"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _entry_identity(scope: str, entry: dict) -> str:
    return str(entry.get("entity_id") or ("game" if scope == "game" else "unknown"))


def _hero_entries(game: Game) -> list[tuple[str, dict]]:
    blobs = (
        ("game", game.highlights_curated_json),
        ("player", game.highlights_curated_player_json),
        ("team", game.highlights_curated_team_json),
    )
    out: list[tuple[str, dict]] = []
    seen: set[tuple[str, str, str]] = set()
    for scope, blob in blobs:
        parsed = _json_dict(blob)
        for entry in parsed.get("hero") or []:
            if not isinstance(entry, dict) or not entry.get("metric_key"):
                continue
            identity = (scope, str(entry["metric_key"]), _entry_identity(scope, entry))
            if identity in seen:
                continue
            seen.add(identity)
            out.append((scope, entry))
    return out


def collect_hero_highlight_cards(session: Session, game: Game) -> list[HeroHighlightCard]:
    entries = _hero_entries(game)
    if not entries:
        return []

    teams = {str(tid): abbr for tid, abbr in session.query(Team.team_id, Team.abbr).all() if tid}
    matchup = _matchup_text(game, teams)
    metric_keys = {str(entry["metric_key"]) for _scope, entry in entries}
    metric_name_map = _metric_names(session, metric_keys)

    cards: list[HeroHighlightCard] = []
    for scope, entry in entries:
        metric_key = str(entry["metric_key"])
        metric_name, metric_name_zh = metric_name_map.get(
            metric_key,
            (entry.get("metric_name_snapshot") or metric_key.replace("_", " ").title(), None),
        )
        entity_label = entry.get("player_name") or entry.get("team_abbr")
        if scope == "team" and not entity_label and entry.get("entity_id"):
            entity_label = teams.get(str(entry.get("entity_id")))
        season = entry.get("season") or game.season
        rank_window, rank_text = _best_rank_context(entry.get("rank_snapshot") or {})
        card = HeroHighlightCard(
            game_id=game.game_id,
            game_date=game.game_date,
            season=season,
            matchup=matchup,
            scope=scope,
            metric_key=metric_key,
            entity_id=entry.get("entity_id"),
            entity_label=entity_label,
            metric_name=metric_name,
            metric_name_zh=metric_name_zh,
            narrative_zh=entry.get("narrative_zh") or entry.get("narrative"),
            narrative_en=entry.get("narrative_en"),
            value_text=_value_text(entry),
            rank_text=rank_text,
            rank_window=rank_window,
            top_results=(),
            metric_url=_public_metric_url(metric_key, season if scope != "game" else None),
            game_url=_public_game_url(game),
        )
        cards.append(replace(card, top_results=_top_three_results(session, card)))
    return cards


def _truncate(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: max(limit - 1, 0)].rstrip() + "…"


def _stable_topic(card: HeroHighlightCard) -> str:
    identity = card.entity_id or "game"
    return f"{HERO_HIGHLIGHT_TOPIC_PREFIX} — {card.game_id} — {card.scope} — {card.metric_key} — {identity}"


def _variant_title(card: HeroHighlightCard, platform: str) -> str:
    label = card.entity_label or card.matchup
    return _truncate(f"{card.matchup}: {label} {card.metric_name} ({platform})", 255)


def render_twitter_hero_highlight(card: HeroHighlightCard) -> str:
    metric_label = card.metric_name
    lead = card.narrative_en or card.narrative_zh or f"{card.matchup} hero metric"
    lines = [
        str(lead).strip(),
        "",
        f"Data: {metric_label} = {card.value_text}",
        f"Ranking: {card.rank_text}",
    ]
    if card.top_results:
        lines.extend(["", "Top 3:", *card.top_results])
    lines.extend(["", f"Source: {card.metric_url}", f"Game: {card.game_url}"])
    return "\n".join(lines).strip()


HERO_HIGHLIGHT_RENDERERS: dict[str, Callable[[HeroHighlightCard], str]] = {
    "twitter": render_twitter_hero_highlight,
}


def _find_existing_post(session: Session, topic: str, source_date: date) -> SocialPost | None:
    return (
        session.query(SocialPost)
        .filter(
            SocialPost.topic == topic,
            SocialPost.source_date == source_date,
            SocialPost.status != "archived",
        )
        .order_by(SocialPost.id.asc())
        .first()
    )


def _create_post_for_card(
    session: Session,
    card: HeroHighlightCard,
    *,
    platforms: list[str],
) -> int:
    now = datetime.now(UTC).replace(tzinfo=None)
    topic = _stable_topic(card)
    source_date = card.game_date or date.today()
    existing = _find_existing_post(session, topic, source_date)
    if existing is not None:
        return int(existing.id)

    post = SocialPost(
        topic=topic,
        source_date=source_date,
        source_metrics=json.dumps([card.metric_key], ensure_ascii=False),
        source_game_ids=json.dumps([card.game_id], ensure_ascii=False),
        status=HERO_HIGHLIGHT_STATUS,
        priority=25,
        llm_model=None,
        admin_comments=None,
        created_at=now,
        updated_at=now,
    )
    session.add(post)
    session.flush()

    for platform in platforms:
        renderer = HERO_HIGHLIGHT_RENDERERS[platform]
        variant = SocialPostVariant(
            post_id=post.id,
            title=_variant_title(card, platform),
            content_raw=renderer(card),
            audience_hint=f"deterministic hero highlight / {platform}",
            created_at=now,
            updated_at=now,
        )
        session.add(variant)
        session.flush()
        session.add(
            SocialPostDelivery(
                variant_id=variant.id,
                platform=platform,
                forum=None,
                is_enabled=True,
                status="pending",
                created_at=now,
                updated_at=now,
            )
        )
    return int(post.id)


def generate_hero_highlight_variants_for_game(
    session: Session,
    game_id: str,
    *,
    platforms: list[str] | tuple[str, ...] | None = None,
) -> dict[str, object]:
    selected_platforms = (
        [_normalize_platform(p) for p in platforms]
        if platforms is not None
        else enabled_hero_highlight_platforms()
    )
    selected_platforms = list(dict.fromkeys(p for p in selected_platforms if p))
    if not selected_platforms:
        return {"ok": True, "game_id": game_id, "platforms": [], "created_post_ids": [], "skipped": "no_platforms"}

    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        return {"ok": False, "game_id": game_id, "error": "game_not_found"}

    cards = collect_hero_highlight_cards(session, game)
    created_or_existing: list[int] = []
    for card in cards:
        created_or_existing.append(_create_post_for_card(session, card, platforms=selected_platforms))
    session.commit()
    return {
        "ok": True,
        "game_id": game_id,
        "platforms": selected_platforms,
        "hero_count": len(cards),
        "post_ids": created_or_existing,
    }

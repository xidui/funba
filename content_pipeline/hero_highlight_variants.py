"""Deterministic social variants for curated game hero highlights.

This pipeline intentionally does not involve Paperclip or LLM drafting.  It
turns the already-curated hero card snapshots into short deterministic social
posts.  Platforms that are explicitly marked safe can be auto-approved and
queued for publish; other platforms still wait for human review.
"""
from __future__ import annotations

import json
import logging
import os
import re
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
    SocialPostImage,
    SocialPostVariant,
    Team,
)
from metrics.framework.family import family_career_key, family_window_key, window_type_from_key
from social_media.funba_internal.hero_highlight import render_hero_highlight as render_funba_hero_highlight
from social_media.instagram.hero_highlight import render_hero_highlight as render_instagram_hero_highlight
from social_media.twitter.hero_highlight import render_hero_highlight as render_twitter_hero_highlight

PUBLIC_BASE_URL = "https://funba.app"
FUNBA_INTERNAL_PLATFORM = "funba"
DEFAULT_HERO_HIGHLIGHT_PLATFORMS = ("twitter", FUNBA_INTERNAL_PLATFORM)
HERO_HIGHLIGHT_PLATFORMS_ENV = "FUNBA_HERO_HIGHLIGHT_PLATFORMS"
# Funba's home feed auto-approves + auto-publishes (no external API to push
# to). Twitter requires human review before any external posting — generate
# the variant, but leave the post in_review so an admin can edit + publish.
DEFAULT_HERO_HIGHLIGHT_AUTO_APPROVE_PLATFORMS = (FUNBA_INTERNAL_PLATFORM,)
HERO_HIGHLIGHT_AUTO_APPROVE_PLATFORMS_ENV = "FUNBA_HERO_HIGHLIGHT_AUTO_APPROVE_PLATFORMS"
DEFAULT_HERO_HIGHLIGHT_AUTO_PUBLISH_PLATFORMS = (FUNBA_INTERNAL_PLATFORM,)
HERO_HIGHLIGHT_AUTO_PUBLISH_PLATFORMS_ENV = "FUNBA_HERO_HIGHLIGHT_AUTO_PUBLISH_PLATFORMS"
HERO_HIGHLIGHT_AUTO_PUBLISH_ENV = "FUNBA_HERO_HIGHLIGHT_AUTO_PUBLISH"
HERO_HIGHLIGHT_TOPIC_PREFIX = "Hero Highlight"
HERO_HIGHLIGHT_STATUS = "in_review"
HERO_POSTER_SLOT = "poster"
HERO_POSTER_SQUARE_SLOT = "poster_ig"
HERO_CARD_PIPELINE_KEY = "hero_card"
HERO_CARD_INSTAGRAM_PLATFORM = "instagram"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HeroHighlightCard:
    game_id: str
    game_date: date | None
    season: str | None
    matchup: str
    scope: str
    metric_key: str
    ranking_metric_key: str
    ranking_season: str | None
    entity_id: str | None
    entity_label: str | None
    metric_name: str
    metric_name_zh: str | None
    narrative_zh: str | None
    narrative_en: str | None
    value_text: str
    value_time_label: str | None
    rank_text: str
    rank_window: str
    top_results: tuple[str, ...]
    metric_url: str
    game_url: str


@dataclass(frozen=True)
class HeroHighlightPostResult:
    post_id: int
    created: bool
    auto_publish_deliveries: tuple[tuple[str, int], ...] = ()


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


def _normalized_platform_list(
    *,
    environ: dict[str, str] | None,
    env_key: str,
    default: tuple[str, ...],
) -> list[str]:
    env = environ if environ is not None else os.environ
    raw = env.get(env_key)
    if raw is None:
        candidates = list(default)
    else:
        candidates = [part.strip() for part in raw.split(",") if part.strip()]

    out: list[str] = []
    for candidate in candidates:
        platform = _normalize_platform(candidate)
        if platform and platform not in out:
            out.append(platform)
    return out


def enabled_hero_highlight_platforms(
    environ: dict[str, str] | None = None,
    *,
    session: Session | None = None,
) -> list[str]:
    """Hero-card pipeline platforms that should receive a generated variant.

    Resolution order:
      1. Env-var override (FUNBA_HERO_HIGHLIGHT_PLATFORMS) — explicit operator pin.
      2. Otherwise: admin-edited matrix in the Setting table
         (Settings → Pipeline Publishing → Hero Card row's Generate column).
    """
    env = environ if environ is not None else os.environ
    if env.get(HERO_HIGHLIGHT_PLATFORMS_ENV) is not None:
        return _normalized_platform_list(
            environ=environ,
            env_key=HERO_HIGHLIGHT_PLATFORMS_ENV,
            default=DEFAULT_HERO_HIGHLIGHT_PLATFORMS,
        )

    from content_pipeline.publishing_registry import enabled_generate_platforms

    if session is not None:
        return enabled_generate_platforms(session, "hero_card")

    # No session passed (e.g. unit tests / inspectors) — open a temporary one.
    from db.models import engine

    with Session(engine) as scope_session:
        return enabled_generate_platforms(scope_session, "hero_card")


def auto_approve_hero_highlight_platforms(
    environ: dict[str, str] | None = None,
    *,
    session: Session | None = None,
) -> list[str]:
    """Platforms whose presence flips post.status to 'approved' on creation.

    Same as auto-publish for hero card — keeping a separate function for env
    override compatibility, but they read the same registry cells.
    """
    env = environ if environ is not None else os.environ
    if env.get(HERO_HIGHLIGHT_AUTO_APPROVE_PLATFORMS_ENV) is not None:
        return _normalized_platform_list(
            environ=environ,
            env_key=HERO_HIGHLIGHT_AUTO_APPROVE_PLATFORMS_ENV,
            default=DEFAULT_HERO_HIGHLIGHT_AUTO_APPROVE_PLATFORMS,
        )
    return list(_registry_autopublish_platforms(session))


def auto_publish_hero_highlight_platforms(
    environ: dict[str, str] | None = None,
    *,
    session: Session | None = None,
) -> list[str]:
    env = environ if environ is not None else os.environ
    if env.get(HERO_HIGHLIGHT_AUTO_PUBLISH_PLATFORMS_ENV) is not None:
        return _normalized_platform_list(
            environ=environ,
            env_key=HERO_HIGHLIGHT_AUTO_PUBLISH_PLATFORMS_ENV,
            default=DEFAULT_HERO_HIGHLIGHT_AUTO_PUBLISH_PLATFORMS,
        )
    return list(_registry_autopublish_platforms(session))


def _registry_autopublish_platforms(session: Session | None) -> set[str]:
    from content_pipeline.publishing_registry import autopublish_platforms

    if session is not None:
        return autopublish_platforms(session, "hero_card")
    from db.models import engine

    with Session(engine) as scope_session:
        return autopublish_platforms(scope_session, "hero_card")


def auto_publish_hero_highlight_enabled(environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    raw = env.get(HERO_HIGHLIGHT_AUTO_PUBLISH_ENV)
    if raw is None:
        return True
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _post_status_for_platforms(
    platforms: list[str],
    environ: dict[str, str] | None = None,
    *,
    session: Session | None = None,
) -> str:
    auto_approve = set(auto_approve_hero_highlight_platforms(environ, session=session))
    if platforms and all(platform in auto_approve for platform in platforms):
        return "approved"
    return HERO_HIGHLIGHT_STATUS


def _auto_publish_platform_set(
    environ: dict[str, str] | None = None,
    *,
    session: Session | None = None,
) -> set[str]:
    if not auto_publish_hero_highlight_enabled(environ):
        return set()
    return set(auto_publish_hero_highlight_platforms(environ, session=session))


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
        ("last10", "Last 10"),
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
        "Last 10": "last10",
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
        if metric_key.endswith("_last10"):
            return f"last10_{suffix}"
        if metric_key.endswith("_last5"):
            return f"last5_{suffix}"
        if metric_key.endswith("_last3"):
            return f"last3_{suffix}"
    return raw


def _career_season_for(season: str | None) -> str | None:
    prefix = _season_type_prefix(season)
    return {
        "2": "all_regular",
        "4": "all_playoffs",
        "5": "all_playin",
    }.get(prefix)


def _all_seasons_param(season: str | None) -> str | None:
    prefix = _season_type_prefix(season)
    return f"all_{prefix}" if prefix else None


def _recent_seasons(session: Session, season: str | None, window: str) -> list[str]:
    prefix = _season_type_prefix(season)
    if prefix is None:
        return []
    limit = {"last3": 3, "last5": 5, "last10": 10}.get(window, 5)
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


def _season_year_label(season: str | None) -> str | None:
    raw = str(season or "")
    if len(raw) == 5 and raw.isdigit():
        return _season_label(raw)
    return None


def _game_year_label(session: Session, *, game_id: str | None, entity_id: str | None) -> str | None:
    candidate = str(game_id or "").strip()
    if not candidate and entity_id:
        candidate = str(entity_id).partition(":")[0]
    if not candidate:
        return None
    row = session.query(Game.game_date, Game.season).filter(Game.game_id == candidate).first()
    if not row:
        return None
    game_date, season = row
    if game_date:
        return str(game_date.year)
    return _season_year_label(season)


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


def _result_time_label(session: Session, row: MetricResult, rank_window: str) -> str | None:
    if rank_window != "alltime":
        return None
    entity_type = str(row.entity_type or "")
    if entity_type == "game":
        return _game_year_label(session, game_id=row.game_id, entity_id=row.entity_id)
    if entity_type == "season":
        return None
    return _season_year_label(row.season)


def _format_top_result(session: Session, row: MetricResult, idx: int, rank_window: str) -> str:
    label = _result_entity_label(session, row)
    time_label = _result_time_label(session, row, rank_window)
    if time_label:
        if str(row.entity_type or "") == "game":
            label = re.sub(r"\s+\([A-Z]{2,4}\)$", "", label)
        label = f"{time_label} {label}"
    return f"{idx}. {label} - {_format_result_value(row)}"


def _value_time_label(rank_window: str, game_date: date | None, ranking_season: str | None, season: str | None) -> str | None:
    if rank_window != "alltime":
        return None
    if game_date:
        return str(game_date.year)
    return _season_year_label(ranking_season or season)


def _entry_value_num(entry: dict) -> float | None:
    value = entry.get("value_snapshot")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _metric_row_matches_entry(session: Session, metric_key: str, season: str | None, entity_id: str | None, entry: dict) -> bool:
    if not metric_key or not season or not entity_id:
        return False
    query = session.query(MetricResult.id).filter(
        MetricResult.metric_key == metric_key,
        MetricResult.season == season,
        MetricResult.entity_id == str(entity_id),
    )
    value_num = _entry_value_num(entry)
    value_str = entry.get("value_str_snapshot")
    if value_num is not None:
        query = query.filter(MetricResult.value_num == value_num)
    elif value_str is not None:
        query = query.filter(MetricResult.value_str == str(value_str))
    return query.first() is not None


def _related_milestone_candidates(entry: dict) -> list[tuple[str, str]]:
    context = entry.get("milestone_context_snapshot")
    if not isinstance(context, dict):
        return []
    related = context.get("related_milestones")
    if not isinstance(related, list):
        return []
    out: list[tuple[str, str]] = []
    for item in related:
        if not isinstance(item, dict):
            continue
        metric_key = str(item.get("metric_key") or "").strip()
        season = str(item.get("season") or "").strip()
        if metric_key and season:
            out.append((metric_key, season))
    return out


def _resolve_ranking_metric_context(session: Session, metric_key: str, season: str | None, entry: dict) -> tuple[str, str | None]:
    """Resolve the actual MetricResult pool behind a curated card.

    Milestone curation can pick a window sibling while the actual crossed value
    lives in a related career pool. Prefer an exact MetricResult row matching the
    curated entity/value so the rendered Source and Top 3 point at the same data.
    """
    entity_id = entry.get("entity_id")
    candidates: list[tuple[str, str | None]] = [(metric_key, season)]
    candidates.extend(_related_milestone_candidates(entry))

    mapped_season = _metric_result_season(metric_key, season)
    if mapped_season != str(season or ""):
        candidates.append((metric_key, mapped_season))

    career_season = _career_season_for(season)
    if career_season:
        candidates.append((family_career_key(metric_key), career_season))

    seen: set[tuple[str, str | None]] = set()
    for candidate_key, candidate_season in candidates:
        candidate = (candidate_key, candidate_season)
        if candidate in seen:
            continue
        seen.add(candidate)
        if _metric_row_matches_entry(session, candidate_key, candidate_season, entity_id, entry):
            return candidate_key, candidate_season
    return metric_key, season


def _metric_link_context(card: "HeroHighlightCard") -> tuple[str, str | None]:
    window_type = window_type_from_key(card.ranking_metric_key)
    if window_type:
        return card.ranking_metric_key, card.ranking_season

    if card.scope == "game":
        if card.rank_window in {"last3", "last5", "last10"}:
            season_param = _all_seasons_param(card.ranking_season)
            if season_param:
                return family_window_key(card.ranking_metric_key, card.rank_window), season_param
        if card.rank_window == "alltime":
            season_param = _all_seasons_param(card.ranking_season)
            if season_param:
                return card.ranking_metric_key, season_param

    return card.ranking_metric_key, card.ranking_season


def _top_result_query(session: Session, card: "HeroHighlightCard", window: str):
    query = session.query(MetricResult).filter(
        MetricResult.metric_key == card.ranking_metric_key,
        MetricResult.value_num.isnot(None),
    )
    season = card.ranking_season or ""
    if window == "season" and season:
        query = query.filter(MetricResult.season == season)
    elif window == "alltime" and _season_type_prefix(season):
        query = query.filter(MetricResult.season.like(f"{_season_type_prefix(season)}%"))
    elif window in {"last3", "last5", "last10"} and season.startswith(f"{window}_"):
        query = query.filter(MetricResult.season == season)
    elif window in {"last3", "last5", "last10"}:
        recent = _recent_seasons(session, season, window)
        if recent:
            query = query.filter(MetricResult.season.in_(recent))
        elif season:
            query = query.filter(MetricResult.season == season)
    elif season:
        query = query.filter(MetricResult.season == season)
    return query


def _top_three_results(session: Session, card: "HeroHighlightCard") -> tuple[str, ...]:
    rank_order = _metric_rank_order(session, card.ranking_metric_key)
    order_col = MetricResult.value_num.asc() if rank_order == "asc" else MetricResult.value_num.desc()

    rows = _top_result_query(session, card, card.rank_window).order_by(order_col, MetricResult.entity_id.asc()).limit(3).all()
    fallback_season = card.ranking_season
    if not rows and fallback_season:
        rows = (
            session.query(MetricResult)
            .filter(
                MetricResult.metric_key == card.ranking_metric_key,
                MetricResult.season == fallback_season,
                MetricResult.value_num.isnot(None),
            )
            .order_by(order_col, MetricResult.entity_id.asc())
            .limit(3)
            .all()
        )

    return tuple(_format_top_result(session, row, idx, card.rank_window) for idx, row in enumerate(rows, start=1))


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
        ranking_metric_key, ranking_season = _resolve_ranking_metric_context(session, metric_key, season, entry)
        card = HeroHighlightCard(
            game_id=game.game_id,
            game_date=game.game_date,
            season=season,
            matchup=matchup,
            scope=scope,
            metric_key=metric_key,
            ranking_metric_key=ranking_metric_key,
            ranking_season=ranking_season,
            entity_id=entry.get("entity_id"),
            entity_label=entity_label,
            metric_name=metric_name,
            metric_name_zh=metric_name_zh,
            narrative_zh=entry.get("narrative_zh") or entry.get("narrative"),
            narrative_en=entry.get("narrative_en"),
            value_text=_value_text(entry),
            value_time_label=_value_time_label(rank_window, game.game_date, ranking_season, season),
            rank_text=rank_text,
            rank_window=rank_window,
            top_results=(),
            metric_url="",
            game_url=_public_game_url(game),
        )
        link_metric_key, link_season = _metric_link_context(card)
        cards.append(
            replace(
                card,
                top_results=_top_three_results(session, card),
                metric_url=_public_metric_url(link_metric_key, link_season),
            )
        )
    return cards


def _truncate(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: max(limit - 1, 0)].rstrip() + "…"


def _stable_topic(card: HeroHighlightCard) -> str:
    identity = card.entity_id or "game"
    return f"{HERO_HIGHLIGHT_TOPIC_PREFIX} — {card.game_id} — {card.scope} — {card.metric_key} — {identity}"


def _variant_title(card: HeroHighlightCard, platform: str) -> str:
    label = " ".join(str(card.narrative_en or "").split())
    if not label:
        subject = card.entity_label or card.matchup
        label = f"{subject} {card.metric_name}" if subject else card.metric_name
    return _truncate(f"{card.matchup}: {label} ({platform})", 255)


HERO_HIGHLIGHT_RENDERERS: dict[str, Callable[[HeroHighlightCard], str]] = {
    "twitter": render_twitter_hero_highlight,
    HERO_CARD_INSTAGRAM_PLATFORM: render_instagram_hero_highlight,
    FUNBA_INTERNAL_PLATFORM: render_funba_hero_highlight,
}


def _is_hero_card_instagram_enabled(session: Session) -> bool:
    """Read the publishing matrix to decide whether to also generate + attach
    the square IG sibling poster. Defaults to False — IG is opt-in and costs a
    second gpt-image-2 call per card."""
    try:
        from content_pipeline.publishing_registry import is_generate_enabled
        return is_generate_enabled(session, HERO_CARD_PIPELINE_KEY, HERO_CARD_INSTAGRAM_PLATFORM)
    except Exception:
        logger.exception("hero poster attach: failed to read IG matrix flag — assuming off")
        return False


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


def _existing_hero_post_ids_for_game(session: Session, game: Game) -> list[int]:
    source_date = game.game_date or date.today()
    topic_prefix = f"Hero Highlight — {game.game_id} —%"
    rows = (
        session.query(SocialPost.id)
        .filter(
            SocialPost.topic.like(topic_prefix),
            SocialPost.source_date == source_date,
            SocialPost.status != "archived",
        )
        .order_by(SocialPost.id.asc())
        .all()
    )
    return [int(row[0]) for row in rows]


def _attach_hero_poster(
    session: Session,
    post: SocialPost,
    card: HeroHighlightCard,
    *,
    now: datetime,
    variant: str = "vertical",
    slot: str = HERO_POSTER_SLOT,
) -> None:
    """Find a pre-generated poster for this card (created by the curator hook),
    copy it into media/social_posts/{post.id}/, and create a SocialPostImage row.

    Silent no-op if no poster file exists. Called inside _create_post_for_card
    after the post has been flushed to obtain its id. `variant` selects the
    on-disk source file (vertical = 2:3, square = 1:1 IG sibling); `slot`
    determines which SocialPostImage slot the row is filed under.
    """
    try:
        from social_media.hero_poster import poster_path_for, read_prompt_sidecar
        from social_media.images import store_prepared_image
    except Exception:
        logger.exception("hero poster attach: import failed for post_id=%s", post.id)
        return

    # Reconstruct the entry shape poster_path_for expects
    entry = {
        "metric_key": card.ranking_metric_key or card.metric_key,
        "entity_id": card.entity_id,
        "scope": card.scope,
    }
    game = session.query(Game).filter(Game.game_id == card.game_id).first()
    if game is None:
        return
    src = poster_path_for(entry, game, variant=variant)
    if not src.exists() or src.stat().st_size == 0:
        logger.info("hero poster attach: no %s poster at %s; skipping", variant, src)
        return

    try:
        stored = store_prepared_image(str(src), post_id=int(post.id), slot=slot)
    except Exception:
        logger.exception("hero poster attach: store_prepared_image failed for post_id=%s slot=%s", post.id, slot)
        return

    # Pull the rendered prompt from its sidecar so the admin Assets page can
    # show what we actually asked the model for.
    prompt_text = read_prompt_sidecar(src) or ""
    spec = {
        "source": "hero_poster",
        "variant": variant,
        "metric_key": card.ranking_metric_key or card.metric_key,
        "entity_id": card.entity_id,
        "scope": card.scope,
        "model": "gpt-image-2",
        "game_id": card.game_id,
        "matchup": card.matchup,
        "metric_name": card.metric_name,
        "rank_text": card.rank_text,
        "value_text": card.value_text,
        "source_poster_path": str(src),
        "prompt": prompt_text,
    }
    note_suffix = " (IG square)" if variant == "square" else ""
    image_row = SocialPostImage(
        post_id=int(post.id),
        slot=slot,
        image_type="ai_generated",
        spec=json.dumps(spec, ensure_ascii=False),
        note=f"Hero card poster — {card.metric_name}{note_suffix}",
        file_path=str(stored),
        is_enabled=True,
        created_at=now,
    )
    session.add(image_row)
    session.flush()


def _create_post_for_card(
    session: Session,
    card: HeroHighlightCard,
    *,
    platforms: list[str],
) -> HeroHighlightPostResult:
    from sqlalchemy.exc import IntegrityError

    now = datetime.now(UTC).replace(tzinfo=None)
    topic = _stable_topic(card)
    source_date = card.game_date or date.today()
    existing = _find_existing_post(session, topic, source_date)
    if existing is not None:
        return HeroHighlightPostResult(post_id=int(existing.id), created=False)

    post_status = _post_status_for_platforms(platforms, session=session)
    # Helper called below if the SocialPost INSERT trips the
    # uq_SocialPost_active_dedup_key unique index — meaning another worker
    # raced past our SELECT-then-INSERT and won. Re-fetch and treat that as
    # the "existing" return path. This is the DB-level safety net under the
    # row-lock-based serialization at the curator entry; if both fail we'd
    # rather get a clean idempotent return than a 500.
    def _on_unique_collision() -> HeroHighlightPostResult:
        session.rollback()
        survivor = _find_existing_post(session, topic, source_date)
        if survivor is None:
            # Should not happen: unique index fired but the surviving row
            # isn't visible. Re-raise so the caller knows.
            raise
        return HeroHighlightPostResult(post_id=int(survivor.id), created=False)
    # Auto-publish runs per-platform from the matrix: any platform with its
    # autopublish toggle on auto-publishes the moment the row is written,
    # regardless of whether the overall post.status is approved or in_review.
    # That lets Funba's home feed go live even when Twitter sits in review.
    auto_publish_platforms = _auto_publish_platform_set(session=session)
    auto_approve_set = set(auto_approve_hero_highlight_platforms(session=session))
    auto_publish_deliveries: list[tuple[str, int]] = []

    post = SocialPost(
        topic=topic,
        source_date=source_date,
        source_metrics=json.dumps([card.ranking_metric_key], ensure_ascii=False),
        source_game_ids=json.dumps([card.game_id], ensure_ascii=False),
        status=post_status,
        priority=25,
        llm_model=None,
        admin_comments=None,
        created_at=now,
        updated_at=now,
    )
    session.add(post)
    try:
        session.flush()
    except IntegrityError:
        return _on_unique_collision()

    # Attach the pre-generated hero poster (if any) to this post's image pool.
    _attach_hero_poster(session, post, card, now=now)
    # When IG is enabled in the publishing matrix, also attach the square
    # (1024x1024) sibling so the admin can manually post it to Instagram.
    if _is_hero_card_instagram_enabled(session):
        _attach_hero_poster(
            session,
            post,
            card,
            now=now,
            variant="square",
            slot=HERO_POSTER_SQUARE_SLOT,
        )

    for platform in platforms:
        renderer = HERO_HIGHLIGHT_RENDERERS[platform]
        # Per-variant approval: a platform that the matrix marks as
        # auto-approve lands its variant directly in 'approved' (so the
        # publish guard lets it ship); other platforms wait in 'in_review'
        # until an admin clicks Approve on that specific variant.
        variant_status = "approved" if platform in auto_approve_set else HERO_HIGHLIGHT_STATUS
        variant = SocialPostVariant(
            post_id=post.id,
            title=_variant_title(card, platform),
            content_raw=renderer(card),
            audience_hint=f"deterministic hero highlight / {platform}",
            status=variant_status,
            created_at=now,
            updated_at=now,
        )
        session.add(variant)
        session.flush()
        # Funba's own platform "publishes" by simply having a SocialPostDelivery
        # row visible to the home feed; there's no external API to push to,
        # so we mark it published immediately when auto-publish is enabled.
        is_funba_auto_publish = (
            platform == FUNBA_INTERNAL_PLATFORM and platform in auto_publish_platforms
        )
        delivery = SocialPostDelivery(
            variant_id=variant.id,
            platform=platform,
            forum=None,
            is_enabled=True,
            status="published" if is_funba_auto_publish else "pending",
            content_final=renderer(card) if is_funba_auto_publish else None,
            published_at=now if is_funba_auto_publish else None,
            created_at=now,
            updated_at=now,
        )
        session.add(delivery)
        session.flush()
        # Only enqueue background publishing for platforms with external APIs
        # (Twitter etc). Funba is published the moment the row hits the DB.
        if platform in auto_publish_platforms and platform != FUNBA_INTERNAL_PLATFORM:
            auto_publish_deliveries.append((platform, int(delivery.id)))

    # If funba_internal auto-published, mirror this post into NewsArticle so it
    # surfaces on the news detail page (and clusters / tagging / scoring).
    if FUNBA_INTERNAL_PLATFORM in auto_publish_platforms:
        try:
            from db.news_internal import mirror_published_social_post
            mirror_published_social_post(session, post)
        except Exception:
            logger.exception("hero highlight: mirror_published_social_post failed post_id=%s", post.id)

    return HeroHighlightPostResult(
        post_id=int(post.id),
        created=True,
        auto_publish_deliveries=tuple(auto_publish_deliveries),
    )


def _enqueue_hero_highlight_auto_publish(post_id: int, delivery_id: int, *, platform: str) -> bool:
    normalized = _normalize_platform(platform)
    if not normalized:
        logger.warning("hero highlight auto-publish unknown platform=%s delivery_id=%s", platform, delivery_id)
        return False
    try:
        from tasks.content import publish_social_delivery_task

        publish_social_delivery_task.apply_async(
            args=(post_id, delivery_id),
            kwargs={"platform": normalized},
            retry=False,
        )
        return True
    except Exception as exc:
        logger.warning(
            "failed to enqueue hero highlight auto-publish post_id=%s delivery_id=%s platform=%s: %s",
            post_id,
            delivery_id,
            normalized,
            exc,
            exc_info=True,
        )
        return False


def generate_hero_highlight_variants_for_game(
    session: Session,
    game_id: str,
    *,
    platforms: list[str] | tuple[str, ...] | None = None,
) -> dict[str, object]:
    selected_platforms = (
        [_normalize_platform(p) for p in platforms]
        if platforms is not None
        else enabled_hero_highlight_platforms(session=session)
    )
    selected_platforms = list(dict.fromkeys(p for p in selected_platforms if p))
    if not selected_platforms:
        return {"ok": True, "game_id": game_id, "platforms": [], "created_post_ids": [], "skipped": "no_platforms"}

    # Row-level lock + idempotency check inside one transaction. Concurrent
    # callers (e.g. multiple curator tasks for the same season firing in
    # parallel) serialize on the Game row; the second one through sees
    # variants_generated_at set and bails without calling _create_post_for_card,
    # so we never accumulate duplicate SocialPost rows.
    game = session.query(Game).filter(Game.game_id == game_id).with_for_update().first()
    if game is None:
        return {"ok": False, "game_id": game_id, "error": "game_not_found"}
    if getattr(game, "variants_generated_at", None) is not None:
        existing_post_ids = _existing_hero_post_ids_for_game(session, game)
        session.rollback()
        return {
            "ok": True,
            "game_id": game_id,
            "platforms": selected_platforms,
            "post_ids": existing_post_ids,
            "created_post_ids": [],
            "skipped": "variants_already_generated",
        }

    cards = collect_hero_highlight_cards(session, game)
    post_results: list[HeroHighlightPostResult] = []
    for card in cards:
        post_results.append(_create_post_for_card(session, card, platforms=selected_platforms))
    # Mark generated under the lock so the next concurrent worker sees it the
    # moment we commit. The caller (run_curator_for_game) also stamps this
    # field after we return, but doing it here is the load-bearing step.
    from datetime import datetime as _dt, timezone as _tz
    game.variants_generated_at = _dt.now(_tz.utc)
    session.commit()

    auto_publish_enqueued: list[int] = []
    auto_publish_enqueue_failed: list[int] = []
    for result in post_results:
        for platform, delivery_id in result.auto_publish_deliveries:
            if _enqueue_hero_highlight_auto_publish(result.post_id, delivery_id, platform=platform):
                auto_publish_enqueued.append(delivery_id)
            else:
                auto_publish_enqueue_failed.append(delivery_id)

    return {
        "ok": True,
        "game_id": game_id,
        "platforms": selected_platforms,
        "hero_count": len(cards),
        "post_ids": [result.post_id for result in post_results],
        "created_post_ids": [result.post_id for result in post_results if result.created],
        "auto_publish_delivery_ids": auto_publish_enqueued,
        "auto_publish_enqueue_failed_delivery_ids": auto_publish_enqueue_failed,
    }

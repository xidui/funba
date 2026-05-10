"""Twitter/X engagement discovery for Paperclip-backed reply work items.

This module finds candidate X posts, creates a Funba review record, and hands
the actual reply writing to the Paperclip Content Analyst agent. The delivery
row uses the non-publishing ``twitter_reply`` platform and starts disabled, so
a human must review and send any reply outside this discovery run.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import logging
import math
import os
from typing import Any, Iterable

import requests
from sqlalchemy import or_
from sqlalchemy.orm import Session

from db.game_status import completed_game_clause
from db.models import (
    Game,
    MetricDefinition,
    MetricResult,
    Player,
    PlayerGameStats,
    Setting,
    SocialPost,
    SocialPostDelivery,
    SocialPostVariant,
    Team,
    TwitterEngagementConversation,
    TwitterEngagementMessage,
)
from web.paperclip_bridge import (
    PaperclipBridgeError,
    PaperclipClient,
    append_admin_comment,
    build_post_issue_title,
    load_paperclip_bridge_config,
)

logger = logging.getLogger(__name__)

ENGAGEMENT_EVENT_TYPE = "twitter_engagement_reply_work_item"
TWITTER_REPLY_DRAFT_PLATFORM = "twitter_reply"
DEFAULT_RECENT_SEARCH_URL = "https://api.x.com/2/tweets/search/recent"
DEFAULT_QUERY_TERMS = (
    "NBA",
    "basketball",
    '"NBA Playoffs"',
    "Lakers",
    "Warriors",
    "Celtics",
    "Knicks",
)
DEFAULT_QUERY = f"({' OR '.join(DEFAULT_QUERY_TERMS)}) lang:en -is:retweet -is:reply"
DEFAULT_MAX_RESULTS = 25
DEFAULT_DAILY_DRAFT_LIMIT = 5
DEFAULT_MIN_SCORE = 8.0
DEFAULT_LOOKBACK_DAYS = 2
DEFAULT_BASE_URL = "https://funba.app"
FOLLOWUP_MENTION_SCORE_BONUS = 20.0
PAPERCLIP_WAKE_REASON = "twitter_engagement_reply_work_item"
DEFAULT_METRIC_CONTEXT_LIMIT_PER_GAME = 8
CURATED_HIGHLIGHT_FIELDS: tuple[tuple[str, str], ...] = (
    ("game", "highlights_curated_json"),
    ("player", "highlights_curated_player_json"),
    ("team", "highlights_curated_team_json"),
)


@dataclass(frozen=True)
class XAuthor:
    id: str
    username: str
    name: str
    verified: bool
    followers_count: int


@dataclass(frozen=True)
class XPostCandidate:
    id: str
    text: str
    author: XAuthor
    created_at: datetime | None
    public_metrics: dict[str, int]
    conversation_id: str | None = None
    parent_tweet_id: str | None = None
    raw_payload: dict[str, Any] | None = None
    possibly_sensitive: bool = False

    @property
    def url(self) -> str:
        username = self.author.username or "i"
        return f"https://x.com/{username}/status/{self.id}"


@dataclass(frozen=True)
class GameContext:
    game_id: str
    game_date: date | None
    url: str
    matchup: str
    score: str
    home_team_terms: tuple[str, ...]
    road_team_terms: tuple[str, ...]
    top_player: str | None = None
    top_player_pts: int | None = None

    @property
    def search_terms(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.home_team_terms, *self.road_team_terms)))


@dataclass(frozen=True)
class TwitterEngagementWorkItem:
    conversation: TwitterEngagementConversation
    message: TwitterEngagementMessage
    post: SocialPost
    variant: SocialPostVariant
    delivery: SocialPostDelivery
    matched_contexts: tuple[GameContext, ...]


def _safe_json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _drop_empty(data: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in data.items()
        if value is not None and value != "" and value != {} and value != []
    }


def _compact_rank_snapshot(rank_snapshot: Any, rank_window: Any = None) -> dict[str, Any] | None:
    if not isinstance(rank_snapshot, dict):
        return None
    keys: list[str] = []
    window = _clean_text(rank_window)
    if window:
        keys.extend([window, f"{window}_total"])
        if window == "season":
            keys.append("season_total")
    for key in ("alltime", "alltime_total", "season", "season_total", "last5", "last5_total", "last3", "last3_total"):
        if key not in keys:
            keys.append(key)
    compact = {key: rank_snapshot.get(key) for key in keys if key in rank_snapshot}
    return compact or None


def _rank_snapshot_summary(rank_snapshot: Any, rank_window: Any = None) -> str | None:
    snapshot = _compact_rank_snapshot(rank_snapshot, rank_window)
    if not snapshot:
        return None
    preferred = [_clean_text(rank_window), "alltime", "season", "last5", "last3"]
    for window in [item for item in preferred if item]:
        rank = snapshot.get(window)
        if rank is None:
            continue
        total = snapshot.get(f"{window}_total")
        if total is None and window == "season":
            total = snapshot.get("season_total")
        return f"{window} #{rank}" + (f"/{total}" if total is not None else "")
    return json.dumps(snapshot, ensure_ascii=False, sort_keys=True)


def _compact_metric_result_context(raw_context: Any) -> dict[str, Any] | None:
    parsed = _safe_json_dict(raw_context)
    if not parsed:
        return None
    keys = (
        "rank",
        "total",
        "all_games_rank",
        "all_games_total",
        "rank_group",
        "team_id",
        "opponent_team_id",
        "scope_reference_en",
        "scope_reference_zh",
    )
    compact = {key: parsed.get(key) for key in keys if key in parsed}
    return compact or None


def _entity_label_for_metric_result(session: Session, result: MetricResult, game: Game | None) -> str | None:
    entity_id = _clean_text(result.entity_id)
    if result.entity_type == "player" and entity_id:
        player = session.get(Player, entity_id)
        return _clean_text(getattr(player, "full_name", None)) or entity_id
    if result.entity_type == "team" and entity_id:
        team = session.get(Team, entity_id)
        return (
            _clean_text(getattr(team, "abbr", None))
            or _clean_text(getattr(team, "full_name", None))
            or entity_id
        )
    if result.entity_type == "game" and game is not None:
        return _clean_text(game.slug) or _clean_text(game.game_id)
    return entity_id


def _curated_metric_signals_for_game(session: Session, game: Game) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for scope, field_name in CURATED_HIGHLIGHT_FIELDS:
        parsed = _safe_json_dict(getattr(game, field_name, None))
        if not parsed:
            continue
        for section in ("hero", "notable"):
            raw_entries = parsed.get(section) if isinstance(parsed.get(section), list) else []
            for entry in raw_entries:
                if not isinstance(entry, dict):
                    continue
                rank_snapshot = _compact_rank_snapshot(entry.get("rank_snapshot"), entry.get("rank_window"))
                signal = _drop_empty(
                    {
                        "source": "curated_highlight",
                        "scope": scope,
                        "section": section,
                        "metric_key": _clean_text(entry.get("metric_key")),
                        "entity_id": _clean_text(entry.get("entity_id")),
                        "player_id": _clean_text(entry.get("player_id")),
                        "player_name": _clean_text(entry.get("player_name")),
                        "team_id": _clean_text(entry.get("team_id")),
                        "team_abbr": _clean_text(entry.get("team_abbr")),
                        "season": _clean_text(entry.get("season")),
                        "rank_window": _clean_text(entry.get("rank_window")),
                        "narrative_en": _clean_text(entry.get("narrative_en")),
                        "narrative_zh": _clean_text(entry.get("narrative_zh")),
                        "value": entry.get("value_snapshot"),
                        "value_str": _clean_text(entry.get("value_str_snapshot")),
                        "rank_snapshot": rank_snapshot,
                    }
                )
                if signal.get("metric_key") or signal.get("narrative_en") or signal.get("narrative_zh"):
                    signals.append(signal)

    metric_keys = sorted({str(signal.get("metric_key")) for signal in signals if signal.get("metric_key")})
    if metric_keys:
        definitions = {
            definition.key: definition
            for definition in session.query(MetricDefinition).filter(MetricDefinition.key.in_(metric_keys)).all()
        }
        for signal in signals:
            definition = definitions.get(str(signal.get("metric_key") or ""))
            if definition is None:
                continue
            signal["metric_name"] = definition.name
            if definition.name_zh:
                signal["metric_name_zh"] = definition.name_zh
    return signals


def _notable_metric_results_for_game(
    session: Session,
    game: Game,
    *,
    limit: int = DEFAULT_METRIC_CONTEXT_LIMIT_PER_GAME,
) -> list[dict[str, Any]]:
    rows = (
        session.query(MetricResult, MetricDefinition)
        .join(MetricDefinition, MetricResult.metric_key == MetricDefinition.key)
        .filter(MetricResult.game_id == str(game.game_id))
        .filter(MetricDefinition.status == "published")
        .filter(or_(MetricResult.noteworthiness.isnot(None), MetricResult.notable_reason.isnot(None)))
        .order_by(MetricResult.noteworthiness.desc(), MetricResult.metric_key.asc(), MetricResult.entity_id.asc())
        .limit(max(1, int(limit)))
        .all()
    )
    out: list[dict[str, Any]] = []
    for result, definition in rows:
        out.append(
            _drop_empty(
                {
                    "source": "metric_result",
                    "metric_key": definition.key,
                    "metric_name": definition.name,
                    "metric_name_zh": definition.name_zh,
                    "scope": definition.scope,
                    "entity_type": result.entity_type,
                    "entity_id": result.entity_id,
                    "entity_label": _entity_label_for_metric_result(session, result, game),
                    "season": result.season,
                    "sub_key": result.sub_key,
                    "rank_group": result.rank_group,
                    "value_num": result.value_num,
                    "value_str": result.value_str,
                    "noteworthiness": result.noteworthiness,
                    "notable_reason": _clean_text(result.notable_reason),
                    "context": _compact_metric_result_context(result.context_json),
                }
            )
        )
    return out


def build_game_metric_contexts(
    session: Session,
    contexts: list[GameContext],
    *,
    limit_per_game: int = DEFAULT_METRIC_CONTEXT_LIMIT_PER_GAME,
) -> list[dict[str, Any]]:
    """Return compact hero/notable metric facts for matched games."""
    game_ids = list(dict.fromkeys(context.game_id for context in contexts if context.game_id))
    if not game_ids:
        return []
    games = {
        str(game.game_id): game
        for game in session.query(Game).filter(Game.game_id.in_(game_ids)).all()
    }
    metric_contexts: list[dict[str, Any]] = []
    for context in contexts:
        game = games.get(context.game_id)
        if game is None:
            continue
        curated = _curated_metric_signals_for_game(session, game)
        hero_signals = [signal for signal in curated if signal.get("section") == "hero"]
        notable_signals = [signal for signal in curated if signal.get("section") == "notable"]
        metric_contexts.append(
            {
                "game_id": context.game_id,
                "game_date": context.game_date.isoformat() if context.game_date else None,
                "matchup": context.matchup,
                "score": context.score,
                "url": context.url,
                "hero_signals": hero_signals,
                "notable_signals": notable_signals,
                "notable_metric_results": _notable_metric_results_for_game(
                    session,
                    game,
                    limit=limit_per_game,
                ),
            }
        )
    return metric_contexts


def _metric_signal_subject(signal: dict[str, Any]) -> str:
    return (
        _clean_text(signal.get("player_name"))
        or _clean_text(signal.get("team_abbr"))
        or _clean_text(signal.get("entity_id"))
        or "game"
    )


def _format_curated_metric_signal(signal: dict[str, Any]) -> str:
    narrative = _clean_text(signal.get("narrative_en")) or _clean_text(signal.get("narrative_zh"))
    metric_name = _clean_text(signal.get("metric_name")) or _clean_text(signal.get("metric_key")) or "metric"
    value = _clean_text(signal.get("value_str"))
    rank = _rank_snapshot_summary(signal.get("rank_snapshot"), signal.get("rank_window"))
    details = [item for item in (metric_name, f"value {value}" if value else None, rank) if item]
    detail_text = f" ({'; '.join(details)})" if details else ""
    if narrative:
        return f"- [{signal.get('scope')}/{signal.get('section')}] {narrative}{detail_text}"
    return f"- [{signal.get('scope')}/{signal.get('section')}] {_metric_signal_subject(signal)}: {metric_name}{detail_text}"


def _format_metric_result_signal(signal: dict[str, Any]) -> str:
    subject = _clean_text(signal.get("entity_label")) or _clean_text(signal.get("entity_id")) or "game"
    metric_name = _clean_text(signal.get("metric_name")) or _clean_text(signal.get("metric_key")) or "metric"
    value = _clean_text(signal.get("value_str"))
    if value is None and signal.get("value_num") is not None:
        value = str(signal.get("value_num"))
    score = signal.get("noteworthiness")
    reason = _clean_text(signal.get("notable_reason"))
    pieces = [f"{subject} - {metric_name}"]
    if value:
        pieces.append(f"value {value}")
    if score is not None:
        pieces.append(f"noteworthiness {float(score):.2f}")
    text = f"- {'; '.join(pieces)}"
    if reason:
        text += f". Reason: {reason}"
    return text


def _format_metric_context_lines(metric_contexts: list[dict[str, Any]] | None) -> str:
    if not metric_contexts:
        return "- No curated hero or notable metric facts were found for the matched game context."
    lines: list[str] = []
    for metric_context in metric_contexts[:5]:
        lines.append(
            f"### {metric_context.get('matchup') or metric_context.get('game_id')} "
            f"({metric_context.get('game_date') or 'unknown date'})"
        )
        hero_signals = metric_context.get("hero_signals") if isinstance(metric_context.get("hero_signals"), list) else []
        notable_signals = metric_context.get("notable_signals") if isinstance(metric_context.get("notable_signals"), list) else []
        metric_results = (
            metric_context.get("notable_metric_results")
            if isinstance(metric_context.get("notable_metric_results"), list)
            else []
        )
        if hero_signals:
            lines.append("Hero signals:")
            lines.extend(_format_curated_metric_signal(signal) for signal in hero_signals[:4] if isinstance(signal, dict))
        if notable_signals:
            lines.append("Curated notable signals:")
            lines.extend(_format_curated_metric_signal(signal) for signal in notable_signals[:6] if isinstance(signal, dict))
        if metric_results:
            lines.append("System notable metrics:")
            lines.extend(_format_metric_result_signal(signal) for signal in metric_results[:8] if isinstance(signal, dict))
        if not hero_signals and not notable_signals and not metric_results:
            lines.append("- No hero/notable metric facts stored for this game yet.")
    return "\n".join(lines)


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        parsed = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _base_url(value: str | None = None) -> str:
    raw = (
        value
        or os.getenv("FUNBA_PUBLIC_BASE_URL")
        or os.getenv("FUNBA_BASE_URL")
        or DEFAULT_BASE_URL
    )
    return str(raw or DEFAULT_BASE_URL).rstrip("/")


def _split_csv(value: str | None) -> list[str]:
    out: list[str] = []
    for part in str(value or "").split(","):
        text = part.strip()
        if text:
            out.append(text)
    return out


def build_recent_search_query(
    *,
    handles: Iterable[str] | None = None,
    terms: Iterable[str] | None = None,
    explicit_query: str | None = None,
    account_handle: str | None = None,
    include_mentions: bool | None = None,
) -> str:
    """Build a recent-search query under X's documented query limit."""
    if explicit_query and explicit_query.strip():
        return explicit_query.strip()

    account = str(
        account_handle
        or os.getenv("FUNBA_TWITTER_ACCOUNT_HANDLE")
        or os.getenv("TWITTER_ACCOUNT_HANDLE")
        or ""
    ).strip().lstrip("@")
    if include_mentions is None:
        include_mentions = _env_bool("FUNBA_TWITTER_ENGAGEMENT_INCLUDE_MENTIONS", default=bool(account))
    target_handles = [
        handle.strip().lstrip("@")
        for handle in (handles if handles is not None else _split_csv(os.getenv("FUNBA_TWITTER_ENGAGEMENT_TARGET_HANDLES")))
        if str(handle or "").strip()
    ]
    target_terms = [
        str(term).strip()
        for term in (terms if terms is not None else DEFAULT_QUERY_TERMS)
        if str(term or "").strip()
    ]
    parts: list[str] = []
    if target_handles:
        parts.append("(" + " OR ".join(f"from:{handle}" for handle in target_handles[:25]) + ")")
    if target_terms:
        parts.append("(" + " OR ".join(target_terms) + ")")
    source_query = " ".join((*parts, "-is:reply")) if parts else ""
    mention_query = f"(@{account} OR to:{account})" if include_mentions and account else ""
    if source_query and mention_query:
        return f"(({source_query}) OR {mention_query}) lang:en -is:retweet"
    if mention_query:
        return f"{mention_query} lang:en -is:retweet"
    if source_query:
        return f"{source_query} lang:en -is:retweet"
    return DEFAULT_QUERY


def _bearer_token(value: str | None = None) -> str | None:
    token = value or os.getenv("X_BEARER_TOKEN") or os.getenv("TWITTER_BEARER_TOKEN")
    token = str(token or "").strip()
    return token or None


def fetch_recent_search(
    *,
    bearer_token: str,
    query: str,
    max_results: int = DEFAULT_MAX_RESULTS,
    since_id: str | None = None,
    timeout_seconds: float = 20.0,
    search_url: str = DEFAULT_RECENT_SEARCH_URL,
) -> dict[str, Any]:
    """Call X Recent Search and return the decoded JSON payload."""
    params = {
        "query": query,
        "max_results": max(10, min(100, int(max_results))),
        "tweet.fields": "author_id,created_at,public_metrics,conversation_id,lang,possibly_sensitive,referenced_tweets",
        "expansions": "author_id",
        "user.fields": "username,name,verified,public_metrics",
        "sort_order": "relevancy",
    }
    if since_id:
        params["since_id"] = str(since_id)
    response = requests.get(
        search_url,
        headers={"Authorization": f"Bearer {bearer_token}"},
        params=params,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("X Recent Search returned a non-object payload")
    return payload


def _parse_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)
    except ValueError:
        return None


def _int_metric(metrics: dict[str, Any], key: str) -> int:
    try:
        return max(0, int(metrics.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def parse_recent_search_payload(payload: dict[str, Any]) -> list[XPostCandidate]:
    users = {
        str(user.get("id")): user
        for user in ((payload.get("includes") or {}).get("users") or [])
        if isinstance(user, dict) and user.get("id")
    }
    candidates: list[XPostCandidate] = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        referenced = item.get("referenced_tweets") or []
        if any(isinstance(ref, dict) and ref.get("type") == "retweeted" for ref in referenced):
            continue
        user = users.get(str(item.get("author_id"))) or {}
        author_metrics = user.get("public_metrics") if isinstance(user.get("public_metrics"), dict) else {}
        author = XAuthor(
            id=str(item.get("author_id") or user.get("id") or ""),
            username=str(user.get("username") or ""),
            name=str(user.get("name") or ""),
            verified=bool(user.get("verified")),
            followers_count=_int_metric(author_metrics, "followers_count"),
        )
        public_metrics = item.get("public_metrics") if isinstance(item.get("public_metrics"), dict) else {}
        parent_tweet_id = None
        referenced = item.get("referenced_tweets") if isinstance(item.get("referenced_tweets"), list) else []
        for ref_type in ("replied_to", "quoted"):
            parent_tweet_id = next(
                (
                    str(ref.get("id"))
                    for ref in referenced
                    if isinstance(ref, dict) and ref.get("type") == ref_type and ref.get("id")
                ),
                None,
            )
            if parent_tweet_id:
                break
        candidates.append(
            XPostCandidate(
                id=str(item["id"]),
                text=str(item.get("text") or ""),
                author=author,
                created_at=_parse_datetime(item.get("created_at")),
                public_metrics={
                    "retweet_count": _int_metric(public_metrics, "retweet_count"),
                    "reply_count": _int_metric(public_metrics, "reply_count"),
                    "like_count": _int_metric(public_metrics, "like_count"),
                    "quote_count": _int_metric(public_metrics, "quote_count"),
                },
                conversation_id=str(item.get("conversation_id") or item.get("id") or ""),
                parent_tweet_id=parent_tweet_id,
                raw_payload=item,
                possibly_sensitive=bool(item.get("possibly_sensitive")),
            )
        )
    return candidates


def _team_terms(team: Team | None) -> tuple[str, ...]:
    if team is None:
        return ()
    raw_terms = (
        getattr(team, "abbr", None),
        getattr(team, "full_name", None),
        getattr(team, "nick_name", None),
        getattr(team, "city", None),
    )
    return tuple(dict.fromkeys(term.strip().lower() for term in raw_terms if str(term or "").strip()))


def _team_label(team: Team | None, fallback: str | None) -> str:
    if team is None:
        return str(fallback or "?")
    return str(team.abbr or team.full_name or fallback or "?")


def _top_player_for_game(session: Session, game_id: str) -> tuple[str | None, int | None]:
    row = (
        session.query(PlayerGameStats, Player)
        .outerjoin(Player, Player.player_id == PlayerGameStats.player_id)
        .filter(PlayerGameStats.game_id == game_id, PlayerGameStats.pts.isnot(None))
        .order_by(PlayerGameStats.pts.desc(), PlayerGameStats.player_id.asc())
        .first()
    )
    if not row:
        return None, None
    stat, player = row
    name = getattr(player, "full_name", None) if player is not None else None
    return (name or getattr(stat, "player_id", None)), getattr(stat, "pts", None)


def recent_game_contexts(
    session: Session,
    *,
    now_utc: datetime | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = 8,
    base_url: str | None = None,
) -> list[GameContext]:
    now_value = now_utc or datetime.now(UTC).replace(tzinfo=None)
    cutoff = now_value.date() - timedelta(days=max(0, lookback_days))
    games = (
        session.query(Game)
        .filter(
            Game.game_date.isnot(None),
            Game.game_date >= cutoff,
            completed_game_clause(Game),
            Game.home_team_score.isnot(None),
            Game.road_team_score.isnot(None),
        )
        .order_by(Game.game_date.desc(), Game.game_id.desc())
        .limit(max(1, int(limit)))
        .all()
    )
    root = _base_url(base_url)
    contexts: list[GameContext] = []
    for game in games:
        home = session.query(Team).filter(Team.team_id == game.home_team_id).first() if game.home_team_id else None
        road = session.query(Team).filter(Team.team_id == game.road_team_id).first() if game.road_team_id else None
        home_label = _team_label(home, game.home_team_id)
        road_label = _team_label(road, game.road_team_id)
        top_player, top_pts = _top_player_for_game(session, str(game.game_id))
        slug = game.slug or game.game_id
        contexts.append(
            GameContext(
                game_id=str(game.game_id),
                game_date=game.game_date,
                url=f"{root}/games/{slug}",
                matchup=f"{road_label} @ {home_label}",
                score=f"{road_label} {game.road_team_score}, {home_label} {game.home_team_score}",
                home_team_terms=_team_terms(home),
                road_team_terms=_team_terms(road),
                top_player=top_player,
                top_player_pts=top_pts,
            )
        )
    return contexts


def score_candidate(candidate: XPostCandidate, *, now_utc: datetime | None = None) -> tuple[float, str]:
    now_value = now_utc or datetime.now(UTC).replace(tzinfo=None)
    metrics = candidate.public_metrics
    engagement_score = (
        metrics.get("retweet_count", 0) * 2.0
        + metrics.get("quote_count", 0) * 2.0
        + metrics.get("reply_count", 0) * 1.25
        + metrics.get("like_count", 0) * 0.2
    )
    follower_score = math.log10(max(candidate.author.followers_count, 0) + 1) * 4.0
    verified_bonus = 8.0 if candidate.author.verified else 0.0
    recency_bonus = 0.0
    if candidate.created_at is not None:
        age_hours = max(0.0, (now_value - candidate.created_at).total_seconds() / 3600.0)
        recency_bonus = max(0.0, 8.0 - age_hours / 3.0)
    sensitive_penalty = 15.0 if candidate.possibly_sensitive else 0.0
    score = engagement_score + follower_score + verified_bonus + recency_bonus - sensitive_penalty
    reason = (
        f"{candidate.author.followers_count} followers, "
        f"{metrics.get('like_count', 0)} likes, "
        f"{metrics.get('reply_count', 0)} replies"
    )
    if candidate.author.verified:
        reason += ", verified author"
    return round(score, 2), reason


def _matched_contexts(candidate: XPostCandidate, contexts: list[GameContext]) -> list[GameContext]:
    text = candidate.text.lower()
    matched = [
        context
        for context in contexts
        if any(term and term in text for term in context.search_terms)
    ]
    return matched or contexts[:1]


def _candidate_mentions_account(candidate: XPostCandidate, account_handle: str | None = None) -> bool:
    account = str(
        account_handle
        or os.getenv("FUNBA_TWITTER_ACCOUNT_HANDLE")
        or os.getenv("TWITTER_ACCOUNT_HANDLE")
        or ""
    ).strip().lstrip("@").lower()
    if not account:
        return False
    return f"@{account}" in candidate.text.lower()


def build_seed_reply_variant(candidate: XPostCandidate, contexts: list[GameContext], *, base_url: str | None = None) -> str:
    """Create a factual seed for the Paperclip agent to rewrite."""
    if contexts:
        context = contexts[0]
        top_line = ""
        if context.top_player and context.top_player_pts is not None:
            top_line = f" Top scorer: {context.top_player} with {context.top_player_pts}."
        return (
            "[Paperclip seed - rewrite before sending]\n"
            f"Useful angle. Funba has the box score and context for {context.matchup}: "
            f"{context.score}.{top_line}\n"
            f"{context.url}"
        )
    return (
        "[Paperclip seed - rewrite before sending]\n"
        "Useful angle. Funba keeps NBA box scores, player pages, and metric context in one place:\n"
        f"{_base_url(base_url)}/games"
    )


def _query_hash(query: str) -> str:
    return hashlib.sha1(query.encode("utf-8")).hexdigest()[:12]


def _since_id_key(query: str) -> str:
    return f"twitter.engage.since.{_query_hash(query)}"


def _read_since_id(session: Session, query: str) -> str | None:
    row = session.get(Setting, _since_id_key(query))
    return str(row.value).strip() if row is not None and str(row.value).strip() else None


def _write_since_id(session: Session, query: str, since_id: str | None, *, now_utc: datetime) -> None:
    if not since_id:
        return
    key = _since_id_key(query)
    row = session.get(Setting, key)
    if row is None:
        session.add(Setting(key=key, value=str(since_id), updated_at=now_utc))
    else:
        row.value = str(since_id)
        row.updated_at = now_utc


def _conversation_key(candidate: XPostCandidate) -> str:
    return str(candidate.conversation_id or candidate.id).strip() or candidate.id


def _message_has_live_reply_post(session: Session, message: TwitterEngagementMessage) -> bool:
    if not message.reply_post_id:
        return False
    return (
        session.query(SocialPost.id)
        .filter(SocialPost.id == message.reply_post_id, SocialPost.status != "archived")
        .first()
        is not None
    )


def _topic_for_candidate(candidate: XPostCandidate) -> str:
    handle = candidate.author.username or candidate.author.id or "unknown"
    return f"Twitter Reply - @{handle} - {candidate.id}"


def _comment_text(candidate: XPostCandidate, *, score: float, reason: str, query: str) -> str:
    preview = " ".join(candidate.text.split())[:500]
    return (
        "Twitter engagement candidate. Manual confirmation is required before replying.\n\n"
        f"Target: {candidate.url}\n"
        f"Author: @{candidate.author.username} ({candidate.author.name})\n"
        f"Score: {score} - {reason}\n"
        f"Query: {query}\n\n"
        f"Post text: {preview}"
    )


def _upsert_conversation(
    session: Session,
    candidate: XPostCandidate,
    *,
    now_utc: datetime,
) -> TwitterEngagementConversation:
    x_conversation_id = _conversation_key(candidate)
    conversation = (
        session.query(TwitterEngagementConversation)
        .filter(TwitterEngagementConversation.x_conversation_id == x_conversation_id)
        .first()
    )
    if conversation is None:
        conversation = TwitterEngagementConversation(
            x_conversation_id=x_conversation_id,
            root_tweet_id=x_conversation_id,
            root_url=candidate.url if candidate.id == x_conversation_id else None,
            target_author_id=candidate.author.id or None,
            target_author_username=candidate.author.username or None,
            target_author_name=candidate.author.name or None,
            status="active",
            last_seen_tweet_id=candidate.id,
            last_seen_at=candidate.created_at or now_utc,
            last_replied_at=None,
            created_at=now_utc,
            updated_at=now_utc,
        )
        session.add(conversation)
        session.flush()
        return conversation

    if not conversation.root_tweet_id:
        conversation.root_tweet_id = x_conversation_id
    if not conversation.root_url and candidate.id == x_conversation_id:
        conversation.root_url = candidate.url
    if not conversation.target_author_username:
        conversation.target_author_id = candidate.author.id or None
        conversation.target_author_username = candidate.author.username or None
        conversation.target_author_name = candidate.author.name or None
    conversation.last_seen_tweet_id = candidate.id
    conversation.last_seen_at = candidate.created_at or now_utc
    conversation.updated_at = now_utc
    return conversation


def _upsert_inbound_message(
    session: Session,
    *,
    conversation: TwitterEngagementConversation,
    candidate: XPostCandidate,
    query: str,
    matched_contexts: list[GameContext],
    score: float,
    reason: str,
    now_utc: datetime,
) -> TwitterEngagementMessage:
    message = (
        session.query(TwitterEngagementMessage)
        .filter(TwitterEngagementMessage.tweet_id == candidate.id)
        .first()
    )
    matched_game_ids = json.dumps([context.game_id for context in matched_contexts], ensure_ascii=False)
    public_metrics_json = json.dumps(candidate.public_metrics, ensure_ascii=False)
    raw_payload_json = json.dumps(candidate.raw_payload or {}, ensure_ascii=False)
    if message is None:
        message = TwitterEngagementMessage(
            conversation_id=conversation.id,
            tweet_id=candidate.id,
            x_conversation_id=conversation.x_conversation_id,
            parent_tweet_id=candidate.parent_tweet_id,
            direction="inbound",
            status="discovered",
            author_id=candidate.author.id or None,
            author_username=candidate.author.username or None,
            author_name=candidate.author.name or None,
            author_verified=bool(candidate.author.verified),
            author_followers_count=int(candidate.author.followers_count or 0),
            text=candidate.text,
            tweet_url=candidate.url,
            posted_at=candidate.created_at,
            discovered_at=now_utc,
            discovered_query=query,
            public_metrics_json=public_metrics_json,
            raw_payload_json=raw_payload_json,
            score=score,
            score_reason=reason,
            matched_game_ids=matched_game_ids,
            reply_post_id=None,
            created_at=now_utc,
            updated_at=now_utc,
        )
        session.add(message)
        session.flush()
        return message

    message.conversation_id = conversation.id
    message.x_conversation_id = conversation.x_conversation_id
    message.parent_tweet_id = candidate.parent_tweet_id
    message.author_id = candidate.author.id or None
    message.author_username = candidate.author.username or None
    message.author_name = candidate.author.name or None
    message.author_verified = bool(candidate.author.verified)
    message.author_followers_count = int(candidate.author.followers_count or 0)
    message.text = candidate.text
    message.tweet_url = candidate.url
    message.posted_at = candidate.created_at
    message.discovered_query = query
    message.public_metrics_json = public_metrics_json
    message.raw_payload_json = raw_payload_json
    message.score = score
    message.score_reason = reason
    message.matched_game_ids = matched_game_ids
    message.updated_at = now_utc
    return message


def _conversation_history(
    session: Session,
    conversation_id: int,
    *,
    limit: int = 12,
) -> list[TwitterEngagementMessage]:
    rows = (
        session.query(TwitterEngagementMessage)
        .filter(TwitterEngagementMessage.conversation_id == conversation_id)
        .order_by(TwitterEngagementMessage.posted_at.desc(), TwitterEngagementMessage.id.desc())
        .limit(max(1, int(limit)))
        .all()
    )
    return list(reversed(rows))


def create_twitter_engagement_work_item(
    session: Session,
    candidate: XPostCandidate,
    *,
    conversation: TwitterEngagementConversation,
    message: TwitterEngagementMessage,
    contexts: list[GameContext],
    score: float,
    reason: str,
    query: str,
    now_utc: datetime,
    base_url: str | None = None,
) -> TwitterEngagementWorkItem:
    matched_contexts = _matched_contexts(candidate, contexts)
    comments: list[dict[str, Any]] = []
    append_admin_comment(
        comments,
        text=_comment_text(candidate, score=score, reason=reason, query=query),
        author="Twitter Engagement Agent",
        origin="system",
        event_type=ENGAGEMENT_EVENT_TYPE,
    )
    post = SocialPost(
        topic=_topic_for_candidate(candidate),
        source_date=(candidate.created_at.date() if candidate.created_at else now_utc.date()),
        source_metrics=json.dumps([], ensure_ascii=False),
        source_game_ids=json.dumps([context.game_id for context in matched_contexts], ensure_ascii=False),
        status="in_review",
        admin_comments=json.dumps(comments, ensure_ascii=False),
        priority=max(5, min(80, 65 - int(score))),
        llm_model=None,
        created_at=now_utc,
        updated_at=now_utc,
    )
    session.add(post)
    session.flush()
    variant = SocialPostVariant(
        post_id=post.id,
        title=f"Paperclip reply work item for @{candidate.author.username or candidate.author.id}",
        content_raw=build_seed_reply_variant(candidate, matched_contexts, base_url=base_url),
        audience_hint="Paperclip should rewrite this X reply. Confirm with Yue before sending.",
        status="in_review",
        created_at=now_utc,
        updated_at=now_utc,
    )
    session.add(variant)
    session.flush()
    delivery = SocialPostDelivery(
        variant_id=variant.id,
        platform=TWITTER_REPLY_DRAFT_PLATFORM,
        forum=f"@{candidate.author.username}" if candidate.author.username else None,
        is_enabled=False,
        status="pending",
        content_final=None,
        published_url=None,
        published_at=None,
        error_message="Manual confirmation required before sending a reply.",
        created_at=now_utc,
        updated_at=now_utc,
    )
    session.add(delivery)
    session.flush()
    message.reply_post_id = post.id
    message.status = "drafted"
    message.updated_at = now_utc
    conversation.updated_at = now_utc
    return TwitterEngagementWorkItem(
        conversation=conversation,
        message=message,
        post=post,
        variant=variant,
        delivery=delivery,
        matched_contexts=tuple(matched_contexts),
    )


def build_twitter_engagement_issue_description(
    *,
    conversation: TwitterEngagementConversation | None = None,
    message: TwitterEngagementMessage | None = None,
    conversation_messages: list[TwitterEngagementMessage] | None = None,
    post: SocialPost,
    variant: SocialPostVariant,
    delivery: SocialPostDelivery,
    candidate: XPostCandidate,
    contexts: list[GameContext],
    metric_contexts: list[dict[str, Any]] | None = None,
    score: float,
    reason: str,
    query: str,
) -> str:
    preview = " ".join(candidate.text.split())[:1200]
    context_lines = []
    for context in contexts[:5]:
        top_player = ""
        if context.top_player and context.top_player_pts is not None:
            top_player = f"; top scorer: {context.top_player} {context.top_player_pts} pts"
        context_lines.append(
            f"- {context.matchup} ({context.game_date or 'unknown date'}): {context.score}{top_player}\n"
            f"  URL: {context.url}"
        )
    if not context_lines:
        context_lines.append(f"- No recent completed game matched. General games URL: {_base_url()}/games")
    history_lines = []
    for item in conversation_messages or []:
        posted = item.posted_at.isoformat() if item.posted_at else "unknown time"
        author = item.author_username or item.author_id or "unknown"
        text = " ".join(str(item.text or "").split())[:500]
        marker = "target" if message is not None and item.id == message.id else item.direction
        history_lines.append(f"- [{posted}] {marker} @{author}: {text}")

    payload = {
        "workflow": "twitter_engagement",
        "conversation_db_id": conversation.id if conversation is not None else None,
        "message_db_id": message.id if message is not None else None,
        "x_conversation_id": conversation.x_conversation_id if conversation is not None else candidate.conversation_id,
        "post_id": post.id,
        "variant_id": variant.id,
        "delivery_id": delivery.id,
        "manual_confirmation_required": True,
        "reply_persona": "NBA data analysis expert",
        "target": {
            "tweet_id": candidate.id,
            "url": candidate.url,
            "author_username": candidate.author.username,
            "author_name": candidate.author.name,
            "author_followers": candidate.author.followers_count,
            "author_verified": candidate.author.verified,
            "created_at": candidate.created_at.isoformat() if candidate.created_at else None,
            "public_metrics": candidate.public_metrics,
            "score": score,
            "score_reason": reason,
            "query": query,
        },
        "matched_game_contexts": [
            {
                "game_id": context.game_id,
                "game_date": context.game_date.isoformat() if context.game_date else None,
                "matchup": context.matchup,
                "score": context.score,
                "top_player": context.top_player,
                "top_player_pts": context.top_player_pts,
                "url": context.url,
            }
            for context in contexts
        ],
        "game_metric_contexts": metric_contexts or [],
    }
    update_endpoint = f"/api/admin/content/{post.id}/variants/{variant.id}/update"
    return (
        "Funba is the source of truth for this X/Twitter reply work item.\n\n"
        "## Objective\n\n"
        "Analyze the target X post and the Funba game context, then rewrite the existing seed variant into a natural, high-signal reply.\n\n"
        "## Reply Voice\n\n"
        "Write as an NBA data analysis expert: precise, concise, and grounded in Funba's stored facts. Use the hero/notable metrics below only when they are relevant to the target post; do not force a stat into the reply.\n\n"
        "## Hard Boundaries\n\n"
        "- Do not publish, submit, or send anything to X/Twitter.\n"
        "- Do not enable the `twitter_reply` delivery. It must stay disabled until Yue confirms the final reply.\n"
        "- Use this Paperclip issue as the LLM work surface; do not invoke a separate Funba LLM API helper for this drafting work.\n"
        "- Do not load normal game-analysis phase docs, platform social post playbooks, image-generation skills, or capture skills for this reply.\n"
        "- Keep the reply concise, usually under 240 characters unless one Funba URL requires more room.\n"
        "- Use English unless the target post is clearly Chinese.\n"
        "- Include at most one Funba URL, and only use facts supported by the context below.\n"
        "- Leave the SocialPost and variant in `in_review` for manual confirmation.\n\n"
        "## Required Funba Write\n\n"
        f"Update the existing variant through `{update_endpoint}`.\n\n"
        "Use a JSON body like:\n\n"
        "```json\n"
        "{\n"
        '  "title": "Paperclip reply for @handle",\n'
        '  "content_raw": "final reply text",\n'
        '  "audience_hint": "Paperclip-written X reply. Confirm with Yue before sending."\n'
        "}\n"
        "```\n\n"
        "Do not create a new SocialPost. Do not add destinations. Do not publish.\n\n"
        "## Target X Post\n\n"
        f"URL: {candidate.url}\n"
        f"Author: @{candidate.author.username} ({candidate.author.name})\n"
        f"Score: {score} - {reason}\n\n"
        f"Post text:\n{preview}\n\n"
        "## Conversation History\n\n"
        + ("\n".join(history_lines) if history_lines else "- No prior messages stored for this conversation.")
        + "\n\n"
        "## Funba Context\n\n"
        + "\n".join(context_lines)
        + "\n\n"
        "## Funba Hero And Notable Metrics\n\n"
        + _format_metric_context_lines(metric_contexts)
        + "\n\n"
        "<twitter_engagement_payload>\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
        "</twitter_engagement_payload>"
    )


def _apply_paperclip_issue_fields(
    post: SocialPost,
    issue: dict[str, Any] | None,
    *,
    sync_error: str | None = None,
    now_utc: datetime | None = None,
) -> None:
    if issue:
        post.paperclip_issue_id = issue.get("id") or post.paperclip_issue_id
        post.paperclip_issue_identifier = issue.get("identifier") or post.paperclip_issue_identifier
        post.paperclip_issue_status = issue.get("status") or post.paperclip_issue_status
        post.paperclip_assignee_agent_id = issue.get("assigneeAgentId")
        post.paperclip_assignee_user_id = issue.get("assigneeUserId")
    post.paperclip_last_synced_at = now_utc or datetime.now(UTC).replace(tzinfo=None)
    post.paperclip_sync_error = sync_error


def ensure_paperclip_issue_for_twitter_engagement_post(
    *,
    session: Session,
    conversation: TwitterEngagementConversation,
    message: TwitterEngagementMessage,
    post: SocialPost,
    variant: SocialPostVariant,
    delivery: SocialPostDelivery,
    candidate: XPostCandidate,
    contexts: list[GameContext],
    score: float,
    reason: str,
    query: str,
    now_utc: datetime,
) -> dict[str, Any]:
    cfg = load_paperclip_bridge_config()
    if cfg is None:
        _apply_paperclip_issue_fields(
            post,
            None,
            sync_error="Paperclip bridge is not configured.",
            now_utc=now_utc,
        )
        return {"ok": False, "post_id": post.id, "error": "paperclip_not_configured"}
    try:
        client = PaperclipClient(cfg)
        cfg = client.discover_defaults()
        if not cfg.company_id:
            raise PaperclipBridgeError("Paperclip bridge could not resolve company_id.")
        if not cfg.project_id:
            raise PaperclipBridgeError("Paperclip bridge could not resolve the Funba project in Paperclip.")
        if not cfg.content_analyst_agent_id:
            raise PaperclipBridgeError("PAPERCLIP_CONTENT_ANALYST_AGENT_ID is not configured.")

        snapshot = {
            "id": post.id,
            "source_date": post.source_date.isoformat() if post.source_date else None,
            "topic": post.topic,
        }
        metric_contexts = build_game_metric_contexts(session, contexts)
        payload = {
            "projectId": cfg.project_id,
            "title": build_post_issue_title(snapshot),
            "description": build_twitter_engagement_issue_description(
                conversation=conversation,
                message=message,
                conversation_messages=_conversation_history(session, conversation.id),
                post=post,
                variant=variant,
                delivery=delivery,
                candidate=candidate,
                contexts=contexts,
                metric_contexts=metric_contexts,
                score=score,
                reason=reason,
                query=query,
            ),
            "status": "todo",
            "priority": "medium",
            "assigneeAgentId": cfg.content_analyst_agent_id,
            "assigneeUserId": None,
        }
        issue = client.update_issue(post.paperclip_issue_id, payload) if post.paperclip_issue_id else client.create_issue(payload)
        _apply_paperclip_issue_fields(post, issue, sync_error=None, now_utc=now_utc)
        post.llm_model = "paperclip_content_analyst"
        return {
            "ok": True,
            "post_id": post.id,
            "variant_id": variant.id,
            "issue_id": post.paperclip_issue_id,
            "issue_identifier": post.paperclip_issue_identifier,
            "agent_id": cfg.content_analyst_agent_id,
            "wake_request": {
                "issue_id": post.paperclip_issue_id,
                "post_id": post.id,
                "variant_id": variant.id,
                "agent_id": cfg.content_analyst_agent_id,
            },
        }
    except Exception as exc:
        message = str(exc) or exc.__class__.__name__
        logger.warning("failed to ensure Paperclip issue for twitter engagement post %s: %s", post.id, message)
        _apply_paperclip_issue_fields(post, None, sync_error=message, now_utc=now_utc)
        return {"ok": False, "post_id": post.id, "error": message}


def wake_paperclip_twitter_engagement_agent(wake_request: dict[str, Any]) -> dict[str, Any]:
    issue_id = str(wake_request.get("issue_id") or "").strip()
    post_id = wake_request.get("post_id")
    variant_id = wake_request.get("variant_id")
    cfg = load_paperclip_bridge_config()
    if not cfg:
        return {"ok": False, "post_id": post_id, "issue_id": issue_id, "error": "paperclip_not_configured"}
    try:
        client = PaperclipClient(cfg)
        cfg = client.discover_defaults()
        agent_id = str(wake_request.get("agent_id") or cfg.content_analyst_agent_id or "").strip()
        if not issue_id:
            raise PaperclipBridgeError("Paperclip issue_id is missing for wakeup.")
        if not agent_id:
            raise PaperclipBridgeError("Paperclip content analyst agent id is missing for wakeup.")
        client.wake_agent(
            agent_id,
            reason=PAPERCLIP_WAKE_REASON,
            payload={
                "issueId": issue_id,
                "postId": post_id,
                "variantId": variant_id,
                "workflow": "twitter_engagement",
            },
            force_fresh_session=True,
        )
        return {"ok": True, "post_id": post_id, "issue_id": issue_id, "agent_id": agent_id}
    except Exception as exc:
        message = str(exc) or exc.__class__.__name__
        logger.warning("failed to wake Paperclip twitter engagement agent for issue %s: %s", issue_id, message)
        return {"ok": False, "post_id": post_id, "issue_id": issue_id, "error": message}


def discover_twitter_engagement_candidates(
    session: Session,
    *,
    query: str | None = None,
    max_results: int | None = None,
    daily_limit: int | None = None,
    min_score: float | None = None,
    lookback_days: int | None = None,
    base_url: str | None = None,
    bearer_token: str | None = None,
    search_payload: dict[str, Any] | None = None,
    dry_run: bool = False,
    paperclip: bool = True,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Discover recent X posts and create Paperclip-backed reply work items."""
    now_value = now_utc or datetime.now(UTC).replace(tzinfo=None)
    account_handle = (
        os.getenv("FUNBA_TWITTER_ACCOUNT_HANDLE")
        or os.getenv("TWITTER_ACCOUNT_HANDLE")
        or None
    )
    search_query = build_recent_search_query(
        explicit_query=query or os.getenv("FUNBA_TWITTER_ENGAGEMENT_QUERY"),
        account_handle=account_handle,
    )
    result_limit = max_results or _env_int(
        "FUNBA_TWITTER_ENGAGEMENT_MAX_RESULTS",
        DEFAULT_MAX_RESULTS,
        minimum=10,
        maximum=100,
    )
    create_limit = daily_limit if daily_limit is not None else _env_int(
        "FUNBA_TWITTER_ENGAGEMENT_DAILY_LIMIT",
        DEFAULT_DAILY_DRAFT_LIMIT,
        minimum=0,
        maximum=50,
    )
    threshold = min_score if min_score is not None else _env_float(
        "FUNBA_TWITTER_ENGAGEMENT_MIN_SCORE",
        DEFAULT_MIN_SCORE,
        minimum=0.0,
        maximum=10000.0,
    )
    if search_payload is None:
        token = _bearer_token(bearer_token)
        if not token:
            return {
                "ok": False,
                "status": "missing_bearer_token",
                "query": search_query,
                "created_reply_post_ids": [],
                "stored_conversation_ids": [],
                "stored_message_ids": [],
                "message": "Set X_BEARER_TOKEN or TWITTER_BEARER_TOKEN to enable discovery.",
            }
        current_since_id = _read_since_id(session, search_query)
        payload = fetch_recent_search(
            bearer_token=token,
            query=search_query,
            max_results=result_limit,
            since_id=current_since_id,
        )
    else:
        payload = search_payload

    candidates = parse_recent_search_payload(payload)
    scored_all: list[tuple[float, str, XPostCandidate]] = []
    scored: list[tuple[float, str, XPostCandidate]] = []
    for candidate in candidates:
        score, reason = score_candidate(candidate, now_utc=now_value)
        if _candidate_mentions_account(candidate, account_handle):
            score += FOLLOWUP_MENTION_SCORE_BONUS
            reason += f", mentions @{str(account_handle).strip().lstrip('@')}"
        scored_all.append((score, reason, candidate))
        if score >= threshold:
            scored.append((score, reason, candidate))
    scored.sort(key=lambda item: (-item[0], item[2].id))

    contexts = recent_game_contexts(
        session,
        now_utc=now_value,
        lookback_days=lookback_days if lookback_days is not None else DEFAULT_LOOKBACK_DAYS,
        base_url=base_url,
    )
    stored_conversation_ids: list[int] = []
    stored_message_ids: list[int] = []
    messages_by_tweet_id: dict[str, tuple[TwitterEngagementConversation, TwitterEngagementMessage, list[GameContext]]] = {}
    if not dry_run:
        for score, reason, candidate in scored_all:
            matched = _matched_contexts(candidate, contexts)
            conversation = _upsert_conversation(session, candidate, now_utc=now_value)
            message = _upsert_inbound_message(
                session,
                conversation=conversation,
                candidate=candidate,
                query=search_query,
                matched_contexts=matched,
                score=score,
                reason=reason,
                now_utc=now_value,
            )
            messages_by_tweet_id[candidate.id] = (conversation, message, matched)
            stored_conversation_ids.append(int(conversation.id))
            stored_message_ids.append(int(message.id))

    created_reply_post_ids: list[int] = []
    skipped_existing_message_ids: list[int] = []
    previews: list[dict[str, Any]] = []
    paperclip_results: list[dict[str, Any]] = []
    paperclip_wakeup_requests: list[dict[str, Any]] = []
    for score, reason, candidate in scored:
        if len(created_reply_post_ids) >= create_limit:
            break
        matched = _matched_contexts(candidate, contexts)
        preview = {
            "tweet_id": candidate.id,
            "url": candidate.url,
            "author": candidate.author.username,
            "score": score,
            "reason": reason,
            "matched_game_ids": [context.game_id for context in matched],
            "seed_draft": build_seed_reply_variant(candidate, matched, base_url=base_url),
        }
        previews.append(preview)
        if dry_run:
            continue
        conversation, message, matched = messages_by_tweet_id[candidate.id]
        if _message_has_live_reply_post(session, message):
            skipped_existing_message_ids.append(int(message.id))
            continue
        artifacts = create_twitter_engagement_work_item(
            session,
            candidate,
            conversation=conversation,
            message=message,
            contexts=contexts,
            score=score,
            reason=reason,
            query=search_query,
            now_utc=now_value,
            base_url=base_url,
        )
        created_reply_post_ids.append(int(artifacts.post.id))
        if paperclip:
            paperclip_result = ensure_paperclip_issue_for_twitter_engagement_post(
                session=session,
                conversation=artifacts.conversation,
                message=artifacts.message,
                post=artifacts.post,
                variant=artifacts.variant,
                delivery=artifacts.delivery,
                candidate=candidate,
                contexts=list(artifacts.matched_contexts),
                score=score,
                reason=reason,
                query=search_query,
                now_utc=now_value,
            )
            paperclip_results.append(paperclip_result)
            wake_request = paperclip_result.get("wake_request")
            if isinstance(wake_request, dict):
                paperclip_wakeup_requests.append(wake_request)

    newest_id = ((payload.get("meta") or {}).get("newest_id") if isinstance(payload.get("meta"), dict) else None)
    if newest_id and not dry_run:
        _write_since_id(session, search_query, str(newest_id), now_utc=now_value)

    return {
        "ok": True,
        "status": "dry_run" if dry_run else "created",
        "query": search_query,
        "since_id": _read_since_id(session, search_query),
        "newest_id": newest_id,
        "candidate_count": len(candidates),
        "scored_count": len(scored),
        "stored_conversation_ids": sorted(set(stored_conversation_ids)),
        "stored_message_ids": sorted(set(stored_message_ids)),
        "created_reply_post_ids": created_reply_post_ids,
        "skipped_existing_message_ids": skipped_existing_message_ids,
        "previews": previews,
        "paperclip_enabled": bool(paperclip),
        "paperclip_results": paperclip_results,
        "paperclip_wakeup_requests": paperclip_wakeup_requests,
        "manual_confirmation_required": True,
    }

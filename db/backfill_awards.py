"""Backfill database-backed NBA awards using nba_api plus tracked playoff data.

Usage:
    python -m db.backfill_awards
    python -m db.backfill_awards --dry-run
    python -m db.backfill_awards --season 22024
"""
from __future__ import annotations

import argparse
import logging
import time
from collections import defaultdict
from dataclasses import dataclass

from requests.exceptions import ConnectionError, Timeout
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from db.models import Award, Game, PlayerGameStats, Team, engine

try:
    from nba_api.stats.endpoints import leagueleaders, playerawards
except ImportError:  # pragma: no cover - exercised only in misconfigured runtimes
    leagueleaders = None
    playerawards = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)
DEFAULT_PLAYER_CANDIDATE_LIMIT = 75

AWARD_TYPE_ORDER = [
    "champion",
    "finals_mvp",
    "mvp",
    "scoring_champion",
    "all_nba_first",
    "all_nba_second",
    "all_nba_third",
]

ALL_NBA_TYPE_BY_TEAM_NUMBER = {
    "1": "all_nba_first",
    "2": "all_nba_second",
    "3": "all_nba_third",
}

TEAM_NAME_ALIASES = {
    "la clippers": "los angeles clippers",
    "new orleans hornets": "new orleans pelicans",
    "new jersey nets": "brooklyn nets",
}


@dataclass(frozen=True)
class AwardSeed:
    award_type: str
    season: int
    player_id: str | None = None
    team_id: str | None = None
    notes: str | None = None


def _normalize_team_name(value: str | None) -> str:
    text = (value or "").strip().lower()
    for old, new in TEAM_NAME_ALIASES.items():
        if text == old:
            text = new
            break
    return "".join(ch for ch in text if ch.isalnum())


def _season_text_to_award_season(season_text: str | None) -> int | None:
    text = (season_text or "").strip()
    if len(text) == 5 and text.isdigit():
        return int(text)
    if len(text) == 7 and text[4] == "-":
        start_year = text[:4]
        if start_year.isdigit():
            return int(f"2{start_year}")
    return None


def _award_season_to_api_text(season_id: int) -> str:
    season = str(season_id)
    if len(season) != 5 or not season.isdigit():
        raise ValueError(f"Unsupported award season id: {season_id}")
    start_year = int(season[1:])
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _season_window_contains(team: Team, season_id: int) -> bool:
    start_year = int(str(season_id)[1:])
    try:
        if team.start_season and start_year < int(str(team.start_season)[:4]):
            return False
        if team.end_season and start_year > int(str(team.end_season)[:4]):
            return False
    except ValueError:
        return True
    return True


def _build_team_lookup(session) -> dict[str, list[Team]]:
    lookup: dict[str, list[Team]] = defaultdict(list)
    for team in session.query(Team).all():
        aliases = {
            team.full_name,
            f"{team.city or ''} {team.nick_name or ''}".strip(),
            team.abbr,
        }
        if team.full_name and team.full_name.startswith("LA "):
            aliases.add(team.full_name.replace("LA ", "Los Angeles ", 1))
        if team.full_name and team.full_name.startswith("Los Angeles "):
            aliases.add(team.full_name.replace("Los Angeles ", "LA ", 1))
        for alias in aliases:
            key = _normalize_team_name(alias)
            if key:
                lookup[key].append(team)
    return lookup


def _resolve_team_id(team_lookup: dict[str, list[Team]], team_name: str | None, season_id: int) -> str | None:
    key = _normalize_team_name(team_name)
    if not key:
        return None
    candidates = team_lookup.get(key, [])
    if not candidates:
        return None
    season_matches = [team for team in candidates if _season_window_contains(team, season_id)]
    preferred = season_matches or candidates
    preferred.sort(key=lambda team: (bool(team.is_legacy), team.full_name or "", team.team_id or ""))
    return preferred[0].team_id


def _target_award_seasons(session, only_season: int | None = None) -> list[int]:
    rows = session.query(Game.season).filter(Game.season.isnot(None)).distinct().all()
    seasons = sorted(
        {
            int(f"2{season[1:]}")
            for (season,) in rows
            if season and len(str(season)) == 5 and str(season).isdigit()
        }
    )
    if only_season is not None:
        return [season for season in seasons if season == only_season]
    return seasons


def _candidate_player_ids_from_rows(
    rows: list[tuple[str | None, str | None, int | float | None]],
    *,
    per_season_limit: int | None,
) -> list[str]:
    if per_season_limit is not None and per_season_limit < 1:
        raise ValueError("per_season_limit must be >= 1 when provided")

    season_players: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for season_token, player_id, total_seconds in rows:
        if not season_token or not player_id:
            continue
        season_players[str(season_token)].append((int(total_seconds or 0), str(player_id)))

    selected_player_ids: set[str] = set()
    for season_token, season_rows in season_players.items():
        ordered = sorted(season_rows, key=lambda item: (-item[0], item[1]))
        if per_season_limit is not None:
            ordered = ordered[:per_season_limit]
        logger.info(
            "Selected %s candidate players for season %s",
            len(ordered),
            season_token,
        )
        selected_player_ids.update(player_id for _, player_id in ordered)

    return sorted(selected_player_ids)


def _player_ids_for_target_seasons(
    session,
    target_seasons: list[int],
    *,
    per_season_limit: int | None = DEFAULT_PLAYER_CANDIDATE_LIMIT,
) -> list[str]:
    if not target_seasons:
        return []
    season_tokens = [str(season) for season in target_seasons]
    total_seconds = (
        func.coalesce(PlayerGameStats.min, 0) * 60
        + func.coalesce(PlayerGameStats.sec, 0)
    )
    rows = (
        session.query(
            Game.season,
            PlayerGameStats.player_id,
            func.sum(total_seconds).label("total_seconds"),
        )
        .join(Game, PlayerGameStats.game_id == Game.game_id)
        .filter(
            PlayerGameStats.player_id.isnot(None),
            Game.season.in_(season_tokens),
        )
        .group_by(Game.season, PlayerGameStats.player_id)
        .all()
    )
    return _candidate_player_ids_from_rows(rows, per_season_limit=per_season_limit)


def _champion_seeds(session, target_seasons: list[int]) -> list[AwardSeed]:
    if not target_seasons:
        return []
    playoff_tokens = [f"4{str(season)[1:]}" for season in target_seasons]
    rows = (
        session.query(Game.season, Game.game_date, Game.game_id, Game.wining_team_id)
        .filter(
            Game.season.in_(playoff_tokens),
            Game.wining_team_id.isnot(None),
            Game.game_date.isnot(None),
        )
        .order_by(Game.season.asc(), Game.game_date.asc(), Game.game_id.asc())
        .all()
    )
    latest_by_season: dict[int, tuple[str, str | None]] = {}
    for season_token, game_date, game_id, winning_team_id in rows:
        award_season = _season_text_to_award_season(f"2{str(season_token)[1:]}")
        if award_season is None or winning_team_id is None:
            continue
        latest_by_season[award_season] = (str(winning_team_id), game_id)
    return [
        AwardSeed(
            award_type="champion",
            season=season,
            team_id=team_id,
            notes=f"Final game: {game_id}" if game_id else None,
        )
        for season, (team_id, game_id) in sorted(latest_by_season.items())
    ]


def _player_award_seed_from_row(row: dict[str, object], team_lookup: dict[str, list[Team]]) -> AwardSeed | None:
    subtype = str(row.get("SUBTYPE2") or "").strip()
    description = str(row.get("DESCRIPTION") or "").strip()
    season_id = _season_text_to_award_season(row.get("SEASON"))  # type: ignore[arg-type]
    if season_id is None:
        return None

    award_type = None
    if subtype == "KIANT" and description == "All-NBA":
        award_type = ALL_NBA_TYPE_BY_TEAM_NUMBER.get(str(row.get("ALL_NBA_TEAM_NUMBER") or "").strip())
    elif subtype == "KIMVP" and description == "NBA Most Valuable Player":
        award_type = "mvp"
    elif subtype == "KFMVP" and description == "NBA Finals Most Valuable Player":
        award_type = "finals_mvp"

    if not award_type:
        return None

    player_id = str(row.get("PERSON_ID") or "").strip() or None
    team_name = str(row.get("TEAM") or "").strip() or None
    team_id = _resolve_team_id(team_lookup, team_name, season_id)
    notes = team_name if team_name and team_id is None else None
    return AwardSeed(
        award_type=award_type,
        season=season_id,
        player_id=player_id,
        team_id=team_id,
        notes=notes,
    )


@retry(
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(8),
    retry=retry_if_exception_type((ConnectionError, Timeout)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def _fetch_player_awards(player_id: str) -> list[dict[str, object]]:
    if playerawards is None:
        raise RuntimeError("nba_api is not installed; cannot fetch player awards")
    response = playerawards.PlayerAwards(player_id=player_id, timeout=30)
    payload = response.player_awards.get_dict()
    headers = payload["headers"]
    return [dict(zip(headers, row)) for row in payload["data"]]


@retry(
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(8),
    retry=retry_if_exception_type((ConnectionError, Timeout)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def _fetch_scoring_leader(season_text: str) -> dict[str, object] | None:
    if leagueleaders is None:
        raise RuntimeError("nba_api is not installed; cannot fetch league leaders")
    response = leagueleaders.LeagueLeaders(
        season=season_text,
        stat_category_abbreviation="PTS",
        season_type_all_star="Regular Season",
        per_mode48="PerGame",
        timeout=30,
    )
    payload = response.league_leaders.get_dict()
    headers = payload["headers"]
    rows = payload["data"]
    if not rows:
        return None
    return dict(zip(headers, rows[0]))


def _player_award_seeds(player_ids: list[str], target_seasons: set[int], team_lookup: dict[str, list[Team]]) -> list[AwardSeed]:
    seeds: list[AwardSeed] = []
    for idx, player_id in enumerate(player_ids, start=1):
        try:
            rows = _fetch_player_awards(player_id)
        except RetryError:
            logger.error("[%s/%s] playerawards failed after retries for %s", idx, len(player_ids), player_id)
            continue
        except Exception as exc:
            logger.error("[%s/%s] playerawards failed for %s: %s", idx, len(player_ids), player_id, exc)
            continue

        for row in rows:
            seed = _player_award_seed_from_row(row, team_lookup)
            if seed and seed.season in target_seasons:
                seeds.append(seed)

        time.sleep(0.6)
    return seeds


def _scoring_champion_seeds(target_seasons: list[int]) -> list[AwardSeed]:
    seeds: list[AwardSeed] = []
    for season_id in target_seasons:
        season_text = _award_season_to_api_text(season_id)
        try:
            row = _fetch_scoring_leader(season_text)
        except RetryError:
            logger.error("leagueleaders failed after retries for %s", season_text)
            continue
        except Exception as exc:
            logger.error("leagueleaders failed for %s: %s", season_text, exc)
            continue

        if row is None:
            continue

        points_per_game = row.get("PTS")
        note = f"{float(points_per_game):.1f} PPG" if isinstance(points_per_game, (int, float)) else None
        seeds.append(
            AwardSeed(
                award_type="scoring_champion",
                season=season_id,
                player_id=str(row.get("PLAYER_ID") or "") or None,
                team_id=str(row.get("TEAM_ID") or "") or None,
                notes=note,
            )
        )
        time.sleep(0.6)
    return seeds


def _upsert_award(session, seed: AwardSeed, dry_run: bool = False) -> str:
    existing = (
        session.query(Award)
        .filter(
            Award.award_type == seed.award_type,
            Award.season == seed.season,
            Award.player_id.is_(seed.player_id) if seed.player_id is None else Award.player_id == seed.player_id,
            Award.team_id.is_(seed.team_id) if seed.team_id is None else Award.team_id == seed.team_id,
        )
        .first()
    )
    if existing:
        if seed.notes and existing.notes != seed.notes:
            if not dry_run:
                existing.notes = seed.notes
            return "updated"
        return "skipped"

    if not dry_run:
        session.add(
            Award(
                award_type=seed.award_type,
                season=seed.season,
                player_id=seed.player_id,
                team_id=seed.team_id,
                notes=seed.notes,
            )
        )
    return "inserted"


def _parse_season_arg(value: str | None) -> int | None:
    if value is None:
        return None
    season_id = _season_text_to_award_season(value)
    if season_id is not None:
        return season_id
    raise ValueError(f"Unsupported season format: {value}")


def run(
    *,
    dry_run: bool = False,
    only_season: int | None = None,
    player_candidate_limit: int | None = DEFAULT_PLAYER_CANDIDATE_LIMIT,
) -> dict[str, int]:
    with Session() as session:
        target_seasons = _target_award_seasons(session, only_season=only_season)
        if not target_seasons:
            logger.warning("No target seasons found in Game table")
            return {"inserted": 0, "updated": 0, "skipped": 0}

        logger.info("Target seasons: %s", ", ".join(str(season) for season in target_seasons))

        team_lookup = _build_team_lookup(session)
        seeds: list[AwardSeed] = []
        seeds.extend(_champion_seeds(session, target_seasons))

        player_ids = _player_ids_for_target_seasons(
            session,
            target_seasons,
            per_season_limit=player_candidate_limit,
        )
        logger.info(
            "Fetching player awards for %s candidate players (%s)",
            len(player_ids),
            (
                f"top {player_candidate_limit} regular-season minute leaders per season"
                if player_candidate_limit is not None
                else "all tracked regular-season players"
            ),
        )
        seeds.extend(_player_award_seeds(player_ids, set(target_seasons), team_lookup))

        logger.info("Fetching scoring champions for %s seasons", len(target_seasons))
        seeds.extend(_scoring_champion_seeds(target_seasons))

        deduped = {
            (seed.award_type, seed.season, seed.player_id, seed.team_id): seed
            for seed in seeds
        }
        ordered = sorted(
            deduped.values(),
            key=lambda seed: (
                AWARD_TYPE_ORDER.index(seed.award_type),
                -seed.season,
                seed.player_id or "",
                seed.team_id or "",
            ),
        )

        stats = {"inserted": 0, "updated": 0, "skipped": 0}
        for seed in ordered:
            action = _upsert_award(session, seed, dry_run=dry_run)
            stats[action] += 1
            logger.info(
                "%s %s season=%s player_id=%s team_id=%s notes=%s",
                "would upsert" if dry_run else action,
                seed.award_type,
                seed.season,
                seed.player_id or "-",
                seed.team_id or "-",
                seed.notes or "-",
            )

        if dry_run:
            session.rollback()
        else:
            session.commit()

        logger.info("Done. inserted=%s updated=%s skipped=%s", stats["inserted"], stats["updated"], stats["skipped"])
        return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill NBA awards into the Award table")
    parser.add_argument("--dry-run", action="store_true", help="Print planned changes without writing to the database")
    parser.add_argument(
        "--season",
        type=str,
        help="Limit to a single season (e.g. 22024 or 2024-25)",
    )
    parser.add_argument(
        "--player-candidate-limit",
        type=int,
        default=DEFAULT_PLAYER_CANDIDATE_LIMIT,
        help=(
            "Scan only the top N regular-season minute leaders per season for player awards; "
            "use 0 to scan every tracked player"
        ),
    )
    args = parser.parse_args()
    run(
        dry_run=args.dry_run,
        only_season=_parse_season_arg(args.season),
        player_candidate_limit=None if args.player_candidate_limit == 0 else args.player_candidate_limit,
    )

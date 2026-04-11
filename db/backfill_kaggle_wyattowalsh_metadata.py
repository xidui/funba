from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from db.backfill_kaggle_wyattowalsh import (
    KAGGLE_DATASET_SLUG,
    KAGGLE_DATASET_URL,
    ImportCounts as TeamImportCounts,
    TeamResolver,
    _canonical_team_name,
    _clean_str,
    _is_missing,
    _load_optional_csv,
    _normalize_game_id,
    _parse_date,
    _to_int_or_none,
)
from db.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_LIVE, GAME_STATUS_UPCOMING
from db.models import Game, Player, engine

logger = logging.getLogger(__name__)

SessionLocal = sessionmaker(bind=engine)

_PLAYER_FILE = "player.csv"
_COMMON_PLAYER_INFO_FILE = "common_player_info.csv"
_TEAM_FILE = "team.csv"
_TEAM_HISTORY_FILE = "team_history.csv"
_TEAM_DETAILS_FILE = "team_details.csv"
_GAME_INFO_FILE = "game_info.csv"
_GAME_SUMMARY_FILE = "game_summary.csv"


@dataclass
class ImportCounts:
    players_created: int = 0
    players_updated: int = 0
    teams_created: int = 0
    teams_updated: int = 0
    games_updated: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "players_created": self.players_created,
            "players_updated": self.players_updated,
            "teams_created": self.teams_created,
            "teams_updated": self.teams_updated,
            "games_updated": self.games_updated,
        }


def _coerce_bool(value, *, default: bool | None = None) -> bool | None:
    if _is_missing(value):
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "active"}:
        return True
    if text in {"0", "false", "no", "n", "inactive"}:
        return False
    return default


def _status_from_code(game_status: int | str | None) -> str:
    code = _to_int_or_none(game_status) or 1
    if code >= 3:
        return GAME_STATUS_COMPLETED
    if code == 2:
        return GAME_STATUS_LIVE
    return GAME_STATUS_UPCOMING


def _player_full_name(row: dict) -> str | None:
    return _clean_str(row.get("display_first_last")) or _clean_str(row.get("full_name"))


def _split_name(full_name: str | None) -> tuple[str | None, str | None]:
    name = _clean_str(full_name)
    if not name:
        return None, None
    parts = name.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _player_snapshot(player: Player | None) -> tuple:
    if player is None:
        return ()
    return (
        player.player_id,
        player.first_name,
        player.last_name,
        player.full_name,
        player.slug,
        player.is_active,
        player.height,
        player.weight,
        player.birth_date,
        player.country,
        player.school,
        player.draft_year,
        player.draft_round,
        player.draft_number,
        player.jersey,
        player.position,
        player.from_year,
        player.to_year,
        player.season_exp,
        player.greatest_75_flag,
    )


def _team_snapshot(team) -> tuple:
    return (
        team.team_id,
        team.full_name,
        team.abbr,
        team.nick_name,
        team.city,
        team.state,
        team.year_founded,
        team.arena,
        team.arena_capacity,
        team.owner,
        team.general_manager,
        team.head_coach,
        team.g_league_affiliation,
        team.facebook_url,
        team.instagram_url,
        team.twitter_url,
    )


class PlayerResolver:
    def __init__(self, session: Session, counts: ImportCounts):
        self.session = session
        self.counts = counts
        self.by_id: dict[str, Player] = {}
        self.by_name: dict[str, list[Player]] = {}
        self.by_slug: dict[str, Player] = {}
        self._load_existing()

    def _load_existing(self) -> None:
        for player in self.session.query(Player).all():
            self._index(player)

    def _index(self, player: Player) -> None:
        if player.player_id:
            self.by_id[str(player.player_id)] = player
        if player.full_name:
            self.by_name.setdefault(player.full_name.casefold(), []).append(player)
        if player.slug:
            self.by_slug[player.slug] = player

    def resolve(self, *, player_row: dict | None = None, info_row: dict | None = None) -> Player:
        source = dict(player_row or {})
        source.update(info_row or {})

        explicit_id = _clean_str(source.get("person_id")) or _clean_str(source.get("id")) or _clean_str(source.get("player_id"))
        full_name = _player_full_name(source)

        existing = None
        if explicit_id:
            existing = self.by_id.get(explicit_id)
        if existing is None and full_name:
            matches = self.by_name.get(full_name.casefold(), [])
            if len(matches) == 1:
                existing = matches[0]

        before = _player_snapshot(existing)
        if existing is None:
            first_name = _clean_str(source.get("first_name"))
            last_name = _clean_str(source.get("last_name"))
            if full_name and (first_name is None or last_name is None):
                first_name, last_name = _split_name(full_name)
            player = Player(
                player_id=explicit_id or f"kaggle-player:{full_name or 'unknown'}",
                first_name=first_name,
                last_name=last_name,
                full_name=full_name,
            )
            self.session.add(player)
            self.counts.players_created += 1
        else:
            player = existing

        self._update_player(player, source)

        if existing is None:
            self._index(player)
        elif _player_snapshot(player) != before:
            self.counts.players_updated += 1
        return player

    def _update_player(self, player: Player, source: dict) -> None:
        full_name = _player_full_name(source)
        first_name = _clean_str(source.get("first_name"))
        last_name = _clean_str(source.get("last_name"))
        if full_name and (first_name is None or last_name is None):
            inferred_first, inferred_last = _split_name(full_name)
            first_name = first_name or inferred_first
            last_name = last_name or inferred_last

        updates = {
            "player_id": _clean_str(source.get("person_id")) or _clean_str(source.get("id")) or _clean_str(source.get("player_id")) or player.player_id,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "slug": _clean_str(source.get("player_slug")),
            "is_active": _coerce_bool(source.get("is_active"), default=None),
            "height": _clean_str(source.get("height")),
            "weight": _to_int_or_none(source.get("weight")),
            "birth_date": _parse_date(source.get("birthdate")),
            "country": _clean_str(source.get("country")),
            "school": _clean_str(source.get("school")),
            "draft_year": _to_int_or_none(source.get("draft_year")),
            "draft_round": _to_int_or_none(source.get("draft_round")),
            "draft_number": _to_int_or_none(source.get("draft_number")),
            "jersey": _clean_str(source.get("jersey")),
            "position": _clean_str(source.get("position")),
            "from_year": _to_int_or_none(source.get("from_year")),
            "to_year": _to_int_or_none(source.get("to_year")),
            "season_exp": _to_int_or_none(source.get("season_exp")),
            "greatest_75_flag": _coerce_bool(source.get("greatest_75_flag"), default=None),
        }

        roster_status = _clean_str(source.get("rosterstatus"))
        if updates["is_active"] is None and roster_status:
            updates["is_active"] = roster_status.casefold() == "active"

        slug = updates.get("slug")
        if slug and slug in self.by_slug and self.by_slug[slug].player_id != player.player_id:
            updates["slug"] = None

        for field, value in updates.items():
            if value is None:
                continue
            if getattr(player, field) != value:
                setattr(player, field, value)
        if player.slug:
            self.by_slug[player.slug] = player


def _update_game_from_rows(game: Game, *, info_row: dict | None, summary_row: dict | None) -> bool:
    updates = {
        "attendance": _to_int_or_none((info_row or {}).get("attendance")),
        "tipoff_time": _clean_str((info_row or {}).get("game_time")),
        "external_game_code": _clean_str((summary_row or {}).get("gamecode")),
        "national_tv_broadcaster": _clean_str((summary_row or {}).get("natl_tv_broadcaster_abbreviation")),
    }
    status_id = (summary_row or {}).get("game_status_id")
    status_text = _clean_str((summary_row or {}).get("game_status_text"))
    if not _is_missing(status_id):
        updates["game_status"] = _status_from_code(status_id)
    elif status_text:
        normalized = status_text.casefold()
        if normalized in {GAME_STATUS_COMPLETED, GAME_STATUS_LIVE, GAME_STATUS_UPCOMING}:
            updates["game_status"] = normalized

    changed = False
    for field, value in updates.items():
        if value is None:
            continue
        if getattr(game, field) != value:
            setattr(game, field, value)
            changed = True
    return changed


def backfill_kaggle_wyattowalsh_metadata(session: Session, source: str | Path) -> ImportCounts:
    counts = ImportCounts()
    team_counts = TeamImportCounts()
    team_resolver = TeamResolver(session, team_counts)
    player_resolver = PlayerResolver(session, counts)

    team_frame = _load_optional_csv(source, _TEAM_FILE)
    team_history_frame = _load_optional_csv(source, _TEAM_HISTORY_FILE)
    team_details_frame = _load_optional_csv(source, _TEAM_DETAILS_FILE)
    player_frame = _load_optional_csv(source, _PLAYER_FILE)
    common_player_info_frame = _load_optional_csv(source, _COMMON_PLAYER_INFO_FILE)
    game_info_frame = _load_optional_csv(source, _GAME_INFO_FILE)
    game_summary_frame = _load_optional_csv(source, _GAME_SUMMARY_FILE)

    if team_frame is not None:
        for row in team_frame.to_dict(orient="records"):
            before = None
            normalized_id = _normalize_game_id(row.get("id")) or _clean_str(row.get("id"))
            if normalized_id and normalized_id in team_resolver.by_id:
                before = _team_snapshot(team_resolver.by_id[normalized_id])
            team = team_resolver.resolve(
                team_id=row.get("id"),
                team_name=row.get("full_name"),
                team_abbr=row.get("abbreviation"),
                team_city=row.get("city"),
                team_state=row.get("state"),
                team_nickname=row.get("nickname"),
                founded_year=row.get("year_founded"),
            )
            if before is not None and _team_snapshot(team) != before:
                counts.teams_updated += 1

    if team_history_frame is not None:
        for row in team_history_frame.to_dict(orient="records"):
            normalized_id = _normalize_game_id(row.get("team_id")) or _clean_str(row.get("team_id"))
            before = _team_snapshot(team_resolver.by_id[normalized_id]) if normalized_id and normalized_id in team_resolver.by_id else None
            team = team_resolver.resolve(
                team_id=row.get("team_id"),
                team_city=row.get("city"),
                team_nickname=row.get("nickname"),
                founded_year=row.get("year_founded"),
            )
            if before is not None and _team_snapshot(team) != before:
                counts.teams_updated += 1

    if team_details_frame is not None:
        for row in team_details_frame.to_dict(orient="records"):
            normalized_id = _normalize_game_id(row.get("team_id")) or _clean_str(row.get("team_id"))
            before = _team_snapshot(team_resolver.by_id[normalized_id]) if normalized_id and normalized_id in team_resolver.by_id else None
            team = team_resolver.resolve(
                team_id=row.get("team_id"),
                team_abbr=row.get("abbreviation"),
                team_city=row.get("city"),
                team_nickname=row.get("nickname"),
                founded_year=row.get("yearfounded"),
                arena=row.get("arena"),
                arena_capacity=row.get("arenacapacity"),
                owner=row.get("owner"),
                general_manager=row.get("generalmanager"),
                head_coach=row.get("headcoach"),
                g_league_affiliation=row.get("dleagueaffiliation"),
                facebook_url=row.get("facebook"),
                instagram_url=row.get("instagram"),
                twitter_url=row.get("twitter"),
            )
            if before is not None and _team_snapshot(team) != before:
                counts.teams_updated += 1

    counts.teams_created = team_counts.teams_created

    common_player_info_by_id: dict[str, dict] = {}
    if common_player_info_frame is not None:
        for row in common_player_info_frame.to_dict(orient="records"):
            explicit_id = _clean_str(row.get("person_id"))
            if explicit_id:
                common_player_info_by_id[explicit_id] = row

    if player_frame is not None:
        for row in player_frame.to_dict(orient="records"):
            explicit_id = _clean_str(row.get("id"))
            player_resolver.resolve(player_row=row, info_row=common_player_info_by_id.get(explicit_id or ""))

    if common_player_info_frame is not None:
        for row in common_player_info_frame.to_dict(orient="records"):
            player_resolver.resolve(info_row=row)

    game_info_by_id = {}
    if game_info_frame is not None:
        for row in game_info_frame.to_dict(orient="records"):
            game_id = _normalize_game_id(row.get("game_id"))
            if game_id:
                game_info_by_id[game_id] = row

    game_summary_by_id = {}
    if game_summary_frame is not None:
        for row in game_summary_frame.to_dict(orient="records"):
            game_id = _normalize_game_id(row.get("game_id"))
            if game_id:
                game_summary_by_id[game_id] = row

    for game_id in sorted(set(game_info_by_id) | set(game_summary_by_id)):
        game = session.get(Game, game_id)
        if game is None:
            continue
        if _update_game_from_rows(game, info_row=game_info_by_id.get(game_id), summary_row=game_summary_by_id.get(game_id)):
            counts.games_updated += 1

    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Team, Player, and Game metadata from the wyattowalsh/basketball Kaggle dataset.",
    )
    parser.add_argument("--source", required=True, help="Path to the downloaded zip file or extracted dataset directory.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and upsert in a transaction, then roll it back.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logger.info("starting kaggle metadata backfill dataset=%s source=%s", KAGGLE_DATASET_SLUG, args.source)
    with SessionLocal() as session:
        counts = backfill_kaggle_wyattowalsh_metadata(session, args.source)
        if args.dry_run:
            session.rollback()
            logger.info("dry run complete counts=%s", counts.as_dict())
        else:
            session.commit()
            logger.info("backfill complete counts=%s", counts.as_dict())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, sessionmaker

from db.models import Game, GameLineScore, Player, PlayerGameStats, Team, TeamGameStats, engine

logger = logging.getLogger(__name__)

KAGGLE_DATASET_SLUG = "eoinamoore/historical-nba-data-and-player-box-scores"
KAGGLE_DATASET_URL = "https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores"
KAGGLE_DATASET_VERSION = 425
KAGGLE_BOX_SCORE_SOURCE = "kaggle_box_scores"

SessionLocal = sessionmaker(bind=engine)

_PLAYER_FILE = "PlayerStatistics.csv"
_TEAM_FILE = "TeamStatistics.csv"
_GAMES_FILE = "Games.csv"
_PLAYERS_FILE = "Players.csv"
_TEAM_HISTORIES_FILE = "TeamHistories.csv"

_TEAM_NAME_ALIASES = {
    "la clippers": "los angeles clippers",
    "la lakers": "los angeles lakers",
    "new jersey americans": "brooklyn nets",
    "new jersey nets": "brooklyn nets",
    "san diego clippers": "los angeles clippers",
    "buffalo braves": "los angeles clippers",
    "kansas city kings": "sacramento kings",
    "kansas city-omaha kings": "sacramento kings",
    "cincinnati royals": "sacramento kings",
    "rochester royals": "sacramento kings",
    "fort wayne pistons": "detroit pistons",
    "minneapolis lakers": "los angeles lakers",
    "seattle supersonics": "oklahoma city thunder",
    "new orleans hornets": "new orleans pelicans",
    "new orleans/oklahoma city hornets": "new orleans pelicans",
    "charlotte bobcats": "charlotte hornets",
    "new orleans jazz": "utah jazz",
    "chicago packers": "washington wizards",
    "chicago zephyrs": "washington wizards",
    "washington bullets": "washington wizards",
    "capital bullets": "washington wizards",
    "baltimore bullets": "washington wizards",
    "vancouver grizzlies": "memphis grizzlies",
    "philadelphia warriors": "golden state warriors",
    "san francisco warriors": "golden state warriors",
    "tri-cities blackhawks": "atlanta hawks",
    "st. louis hawks": "atlanta hawks",
    "syracuse nationals": "philadelphia 76ers",
}


@dataclass
class ImportCounts:
    games_created: int = 0
    games_updated: int = 0
    team_stats_created: int = 0
    team_stats_updated: int = 0
    player_stats_created: int = 0
    player_stats_updated: int = 0
    line_scores_created: int = 0
    line_scores_updated: int = 0
    teams_created: int = 0
    teams_updated: int = 0
    players_created: int = 0
    players_updated: int = 0
    skipped_games_missing_team_stats: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "games_created": self.games_created,
            "games_updated": self.games_updated,
            "team_stats_created": self.team_stats_created,
            "team_stats_updated": self.team_stats_updated,
            "player_stats_created": self.player_stats_created,
            "player_stats_updated": self.player_stats_updated,
            "line_scores_created": self.line_scores_created,
            "line_scores_updated": self.line_scores_updated,
            "teams_created": self.teams_created,
            "teams_updated": self.teams_updated,
            "players_created": self.players_created,
            "players_updated": self.players_updated,
            "skipped_games_missing_team_stats": self.skipped_games_missing_team_stats,
        }


def _normalize_column_name(name: str) -> str:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name).strip())
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", normalized).strip("_").lower()
    replacements = {
        "hometeam_": "home_team_",
        "awayteam_": "away_team_",
        "visitorteam_": "visitor_team_",
        "roadteam_": "road_team_",
        "opponentteam_": "opponent_team_",
        "playerteam_": "player_team_",
    }
    for source, target in replacements.items():
        normalized = normalized.replace(source, target)
    return normalized


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [_normalize_column_name(col) for col in normalized.columns]
    return normalized


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return bool(pd.isna(value))


def _clean_str(value) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def _slugify(value: str | None) -> str:
    text = _clean_str(value) or "unknown"
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "unknown"


def _row_value(row: dict, *names: str):
    for name in names:
        value = row.get(name)
        if not _is_missing(value):
            return value
    return None


def _to_int_or_none(value) -> int | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _to_float_or_none(value) -> float | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    if abs(number) > 1.0 and "%" in str(value):
        return number / 100.0
    return number


def _parse_minutes(value) -> tuple[int | None, int | None]:
    if _is_missing(value):
        return None, None
    text = str(value).strip()
    try:
        if ":" in text:
            minute_part, second_part = text.split(":", 1)
            return int(float(minute_part or 0)), int(float(second_part or 0))
        if "." in text:
            return int(float(text)), 0
        return int(text), 0
    except (TypeError, ValueError):
        return None, None


def _parse_date(value) -> date | None:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return pd.to_datetime(text).date()
    except Exception:
        return None


def _compose_team_full_name(city: str | None, name: str | None) -> str | None:
    clean_city = _clean_str(city)
    clean_name = _clean_str(name)
    if clean_city and clean_name:
        return f"{clean_city} {clean_name}"
    return clean_name or clean_city


def _coerce_bool(value, *, default: bool | None = None) -> bool | None:
    if _is_missing(value):
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    try:
        number = float(text)
    except (TypeError, ValueError):
        number = None
    if number is not None:
        return bool(int(number))
    if text in {"1", "true", "yes", "y", "home", "starter"}:
        return True
    if text in {"0", "false", "no", "n", "away", "bench"}:
        return False
    return default


def _canonical_team_name(name: str | None) -> str | None:
    normalized = _clean_str(name)
    if normalized is None:
        return None
    key = normalized.casefold()
    return _TEAM_NAME_ALIASES.get(key, normalized)


def _season_start_year_from_row(row: dict) -> int | None:
    season_id = _clean_str(_row_value(row, "season_id", "seasonid"))
    if season_id and len(season_id) >= 5 and season_id[1:].isdigit():
        return int(season_id[1:])

    for name in ("season_start_year", "season_start", "start_year"):
        year = _to_int_or_none(row.get(name))
        if year is not None:
            return year

    season_text = _clean_str(_row_value(row, "season", "season_name", "season_label"))
    if season_text:
        match = re.search(r"(19|20)\d{2}", season_text)
        if match:
            return int(match.group(0))
    game_date = _parse_date(_row_value(row, "game_date", "gamedate", "game_date_time_est", "gamedatetimeest", "date"))
    if game_date is not None:
        return game_date.year if game_date.month >= 9 else game_date.year - 1
    return None


def _season_type_prefix(row: dict) -> str:
    season_id = _clean_str(_row_value(row, "season_id", "seasonid"))
    if season_id and len(season_id) >= 1:
        return season_id[0]

    season_type = (_clean_str(_row_value(row, "season_type", "season_type_text", "gametype", "game_type")) or "").casefold()
    if "play in" in season_type or "play-in" in season_type:
        return "5"
    if "playoff" in season_type:
        return "4"
    return "2"


def _season_token(row: dict) -> str | None:
    season_id = _clean_str(_row_value(row, "season_id", "seasonid"))
    if season_id:
        return season_id
    start_year = _season_start_year_from_row(row)
    if start_year is None:
        return None
    return f"{_season_type_prefix(row)}{start_year}"


def _build_stable_game_id(row: dict, *, home_team_id: str, road_team_id: str) -> str:
    explicit = _clean_str(_row_value(row, "game_id", "gameid", "id", "box_score_game_id"))
    if explicit:
        return explicit

    game_date = _parse_date(_row_value(row, "game_date", "gamedate", "game_date_time_est", "gamedatetimeest", "date"))
    date_token = game_date.isoformat() if game_date is not None else "unknown-date"
    home_score = _clean_str(_row_value(row, "home_team_score", "home_score", "home_points", "homescore"))
    road_score = _clean_str(_row_value(row, "away_team_score", "road_team_score", "away_score", "visitor_score", "road_score", "awayscore"))
    score_token = f"{road_score or 'x'}-{home_score or 'x'}"
    return f"kaggle:{date_token}:{road_team_id}:{home_team_id}:{score_token}"


def _match_existing_game_id(
    session: Session,
    *,
    source_game_id: str,
    game_row: dict,
    home_team_id: str,
    road_team_id: str,
    home_score: int | None,
    road_score: int | None,
) -> str:
    with session.no_autoflush:
        existing = session.query(Game.game_id).filter(Game.game_id == source_game_id).one_or_none()
    if existing is not None:
        return source_game_id

    game_date = _parse_date(_row_value(game_row, "game_date", "gamedate", "game_date_time_est", "gamedatetimeest", "date"))
    if game_date is None:
        return source_game_id

    with session.no_autoflush:
        candidates = session.query(Game.game_id, Game.home_team_score, Game.road_team_score).filter(
            Game.game_date == game_date,
            Game.home_team_id == home_team_id,
            Game.road_team_id == road_team_id,
        ).all()
    if not candidates:
        return source_game_id
    if len(candidates) == 1:
        return str(candidates[0].game_id)

    if home_score is not None and road_score is not None:
        for candidate in candidates:
            if candidate.home_team_score == home_score and candidate.road_team_score == road_score:
                return str(candidate.game_id)
    return source_game_id


def _value_map(frame: pd.DataFrame | None, *, key_names: tuple[str, ...], value_names: tuple[str, ...]) -> dict[str, dict]:
    if frame is None or frame.empty:
        return {}
    items: dict[str, dict] = {}
    for row in frame.to_dict(orient="records"):
        key_value = _clean_str(_row_value(row, *key_names))
        if not key_value:
            continue
        items[key_value] = {name: _row_value(row, name) for name in value_names}
        items[key_value].update(row)
    return items


def _resolve_input_root(source: str | Path) -> tuple[Path, Path | None]:
    path = Path(source).expanduser().resolve()
    if path.is_dir():
        return path, None
    if path.is_file() and path.suffix.lower() == ".zip":
        temp_dir = Path(tempfile.mkdtemp(prefix="funba-kaggle-"))
        with zipfile.ZipFile(path) as zipped:
            zipped.extractall(temp_dir)
        return temp_dir, temp_dir
    raise FileNotFoundError(f"Expected Kaggle dataset directory or zip file, got {path}")


def _find_file(root: Path, filename: str) -> Path | None:
    lower = filename.casefold()
    for path in root.rglob("*"):
        if path.is_file() and path.name.casefold() == lower:
            return path
    return None


def _load_optional_csv(root: Path, filename: str) -> pd.DataFrame | None:
    path = _find_file(root, filename)
    if path is None:
        return None
    return _normalize_frame(pd.read_csv(path, low_memory=False))


def _load_required_csv(root: Path, filename: str) -> pd.DataFrame:
    frame = _load_optional_csv(root, filename)
    if frame is None:
        raise FileNotFoundError(f"Required Kaggle file not found: {filename}")
    return frame


class TeamResolver:
    def __init__(self, session: Session, counts: ImportCounts):
        self.session = session
        self.counts = counts
        self.by_id: dict[str, Team] = {}
        self.by_abbr: dict[str, Team] = {}
        self.by_name: dict[str, Team] = {}
        self.next_numeric_id = 1
        self._load_existing()

    def _load_existing(self) -> None:
        for team in self.session.query(Team).all():
            self._index(team)
            if team.id is not None and team.id >= self.next_numeric_id:
                self.next_numeric_id = team.id + 1

    def _index(self, team: Team) -> None:
        if team.team_id:
            self.by_id[str(team.team_id)] = team
        if team.abbr:
            self.by_abbr[str(team.abbr).casefold()] = team
        if team.full_name:
            self.by_name[_canonical_team_name(team.full_name).casefold()] = team

    def resolve(
        self,
        row: dict,
        *,
        fallback_name: str | None = None,
        fallback_abbr: str | None = None,
        allow_create: bool = True,
    ) -> Team:
        explicit_id = _clean_str(_row_value(row, "team_id", "teamid", "nba_team_id", "id"))
        if explicit_id and explicit_id in self.by_id:
            team = self.by_id[explicit_id]
            self._update_team(team, row, fallback_name=fallback_name, fallback_abbr=fallback_abbr)
            return team

        abbr = _clean_str(_row_value(row, "team_abbr", "team_tricode", "team_abbreviation", "abbr", "team_code")) or fallback_abbr
        if abbr and abbr.casefold() in self.by_abbr:
            team = self.by_abbr[abbr.casefold()]
            self._update_team(team, row, fallback_name=fallback_name, fallback_abbr=abbr)
            return team

        full_name = _canonical_team_name(
            _clean_str(
                _row_value(
                    row,
                    "team_name",
                    "full_name",
                    "team_full_name",
                    "franchise_name",
                    "display_name",
                )
            )
            or _compose_team_full_name(
                _row_value(row, "team_city", "city"),
                _row_value(row, "team_name", "nickname", "team_nickname"),
            )
            or fallback_name
        )
        if full_name and full_name.casefold() in self.by_name:
            team = self.by_name[full_name.casefold()]
            self._update_team(team, row, fallback_name=full_name, fallback_abbr=abbr)
            return team

        if not allow_create:
            raise ValueError(f"Unable to resolve team row: {row}")

        team_id = explicit_id or f"kaggle-team:{_slugify(full_name or abbr)}"
        team = Team(id=self.next_numeric_id, team_id=team_id)
        self.next_numeric_id += 1
        self.session.add(team)
        self.counts.teams_created += 1
        self._update_team(team, row, fallback_name=full_name, fallback_abbr=abbr)
        self._index(team)
        return team

    def _update_team(self, team: Team, row: dict, *, fallback_name: str | None, fallback_abbr: str | None) -> None:
        changed = False
        full_name = _canonical_team_name(
            _clean_str(
                _row_value(row, "full_name", "team_name", "team_full_name", "franchise_name", "display_name")
            )
            or _compose_team_full_name(
                _row_value(row, "team_city", "city"),
                _row_value(row, "team_name", "nickname", "team_nickname"),
            )
            or fallback_name
        )
        preserve_identity = False
        if full_name and team.full_name and full_name.casefold() == team.full_name.casefold():
            full_name = team.full_name
            incoming_abbr = _clean_str(_row_value(row, "abbr", "team_abbr", "team_tricode", "team_abbreviation", "team_code")) or fallback_abbr
            incoming_city = _clean_str(_row_value(row, "city", "team_city"))
            if (incoming_abbr and team.abbr and incoming_abbr.casefold() != team.abbr.casefold()) or (
                incoming_city and team.city and incoming_city.casefold() != team.city.casefold()
            ):
                preserve_identity = True
        elif full_name and team.full_name:
            preserve_identity = True
        abbr = _clean_str(_row_value(row, "abbr", "team_abbr", "team_tricode", "team_abbreviation", "team_code", "team_abbrev")) or fallback_abbr
        nick_name = _clean_str(_row_value(row, "nick_name", "nickname", "team_nickname"))
        city = _clean_str(_row_value(row, "city", "team_city"))
        start_year = _to_int_or_none(_row_value(row, "start_year", "year_founded", "first_season", "season_founded"))
        end_year = _to_int_or_none(_row_value(row, "end_year", "last_season", "season_active_till"))

        updates = {
            "full_name": team.full_name if preserve_identity else full_name,
            "abbr": team.abbr if preserve_identity else abbr,
            "nick_name": team.nick_name if preserve_identity else (nick_name or (full_name.split()[-1] if full_name else None)),
            "city": team.city if preserve_identity else city,
            "year_founded": start_year,
            "is_legacy": False if end_year is None else True,
            "start_season": str(start_year) if start_year is not None else None,
            "end_season": str(end_year) if end_year is not None else None,
            "active": end_year is None,
        }
        for field, value in updates.items():
            if value is None:
                continue
            if getattr(team, field) != value:
                setattr(team, field, value)
                changed = True
        if changed and sa_inspect(team).persistent:
            self.counts.teams_updated += 1
            self._index(team)


class PlayerResolver:
    def __init__(self, session: Session, counts: ImportCounts):
        self.session = session
        self.counts = counts
        self.by_id: dict[str, Player] = {}
        self.by_name: dict[str, list[Player]] = defaultdict(list)
        self._load_existing()

    def _load_existing(self) -> None:
        for player in self.session.query(Player).all():
            self._index(player)

    def _index(self, player: Player) -> None:
        if player.player_id:
            self.by_id[str(player.player_id)] = player
        if player.full_name:
            self.by_name[player.full_name.casefold()].append(player)

    def resolve(self, row: dict, *, players_row: dict | None = None) -> Player:
        explicit_id = _clean_str(
            _row_value(
                row,
                "player_id",
                "person_id",
                "personid",
                "nba_player_id",
                "id",
            )
        ) or _clean_str(_row_value(players_row or {}, "player_id", "person_id", "personid", "nba_player_id", "id"))
        if explicit_id and explicit_id in self.by_id:
            player = self.by_id[explicit_id]
            self._update_player(player, row, players_row=players_row)
            return player

        full_name = _clean_str(
            _row_value(
                row,
                "player_name",
                "full_name",
                "name",
                "player_full_name",
                "player",
            )
        ) or _clean_str(_row_value(players_row or {}, "full_name", "player_name", "name")) or _compose_team_full_name(
            _row_value(row, "first_name"),
            _row_value(row, "last_name"),
        )
        if full_name and len(self.by_name.get(full_name.casefold(), [])) == 1:
            player = self.by_name[full_name.casefold()][0]
            self._update_player(player, row, players_row=players_row)
            return player

        first_name, last_name = _split_name(full_name)
        player_id = explicit_id or f"kaggle-player:{_slugify(full_name)}"
        player = Player(player_id=player_id, first_name=first_name, last_name=last_name, full_name=full_name)
        self.session.add(player)
        self.counts.players_created += 1
        self._update_player(player, row, players_row=players_row)
        self._index(player)
        return player

    def _update_player(self, player: Player, row: dict, *, players_row: dict | None) -> None:
        source = dict(players_row or {})
        source.update(row)
        full_name = _clean_str(_row_value(source, "full_name", "player_name", "name", "player_full_name")) or _compose_team_full_name(
            _row_value(source, "first_name", "firstname"),
            _row_value(source, "last_name", "lastname", "family_name"),
        )
        first_name = _clean_str(_row_value(source, "first_name", "firstname"))
        last_name = _clean_str(_row_value(source, "last_name", "lastname", "family_name"))
        if full_name and (first_name is None or last_name is None):
            inferred_first, inferred_last = _split_name(full_name)
            first_name = first_name or inferred_first
            last_name = last_name or inferred_last

        updates = {
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "nick_name": _clean_str(_row_value(source, "nickname", "nick_name")),
            "position": _clean_str(_row_value(source, "position", "pos")) or _position_from_flags(source),
            "height": _clean_str(_row_value(source, "height")) or _height_inches_to_feet_inches(_row_value(source, "height_inches")),
            "weight": _to_int_or_none(_row_value(source, "weight", "body_weight_lbs")),
            "birth_date": _parse_date(_row_value(source, "birth_date", "birthdate")),
            "country": _clean_str(_row_value(source, "country")),
            "school": _clean_str(_row_value(source, "school")),
            "draft_year": _to_int_or_none(_row_value(source, "draft_year")),
            "draft_round": _to_int_or_none(_row_value(source, "draft_round")),
            "draft_number": _to_int_or_none(_row_value(source, "draft_number")),
            "from_year": _to_int_or_none(_row_value(source, "from_year", "rookie_year", "start_year")),
            "to_year": _to_int_or_none(_row_value(source, "to_year", "last_year", "end_year")),
            "is_active": _coerce_bool(_row_value(source, "is_active", "active"), default=True),
        }
        changed = False
        for field, value in updates.items():
            if value is None:
                continue
            if getattr(player, field) != value:
                setattr(player, field, value)
                changed = True
        if changed and sa_inspect(player).persistent:
            self.counts.players_updated += 1


def _split_name(full_name: str | None) -> tuple[str | None, str | None]:
    name = _clean_str(full_name)
    if not name:
        return None, None
    parts = name.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _height_inches_to_feet_inches(value) -> str | None:
    inches = _to_int_or_none(value)
    if inches is None or inches <= 0:
        return None
    feet, remainder = divmod(inches, 12)
    return f"{feet}-{remainder}"


def _position_from_flags(row: dict) -> str | None:
    positions = []
    if _coerce_bool(row.get("guard")):
        positions.append("G")
    if _coerce_bool(row.get("forward")):
        positions.append("F")
    if _coerce_bool(row.get("center")):
        positions.append("C")
    if not positions:
        return None
    return "-".join(positions)


def _stats_value(row: dict, *names: str):
    return _row_value(row, *names)


def _extract_team_stat_payload(row: dict) -> dict[str, int | float | None]:
    return {
        "min": _parse_minutes(_stats_value(row, "minutes", "min", "team_minutes"))[0],
        "pts": _to_int_or_none(_stats_value(row, "points", "pts", "team_points")),
        "fgm": _to_int_or_none(_stats_value(row, "field_goals_made", "fgm")),
        "fga": _to_int_or_none(_stats_value(row, "field_goals_attempted", "fga")),
        "fg_pct": _to_float_or_none(_stats_value(row, "field_goals_percentage", "fg_pct")),
        "fg3m": _to_int_or_none(_stats_value(row, "three_pointers_made", "fg3m", "three_point_field_goals_made")),
        "fg3a": _to_int_or_none(_stats_value(row, "three_pointers_attempted", "fg3a", "three_point_field_goals_attempted")),
        "fg3_pct": _to_float_or_none(_stats_value(row, "three_pointers_percentage", "fg3_pct", "three_point_field_goal_percentage")),
        "ftm": _to_int_or_none(_stats_value(row, "free_throws_made", "ftm")),
        "fta": _to_int_or_none(_stats_value(row, "free_throws_attempted", "fta")),
        "ft_pct": _to_float_or_none(_stats_value(row, "free_throws_percentage", "ft_pct")),
        "oreb": _to_int_or_none(_stats_value(row, "rebounds_offensive", "oreb", "offensive_rebounds")),
        "dreb": _to_int_or_none(_stats_value(row, "rebounds_defensive", "dreb", "defensive_rebounds")),
        "reb": _to_int_or_none(_stats_value(row, "rebounds_total", "reb", "total_rebounds")),
        "ast": _to_int_or_none(_stats_value(row, "assists", "ast")),
        "stl": _to_int_or_none(_stats_value(row, "steals", "stl")),
        "blk": _to_int_or_none(_stats_value(row, "blocks", "blk")),
        "tov": _to_int_or_none(_stats_value(row, "turnovers", "to", "tov")),
        "pf": _to_int_or_none(_stats_value(row, "fouls_personal", "personal_fouls", "pf")),
    }


def _extract_player_stat_payload(row: dict) -> dict[str, int | float | str | None]:
    minutes, seconds = _parse_minutes(_stats_value(row, "minutes", "min", "minutes_played"))
    position = _clean_str(_stats_value(row, "position", "start_position", "pos"))
    return {
        "comment": _clean_str(_stats_value(row, "comment", "status")),
        "min": minutes,
        "sec": seconds,
        "starter": _coerce_bool(_stats_value(row, "starter", "started"), default=bool(position)),
        "position": position,
        "pts": _to_int_or_none(_stats_value(row, "points", "pts")),
        "fgm": _to_int_or_none(_stats_value(row, "field_goals_made", "fgm")),
        "fga": _to_int_or_none(_stats_value(row, "field_goals_attempted", "fga")),
        "fg_pct": _to_float_or_none(_stats_value(row, "field_goals_percentage", "fg_pct")),
        "fg3m": _to_int_or_none(_stats_value(row, "three_pointers_made", "fg3m", "three_point_field_goals_made")),
        "fg3a": _to_int_or_none(_stats_value(row, "three_pointers_attempted", "fg3a", "three_point_field_goals_attempted")),
        "fg3_pct": _to_float_or_none(_stats_value(row, "three_pointers_percentage", "fg3_pct", "three_point_field_goal_percentage")),
        "ftm": _to_int_or_none(_stats_value(row, "free_throws_made", "ftm")),
        "fta": _to_int_or_none(_stats_value(row, "free_throws_attempted", "fta")),
        "ft_pct": _to_float_or_none(_stats_value(row, "free_throws_percentage", "ft_pct")),
        "oreb": _to_int_or_none(_stats_value(row, "rebounds_offensive", "oreb", "offensive_rebounds")),
        "dreb": _to_int_or_none(_stats_value(row, "rebounds_defensive", "dreb", "defensive_rebounds")),
        "reb": _to_int_or_none(_stats_value(row, "rebounds_total", "reb", "total_rebounds")),
        "ast": _to_int_or_none(_stats_value(row, "assists", "ast")),
        "stl": _to_int_or_none(_stats_value(row, "steals", "stl")),
        "blk": _to_int_or_none(_stats_value(row, "blocks", "blk")),
        "tov": _to_int_or_none(_stats_value(row, "turnovers", "to", "tov")),
        "pf": _to_int_or_none(_stats_value(row, "fouls_personal", "personal_fouls", "pf")),
        "plus": _to_int_or_none(_stats_value(row, "plus_minus_points", "plus_minus", "plusminus")),
    }


def _period_points(row: dict, prefix: str) -> tuple[int | None, int | None, int | None, int | None, list[int]]:
    values: list[int | None] = []
    for period in range(1, 5):
        values.append(
            _to_int_or_none(
                _stats_value(
                    row,
                    f"{prefix}q{period}_pts",
                    f"{prefix}q{period}_points",
                    f"{prefix}q{period}",
                    f"{prefix}period_{period}",
                    f"{prefix}pts_qtr{period}",
                    f"{prefix}period{period}_points",
                )
            )
        )
    overtime_values: list[int] = []
    for overtime in range(1, 8):
        score = _to_int_or_none(
            _stats_value(
                row,
                f"{prefix}ot{overtime}_pts",
                f"{prefix}ot{overtime}",
                f"{prefix}period_{4 + overtime}",
                f"{prefix}pts_ot{overtime}",
            )
        )
        if score is not None:
            overtime_values.append(score)
    return values[0], values[1], values[2], values[3], overtime_values


def _resolve_player_game_team(
    team_resolver: TeamResolver,
    source_row: dict,
    *,
    home_team: Team,
    road_team: Team,
) -> Team:
    explicit_team_id = _clean_str(_row_value(source_row, "team_id", "teamid", "nba_team_id"))
    if explicit_team_id:
        return team_resolver.resolve(
            {
                "team_id": explicit_team_id,
                "team_name": _row_value(source_row, "team_name", "team_full_name"),
                "team_abbr": _row_value(source_row, "team_abbr", "team_tricode", "team_abbreviation"),
            },
            allow_create=True,
        )

    home_flag = _coerce_bool(_row_value(source_row, "home"), default=None)
    if home_flag is True:
        return home_team
    if home_flag is False:
        return road_team

    player_team_name = _compose_team_full_name(
        _row_value(source_row, "player_team_city", "team_city"),
        _row_value(source_row, "player_team_name", "team_name", "team_full_name"),
    )
    if player_team_name:
        canonical = _canonical_team_name(player_team_name)
        if canonical and home_team.full_name and canonical.casefold() == home_team.full_name.casefold():
            return home_team
        if canonical and road_team.full_name and canonical.casefold() == road_team.full_name.casefold():
            return road_team

    return team_resolver.resolve(
        {
            "team_id": explicit_team_id,
            "team_name": player_team_name,
            "team_abbr": _row_value(source_row, "team_abbr", "team_tricode", "team_abbreviation"),
        },
        fallback_name=player_team_name,
        allow_create=True,
    )


def _upsert_game_line_score(
    session: Session,
    counts: ImportCounts,
    *,
    game_id: str,
    team_id: str,
    total_pts: int | None,
    on_road: bool,
    row: dict,
) -> None:
    q1, q2, q3, q4, overtime_values = _period_points(row, "")
    if total_pts is None and all(value is None for value in (q1, q2, q3, q4)) and not overtime_values:
        return

    record = session.query(GameLineScore).filter_by(game_id=game_id, team_id=team_id).first()
    is_new = record is None
    now = datetime.now(UTC).replace(tzinfo=None)
    if record is None:
        record = GameLineScore(game_id=game_id, team_id=team_id, fetched_at=now, updated_at=now)
        session.add(record)
    record.on_road = on_road
    record.q1_pts = q1
    record.q2_pts = q2
    record.q3_pts = q3
    record.q4_pts = q4
    record.ot1_pts = overtime_values[0] if len(overtime_values) > 0 else None
    record.ot2_pts = overtime_values[1] if len(overtime_values) > 1 else None
    record.ot3_pts = overtime_values[2] if len(overtime_values) > 2 else None
    record.ot_extra_json = json.dumps(overtime_values[3:]) if len(overtime_values) > 3 else None
    record.first_half_pts = (q1 or 0) + (q2 or 0) if q1 is not None or q2 is not None else None
    record.second_half_pts = (q3 or 0) + (q4 or 0) if q3 is not None or q4 is not None else None
    record.regulation_total_pts = sum(value for value in (q1, q2, q3, q4) if value is not None) if any(
        value is not None for value in (q1, q2, q3, q4)
    ) else None
    record.total_pts = total_pts or 0
    record.source = KAGGLE_BOX_SCORE_SOURCE
    record.updated_at = now
    if record.fetched_at is None:
        record.fetched_at = now
    if is_new:
        counts.line_scores_created += 1
    else:
        counts.line_scores_updated += 1


def _game_row_to_dict(row) -> dict:
    if isinstance(row, dict):
        return row
    return dict(row)


def backfill_kaggle_historical(
    session: Session,
    source: str | Path,
    *,
    season_start: int | None = None,
    season_end: int | None = None,
    limit_games: int | None = None,
) -> ImportCounts:
    root, extracted_dir = _resolve_input_root(source)
    counts = ImportCounts()
    try:
        games_frame = _load_required_csv(root, _GAMES_FILE)
        team_stats_frame = _load_required_csv(root, _TEAM_FILE)
        player_stats_frame = _load_required_csv(root, _PLAYER_FILE)
        players_frame = _load_optional_csv(root, _PLAYERS_FILE)
        team_histories_frame = _load_optional_csv(root, _TEAM_HISTORIES_FILE)

        players_by_id = _value_map(players_frame, key_names=("player_id", "person_id", "personid", "nba_player_id", "id"), value_names=("full_name",))
        players_by_name = _value_map(players_frame, key_names=("full_name", "player_name", "name"), value_names=("player_id",))
        if players_frame is not None:
            for row in players_frame.to_dict(orient="records"):
                full_name = _clean_str(_row_value(row, "full_name", "player_name", "name")) or _compose_team_full_name(
                    _row_value(row, "first_name", "firstname"),
                    _row_value(row, "last_name", "lastname"),
                )
                if full_name:
                    players_by_name.setdefault(full_name, {"player_id": _row_value(row, "player_id", "person_id", "personid", "nba_player_id", "id"), **row})

        team_resolver = TeamResolver(session, counts)
        if team_histories_frame is not None:
            for row in team_histories_frame.to_dict(orient="records"):
                fallback_name = _compose_team_full_name(
                    _row_value(row, "team_city", "city"),
                    _row_value(row, "team_name", "nickname", "team_nickname"),
                )
                team_resolver.resolve(row, fallback_name=fallback_name, allow_create=True)
        player_resolver = PlayerResolver(session, counts)

        team_rows_by_game: dict[str, list[dict]] = defaultdict(list)
        for row in team_stats_frame.to_dict(orient="records"):
            game_id = _clean_str(_row_value(row, "game_id", "gameid", "box_score_game_id"))
            if game_id:
                team_rows_by_game[game_id].append(row)

        player_rows_by_game: dict[str, list[dict]] = defaultdict(list)
        for row in player_stats_frame.to_dict(orient="records"):
            game_id = _clean_str(_row_value(row, "game_id", "gameid", "box_score_game_id"))
            if game_id:
                player_rows_by_game[game_id].append(row)

        processed_games = 0
        for game_row in games_frame.to_dict(orient="records"):
            start_year = _season_start_year_from_row(game_row)
            if season_start is not None and (start_year is None or start_year < season_start):
                continue
            if season_end is not None and (start_year is None or start_year > season_end):
                continue

            home_name = _compose_team_full_name(
                _row_value(game_row, "home_team_city", "hometeamcity"),
                _row_value(game_row, "home_team_name", "home_team", "hometeamname"),
            )
            road_name = _compose_team_full_name(
                _row_value(game_row, "away_team_city", "road_team_city", "awayteamcity"),
                _row_value(game_row, "away_team_name", "road_team_name", "away_team", "visitor_team", "awayteamname"),
            )

            home_team = team_resolver.resolve(
                {
                    "team_id": _row_value(game_row, "home_team_id", "hometeamid"),
                    "team_name": home_name,
                    "team_abbr": _row_value(game_row, "home_team_abbr", "home_team_tricode", "home_team_abbreviation"),
                },
                fallback_name=home_name,
                fallback_abbr=_clean_str(_row_value(game_row, "home_team_abbr", "home_team_tricode", "home_team_abbreviation")),
            )
            road_team = team_resolver.resolve(
                {
                    "team_id": _row_value(game_row, "away_team_id", "road_team_id", "visitorteamid"),
                    "team_name": road_name,
                    "team_abbr": _row_value(game_row, "away_team_abbr", "road_team_abbr", "away_team_tricode", "away_team_abbreviation"),
                },
                fallback_name=road_name,
                fallback_abbr=_clean_str(_row_value(game_row, "away_team_abbr", "road_team_abbr", "away_team_tricode", "away_team_abbreviation")),
            )

            source_game_id = _build_stable_game_id(game_row, home_team_id=str(home_team.team_id), road_team_id=str(road_team.team_id))
            team_rows = team_rows_by_game.get(source_game_id, [])
            if len(team_rows) < 2:
                counts.skipped_games_missing_team_stats += 1
                logger.warning("skip kaggle game %s: expected 2 team rows, found %s", source_game_id, len(team_rows))
                continue

            home_score = _to_int_or_none(_row_value(game_row, "home_team_score", "home_score", "home_points", "homescore"))
            road_score = _to_int_or_none(_row_value(game_row, "away_team_score", "road_team_score", "away_score", "visitor_score", "road_score", "awayscore"))

            game_id = _match_existing_game_id(
                session,
                source_game_id=source_game_id,
                game_row=game_row,
                home_team_id=str(home_team.team_id),
                road_team_id=str(road_team.team_id),
                home_score=home_score,
                road_score=road_score,
            )

            with session.no_autoflush:
                game_record = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            is_new_game = game_record is None
            if game_record is None:
                game_record = Game(game_id=game_id)
                session.add(game_record)

            game_record.data_source = KAGGLE_BOX_SCORE_SOURCE
            game_record.season = _season_token(game_row)
            game_record.game_date = _parse_date(_row_value(game_row, "game_date", "gamedate", "game_date_time_est", "gamedatetimeest", "date"))
            game_record.home_team_id = str(home_team.team_id)
            game_record.road_team_id = str(road_team.team_id)
            game_record.home_team_score = home_score
            game_record.road_team_score = road_score
            if home_score is not None and road_score is not None:
                game_record.wining_team_id = str(home_team.team_id) if home_score > road_score else str(road_team.team_id)

            if is_new_game:
                counts.games_created += 1
            else:
                counts.games_updated += 1

            session.flush()

            team_rows_by_team_id: dict[str, dict] = {}
            for row in team_rows:
                resolved_team = team_resolver.resolve(row)
                team_rows_by_team_id[str(resolved_team.team_id)] = row

            for team_id, on_road, win in (
                (str(home_team.team_id), False, bool(home_score is not None and road_score is not None and home_score > road_score)),
                (str(road_team.team_id), True, bool(home_score is not None and road_score is not None and road_score > home_score)),
            ):
                source_row = team_rows_by_team_id.get(team_id)
                if source_row is None:
                    continue
                with session.no_autoflush:
                    team_stat = session.query(TeamGameStats).filter_by(game_id=game_id, team_id=team_id).first()
                is_new_stat = team_stat is None
                if team_stat is None:
                    team_stat = TeamGameStats(game_id=game_id, team_id=team_id)
                    session.add(team_stat)
                team_stat.data_source = KAGGLE_BOX_SCORE_SOURCE
                team_stat.on_road = on_road
                team_stat.win = win
                for field, value in _extract_team_stat_payload(source_row).items():
                    setattr(team_stat, field, value)
                if is_new_stat:
                    counts.team_stats_created += 1
                else:
                    counts.team_stats_updated += 1
                total_pts = team_stat.pts
                _upsert_game_line_score(
                    session,
                    counts,
                    game_id=game_id,
                    team_id=team_id,
                    total_pts=total_pts,
                    on_road=on_road,
                    row=source_row,
                )

            for source_row in player_rows_by_game.get(source_game_id, []):
                player_id = _clean_str(_row_value(source_row, "player_id", "person_id", "personid", "nba_player_id", "id"))
                players_row = players_by_id.get(player_id or "") if player_id else None
                if players_row is None:
                    full_name = _clean_str(_row_value(source_row, "player_name", "full_name", "name"))
                    players_row = players_by_name.get(full_name or "")
                player = player_resolver.resolve(source_row, players_row=players_row)
                team = _resolve_player_game_team(
                    team_resolver,
                    source_row,
                    home_team=home_team,
                    road_team=road_team,
                )
                if str(team.team_id).startswith("kaggle-team:"):
                    session.flush()

                with session.no_autoflush:
                    player_stat = session.query(PlayerGameStats).filter_by(
                        game_id=game_id,
                        team_id=str(team.team_id),
                        player_id=str(player.player_id),
                    ).first()
                is_new_stat = player_stat is None
                if player_stat is None:
                    player_stat = PlayerGameStats(
                        game_id=game_id,
                        team_id=str(team.team_id),
                        player_id=str(player.player_id),
                    )
                    session.add(player_stat)
                player_stat.data_source = KAGGLE_BOX_SCORE_SOURCE
                for field, value in _extract_player_stat_payload(source_row).items():
                    setattr(player_stat, field, value)
                if is_new_stat:
                    counts.player_stats_created += 1
                else:
                    counts.player_stats_updated += 1

            processed_games += 1
            if limit_games is not None and processed_games >= limit_games:
                break

        return counts
    finally:
        if extracted_dir is not None:
            shutil.rmtree(extracted_dir, ignore_errors=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical Game/PlayerGameStats/TeamGameStats rows from the Kaggle historical NBA dataset.",
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to the extracted Kaggle dataset directory or the downloaded zip file.",
    )
    parser.add_argument("--season-start", type=int, help="Only import seasons starting from this year (e.g. 1947).")
    parser.add_argument("--season-end", type=int, help="Only import seasons up to this start year.")
    parser.add_argument("--limit-games", type=int, help="Stop after importing this many games.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and upsert in a transaction, then roll it back.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "starting kaggle historical backfill dataset=%s version=%s source=%s",
        KAGGLE_DATASET_SLUG,
        KAGGLE_DATASET_VERSION,
        args.source,
    )
    with SessionLocal() as session:
        counts = backfill_kaggle_historical(
            session,
            args.source,
            season_start=args.season_start,
            season_end=args.season_end,
            limit_games=args.limit_games,
        )
        if args.dry_run:
            session.rollback()
            logger.info("dry run complete counts=%s", counts.as_dict())
        else:
            session.commit()
            logger.info("backfill complete counts=%s", counts.as_dict())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

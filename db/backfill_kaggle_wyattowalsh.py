from __future__ import annotations

import argparse
import json
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session, sessionmaker

from db.backfill_kaggle_historical import KAGGLE_BOX_SCORE_SOURCE, _TEAM_NAME_ALIASES
from db.models import Game, GameLineScore, Team, TeamGameStats, engine

logger = logging.getLogger(__name__)

KAGGLE_DATASET_SLUG = "wyattowalsh/basketball"
KAGGLE_DATASET_URL = "https://www.kaggle.com/datasets/wyattowalsh/basketball"

SessionLocal = sessionmaker(bind=engine)

_GAME_FILE = "game.csv"
_LINE_SCORE_FILE = "line_score.csv"
_TEAM_FILE = "team.csv"
_TEAM_HISTORY_FILE = "team_history.csv"


@dataclass
class ImportCounts:
    games_created: int = 0
    games_updated: int = 0
    team_stats_created: int = 0
    team_stats_updated: int = 0
    line_scores_created: int = 0
    line_scores_updated: int = 0
    teams_created: int = 0
    skipped_existing_nba_api: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "games_created": self.games_created,
            "games_updated": self.games_updated,
            "team_stats_created": self.team_stats_created,
            "team_stats_updated": self.team_stats_updated,
            "line_scores_created": self.line_scores_created,
            "line_scores_updated": self.line_scores_updated,
            "teams_created": self.teams_created,
            "skipped_existing_nba_api": self.skipped_existing_nba_api,
        }


def _normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


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


def _to_int_or_none(value) -> int | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _to_float_or_none(value) -> float | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_date(value) -> date | None:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        return pd.to_datetime(text).date()
    except Exception:
        return None


def _normalize_game_id(value) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d+(\.0+)?", text):
        return f"{int(float(text)):010d}"
    return text


def _canonical_team_name(name: str | None) -> str | None:
    normalized = _clean_str(name)
    if normalized is None:
        return None
    return _TEAM_NAME_ALIASES.get(normalized.casefold(), normalized)


def _season_start_year(value) -> int | None:
    text = _clean_str(value)
    if text and re.fullmatch(r"\d{5}", text):
        return int(text[1:])
    return _to_int_or_none(value)


def _find_zip_member(zipped: zipfile.ZipFile, filename: str) -> str | None:
    lower = filename.casefold()
    for member in zipped.namelist():
        if member.casefold().endswith(f"/{lower}") or Path(member).name.casefold() == lower:
            return member
    return None


def _load_optional_csv(source: str | Path, filename: str) -> pd.DataFrame | None:
    path = Path(source).expanduser().resolve()
    if path.is_dir():
        for candidate in path.rglob("*"):
            if candidate.is_file() and candidate.name.casefold() == filename.casefold():
                return _normalize_frame(pd.read_csv(candidate, low_memory=False))
        return None

    if path.is_file() and path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zipped:
            member = _find_zip_member(zipped, filename)
            if member is None:
                return None
            with zipped.open(member) as handle:
                return _normalize_frame(pd.read_csv(handle, low_memory=False))

    raise FileNotFoundError(f"Expected dataset directory or zip file, got {path}")


def _load_required_csv(source: str | Path, filename: str) -> pd.DataFrame:
    frame = _load_optional_csv(source, filename)
    if frame is None:
        raise FileNotFoundError(f"Required dataset file not found: {filename}")
    return frame


class TeamResolver:
    def __init__(self, session: Session, counts: ImportCounts):
        self.session = session
        self.counts = counts
        self.by_id: dict[str, Team] = {}
        self.by_name: dict[str, Team] = {}
        self.by_abbr: dict[str, Team] = {}
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
        if team.full_name:
            self.by_name[_canonical_team_name(team.full_name).casefold()] = team
        if team.abbr:
            self.by_abbr[str(team.abbr).casefold()] = team

    def resolve(
        self,
        *,
        team_id,
        team_name=None,
        team_abbr=None,
        team_city=None,
        team_nickname=None,
        founded_year=None,
    ) -> Team:
        normalized_id = _normalize_game_id(team_id) or _clean_str(team_id)
        canonical_name = _canonical_team_name(team_name)
        abbr = _clean_str(team_abbr)

        if normalized_id and normalized_id in self.by_id:
            return self.by_id[normalized_id]

        if canonical_name and canonical_name.casefold() in self.by_name:
            return self.by_name[canonical_name.casefold()]

        if abbr and abbr.casefold() in self.by_abbr:
            return self.by_abbr[abbr.casefold()]

        team = Team(
            id=self.next_numeric_id,
            team_id=normalized_id or f"kaggle-team:{re.sub(r'[^a-z0-9]+', '-', (canonical_name or abbr or 'unknown').lower()).strip('-')}",
            canonical_team_id=normalized_id or None,
            full_name=canonical_name or _clean_str(team_name),
            abbr=abbr,
            nick_name=_clean_str(team_nickname) or ((canonical_name or "").split()[-1] if canonical_name else None),
            city=_clean_str(team_city),
            year_founded=_to_int_or_none(founded_year),
            active=False,
            is_legacy=True,
            start_season=str(_to_int_or_none(founded_year)) if _to_int_or_none(founded_year) is not None else None,
        )
        self.next_numeric_id += 1
        if not team.canonical_team_id and team.team_id:
            team.canonical_team_id = team.team_id
        self.session.add(team)
        self.counts.teams_created += 1
        self._index(team)
        return team


def _extract_team_stat_payload(row: dict, suffix: str) -> dict[str, int | float | None]:
    min_value = _to_int_or_none(row.get("min"))
    if min_value == 0:
        min_value = None
    return {
        "min": min_value,
        "pts": _to_int_or_none(row.get(f"pts_{suffix}")),
        "fgm": _to_int_or_none(row.get(f"fgm_{suffix}")),
        "fga": _to_int_or_none(row.get(f"fga_{suffix}")),
        "fg_pct": _to_float_or_none(row.get(f"fg_pct_{suffix}")),
        "fg3m": _to_int_or_none(row.get(f"fg3m_{suffix}")),
        "fg3a": _to_int_or_none(row.get(f"fg3a_{suffix}")),
        "fg3_pct": _to_float_or_none(row.get(f"fg3_pct_{suffix}")),
        "ftm": _to_int_or_none(row.get(f"ftm_{suffix}")),
        "fta": _to_int_or_none(row.get(f"fta_{suffix}")),
        "ft_pct": _to_float_or_none(row.get(f"ft_pct_{suffix}")),
        "oreb": _to_int_or_none(row.get(f"oreb_{suffix}")),
        "dreb": _to_int_or_none(row.get(f"dreb_{suffix}")),
        "reb": _to_int_or_none(row.get(f"reb_{suffix}")),
        "ast": _to_int_or_none(row.get(f"ast_{suffix}")),
        "stl": _to_int_or_none(row.get(f"stl_{suffix}")),
        "blk": _to_int_or_none(row.get(f"blk_{suffix}")),
        "tov": _to_int_or_none(row.get(f"tov_{suffix}")),
        "pf": _to_int_or_none(row.get(f"pf_{suffix}")),
    }


def _extract_periods(row: dict, suffix: str) -> tuple[list[int | None], list[int]]:
    regulation = [_to_int_or_none(row.get(f"pts_qtr{period}_{suffix}")) for period in range(1, 5)]
    overtime: list[int] = []
    for overtime_period in range(1, 11):
        points = _to_int_or_none(row.get(f"pts_ot{overtime_period}_{suffix}"))
        if points is not None:
            overtime.append(points)
    return regulation, overtime


def _upsert_game_line_score(
    session: Session,
    counts: ImportCounts,
    *,
    game_id: str,
    team_id: str,
    total_pts: int | None,
    on_road: bool,
    row: dict | None,
    suffix: str,
) -> None:
    if row is None:
        return

    regulation, overtime = _extract_periods(row, suffix)
    if total_pts is None and all(value is None for value in regulation) and not overtime:
        return

    record = session.query(GameLineScore).filter_by(game_id=game_id, team_id=team_id).first()
    is_new = record is None
    now = datetime.now(UTC).replace(tzinfo=None)
    if record is None:
        record = GameLineScore(game_id=game_id, team_id=team_id, fetched_at=now, updated_at=now)
        session.add(record)

    q1, q2, q3, q4 = regulation
    record.on_road = on_road
    record.q1_pts = q1
    record.q2_pts = q2
    record.q3_pts = q3
    record.q4_pts = q4
    record.ot1_pts = overtime[0] if len(overtime) > 0 else None
    record.ot2_pts = overtime[1] if len(overtime) > 1 else None
    record.ot3_pts = overtime[2] if len(overtime) > 2 else None
    record.ot_extra_json = json.dumps(overtime[3:]) if len(overtime) > 3 else None
    record.first_half_pts = (q1 or 0) + (q2 or 0) if q1 is not None or q2 is not None else None
    record.second_half_pts = (q3 or 0) + (q4 or 0) if q3 is not None or q4 is not None else None
    record.regulation_total_pts = sum(value for value in regulation if value is not None) if any(value is not None for value in regulation) else None
    record.total_pts = total_pts or 0
    record.source = KAGGLE_BOX_SCORE_SOURCE
    record.updated_at = now
    if record.fetched_at is None:
        record.fetched_at = now

    if is_new:
        counts.line_scores_created += 1
    else:
        counts.line_scores_updated += 1


def backfill_kaggle_wyattowalsh(
    session: Session,
    source: str | Path,
    *,
    season_start: int | None = None,
    season_end: int | None = None,
    limit_games: int | None = None,
) -> ImportCounts:
    games_frame = _load_required_csv(source, _GAME_FILE)
    line_score_frame = _load_optional_csv(source, _LINE_SCORE_FILE)
    team_frame = _load_optional_csv(source, _TEAM_FILE)
    team_history_frame = _load_optional_csv(source, _TEAM_HISTORY_FILE)

    counts = ImportCounts()
    team_resolver = TeamResolver(session, counts)

    team_reference_by_id: dict[str, dict] = {}
    if team_frame is not None:
        for row in team_frame.to_dict(orient="records"):
            team_reference_by_id[_normalize_game_id(row.get("id")) or str(row.get("id"))] = row

    history_by_id: dict[str, list[dict]] = {}
    if team_history_frame is not None:
        for row in team_history_frame.to_dict(orient="records"):
            normalized_id = _normalize_game_id(row.get("team_id")) or str(row.get("team_id"))
            history_by_id.setdefault(normalized_id, []).append(row)

    line_rows_by_game: dict[str, dict] = {}
    if line_score_frame is not None:
        for row in line_score_frame.to_dict(orient="records"):
            game_id = _normalize_game_id(row.get("game_id"))
            if game_id:
                line_rows_by_game[game_id] = row

    processed_games = 0
    seen_game_ids: set[str] = set()
    for game_row in games_frame.to_dict(orient="records"):
        start_year = _season_start_year(game_row.get("season_id"))
        if season_start is not None and (start_year is None or start_year < season_start):
            continue
        if season_end is not None and (start_year is None or start_year > season_end):
            continue

        game_id = _normalize_game_id(game_row.get("game_id"))
        if game_id is None:
            continue
        if game_id in seen_game_ids:
            continue
        seen_game_ids.add(game_id)

        existing_game = session.query(Game).filter_by(game_id=game_id).one_or_none()
        if existing_game is not None and existing_game.data_source == "nba_api_box_scores":
            counts.skipped_existing_nba_api += 1
            continue

        home_team_id = _normalize_game_id(game_row.get("team_id_home")) or _clean_str(game_row.get("team_id_home"))
        away_team_id = _normalize_game_id(game_row.get("team_id_away")) or _clean_str(game_row.get("team_id_away"))

        home_reference = team_reference_by_id.get(home_team_id or "", {})
        away_reference = team_reference_by_id.get(away_team_id or "", {})
        home_history = (history_by_id.get(home_team_id or "", []) or [None])[0] or {}
        away_history = (history_by_id.get(away_team_id or "", []) or [None])[0] or {}

        home_team = team_resolver.resolve(
            team_id=home_team_id,
            team_name=game_row.get("team_name_home"),
            team_abbr=game_row.get("team_abbreviation_home"),
            team_city=home_reference.get("city") or home_history.get("city"),
            team_nickname=home_reference.get("nickname") or home_history.get("nickname"),
            founded_year=home_reference.get("year_founded") or home_history.get("year_founded"),
        )
        away_team = team_resolver.resolve(
            team_id=away_team_id,
            team_name=game_row.get("team_name_away"),
            team_abbr=game_row.get("team_abbreviation_away"),
            team_city=away_reference.get("city") or away_history.get("city"),
            team_nickname=away_reference.get("nickname") or away_history.get("nickname"),
            founded_year=away_reference.get("year_founded") or away_history.get("year_founded"),
        )
        session.flush()

        game_record = existing_game
        is_new_game = game_record is None
        if game_record is None:
            game_record = Game(game_id=game_id)
            session.add(game_record)

        home_score = _to_int_or_none(game_row.get("pts_home"))
        away_score = _to_int_or_none(game_row.get("pts_away"))

        game_record.data_source = KAGGLE_BOX_SCORE_SOURCE
        game_record.season = str(int(float(game_row["season_id"]))) if not _is_missing(game_row.get("season_id")) else None
        game_record.game_date = _parse_date(game_row.get("game_date"))
        game_record.home_team_id = str(home_team.team_id)
        game_record.road_team_id = str(away_team.team_id)
        game_record.home_team_score = home_score
        game_record.road_team_score = away_score
        if home_score is not None and away_score is not None:
            game_record.wining_team_id = str(home_team.team_id) if home_score > away_score else str(away_team.team_id)

        if is_new_game:
            counts.games_created += 1
        else:
            counts.games_updated += 1

        for suffix, team, on_road, own_score, other_score in (
            ("home", home_team, False, home_score, away_score),
            ("away", away_team, True, away_score, home_score),
        ):
            team_stat = session.query(TeamGameStats).filter_by(game_id=game_id, team_id=str(team.team_id)).first()
            is_new_stat = team_stat is None
            if team_stat is None:
                team_stat = TeamGameStats(game_id=game_id, team_id=str(team.team_id))
                session.add(team_stat)

            team_stat.data_source = KAGGLE_BOX_SCORE_SOURCE
            team_stat.on_road = on_road
            team_stat.win = bool(own_score is not None and other_score is not None and own_score > other_score)
            for field, value in _extract_team_stat_payload(game_row, suffix).items():
                setattr(team_stat, field, value)

            if is_new_stat:
                counts.team_stats_created += 1
            else:
                counts.team_stats_updated += 1

            _upsert_game_line_score(
                session,
                counts,
                game_id=game_id,
                team_id=str(team.team_id),
                total_pts=own_score,
                on_road=on_road,
                row=line_rows_by_game.get(game_id),
                suffix=suffix,
            )

        processed_games += 1
        if limit_games is not None and processed_games >= limit_games:
            break

    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical Game, TeamGameStats, and line scores from the wyattowalsh/basketball Kaggle dataset.",
    )
    parser.add_argument("--source", required=True, help="Path to the downloaded zip file or extracted dataset directory.")
    parser.add_argument("--season-start", type=int, help="Only import seasons starting from this year.")
    parser.add_argument("--season-end", type=int, help="Only import seasons up to this start year.")
    parser.add_argument("--limit-games", type=int, help="Stop after importing this many games.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and upsert in a transaction, then roll it back.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logger.info("starting kaggle backfill dataset=%s source=%s", KAGGLE_DATASET_SLUG, args.source)
    with SessionLocal() as session:
        counts = backfill_kaggle_wyattowalsh(
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

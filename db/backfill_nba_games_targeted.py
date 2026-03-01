"""
Targeted NBA game backfill CLI.

Use this script when you want to backfill games for:
- a specific day (`--day`)
- a date range (`--date-from` / `--date-to`)
- a season (`--season`)
- a team (`--team-id` or `--team-abbr`)
- a player (`--player-id` or `--player-name`)
- or any combination of the above

Examples:
  # 1) Backfill Warriors games in 2025-26 (regular + playoffs)
  python -m db.backfill_nba_games_targeted --team-abbr GSW --season 2025-26

  # 2) Backfill games for one day
  python -m db.backfill_nba_games_targeted --day 2026-02-10

  # 3) Backfill games for a player in a season
  python -m db.backfill_nba_games_targeted --player-name "Stephen Curry" --season 2025-26

  # 4) Backfill a date range for one team and include PlayIn too
  python -m db.backfill_nba_games_targeted \
      --team-id 1610612744 \
      --date-from 2025-10-01 \
      --date-to 2026-03-01 \
      --season-type "Regular Season" \
      --season-type Playoffs \
      --season-type PlayIn

Notes:
- By default this script processes games that are not fully backfilled.
  "Fully backfilled" means `Game` + game detail + play-by-play are present.
- Add `--include-existing` to reprocess existing games too.
"""

from __future__ import annotations

import argparse
import logging
from typing import Iterable

from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.static import players as players_static
from nba_api.stats.static import teams as teams_static
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_games import process_and_store_game
from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.models import Game, engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _normalize_season_type(value: str) -> str:
    v = value.strip().lower()
    mapping = {
        "regular": "Regular Season",
        "regular season": "Regular Season",
        "playoffs": "Playoffs",
        "playoff": "Playoffs",
        "pre": "Pre Season",
        "preseason": "Pre Season",
        "pre season": "Pre Season",
        "all-star": "All Star",
        "all star": "All Star",
        "playin": "PlayIn",
        "play-in": "PlayIn",
        "play in": "PlayIn",
    }
    return mapping.get(v, value)


def _resolve_team_id(team_id: str | None, team_abbr: str | None) -> str | None:
    if team_id:
        return str(team_id)
    if not team_abbr:
        return None
    team = teams_static.find_team_by_abbreviation(team_abbr.upper())
    if not team:
        raise ValueError(f"Unknown team abbreviation: {team_abbr}")
    return str(team["id"])


def _resolve_player_id(player_id: str | None, player_name: str | None) -> str | None:
    if player_id:
        return str(player_id)
    if not player_name:
        return None
    matches = players_static.find_players_by_full_name(player_name)
    if not matches:
        raise ValueError(f"No player found for name: {player_name}")
    active = [p for p in matches if p.get("is_active")]
    chosen = active[0] if active else matches[0]
    logger.info("Resolved player '%s' to %s", player_name, chosen["full_name"])
    return str(chosen["id"])


def _fetch_games_for_filter(
    *,
    season_type: str,
    season: str | None,
    date_from: str | None,
    date_to: str | None,
    team_id: str | None,
    player_id: str | None,
    game_id: str | None,
):
    finder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season or "",
        season_type_nullable=season_type,
        date_from_nullable=date_from or "",
        date_to_nullable=date_to or "",
        team_id_nullable=team_id or "",
        player_id_nullable=player_id or "",
        game_id_nullable=game_id or "",
        league_id_nullable="00",
    )
    df = finder.get_data_frames()[0]

    # Keep only played games.
    if "WL" in df.columns:
        df = df[df["WL"].notna()]

    if df.empty:
        return df

    return df.sort_values("GAME_DATE").drop_duplicates(subset=["GAME_ID"])


def _iter_unique_games(data_frames: Iterable):
    seen = set()
    for df in data_frames:
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            gid = str(row["GAME_ID"])
            if gid in seen:
                continue
            seen.add(gid)
            yield row


def parse_args():
    parser = argparse.ArgumentParser(
        description="Targeted NBA game backfill (day/season/team/player).",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument("--day", help="Single date in YYYY-MM-DD (sets both date-from and date-to).")
    parser.add_argument("--date-from", help="Start date in YYYY-MM-DD.")
    parser.add_argument("--date-to", help="End date in YYYY-MM-DD.")
    parser.add_argument("--season", help="Season string like 2025-26.")
    parser.add_argument("--game-id", help="Single GAME_ID filter.")

    parser.add_argument("--team-id", help="NBA team id, e.g. 1610612744 for GSW.")
    parser.add_argument("--team-abbr", help="Team abbreviation, e.g. GSW.")

    parser.add_argument("--player-id", help="NBA player id.")
    parser.add_argument("--player-name", help='Player full name, e.g. "Stephen Curry".')

    parser.add_argument(
        "--season-type",
        action="append",
        default=[],
        help=(
            "Repeatable. Defaults to: Regular Season + Playoffs.\n"
            "Allowed by nba_api include: Pre Season, Regular Season, PlayIn, Playoffs, All Star."
        ),
    )
    parser.add_argument(
        "--include-existing",
        action="store_true",
        dest="include_existing",
        help="Reprocess all matched games (default: only games not fully backfilled).",
    )
    parser.set_defaults(include_existing=False)

    args = parser.parse_args()

    if args.day:
        args.date_from = args.day
        args.date_to = args.day

    if not any(
        [
            args.day,
            args.date_from,
            args.date_to,
            args.season,
            args.game_id,
            args.team_id,
            args.team_abbr,
            args.player_id,
            args.player_name,
        ]
    ):
        parser.error(
            "At least one filter is required: --day/--date-from/--date-to/--season/--game-id/"
            "--team-id/--team-abbr/--player-id/--player-name"
        )

    return args


def main():
    args = parse_args()

    season_types = args.season_type or ["Regular Season", "Playoffs"]
    season_types = [_normalize_season_type(s) for s in season_types]

    team_id = _resolve_team_id(args.team_id, args.team_abbr)
    player_id = _resolve_player_id(args.player_id, args.player_name)

    logger.info(
        "filters season=%s day=%s date_from=%s date_to=%s game_id=%s team_id=%s player_id=%s season_types=%s include_existing=%s",
        args.season,
        args.day,
        args.date_from,
        args.date_to,
        args.game_id,
        team_id,
        player_id,
        season_types,
        args.include_existing,
    )

    data_frames = []
    for season_type in season_types:
        try:
            df = _fetch_games_for_filter(
                season_type=season_type,
                season=args.season,
                date_from=args.date_from,
                date_to=args.date_to,
                team_id=team_id,
                player_id=player_id,
                game_id=args.game_id,
            )
            logger.info("season_type=%s fetched_games=%s", season_type, len(df))
            data_frames.append(df)
        except Exception as exc:
            logger.info("fetch failed for season_type=%s: %s", season_type, exc)

    rows = list(_iter_unique_games(data_frames))
    if not rows:
        logger.info("No games found for given filters.")
        return

    Session = sessionmaker(bind=engine)
    success = 0
    failed = 0
    skipped_fully_backfilled = 0

    def _backfill_status(sess, game_id: str) -> tuple[bool, bool, bool]:
        """
        Return backfill status as:
          (exists_game_row, has_detail, has_pbp)
        """
        exists_game = sess.query(Game.game_id).filter(Game.game_id == game_id).first() is not None
        if not exists_game:
            return False, False, False
        try:
            has_detail = is_game_detail_back_filled(game_id, sess)
            has_pbp = is_game_pbp_back_filled(game_id, sess)
            return True, has_detail, has_pbp
        except Exception:
            # If checks fail (e.g., transient DB issue), do not skip by default.
            return exists_game, False, False

    with Session() as sess:
        total_rows = len(rows)
        for idx, row in enumerate(rows, start=1):
            gid = str(row["GAME_ID"])
            exists_game, has_detail, has_pbp = _backfill_status(sess, gid)
            missing_parts = []
            if not exists_game:
                missing_parts.append("Game")
            if not has_detail:
                missing_parts.append("detail")
            if not has_pbp:
                missing_parts.append("PBP")

            logger.info(
                "[%s/%s] game_id=%s date=%s season_id=%s matchup=%s status(game=%s,detail=%s,pbp=%s) missing=%s",
                idx,
                total_rows,
                gid,
                row.get("GAME_DATE", ""),
                row.get("SEASON_ID", ""),
                row.get("MATCHUP", ""),
                exists_game,
                has_detail,
                has_pbp,
                ",".join(missing_parts) if missing_parts else "-",
            )

            if not args.include_existing and exists_game and has_detail and has_pbp:
                skipped_fully_backfilled += 1
                logger.info("[%s/%s] skip game_id=%s reason=fully_backfilled", idx, total_rows, gid)
                continue

            try:
                logger.info("[%s/%s] backfill game_id=%s start", idx, total_rows, gid)
                process_and_store_game(sess, row)
                rec = sess.query(Game.game_id).filter(Game.game_id == gid).first()
                if rec is not None:
                    success += 1
                    exists_game_after, has_detail_after, has_pbp_after = _backfill_status(sess, gid)
                    logger.info(
                        "[%s/%s] backfill game_id=%s done status_after(game=%s,detail=%s,pbp=%s)",
                        idx,
                        total_rows,
                        gid,
                        exists_game_after,
                        has_detail_after,
                        has_pbp_after,
                    )
                else:
                    failed += 1
                    logger.info("[%s/%s] backfill game_id=%s failed reason=game_row_missing_after_run", idx, total_rows, gid)
            except Exception as exc:
                failed += 1
                sess.rollback()
                logger.info("[%s/%s] backfill game_id=%s failed err=%s", idx, total_rows, gid, exc)

    logger.info(
        "done total_candidates=%s processed_ok=%s failed=%s skipped_fully_backfilled=%s",
        len(rows),
        success,
        failed,
        skipped_fully_backfilled,
    )


if __name__ == "__main__":
    main()

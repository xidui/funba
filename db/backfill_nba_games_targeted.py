"""
DEPRECATED: Use `python -m tasks.dispatch discover` instead.

  python -m tasks.dispatch discover --date-from 2026-03-02 --date-to 2026-03-07

That command discovers games via the NBA API and enqueues them through the
event-driven Celery pipeline (ingest → metrics), which replaces what this
script did manually and also handles shot records and metric computation.

---
Targeted NBA game backfill CLI (legacy, direct/synchronous).

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
  "Fully backfilled" means `Game` + game detail + play-by-play + shot detail are present.
- Add `--include-existing` to reprocess existing games too.
- Add `--without-shot-detail` to skip shot-detail backfill.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
from datetime import datetime
from typing import Iterable

from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.static import players as players_static
from nba_api.stats.static import teams as teams_static
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_games import process_and_store_game
from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.backfill_nba_player_shot_detail import (
    back_fill_game_shot_record,
    get_un_back_filled_game_and_player,
    is_game_shot_back_filled,
)
from db.models import Game, PlayerGameStats, ShotRecord, engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DEFAULT_WORKERS = 3


def _build_shot_mismatch_note(sess, game_id: str) -> str:
    expected = int(
        sess.query(func.coalesce(func.sum(PlayerGameStats.fga), 0))
        .filter(PlayerGameStats.game_id == game_id)
        .scalar()
        or 0
    )
    actual = int(
        sess.query(func.count(ShotRecord.id))
        .filter(ShotRecord.game_id == game_id)
        .scalar()
        or 0
    )
    missing = max(expected - actual, 0)

    missing_pairs = list(get_un_back_filled_game_and_player(sess, game_id, None))
    pair_labels = [f"{str(player_id)}@{str(team_id)}" for _, player_id, team_id in missing_pairs[:8]]
    if len(missing_pairs) > 8:
        pair_labels.append(f"...+{len(missing_pairs) - 8}")
    pairs_text = "|".join(pair_labels) if pair_labels else "-"

    return (
        "SHOT_FGA_GAP "
        f"expected={expected} actual={actual} missing={missing} "
        f"missing_pairs={pairs_text}"
    )


def _set_game_mismatch(sess, game_id: str, note: str) -> bool:
    game = sess.query(Game).filter(Game.game_id == game_id).first()
    if not game:
        return False

    changed = (
        game.backfill_mismatch is not True
        or (game.backfill_mismatch_note or "") != note
    )
    if not changed:
        return False

    game.backfill_mismatch = True
    game.backfill_mismatch_note = note
    game.backfill_mismatch_updated_at = datetime.now()
    return True


def _clear_game_mismatch(sess, game_id: str) -> bool:
    game = sess.query(Game).filter(Game.game_id == game_id).first()
    if not game:
        return False

    changed = bool(game.backfill_mismatch or game.backfill_mismatch_note)
    if not changed:
        return False

    game.backfill_mismatch = False
    game.backfill_mismatch_note = None
    game.backfill_mismatch_updated_at = datetime.now()
    return True


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
    parser.add_argument(
        "--without-shot-detail",
        action="store_true",
        help="Skip shot-detail backfill (faster but not fully complete).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of worker threads (default: {DEFAULT_WORKERS}). Lower this when timeouts are frequent.",
    )

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
    with_shot_detail = not args.without_shot_detail
    workers = max(1, int(args.workers or DEFAULT_WORKERS))

    season_types = args.season_type or ["Regular Season", "Playoffs"]
    season_types = [_normalize_season_type(s) for s in season_types]

    team_id = _resolve_team_id(args.team_id, args.team_abbr)
    player_id = _resolve_player_id(args.player_id, args.player_name)

    logger.info(
        "filters season=%s day=%s date_from=%s date_to=%s game_id=%s team_id=%s player_id=%s season_types=%s include_existing=%s with_shot_detail=%s",
        args.season,
        args.day,
        args.date_from,
        args.date_to,
        args.game_id,
        team_id,
        player_id,
        season_types,
        args.include_existing,
        with_shot_detail,
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
    upstream_mismatch = 0
    failed_game_ids: list[str] = []
    mismatch_game_ids: list[str] = []

    def _backfill_status(sess, game_id: str) -> tuple[bool, bool, bool, bool]:
        """
        Return backfill status as:
          (exists_game_row, has_detail, has_pbp, has_shot)
        """
        exists_game = sess.query(Game.game_id).filter(Game.game_id == game_id).first() is not None
        if not exists_game:
            return False, False, False, False
        try:
            has_detail = is_game_detail_back_filled(game_id, sess)
            has_pbp = is_game_pbp_back_filled(game_id, sess)
            has_shot = is_game_shot_back_filled(sess, game_id) if with_shot_detail else True
            return True, has_detail, has_pbp, has_shot
        except Exception:
            # If checks fail (e.g., transient DB issue), do not skip by default.
            return exists_game, False, False, False

    total_rows = len(rows)
    task_rows = [(idx, row.to_dict()) for idx, row in enumerate(rows, start=1)]

    def _process_one(task_idx: int, task_row: dict):
        gid = str(task_row["GAME_ID"])
        with Session() as sess:
            exists_game, has_detail, has_pbp, has_shot = _backfill_status(sess, gid)
            missing_parts = []
            if not exists_game:
                missing_parts.append("Game")
            if not has_detail:
                missing_parts.append("detail")
            if not has_pbp:
                missing_parts.append("PBP")
            if with_shot_detail and not has_shot:
                missing_parts.append("shot")

            logger.info(
                "[%s/%s] game_id=%s date=%s season_id=%s matchup=%s status(game=%s,detail=%s,pbp=%s,shot=%s) missing=%s",
                task_idx,
                total_rows,
                gid,
                task_row.get("GAME_DATE", ""),
                task_row.get("SEASON_ID", ""),
                task_row.get("MATCHUP", ""),
                exists_game,
                has_detail,
                has_pbp,
                has_shot,
                ",".join(missing_parts) if missing_parts else "-",
            )

            fully_backfilled_before = exists_game and has_detail and has_pbp and (has_shot or not with_shot_detail)
            if not args.include_existing and fully_backfilled_before:
                if _clear_game_mismatch(sess, gid):
                    sess.commit()
                logger.info("[%s/%s] skip game_id=%s reason=fully_backfilled", task_idx, total_rows, gid)
                return "skipped", gid

            try:
                logger.info("[%s/%s] backfill game_id=%s start", task_idx, total_rows, gid)
                process_and_store_game(sess, task_row)
                if with_shot_detail:
                    # Detail/PBP refresh can change expected shot totals.
                    # Re-check shot completeness after processing to avoid leaving shot=False.
                    _exists_now, _detail_now, _pbp_now, has_shot_now = _backfill_status(sess, gid)
                    if args.include_existing or not has_shot_now:
                        back_fill_game_shot_record(sess, gid, False)
                        sess.commit()
                rec = sess.query(Game.game_id).filter(Game.game_id == gid).first()
                if rec is not None:
                    exists_game_after, has_detail_after, has_pbp_after, has_shot_after = _backfill_status(sess, gid)
                    logger.info(
                        "[%s/%s] backfill game_id=%s done status_after(game=%s,detail=%s,pbp=%s,shot=%s)",
                        task_idx,
                        total_rows,
                        gid,
                        exists_game_after,
                        has_detail_after,
                        has_pbp_after,
                        has_shot_after,
                    )
                    fully_backfilled_after = exists_game_after and has_detail_after and has_pbp_after and (
                        has_shot_after or not with_shot_detail
                    )
                    if fully_backfilled_after:
                        if _clear_game_mismatch(sess, gid):
                            sess.commit()
                        return "success", gid

                    missing_after = []
                    if not exists_game_after:
                        missing_after.append("Game")
                    if not has_detail_after:
                        missing_after.append("detail")
                    if not has_pbp_after:
                        missing_after.append("PBP")
                    if with_shot_detail and not has_shot_after:
                        missing_after.append("shot")
                    logger.info(
                        "[%s/%s] backfill game_id=%s failed reason=incomplete_after_run missing_after=%s",
                        task_idx,
                        total_rows,
                        gid,
                        ",".join(missing_after) if missing_after else "-",
                    )
                    if exists_game_after and has_detail_after and has_pbp_after and with_shot_detail and not has_shot_after:
                        mismatch_note = _build_shot_mismatch_note(sess, gid)
                        if _set_game_mismatch(sess, gid, mismatch_note):
                            sess.commit()
                        logger.info(
                            "[%s/%s] backfill game_id=%s upstream_mismatch note=%s",
                            task_idx,
                            total_rows,
                            gid,
                            mismatch_note,
                        )
                        return "upstream_mismatch", gid
                    return "failed", gid

                logger.info(
                    "[%s/%s] backfill game_id=%s failed reason=game_row_missing_after_run",
                    task_idx,
                    total_rows,
                    gid,
                )
                return "failed", gid
            except Exception as exc:
                sess.rollback()
                logger.info("[%s/%s] backfill game_id=%s failed err=%s", task_idx, total_rows, gid, exc)
                return "failed", gid

    logger.info("processing %s candidate games with max_workers=%s", total_rows, workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_process_one, idx, row_dict) for idx, row_dict in task_rows]
        for future in concurrent.futures.as_completed(futures):
            try:
                result, _gid = future.result()
            except Exception as exc:
                failed += 1
                logger.info("worker future failed err=%s", exc)
                continue

            if result == "success":
                success += 1
            elif result == "failed":
                failed += 1
                failed_game_ids.append(_gid)
            elif result == "upstream_mismatch":
                upstream_mismatch += 1
                mismatch_game_ids.append(_gid)
            else:
                skipped_fully_backfilled += 1

    logger.info(
        "done total_candidates=%s processed_ok=%s upstream_mismatch=%s failed=%s skipped_fully_backfilled=%s",
        len(rows),
        success,
        upstream_mismatch,
        failed,
        skipped_fully_backfilled,
    )
    if mismatch_game_ids:
        logger.info("upstream_mismatch_game_ids=%s", ",".join(sorted(set(mismatch_game_ids))))
    if failed_game_ids:
        logger.info("failed_game_ids=%s", ",".join(sorted(set(failed_game_ids))))


if __name__ == "__main__":
    main()

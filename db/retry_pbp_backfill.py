import argparse
import logging
import random
import time

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_game_detail import back_fill_game_detail, is_game_detail_back_filled
from db.backfill_nba_game_pbp import back_fill_pbp, is_game_pbp_back_filled
from db.models import Game, Team, engine


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts(sess, season_id):
    total = sess.execute(
        text("SELECT COUNT(*) FROM Game WHERE season=:season_id"),
        {"season_id": season_id},
    ).scalar()
    detail = sess.execute(
        text(
            """
            SELECT COUNT(*)
            FROM (
              SELECT g.game_id
              FROM Game g
              JOIN TeamGameStats t ON g.game_id=t.game_id
              WHERE g.season=:season_id
              GROUP BY g.game_id
            ) x
            """
        ),
        {"season_id": season_id},
    ).scalar()
    pbp = sess.execute(
        text(
            """
            SELECT COUNT(*)
            FROM (
              SELECT g.game_id
              FROM Game g
              JOIN GamePlayByPlay p ON g.game_id=p.game_id
              WHERE g.season=:season_id
              GROUP BY g.game_id
            ) y
            """
        ),
        {"season_id": season_id},
    ).scalar()
    return int(total or 0), int(detail or 0), int(pbp or 0)


def build_payload(game_rec, abbr_by_id):
    home_abbr = abbr_by_id.get(str(game_rec.home_team_id), str(game_rec.home_team_id))
    road_abbr = abbr_by_id.get(str(game_rec.road_team_id), str(game_rec.road_team_id))
    return {
        "GAME_ID": str(game_rec.game_id),
        "SEASON_ID": str(game_rec.season),
        "GAME_DATE": game_rec.game_date.strftime("%Y-%m-%d"),
        "MATCHUP": f"{road_abbr} @ {home_abbr}",
    }


def run(season_id, batch_size, max_passes, sleep_seconds):
    Session = sessionmaker(bind=engine)
    no_progress_passes = 0

    for pass_idx in range(1, max_passes + 1):
        with Session() as sess:
            total_before, detail_before, pbp_before = get_counts(sess, season_id)
            logger.info(
                "pass=%s before: total=%s detail=%s pbp=%s",
                pass_idx,
                total_before,
                detail_before,
                pbp_before,
            )

            if detail_before >= total_before and pbp_before >= total_before:
                logger.info("all done")
                return

            abbr_by_id = {str(tid): abbr for tid, abbr in sess.query(Team.team_id, Team.abbr).all()}
            games = sess.query(Game).filter(Game.season == season_id).all()
            missing = []
            for game in games:
                gid = str(game.game_id)
                if (not is_game_detail_back_filled(gid, sess)) or (not is_game_pbp_back_filled(gid, sess)):
                    missing.append(game)

            if not missing:
                logger.info("no missing games found")
                return

            random.shuffle(missing)
            batch = missing[:batch_size]

            detail_attempted = detail_ok = detail_failed = 0
            pbp_attempted = pbp_ok = pbp_failed = 0

            for game in batch:
                gid = str(game.game_id)

                try:
                    if not is_game_detail_back_filled(gid, sess):
                        detail_attempted += 1
                        payload = build_payload(game, abbr_by_id)
                        ok = back_fill_game_detail(payload, game, sess, False)
                        if ok:
                            sess.commit()
                            detail_ok += 1
                        else:
                            sess.rollback()
                            detail_failed += 1
                            continue
                except Exception as exc:
                    sess.rollback()
                    detail_failed += 1
                    logger.info("detail_failed game_id=%s err=%s", gid, exc)
                    continue

                try:
                    if not is_game_pbp_back_filled(gid, sess):
                        pbp_attempted += 1
                        back_fill_pbp(gid, sess, False)
                        sess.commit()
                        pbp_ok += 1
                except Exception as exc:
                    sess.rollback()
                    pbp_failed += 1
                    logger.info("pbp_failed game_id=%s err=%s", gid, exc)

            total_after, detail_after, pbp_after = get_counts(sess, season_id)
            logger.info(
                "pass=%s batch=%s attempts: detail %s/%s ok (%s failed), pbp %s/%s ok (%s failed)",
                pass_idx,
                len(batch),
                detail_ok,
                detail_attempted,
                detail_failed,
                pbp_ok,
                pbp_attempted,
                pbp_failed,
            )
            logger.info(
                "pass=%s after: total=%s detail=%s pbp=%s",
                pass_idx,
                total_after,
                detail_after,
                pbp_after,
            )

            progressed = (detail_after > detail_before) or (pbp_after > pbp_before)
            if not progressed:
                no_progress_passes += 1
            else:
                no_progress_passes = 0

            if no_progress_passes >= 3:
                logger.info("stopping after 3 no-progress passes")
                return

        time.sleep(sleep_seconds)


def main():
    parser = argparse.ArgumentParser(description="Retry 1-season NBA game detail + PBP backfill.")
    parser.add_argument("--season-id", default="22025", help="Season id in DB (e.g., 22025)")
    parser.add_argument("--batch-size", type=int, default=40, help="Games per pass")
    parser.add_argument("--max-passes", type=int, default=50, help="Maximum retry passes")
    parser.add_argument("--sleep-seconds", type=int, default=2, help="Seconds between passes")
    args = parser.parse_args()

    run(
        season_id=args.season_id,
        batch_size=args.batch_size,
        max_passes=args.max_passes,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()

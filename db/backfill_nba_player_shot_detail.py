from nba_api.stats.endpoints import shotchartdetail
from sqlalchemy.orm import aliased, sessionmaker
from db.models import Game, PlayerGameStats, ShotRecord, engine
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log, RetryError
from requests.exceptions import ConnectionError, Timeout
from sqlalchemy import func, or_, and_, text
from collections import defaultdict

import logging
import concurrent.futures


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


def _int_or_none(value):
    """Return int(value) when present; preserve upstream nulls as None."""
    if value is None or value == "":
        return None
    return int(value)


@retry(
    wait=wait_exponential(multiplier=1, max=60),  # Wait 1, 2, 4, ..., up to 60 seconds
    stop=stop_after_attempt(10),  # Retry up to 5 times
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),  # Retry on network issues and timeouts
    before_sleep=before_sleep_log(logger, logging.INFO)  # Log before sleep
)
def fetch_shot_chart(team_id, player_id, game_id, season=None, season_type=None):
    if season is None:
        shot_chart = shotchartdetail.ShotChartDetail(
            team_id=team_id,
            player_id=player_id,
            game_id_nullable=game_id,
            context_measure_simple='FGA'  # 'FGA' for field goal attempts
        )
        return shot_chart.get_normalized_dict()
    else:
        shot_chart = shotchartdetail.ShotChartDetail(
            team_id=0,
            player_id=player_id,
            context_measure_simple='FGA',
            season_nullable=season,
            season_type_all_star=season_type,
        )
        return shot_chart.get_normalized_dict()


def get_un_back_filled_game_and_player(sess, game_id=None, player_id=None):
    a_cte = sess.query(
        ShotRecord.game_id,
        ShotRecord.player_id,
        ShotRecord.team_id,
        func.count().label('ac')
    ).filter(
        and_(
            or_(ShotRecord.game_id == game_id, game_id is None),
            or_(ShotRecord.player_id == player_id, player_id is None),
        ),
    ).group_by(
        ShotRecord.game_id,
        ShotRecord.player_id,
        ShotRecord.team_id
    ).cte(name='A')

    b_cte = sess.query(
        PlayerGameStats.game_id,
        PlayerGameStats.player_id,
        PlayerGameStats.team_id,
        func.sum(PlayerGameStats.fga).label('bc')
    ).filter(
        and_(
            or_(PlayerGameStats.game_id == game_id, game_id is None),
            or_(PlayerGameStats.player_id == player_id, player_id is None),
        ),
    ).group_by(
        PlayerGameStats.game_id,
        PlayerGameStats.player_id,
        PlayerGameStats.team_id
    ).cte(name='B')

    # Creating aliases for the CTEs for use in the final query
    a_alias = aliased(a_cte, name='a_alias')
    b_alias = aliased(b_cte, name='b_alias')

    # Construct the final query using ORM join and coalesce
    return sess.query(
        b_alias.c.game_id,
        b_alias.c.player_id,
        b_alias.c.team_id,
    ).outerjoin(
        a_alias, (a_alias.c.game_id == b_alias.c.game_id) &
                 (a_alias.c.player_id == b_alias.c.player_id) &
                 (a_alias.c.team_id == b_alias.c.team_id)
    ).filter(
        func.coalesce(a_alias.c.ac, 0) < func.coalesce(b_alias.c.bc, 0)
    )


def is_game_shot_back_filled(sess, game_id):
    # A real NBA game always has shot attempts. If zero ShotRecords exist,
    # the game has never been backfilled regardless of PlayerGameStats.fga.
    if sess.query(ShotRecord).filter(ShotRecord.game_id == game_id).limit(1).count() == 0:
        return False
    return len(list(get_un_back_filled_game_and_player(sess, game_id, None))) == 0


SEASON_TYPES = ['Regular Season', 'Playoffs']


def get_season_id(season, season_type):
    # convert 2023-24, 'Regular Season' to 22023
    year = season.split('-')[0]
    if season_type == 'Regular Season':
        return '2' + year
    if season_type == 'Playoffs':
        return '4' + year
    return ''


def get_season_and_type_from_season_id(season_id):
    # convert 22023 tp 2023-24, 'Regular Season'
    if season_id[0] == '2':
        season_type = 'Regular Season'
    elif season_id[0] == '4':
        season_type = 'Playoffs'
    else:
        return '', ''  # Return empty if the format is incorrect

    # Extract the year part and format it correctly
    year = season_id[1:]
    next_year = str(int(year) + 1)[-2:]  # Get the last two digits of the next year
    season = f"{year}-{next_year}"

    return season, season_type



def back_fill_game_shot_record(sess, game_id, commit=False):
    if is_game_shot_back_filled(sess, game_id):
        logger.info('skip game {} as it has back filled'.format(game_id))
        return

    # One API call per game for both teams/all players.
    # ShotChartDetail supports team_id=0, player_id=0 with game_id filter.
    shots = fetch_shot_chart(0, 0, game_id).get('Shot_Chart_Detail', [])

    # Group full-shot payload by (player_id, team_id) so we can fill only missing pairs.
    shots_by_pair = defaultdict(list)
    for shot in shots:
        pair = (str(shot['PLAYER_ID']), str(shot['TEAM_ID']))
        shots_by_pair[pair].append(shot)

    missing_pairs = list(get_un_back_filled_game_and_player(sess, game_id))
    with sess.no_autoflush:
        for _, player_id, team_id in missing_pairs:
            player_id = str(player_id)
            team_id = str(team_id)
            pair_shots = shots_by_pair.get((player_id, team_id), [])

            if not pair_shots:
                logger.warning(
                    "No shot-chart rows found for game_id=%s player_id=%s team_id=%s",
                    game_id, player_id, team_id
                )
                continue

            # Replace partial/incomplete rows for this player-game-team to avoid duplicates
            # and guarantee count aligns with upstream shot chart data.
            sess.query(ShotRecord).filter(
                ShotRecord.game_id == game_id,
                ShotRecord.player_id == player_id,
                ShotRecord.team_id == team_id,
            ).delete(synchronize_session=False)

            for shot in pair_shots:
                sess.add(ShotRecord(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=player_id,
                    season='TBD',
                    period=_int_or_none(shot['PERIOD']),
                    min=_int_or_none(shot['MINUTES_REMAINING']),
                    sec=_int_or_none(shot['SECONDS_REMAINING']),
                    event_type=shot['EVENT_TYPE'],
                    action_type=shot['ACTION_TYPE'],
                    shot_type=shot['SHOT_TYPE'],
                    shot_zone_basic=shot['SHOT_ZONE_BASIC'],
                    shot_zone_area=shot['SHOT_ZONE_AREA'],
                    shot_zone_range=shot['SHOT_ZONE_RANGE'],
                    shot_distance=_int_or_none(shot['SHOT_DISTANCE']),
                    loc_x=_int_or_none(shot['LOC_X']),
                    loc_y=_int_or_none(shot['LOC_Y']),
                    shot_attempted=bool(shot['SHOT_ATTEMPTED_FLAG']),
                    shot_made=bool(shot['SHOT_MADE_FLAG']),
                ))

    if commit:
        try:
            sess.commit()
        except Exception as e:
            logger.info(f"Failed to back fill shot record {game_id}, {player_id}: {e}")
            sess.rollback()


def back_fill_game_shot_record_from_api(sess, game_id, commit=False, replace_existing=False):
    """
    Manual one-shot fetch for full game shot chart from nba_api.
    This does not rely on PlayerGameStats presence and can be used from UI actions.
    """
    logger.info("manual backfill shot chart for game_id(%s)", game_id)
    if sess is None:
        sess = Session()

    shots = fetch_shot_chart(0, 0, game_id).get('Shot_Chart_Detail', [])

    if replace_existing:
        sess.query(ShotRecord).filter(ShotRecord.game_id == game_id).delete(synchronize_session=False)

    for shot in shots:
        sess.add(ShotRecord(
            game_id=game_id,
            team_id=str(shot['TEAM_ID']),
            player_id=str(shot['PLAYER_ID']),
            season='TBD',
            period=_int_or_none(shot['PERIOD']),
            min=_int_or_none(shot['MINUTES_REMAINING']),
            sec=_int_or_none(shot['SECONDS_REMAINING']),
            event_type=shot['EVENT_TYPE'],
            action_type=shot['ACTION_TYPE'],
            shot_type=shot['SHOT_TYPE'],
            shot_zone_basic=shot['SHOT_ZONE_BASIC'],
            shot_zone_area=shot['SHOT_ZONE_AREA'],
            shot_zone_range=shot['SHOT_ZONE_RANGE'],
            shot_distance=_int_or_none(shot['SHOT_DISTANCE']),
            loc_x=_int_or_none(shot['LOC_X']),
            loc_y=_int_or_none(shot['LOC_Y']),
            shot_attempted=bool(shot['SHOT_ATTEMPTED_FLAG']),
            shot_made=bool(shot['SHOT_MADE_FLAG']),
        ))

    if commit:
        try:
            sess.commit()
        except Exception as e:
            logger.info(f"Failed manual shot chart backfill for {game_id}: {e}")
            sess.rollback()
            raise

    return len(shots)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill shot chart data per game.")
    parser.add_argument("--season", default=None, help="Season ID prefix to backfill (e.g. 22024). Defaults to all seasons.")
    parser.add_argument("--workers", type=int, default=3, help="Parallel workers (default: 3).")
    args = parser.parse_args()

    sess = Session()
    q = sess.query(Game.game_id, Game.season).filter(Game.game_date.isnot(None))
    if args.season:
        q = q.filter(Game.season.like(f"{args.season}%"))
    q = q.order_by(Game.game_date.asc(), Game.game_id.asc())
    games = q.all()

    # Filter to games not yet fully backfilled
    jobs = []
    for game_id, season in games:
        if not is_game_shot_back_filled(sess, game_id):
            jobs.append(game_id)
    sess.close()

    logger.info("Found %d games needing shot backfill.", len(jobs))

    def _process(game_id):
        s = Session()
        try:
            back_fill_game_shot_record(s, game_id, commit=True)
            logger.info("Done: %s", game_id)
        except Exception as e:
            logger.error("Failed %s: %s", game_id, e)
        finally:
            s.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        executor.map(_process, jobs)

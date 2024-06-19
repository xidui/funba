from nba_api.stats.endpoints import shotchartdetail
from sqlalchemy.orm import aliased, sessionmaker
from models import PlayerGameStats, ShotRecord, engine
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log, RetryError
from requests.exceptions import ConnectionError, Timeout
from sqlalchemy import func, or_, and_
from collections import defaultdict

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@retry(
    wait=wait_exponential(multiplier=1, max=60),  # Wait 1, 2, 4, ..., up to 60 seconds
    stop=stop_after_attempt(10),  # Retry up to 5 times
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),  # Retry on network issues and timeouts
    before_sleep=before_sleep_log(logger, logging.INFO)  # Log before sleep
)
def fetch_shot_chart(team_id, player_id, game_id):
    shot_chart = shotchartdetail.ShotChartDetail(
        team_id=team_id,
        player_id=player_id,
        game_id_nullable=game_id,
        context_measure_simple='FGA'  # 'FGA' for field goal attempts
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
    return len(list(get_un_back_filled_game_and_player(sess, game_id, None))) == 0


# This method is not safe guarded, so if it's called multiple times, it will result in duplicate records in DB.
def back_fill_game_player_shot_record(sess, game_id, player_id, team_id, commit=False):
    shots = fetch_shot_chart(team_id, player_id, game_id)
    for shot in shots['Shot_Chart_Detail']:
        sess.add(ShotRecord(
            game_id=game_id,
            team_id=team_id,
            player_id=player_id,
            season='TBD',
            period=int(shot['PERIOD']),
            min=int(shot['MINUTES_REMAINING']),
            sec=int(shot['SECONDS_REMAINING']),
            event_type=shot['EVENT_TYPE'],
            action_type=shot['ACTION_TYPE'],
            shot_type=shot['SHOT_TYPE'],
            shot_zone_basic=shot['SHOT_ZONE_BASIC'],
            shot_zone_area=shot['SHOT_ZONE_AREA'],
            shot_zone_range=shot['SHOT_ZONE_RANGE'],
            shot_distance=int(shot['SHOT_DISTANCE']),
            loc_x=int(shot['LOC_X']),
            loc_y=int(shot['LOC_Y']),
            shot_attempted=bool(shot['SHOT_ATTEMPTED_FLAG']),
            shot_made=bool(shot['SHOT_MADE_FLAG']),
        ))

    if commit:
        try:
            sess.commit()
        except Exception as e:
            logger.info(f"Failed to back fill shot record {game_id}, {player_id}: {e}")
            sess.rollback()


def back_fill_game_shot_record(sess, game_id, commit=False):
    if is_game_shot_back_filled(sess, game_id):
        logger.info('skip game {} as it has back filled'.format(game_id))
        return

    for _, player_id, team_id in get_un_back_filled_game_and_player(sess, game_id):
        back_fill_game_player_shot_record(sess, game_id, player_id, team_id)

    if commit:
        try:
            sess.commit()
        except Exception as e:
            logger.info(f"Failed to back fill shot record {game_id}, {player_id}: {e}")
            sess.rollback()


if __name__ == "__main__":
    Session = sessionmaker(bind=engine)
    session = Session()

    jobs = []
    for game_id, player_id, team_id in get_un_back_filled_game_and_player(session):
        jobs.append((game_id, player_id, team_id))

    for game_id, player_id, team_id in jobs:
        back_fill_game_player_shot_record(session, game_id, player_id, team_id, True)

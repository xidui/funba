from nba_api.stats.endpoints import shotchartdetail
from sqlalchemy.orm import sessionmaker
from models import Game, PlayerGameStats, ShotRecord, engine
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
    retry=retry_if_exception_type((ConnectionError, Timeout)),  # Retry on network issues and timeouts
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
        func.count().label('ac')
    ).filter(
        and_(
            or_(ShotRecord.game_id == game_id, game_id is None),
            or_(ShotRecord.player_id == player_id, player_id is None),
        ),
    ).group_by(
        ShotRecord.game_id,
        ShotRecord.player_id
    ).cte(name='A')

    b_cte = sess.query(
        PlayerGameStats.game_id,
        PlayerGameStats.player_id,
        func.count().label('bc')
    ).filter(
        and_(
            PlayerGameStats.fga > 0,
            or_(PlayerGameStats.game_id == game_id, game_id is None),
            or_(PlayerGameStats.player_id == player_id, player_id is None),
        ),
    ).group_by(
        PlayerGameStats.game_id,
        PlayerGameStats.player_id
    ).cte(name='B')

    # Left join on CTEs
    c_cte = sess.query(
        b_cte.c.game_id,
        b_cte.c.player_id,
        a_cte.c.ac,
        b_cte.c.bc
    ).outerjoin(
        a_cte, (a_cte.c.game_id == b_cte.c.game_id) & (a_cte.c.player_id == b_cte.c.player_id)
    ).cte(name='C')

    return sess.query(
        c_cte.c.game_id,
        c_cte.c.player_id
    ).filter(
        c_cte.c.ac.is_(None) | (c_cte.c.ac != c_cte.c.bc)
    )


def is_game_shot_back_filled(sess, game_id):
    return len(list(get_un_back_filled_game_and_player(sess, game_id, None))) == 0


def back_fill_game_shot_record(sess, game_id, commit=False):
    if is_game_shot_back_filled(sess, game_id):
        logger.info('skip game {} as it has back filled'.format(game_id))
        return

    for _, player_id in get_un_back_filled_game_and_player(game_id):
        shot = fetch_shot_chart(0, player_id, game_id)

    if commit:
        try:
            sess.commit()
        except Exception as e:
            logger.info(f"Failed to back fill shot record {game_id}, {player_id}: {e}")
            sess.rollback()


if __name__ == "__main__":
    Session = sessionmaker(bind=engine)
    session = Session()

    d = defaultdict(int)
    for game_id, player_id in get_un_back_filled_game_and_player(session):
        d[game_id] += 1

    for k, v in d.items():
        back_fill_game_shot_record(session, k, True)

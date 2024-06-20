from nba_api.stats.endpoints import shotchartdetail
from sqlalchemy.orm import aliased, sessionmaker
from models import PlayerGameStats, ShotRecord, engine
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

PLAYER_SEASON_FGA_TOTAL_FROM_GAME_STATS_SQL='SELECT PlayerGameStats.player_id, Game.season, sum(fga) as total_fga FROM PlayerGameStats left join Game on PlayerGameStats.game_id = Game.game_id group by PlayerGameStats.player_id, Game.season order by Game.season desc'
PLAYER_SEASON_FGA_TOTAL_FROM_SHOT_RECORD_SQL='SELECT player_id, season, count(*) as total_fga FROM nba_data.ShotRecord group by player_id, season;'

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


# This method is not safe guarded, so if it's called multiple times, it will result in duplicate records in DB.
def back_fill_player_season_shot_record(sess, player_id, season_id, total_fga_from_highlight, commit=False):
    logger.info("backfill for player_id({}) season_id({})".format(player_id, season_id))
    if sess is None:
        sess = Session()

    season, season_type = get_season_and_type_from_season_id(season_id)
    shots = fetch_shot_chart(0, player_id, 0, season, season_type)
    if len(shots['Shot_Chart_Detail']) != total_fga_from_highlight:
        logger.warning("shot record {} doesn't match expected fga {}".format(
            len(shots['Shot_Chart_Detail']), total_fga_from_highlight))
    for shot in shots['Shot_Chart_Detail']:
        sess.add(ShotRecord(
            game_id=shot['GAME_ID'],
            team_id=shot['TEAM_ID'],
            player_id=shot['PLAYER_ID'],
            season=season_id, # 22019
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
            logger.info(f"Failed to back fill shot record {season_id}, {player_id}: {e}")
            sess.rollback()


# This method is not safe guarded, so if it's called multiple times, it will result in duplicate records in DB.
def back_fill_game_player_shot_record(sess, game_id, player_id, team_id, commit=False):
    logger.info("backfill for game_id({}) player_id({}) team_id({})".format(game_id, player_id, team_id))
    if sess is None:
        sess = Session()

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
    session = Session()

    d = defaultdict(int)
    for player_id, season_id, total_fga_from_shot_record in session.execute(text(PLAYER_SEASON_FGA_TOTAL_FROM_SHOT_RECORD_SQL)).all():
        d[(player_id, season_id)] = total_fga_from_shot_record

    jobs = []
    for player_id, season_id, total_fga_from_highlight in session.execute(text(PLAYER_SEASON_FGA_TOTAL_FROM_GAME_STATS_SQL)).all():
        if total_fga_from_highlight and d[(player_id, season_id)] == 0:
            if season_id[1:] > '1995':
                jobs.append((player_id, season_id, total_fga_from_highlight))

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for player_id, season_id, total_fga_from_highlight in jobs:
            executor.submit(back_fill_player_season_shot_record, None, player_id, season_id, total_fga_from_highlight, True)

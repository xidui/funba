from nba_api.stats.endpoints import playbyplayv2
from sqlalchemy.orm import sessionmaker
from models import Game, GamePlayByPlay, Player, engine
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log, RetryError
from requests.exceptions import ConnectionError, Timeout
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
def fetch_game_play_by_play(game_id):
    try:
        return playbyplayv2.PlayByPlayV2(game_id=game_id).get_normalized_dict()
    except Exception as e:
        logger.error(f"Failed to fetch game pbp for {game_id}, error: {e}")
        raise e


def is_game_pbp_back_filled(game_id, sess):
    if sess.query(GamePlayByPlay).filter_by(game_id=game_id).count():
        return True
    return False


def back_fill_pbp(game_id, sess, commit):
    if is_game_pbp_back_filled(game_id, sess):
        logger.info(f'skip back filling game pbp for game id {game_id}')
        return

    player_cache = defaultdict(bool)

    pbp = fetch_game_play_by_play(game_id)
    for event in pbp['PlayByPlay']:
        # check if player exist
        for player_id_key in ['PLAYER1_ID', 'PLAYER2_ID', 'PLAYER3_ID']:
            if event[player_id_key] == 0:
                continue
            player_id = str(event[player_id_key])
            if not player_cache[player_id] and not sess.query(Player).filter_by(player_id=player_id).first():
                logger.info('player {} doesn\'t exist, created'.format(str(event[player_id_key])))
                Session = sessionmaker(bind=engine)
                tmp_session = Session()
                tmp_session.add(Player(
                    player_id=player_id
                ))
                try:
                    tmp_session.commit()
                except Exception as e:
                    logger.info(f"Failed to insert player {player_id}: {e}")
                    tmp_session.rollback()

            player_cache[player_id] = True

        sess.add(GamePlayByPlay(
            game_id=str(game_id),
            event_num=event['EVENTNUM'],
            event_msg_type=event['EVENTMSGTYPE'],
            event_msg_action_type=event['EVENTMSGACTIONTYPE'],
            period=event['PERIOD'],
            wc_time=event['WCTIMESTRING'],
            pc_time=event['PCTIMESTRING'],
            home_description=event['HOMEDESCRIPTION'],
            neutral_description=event['NEUTRALDESCRIPTION'],
            visitor_description=event['VISITORDESCRIPTION'],
            score=event['SCORE'],
            score_margin=event['SCOREMARGIN'],
            player1_id=str(event['PLAYER1_ID']) if event['PLAYER1_ID'] else None,
            player2_id=str(event['PLAYER2_ID']) if event['PLAYER2_ID'] else None,
            player3_id=str(event['PLAYER3_ID']) if event['PLAYER3_ID'] else None,
        ))

    if commit:
        try:
            sess.commit()
        except Exception as e:
            logger.info(f"Failed to insert game pbp {game_id}: {e}")
            sess.rollback()


if __name__ == "__main__":
    Session = sessionmaker(bind=engine)
    session = Session()

    for game in session.query(Game):
        back_fill_pbp(game.game_id, session, True)

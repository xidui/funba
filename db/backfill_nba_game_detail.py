from nba_api.stats.endpoints import boxscoretraditionalv2
from datetime import datetime
from models import Team, TeamGameStats, PlayerGameStats, Player, engine
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log, RetryError
from requests.exceptions import ConnectionError, Timeout
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_team_id(session, matchup):
    parts = matchup.split(' @ ')
    if len(parts) == 2:
        home, road = parts[1], parts[0]
    else:
        parts = matchup.split(' vs. ')
        home, road = parts[0], parts[1]

    homeTeamList = [team_id[0] for team_id in session.query(Team.canonical_team_id).filter_by(abbr=home).all()]
    roadTeamList = [team_id[0] for team_id in session.query(Team.canonical_team_id).filter_by(abbr=road).all()]

    if not homeTeamList or not roadTeamList:
        raise "Failed to get home or road team"

    return homeTeamList, roadTeamList


@retry(
    wait=wait_exponential(multiplier=1, max=60),  # Wait 1, 2, 4, ..., up to 60 seconds
    stop=stop_after_attempt(10),  # Retry up to 5 times
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),  # Retry on network issues and timeouts
    before_sleep=before_sleep_log(logger, logging.INFO)  # Log before sleep
)
def fetch_game_details(game_id):
    try:
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        return boxscore.get_normalized_dict()  # Includes stats for each team in the game
    except Exception as e:
        logger.error(f"Failed to fetch game details for {game_id}, error: {e}")
        raise e


def create_team_game_stats(session, game_id, team_stats, on_road, win):
    team_game_status_record = session.query(TeamGameStats).filter_by(game_id=game_id, team_id=team_stats['TEAM_ID']).first()
    if team_game_status_record is None:
        team_game_status_record = TeamGameStats(
            game_id=game_id,
            team_id=team_stats['TEAM_ID'],
            on_road=on_road,
            win=win,
            min=str(team_stats['MIN']).split('.')[0],  # Assumes 'MIN' contains a period
            pts=team_stats['PTS'],
            fgm=team_stats['FGM'],
            fga=team_stats['FGA'],
            fg_pct=team_stats['FG_PCT'],
            fg3m=team_stats['FG3M'],
            fg3a=team_stats['FG3A'],
            fg3_pct=team_stats['FG3_PCT'],
            ftm=team_stats['FTM'],
            fta=team_stats['FTA'],
            ft_pct=team_stats['FT_PCT'],
            oreb=team_stats['OREB'],
            dreb=team_stats['DREB'],
            reb=team_stats['REB'],
            ast=team_stats['AST'],
            stl=team_stats['STL'],
            blk=team_stats['BLK'],
            tov=team_stats['TO'],
            pf=team_stats['PF']
        )
    session.add(team_game_status_record)


def create_player_game_stats(session, player_stats):
    # create player if not exist
    player_record=session.query(Player).filter_by(
        player_id=str(player_stats['PLAYER_ID']),
    ).first()
    if player_record is None:
        logger.error(f"Create player {player_stats['PLAYER_NAME']}, id: {player_stats['PLAYER_ID']}")
        player_record = Player(
            player_id=str(player_stats['PLAYER_ID']),
            first_name=player_stats['PLAYER_NAME'].split()[0],
            last_name=player_stats['PLAYER_NAME'].split()[1],
            full_name=player_stats['PLAYER_NAME'],
            nick_name=player_stats['NICKNAME'],
            is_active=False,
        )
        session.add(player_record)

    player_game_status_record = session.query(PlayerGameStats).filter_by(
        game_id=player_stats['GAME_ID'],
        player_id=str(player_stats['PLAYER_ID']),
        team_id=str(player_stats['TEAM_ID'])
    ).first()
    if player_game_status_record is None:
        player_game_status_record = PlayerGameStats(
            game_id=player_stats['GAME_ID'],
            team_id=str(player_stats['TEAM_ID']),
            player_id=str(player_stats['PLAYER_ID']),
            comment=player_stats['COMMENT'],
            min=0 if player_stats['MIN'] is None else int(str(player_stats['MIN']).split('.')[0]),
            sec=0 if player_stats['MIN'] is None or len(str(player_stats['MIN']).split(':')) < 2 else int(str(player_stats['MIN']).split(':')[1]),
            starter=bool(player_stats['START_POSITION']),
            position=player_stats['START_POSITION'],
            pts=player_stats['PTS'],
            fgm=player_stats['FGM'],
            fga=player_stats['FGA'],
            fg_pct=player_stats['FG_PCT'],
            fg3m=player_stats['FG3M'],
            fg3a=player_stats['FG3A'],
            fg3_pct=player_stats['FG3_PCT'],
            ftm=player_stats['FTM'],
            fta=player_stats['FTA'],
            ft_pct=player_stats['FT_PCT'],
            oreb=player_stats['OREB'],
            dreb=player_stats['DREB'],
            reb=player_stats['REB'],
            ast=player_stats['AST'],
            stl=player_stats['STL'],
            blk=player_stats['BLK'],
            tov=player_stats['TO'],
            pf=player_stats['PF'],
            plus=player_stats['PLUS_MINUS'],
        )
        session.add(player_game_status_record)
    return bool(player_stats['START_POSITION'])


def is_game_detail_back_filled(game_id, sess):
    player_game_record = sess.query(PlayerGameStats).filter_by(game_id=game_id).count()
    team_game_record = sess.query(TeamGameStats).filter_by(game_id=game_id).count()

    # if both game record for player and team are back filled, we consider it back filled
    return player_game_record != 0 and team_game_record != 0


def back_fill_game_detail(game, game_record, sess, commit):
    if is_game_detail_back_filled(game['GAME_ID'], sess):
        logger.info("skip back filling game detail for game {}, id {}".format(game['MATCHUP'], game['GAME_ID']))
        return

    # get game detail
    try:
        game_details = fetch_game_details(game['GAME_ID'])
    except RetryError as e:
        logger.info(f"Final retry failed for game ID {game['GAME_ID']}: {e}")
        raise e

    # figure out who is home and visitor
    home_team_stats = None
    road_team_stats = None
    home_team_id_list, road_team_id_list = get_team_id(sess, game['MATCHUP'])
    home_team_id, road_team_id = None, None

    for team_status in game_details['TeamStats']:
        if str(team_status['TEAM_ID']) in home_team_id_list:
            home_team_stats = team_status
            home_team_id = str(team_status['TEAM_ID'])
        if str(team_status['TEAM_ID']) in road_team_id_list:
            road_team_stats = team_status
            road_team_id = str(team_status['TEAM_ID'])

    if home_team_id is None or road_team_id is None:
        raise "empty team id"

    # store to the Game table
    game_record.season = game['SEASON_ID'],
    game_record.game_date = datetime.strptime(game['GAME_DATE'], '%Y-%m-%d'),
    game_record.home_team_id = home_team_id,
    game_record.road_team_id = road_team_id,
    game_record.home_team_score = home_team_stats['PTS'],
    game_record.road_team_score = road_team_stats['PTS'],
    game_record.wining_team_id = home_team_id if home_team_stats['PTS'] > road_team_stats['PTS'] else road_team_id,

    # Store stats for home and visitor team
    create_team_game_stats(sess, game['GAME_ID'], home_team_stats, False,
                           home_team_stats['PTS'] > road_team_stats['PTS'])
    create_team_game_stats(sess, game['GAME_ID'], road_team_stats, True,
                           home_team_stats['PTS'] < road_team_stats['PTS'])

    # Backfill player game status
    starter = 0
    for player_stats in game_details['PlayerStats']:
        starter += create_player_game_stats(sess, player_stats)
    if starter != 10:
        logger.warning('not 10 starters in the game {}'.format(game['MATCHUP']))

    if commit:
        try:
            sess.commit()
        except Exception as e:
            logger.info(f"Failed to insert game detail for game {game['GAME_ID']}: {e}")
            sess.rollback()


if __name__ == "__main__":
    pass

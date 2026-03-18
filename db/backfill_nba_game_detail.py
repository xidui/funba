from nba_api.stats.endpoints import boxscoretraditionalv3
from datetime import datetime
from db.models import Team, TeamGameStats, PlayerGameStats, Player, engine
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log, RetryError
from requests.exceptions import ConnectionError, Timeout
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _to_int(value, default=0):
    if value is None or value == '':
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value).strip()
    if not s:
        return default
    if '.' in s:
        return int(float(s))
    return int(s)


def _to_float(value, default=0.0):
    if value is None or value == '':
        return default
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).strip())


def _build_player_name(player):
    first = (player.get('firstName') or '').strip()
    last = (player.get('familyName') or '').strip()
    full_name = f"{first} {last}".strip()
    return full_name or (player.get('nameI') or '')


def _parse_minutes(value):
    if value is None:
        return 0, 0
    s = str(value).strip()
    if not s:
        return 0, 0
    try:
        if ':' in s:
            min_part, sec_part = s.split(':', 1)
            return int(float(min_part or 0)), int(float(sec_part or 0))
        if '.' in s:
            return int(float(s)), 0
        return int(s), 0
    except (TypeError, ValueError):
        return 0, 0


def _normalize_team_stats(team):
    stats = team.get('statistics') or {}
    return {
        'TEAM_ID': team.get('teamId'),
        'MIN': stats.get('minutes'),
        'PTS': _to_int(stats.get('points')),
        'FGM': _to_int(stats.get('fieldGoalsMade')),
        'FGA': _to_int(stats.get('fieldGoalsAttempted')),
        'FG_PCT': _to_float(stats.get('fieldGoalsPercentage')),
        'FG3M': _to_int(stats.get('threePointersMade')),
        'FG3A': _to_int(stats.get('threePointersAttempted')),
        'FG3_PCT': _to_float(stats.get('threePointersPercentage')),
        'FTM': _to_int(stats.get('freeThrowsMade')),
        'FTA': _to_int(stats.get('freeThrowsAttempted')),
        'FT_PCT': _to_float(stats.get('freeThrowsPercentage')),
        'OREB': _to_int(stats.get('reboundsOffensive')),
        'DREB': _to_int(stats.get('reboundsDefensive')),
        'REB': _to_int(stats.get('reboundsTotal')),
        'AST': _to_int(stats.get('assists')),
        'STL': _to_int(stats.get('steals')),
        'BLK': _to_int(stats.get('blocks')),
        'TO': _to_int(stats.get('turnovers')),
        'PF': _to_int(stats.get('foulsPersonal')),
    }


def _normalize_player_stats(player, team_id, game_id):
    stats = player.get('statistics') or {}
    full_name = _build_player_name(player)
    return {
        'GAME_ID': game_id,
        'TEAM_ID': team_id,
        'PLAYER_ID': player.get('personId'),
        'PLAYER_NAME': full_name,
        'NICKNAME': player.get('nameI') or full_name,
        'COMMENT': player.get('comment') or '',
        'MIN': stats.get('minutes'),
        # In V3 the five starters carry position values; bench rows are blank.
        'START_POSITION': (player.get('position') or '').strip(),
        'PTS': _to_int(stats.get('points')),
        'FGM': _to_int(stats.get('fieldGoalsMade')),
        'FGA': _to_int(stats.get('fieldGoalsAttempted')),
        'FG_PCT': _to_float(stats.get('fieldGoalsPercentage')),
        'FG3M': _to_int(stats.get('threePointersMade')),
        'FG3A': _to_int(stats.get('threePointersAttempted')),
        'FG3_PCT': _to_float(stats.get('threePointersPercentage')),
        'FTM': _to_int(stats.get('freeThrowsMade')),
        'FTA': _to_int(stats.get('freeThrowsAttempted')),
        'FT_PCT': _to_float(stats.get('freeThrowsPercentage')),
        'OREB': _to_int(stats.get('reboundsOffensive')),
        'DREB': _to_int(stats.get('reboundsDefensive')),
        'REB': _to_int(stats.get('reboundsTotal')),
        'AST': _to_int(stats.get('assists')),
        'STL': _to_int(stats.get('steals')),
        'BLK': _to_int(stats.get('blocks')),
        'TO': _to_int(stats.get('turnovers')),
        'PF': _to_int(stats.get('foulsPersonal')),
        'PLUS_MINUS': _to_int(stats.get('plusMinusPoints')),
    }


def get_team_id(session, matchup):
    parts = matchup.split(' @ ')
    if len(parts) == 2:
        home, road = parts[1], parts[0]
    else:
        parts = matchup.split(' vs. ')
        home, road = parts[0], parts[1]

    home_team_rows = session.query(Team.team_id, Team.canonical_team_id).filter_by(abbr=home).all()
    road_team_rows = session.query(Team.team_id, Team.canonical_team_id).filter_by(abbr=road).all()

    homeTeamList = [str(canonical_id or team_id) for team_id, canonical_id in home_team_rows]
    roadTeamList = [str(canonical_id or team_id) for team_id, canonical_id in road_team_rows]

    if not homeTeamList or not roadTeamList:
        raise RuntimeError(f"Failed to get home/road team for matchup: {matchup}")

    return homeTeamList, roadTeamList


@retry(
    wait=wait_exponential(multiplier=1, max=4),  # Wait 1, 2, 4 seconds
    stop=stop_after_attempt(5),  # Retry up to 5 times
    retry=retry_if_exception_type((ConnectionError, Timeout)),  # Retry only network issues
    before_sleep=before_sleep_log(logger, logging.INFO)  # Log before sleep
)
def fetch_game_details(game_id):
    try:
        raw = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=30).get_dict()
        boxscore = raw.get('boxScoreTraditional') or {}

        home_team = boxscore.get('homeTeam') or {}
        away_team = boxscore.get('awayTeam') or {}
        teams = [home_team, away_team]

        team_stats = []
        player_stats = []
        for team in teams:
            if not team:
                continue
            if team.get('statistics'):
                team_stats.append(_normalize_team_stats(team))

            team_id = team.get('teamId')
            for player in team.get('players') or []:
                player_stats.append(_normalize_player_stats(player, team_id, game_id))

        return {
            'TeamStats': team_stats,
            'PlayerStats': player_stats,
        }
    except Exception as e:
        logger.error(f"Failed to fetch game details for {game_id}, error: {e}")
        raise e


def create_team_game_stats(session, game_id, team_stats, on_road, win):
    team_id = str(team_stats['TEAM_ID'])
    team_game_status_record = session.query(TeamGameStats).filter_by(game_id=game_id, team_id=team_id).first()
    min_value, _ = _parse_minutes(team_stats.get('MIN'))
    if team_game_status_record is None:
        team_game_status_record = TeamGameStats(
            game_id=game_id,
            team_id=team_id,
        )
    team_game_status_record.on_road = on_road
    team_game_status_record.win = win
    team_game_status_record.min = min_value
    team_game_status_record.pts = team_stats['PTS']
    team_game_status_record.fgm = team_stats['FGM']
    team_game_status_record.fga = team_stats['FGA']
    team_game_status_record.fg_pct = team_stats['FG_PCT']
    team_game_status_record.fg3m = team_stats['FG3M']
    team_game_status_record.fg3a = team_stats['FG3A']
    team_game_status_record.fg3_pct = team_stats['FG3_PCT']
    team_game_status_record.ftm = team_stats['FTM']
    team_game_status_record.fta = team_stats['FTA']
    team_game_status_record.ft_pct = team_stats['FT_PCT']
    team_game_status_record.oreb = team_stats['OREB']
    team_game_status_record.dreb = team_stats['DREB']
    team_game_status_record.reb = team_stats['REB']
    team_game_status_record.ast = team_stats['AST']
    team_game_status_record.stl = team_stats['STL']
    team_game_status_record.blk = team_stats['BLK']
    team_game_status_record.tov = team_stats['TO']
    team_game_status_record.pf = team_stats['PF']
    session.add(team_game_status_record)


def create_player_game_stats(session, player_stats):
    # create player if not exist
    player_record=session.query(Player).filter_by(
        player_id=str(player_stats['PLAYER_ID']),
    ).first()
    if player_record is None:
        logger.error(f"Create player {player_stats['PLAYER_NAME']}, id: {player_stats['PLAYER_ID']}")
        name_parts = player_stats['PLAYER_NAME'].split()
        player_record = Player(
            player_id=str(player_stats['PLAYER_ID']),
            first_name=name_parts[0] if name_parts else player_stats['PLAYER_NAME'],
            last_name=' '.join(name_parts[1:]) if len(name_parts) > 1 else '',
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
    min_value, sec_value = _parse_minutes(player_stats.get('MIN'))
    if player_game_status_record is None:
        player_game_status_record = PlayerGameStats(
            game_id=player_stats['GAME_ID'],
            team_id=str(player_stats['TEAM_ID']),
            player_id=str(player_stats['PLAYER_ID']),
        )
    player_game_status_record.comment = player_stats['COMMENT']
    player_game_status_record.min = min_value
    player_game_status_record.sec = sec_value
    player_game_status_record.starter = bool(player_stats['START_POSITION'])
    player_game_status_record.position = player_stats['START_POSITION']
    player_game_status_record.pts = player_stats['PTS']
    player_game_status_record.fgm = player_stats['FGM']
    player_game_status_record.fga = player_stats['FGA']
    player_game_status_record.fg_pct = player_stats['FG_PCT']
    player_game_status_record.fg3m = player_stats['FG3M']
    player_game_status_record.fg3a = player_stats['FG3A']
    player_game_status_record.fg3_pct = player_stats['FG3_PCT']
    player_game_status_record.ftm = player_stats['FTM']
    player_game_status_record.fta = player_stats['FTA']
    player_game_status_record.ft_pct = player_stats['FT_PCT']
    player_game_status_record.oreb = player_stats['OREB']
    player_game_status_record.dreb = player_stats['DREB']
    player_game_status_record.reb = player_stats['REB']
    player_game_status_record.ast = player_stats['AST']
    player_game_status_record.stl = player_stats['STL']
    player_game_status_record.blk = player_stats['BLK']
    player_game_status_record.tov = player_stats['TO']
    player_game_status_record.pf = player_stats['PF']
    player_game_status_record.plus = player_stats['PLUS_MINUS']
    session.add(player_game_status_record)
    return bool(player_stats['START_POSITION'])


def is_game_detail_back_filled(game_id, sess):
    from sqlalchemy import func

    player_game_record = sess.query(PlayerGameStats).filter_by(game_id=game_id).count()
    team_game_record = sess.query(TeamGameStats).filter_by(game_id=game_id).count()
    if player_game_record == 0 or team_game_record == 0:
        return False

    total_pts = sess.query(func.sum(TeamGameStats.pts)).filter(TeamGameStats.game_id == game_id).scalar() or 0
    return total_pts > 0


def back_fill_game_detail(game, game_record, sess, commit):
    if is_game_detail_back_filled(game['GAME_ID'], sess):
        logger.info("skip back filling game detail for game {}, id {}".format(game['MATCHUP'], game['GAME_ID']))
        return True

    # get game detail
    try:
        game_details = fetch_game_details(game['GAME_ID'])
    except RetryError as e:
        logger.info(f"Final retry failed for game ID {game['GAME_ID']}: {e}")
        raise e

    # figure out who is home and visitor
    team_stats = game_details.get('TeamStats', [])
    if len(team_stats) < 2:
        logger.info("skip game detail for %s: no team stats yet", game['GAME_ID'])
        return False

    home_team_stats = None
    road_team_stats = None
    home_team_id_list, road_team_id_list = get_team_id(sess, game['MATCHUP'])
    home_team_id, road_team_id = None, None

    for team_status in team_stats:
        if str(team_status['TEAM_ID']) in home_team_id_list:
            home_team_stats = team_status
            home_team_id = str(team_status['TEAM_ID'])
        if str(team_status['TEAM_ID']) in road_team_id_list:
            road_team_stats = team_status
            road_team_id = str(team_status['TEAM_ID'])

    if home_team_id is None or road_team_id is None:
        raise RuntimeError(f"Unable to resolve team IDs for game {game['GAME_ID']}")

    # store to the Game table
    game_record.season = game['SEASON_ID']
    game_record.game_date = datetime.strptime(game['GAME_DATE'], '%Y-%m-%d')
    game_record.home_team_id = home_team_id
    game_record.road_team_id = road_team_id
    game_record.home_team_score = home_team_stats['PTS']
    game_record.road_team_score = road_team_stats['PTS']
    game_record.wining_team_id = home_team_id if home_team_stats['PTS'] > road_team_stats['PTS'] else road_team_id

    # Ensure parent Game row exists before inserting child stats rows.
    sess.add(game_record)
    sess.flush()

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

    return True


if __name__ == "__main__":
    pass

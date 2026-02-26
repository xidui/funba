from nba_api.stats.endpoints import leaguegamefinder
from sqlalchemy.orm import sessionmaker
from db.models import Game, engine
from db.backfill_nba_game_pbp import back_fill_pbp
from db.backfill_nba_game_detail import back_fill_game_detail
from db.backfill_nba_player_shot_detail import back_fill_game_shot_record
from concurrent.futures import ThreadPoolExecutor
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_games(season='2023-24', season_type='Regular Season'):
    # This fetches games for the NBA (league_id_nullable='00' for NBA)
    game_finder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        league_id_nullable='00',
        season_type_nullable=season_type,
    )
    games = game_finder.get_data_frames()[0]

    # LeagueGameFinder returns one row per team per game and can include not-yet-played games.
    if 'WL' in games.columns:
        games = games[games['WL'].notna()]

    games = games.sort_values('GAME_DATE').drop_duplicates(subset=['GAME_ID'])
    return games


def process_and_store_game(sess, game):
    game_id = game['GAME_ID']
    game_record = sess.query(Game).filter_by(game_id=game_id).first()
    if game_record is None:
        game_record = Game(game_id=game_id)

    logger.info("process game {}".format(game['MATCHUP']))

    try:
        # backfill game detail info
        has_game_detail = back_fill_game_detail(game, game_record, sess, False)
        if not has_game_detail:
            sess.rollback()
            return

        # backfill game play by play info only when game detail exists
        if game['SEASON_ID'][1:] > '1995':
            # play by play info is not available 1995 and before
            back_fill_pbp(game_id, sess, False)

        if False:
            back_fill_game_shot_record(sess, game_id, False)

        sess.commit()
    except Exception as e:
        logger.info(f"Failed to insert game {game_id}: {e}")
        sess.rollback()


def process_and_store_season(season, sess=None):
    Session = sessionmaker(bind=engine)
    season_types = ['Regular Season', 'Playoffs']
    for season_type in season_types:
        games_df = fetch_games(season, season_type)
        for _, game in games_df.iterrows():
            # Isolate each game in its own session to avoid long-lived rollback side effects.
            local_sess = sess or Session()
            try:
                process_and_store_game(local_sess, game)
            finally:
                if sess is None:
                    local_sess.close()


if __name__ == "__main__":
    seasons = []
    if len(sys.argv) > 1:
        logger.info("Arguments received:")
        for index, arg in enumerate(sys.argv[1:], start=1):  # sys.argv[1:] to skip the script name
            logger.info(f"Argument {index}: {arg}")
            seasons.append(arg)
    else:
        logger.info("No arguments provided.")

    if len(seasons) == 0:
        seasons = [f"{year}-{str(year + 1)[-2:]}" for year in range(1983, 1985)]

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(process_and_store_season, seasons[::-1])

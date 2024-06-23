import json
import os
import time
import threading

from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplay
from nba_api.stats.library.parameters import SeasonType


def get_all_nba_seasons():
    # return ['{}-{}'.format(year, str(year+1)[2:]) for year in range(1946, 2024)]
    return ['{}-{}'.format(year, str(year+1)[2:]) for year in range(2023, 2024)]


error_game_ids = []


def get_play_by_play_game_with_retry(game, game_folder_path, j):
    max_retries = 10
    retry_interval = 10

    for i in range(max_retries):
        try:
            game_play_by_play_path = '{}/playbyplay.json'.format(game_folder_path)

            if not os.path.exists(game_play_by_play_path) or os.path.getsize(game_play_by_play_path) < 10000:
                print('fetching {0}-th game play by play : {1} {2}, season: {3}'.format(j, game['GAME_DATE'], game['MATCHUP'], game['SEASON_ID']))
                game_play_by_play_json = playbyplay.PlayByPlay(game['GAME_ID']).get_normalized_json()

                with open(game_play_by_play_path, 'w') as json_file:
                    json.dump(game_play_by_play_json, json_file, indent=4)
            break
        except Exception as e:
            print('error to get {}-th play by play game {} with attempt {}/{}: {}'.format(j, game['MATCHUP'], i+1, max_retries, e))
            error_game_ids.append(game['GAME_ID'])
            time.sleep(retry_interval)
    else:
        # Code to execute if all retries fail
        print("Max retries reached. Could not complete the operation.")


def game_finder_with_retry(team, season_str, season_type):
    max_retries = 10
    retry_interval = 10

    for i in range(max_retries):
        try:
            print('fetching games for team {} of season {}'.format(team['full_name'], season_str))
            return leaguegamefinder.LeagueGameFinder(team_id_nullable=team['id'],
                                                     season_nullable=season_str,
                                                     season_type_nullable=season_type)
        except Exception as e:
            print('error to get season {} for team {} with retry {}/{}: {}'.format(
                season_str, team['full_name'], i+1, max_retries, e
            ))
            time.sleep(retry_interval)
    else:
        # Code to execute if all retries fail
        print("Max retries reached. Could not complete the operation.")


def fetch_team(team_obj, season_str):
    print('fetching team: {0}'.format(team_obj['full_name']))

    team_folder_path = '{0}/{1}'.format('./data', team_obj['full_name'])

    # Check if the folder doesn't exist, then create it
    if not os.path.exists(team_folder_path):
        os.makedirs(team_folder_path)

    team_meta_path = '{0}/team_meta.json'.format(team_folder_path)
    if not os.path.exists(team_meta_path):
        with open(team_meta_path, 'w') as json_file:
            json.dump(team_obj, json_file, indent=4)

    gamefinder = game_finder_with_retry(team_obj, season_str, SeasonType.regular)
    games_dict = gamefinder.get_normalized_dict()
    games = games_dict['LeagueGameFinderResults']

    i = 0
    for game in games:
        i += 1

        season_folder_path = '{}/{}'.format(team_folder_path, game['SEASON_ID'])
        if not os.path.exists(season_folder_path):
            os.makedirs(season_folder_path)

        game_folder_path = '{}/{}-{}'.format(season_folder_path, game['GAME_DATE'], game['MATCHUP'])
        if not os.path.exists(game_folder_path):
            os.makedirs(game_folder_path)

        game_highlight_path = '{}/highlight.json'.format(game_folder_path)
        with open(game_highlight_path, 'w') as json_file:
            json.dump(game, json_file, indent=4)

        get_play_by_play_game_with_retry(game, game_folder_path, i)

        time.sleep(0.1)


def fetch_single_game(game_id):
    game = leaguegamefinder.LeagueGameFinder(game_id_nullable=game_id)


def __main__():
    for season in get_all_nba_seasons()[::-1]:

        threads = []

        for team in teams.get_teams():
            # if team['abbreviation'] not in ['GSW']:
            #     continue

            thread = threading.Thread(target=fetch_team, args=(team, season))
            thread.start()
            threads.append(thread)

            thread.join()

        # Wait for all threads to finish
        # for thread in threads:
        #     thread.join()

    with open('./error_ids.json', 'w') as json_file:
        json.dump(error_game_ids, json_file, indent=4)


__main__()

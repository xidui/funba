import os
import json


class EventMsgType:
    FIELD_GOAL_MADE = 1
    FIELD_GOAL_MISSED = 2
    FREE_THROW = 3
    REBOUND = 4
    TURNOVER = 5
    FOUL = 6
    VIOLATION = 7
    SUBSTITUTION = 8
    TIMEOUT = 9
    JUMP_BALL = 10
    EJECTION = 11
    PERIOD_BEGIN = 12
    PERIOD_END = 13
    # Deprecated as of 2023.11.10
    UNKNOWN = 18, "'UNKNOWN' member is deprecated; use 'INSTANT_REPLAY' instead."
    INSTANT_REPLAY = 18


def for_each_game(season=None):
    for root, dirs, files in os.walk('./data'):
        if '2023-07' in root:
            # todo: cleanup this preseason data
            continue

        for file_name in files:
            if file_name == 'highlight.json':
                file_path = os.path.join(root, file_name)
                with open(file_path, 'r') as f1:
                    highlight = json.load(f1)
                    if highlight['SEASON_ID'][1:] != season:
                        continue
                    with open(file_path.replace('highlight', 'playbyplay'), 'r') as f2:
                        yield highlight, json.loads(json.load(f2))


def generate_game_map(folder_path):
    game_map = {}

    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_name == 'highlight.json':
                with open(file_path, 'r') as f:
                    highlight = json.load(f)
                    game_map[highlight['GAME_ID']] = file_path

    with open('{}/game_map.json'.format(folder_path), 'w') as f:
        json.dump(game_map, f)


def load_game_highlight(game_id):
    with open('./data/game_map.json') as f:
        game_map = json.load(f)
    game_highlight_path = game_map[game_id]
    with open(game_highlight_path) as f:
        return json.load(f)


def load_game(game_id):
    with open('./data/game_map.json') as f:
        game_map = json.load(f)
    game_highlight_path = game_map[game_id]
    game_pbp_path = game_highlight_path.replace('highlight', 'playbyplay')

    with open(game_pbp_path) as f:
        return json.loads(json.load(f))


def load_pbp_game_to_csv(game_id, file_path=None):
    game_in_json = load_game(game_id)

    keys = game_in_json['PlayByPlay'][0].keys()
    if not file_path:
        print(','.join(keys))
    for play in game_in_json['PlayByPlay']:
        print(','.join([str(play[key]) if play[key] else '' for key in keys]))


generate_game_map('./data')

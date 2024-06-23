import game_lib
import re

from collections import defaultdict
from game_lib import EventMsgType
from nba_api.stats.library import playbyplayregex


def validate_score(game_id, game_playbyplay=None):
    player_score_cumulative = defaultdict(lambda: defaultdict(int))
    player_score_total = defaultdict(lambda: defaultdict(int))

    if game_playbyplay == None:
        game_playbyplay = game_lib.load_game(game_id)

    error = ''

    for play in game_playbyplay['PlayByPlay']:
        home_or_visitor = 'home' if play['HOMEDESCRIPTION'] is not None else 'visitor'
        description = play['HOMEDESCRIPTION'] if play['HOMEDESCRIPTION'] is not None else play['VISITORDESCRIPTION']

        if play['EVENTMSGTYPE'] in [EventMsgType.FIELD_GOAL_MADE, EventMsgType.FREE_THROW]:
            reg = playbyplayregex.pattern_field_goal_made
            if play['EVENTMSGTYPE'] == EventMsgType.FREE_THROW:
                reg = playbyplayregex.pattern_free_throw_made

            matched = re.match(reg, description)
            if not matched:
                # free throw miss
                continue
            matched = matched.groupdict()

            player_name = matched['player']

            score_this_play = 1
            if play['EVENTMSGTYPE'] != EventMsgType.FREE_THROW:
                score_this_play += 1
                if '3PT' in matched['field_goal_type']:
                    score_this_play += 1

            player_score_cumulative[home_or_visitor][player_name] += score_this_play
            player_score_total[home_or_visitor][player_name] = max(player_score_total[home_or_visitor][player_name],
                int(matched['points']))

            if player_score_cumulative[home_or_visitor][player_name] != int(matched['points']):
                error = 'cumulative and intermediate total score doesn\'t match'

    if error:
        print(error, game_id)
        for home_or_visitor, dic in player_score_cumulative.items():
            for k, v in dic.items():
                if player_score_total[home_or_visitor][k] != v:
                    raise error

            if len(player_score_cumulative[home_or_visitor]) != len(player_score_total[home_or_visitor]):
                raise 'error 2'

# wrong_games = [
#     # '0022200107', # Mobley cumulative score is wrong,
#     # '0022201045', # Middleton score wrong
# ]
#
# for highlight, playbyplay in game_lib.for_each_game(season='2020'):
#     if highlight['GAME_ID'] in wrong_games:
#         continue
#
#     validate_score(highlight['GAME_ID'], playbyplay)

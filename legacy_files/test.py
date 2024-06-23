from nba_api.stats.endpoints import playerprofilev2, playergamelog, playercareerstats, TeamPlayerOnOffSummary, TeamPlayerOnOffDetails
from nba_api.stats.static import players

# print(players.get_players())

# print(playerprofilev2.PlayerProfileV2(player_id=1629673).get_normalized_json())
# print(playergamelog.PlayerGameLog(player_id=1629673).get_normalized_json())
# print(playercareerstats.PlayerCareerStats(player_id=1629673).get_normalized_json())
# print(playercareerstats.PlayerCareerStats(player_id=1629673).get_normalized_json())
# print(TeamPlayerOnOffSummary(team_id=1610612738).get_normalized_json())
# print(TeamPlayerOnOffDetails(team_id=1610612738).get_normalized_json())

from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playergamelogs
from nba_api.stats.library.parameters import SeasonType

# g = leaguegamefinder.LeagueGameFinder(team_id_nullable='1610612744',
#                                       season_nullable='2022-23',
#                                       season_type_nullable=SeasonType.regular)
# d = g.get_normalized_dict()

p = playergamelogs.PlayerGameLogs(team_id_nullable='1610612744', season_nullable='2023-24').get_normalized_dict()
for a in p:
    print(p)


#Get the Warriors team_id
from nba_api.stats.static import teams

nba_teams = teams.get_teams()

# Select the dictionary for the Pacers, which contains their team ID
warriors = [team for team in nba_teams if team['abbreviation'] == 'GSW'][0]
warriors_id = warriors['id']
print(f'warriors: {warriors_id}')

#########################

# Query for the last regular season game where the Pacers were playing
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType

gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=warriors_id,
                            season_nullable=Season.default,
                            season_type_nullable=SeasonType.regular)

games_dict = gamefinder.get_normalized_dict()
games = games_dict['LeagueGameFinderResults']
game = games[0]
game_id = game['GAME_ID']
game_matchup = game['MATCHUP']

print(f'Searching through {len(games)} game(s) for the game_id of {game_id} where {game_matchup}')

#########################
from nba_api.stats.endpoints import playbyplay
df = playbyplay.PlayByPlay(game_id).get_data_frames()[0]
csv_file_path = 'example.csv'
# Use the to_csv() method to write the DataFrame to a CSV file
df.to_csv(csv_file_path, index=False)
print(df.head()) #just looking at the head of the data



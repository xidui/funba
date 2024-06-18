from nba_api.stats.static import teams
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from models import Team, Player, engine

Session = sessionmaker(bind=engine)
session = Session()


def get_nba_teams():
    # Fetch a list of all NBA teams
    nba_teams = teams.get_teams()

    # Process and add each team to the session
    for team in nba_teams:
        team_record = Team(
            team_id=str(team['id']),
            full_name=team['full_name'],
            abbr=team['abbreviation'],
            nick_name=team['nickname'],
            city=team['city'],
            state='',  # NBA API does not provide state, fill with appropriate value
            year_founded=int(team['year_founded'])
        )
        session.add(team_record)
        try:
            session.commit()
        except IntegrityError:
            # Handle the case where the team already exists to prevent duplication
            session.rollback()
            print(f"Skipped duplicate team: {team['full_name']}")

        # backfill team as a player in case some Rebound/Turnover are on the team
        player_record = Player(
            player_id=str(team['id']),
            full_name=team['full_name'],
            nick_name=team['nickname'],
            is_team=True,
        )
        session.add(player_record)
        try:
            session.commit()
        except IntegrityError:
            # Handle the case where the team already exists to prevent duplication
            session.rollback()
            print(f"Skipped duplicate team as player: {team['full_name']}")


if __name__ == "__main__":
    get_nba_teams()

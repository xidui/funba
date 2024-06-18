from nba_api.stats.static import players
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from models import Player, engine

Session = sessionmaker(bind=engine)
session = Session()


def get_all_players():
    # Retrieve a list of all players currently in the NBA
    all_players = players.get_players()
    for player in all_players:
        player_record = Player(
            player_id=str(player['id']),
            first_name=player['first_name'],
            last_name=player['last_name'],
            full_name=player['full_name'],
            is_active=player['is_active'],
        )
        session.add(player_record)
        try:
            session.commit()
        except IntegrityError:
            # Handle the case where the team already exists to prevent duplication
            session.rollback()
            print(f"Skipped duplicate player: {player['full_name']}")


if __name__ == "__main__":
    get_all_players()

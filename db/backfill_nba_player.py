from nba_api.stats.static import players
from sqlalchemy.orm import sessionmaker

from db.models import Player, engine

Session = sessionmaker(bind=engine)


def upsert_player(session, player_data):
    player_id = str(player_data["id"])
    values = {
        "first_name": player_data["first_name"],
        "last_name": player_data["last_name"],
        "full_name": player_data["full_name"],
        "is_active": bool(player_data["is_active"]),
    }

    player_record = session.get(Player, player_id)
    if player_record is None:
        session.add(Player(player_id=player_id, **values))
        return "created"

    changed = False
    for field, value in values.items():
        if getattr(player_record, field) != value:
            setattr(player_record, field, value)
            changed = True

    if changed:
        session.add(player_record)
        return "updated"

    return "skipped"


def get_all_players(player_rows=None):
    session = Session()
    counts = {"created": 0, "updated": 0, "skipped": 0}

    try:
        all_players = player_rows if player_rows is not None else players.get_players()
        for player in all_players:
            outcome = upsert_player(session, player)
            counts[outcome] += 1

        session.commit()
        return counts
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    get_all_players()

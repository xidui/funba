from db.models import ShotRecord, PlayerSeasonMetrics, engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)


SHOT_PCT_AFTER_MADE_SQL='''
SELECT 
    full_name,
    PlayerSeasonMetrics.player_id,
    season,
    team_id,
    shot_made / shot_attempt AS pct,
    shot_made_after_made / shot_attempt_after_made AS pct_after_made,
    shot_made_after_made / shot_attempt_after_made - shot_made / shot_attempt AS diff
FROM
    PlayerSeasonMetrics
        LEFT JOIN
    Player ON Player.player_id = PlayerSeasonMetrics.player_id
WHERE
    shot_attempt_after_made > 200
ORDER BY diff DESC;
'''


def process_player(player_id, sess=None, three=False, commit=False):
    if sess is None:
        sess = Session()

    query = (sess.query(ShotRecord.player_id, ShotRecord.team_id, ShotRecord.season, ShotRecord.game_id,
                        ShotRecord.shot_type, ShotRecord.shot_made)
             .filter(ShotRecord.player_id == player_id)
             .order_by(ShotRecord.season, ShotRecord.team_id, ShotRecord.game_id))

    # Conditionally modify the query for 3-point field goals
    if three:
        query = query.filter(ShotRecord.shot_type == '3PT Field Goal')

    shot_made = 0
    shot_attempt = 0
    shot_made_after_made = 0
    shot_attempt_after_made = 0
    previous_made = False
    current_game_id = None
    current_season = None
    current_team_id = None

    all_data = list(query.all())
    all_data.append((None, None, None, None, None, None))

    for _, team_id, season, game_id, shot_type, made in all_data:
        if game_id != current_game_id:
            previous_made = False
            current_game_id = game_id

        if season != current_season or team_id != current_team_id:
            if current_season is not None:
                record = sess.query(PlayerSeasonMetrics).filter(
                    PlayerSeasonMetrics.player_id == player_id,
                    PlayerSeasonMetrics.team_id == current_team_id,
                    PlayerSeasonMetrics.season == current_season,
                ).first()
                if record is None:
                    record = PlayerSeasonMetrics(
                        player_id=player_id,
                        team_id=current_team_id,
                        season=current_season,
                    )
                record.shot_made = shot_made
                record.shot_attempt = shot_attempt
                record.shot_made_after_made = shot_made_after_made
                record.shot_attempt_after_made = shot_attempt_after_made
                sess.add(record)

                if commit:
                    sess.commit()

            if season is None:
                break

            current_season = season
            current_team_id = team_id
            current_game_id = game_id
            previous_made = False
            shot_made = 0
            shot_attempt = 0
            shot_made_after_made = 0
            shot_attempt_after_made = 0

        shot_made += made
        shot_attempt += 1

        if previous_made:
            shot_attempt_after_made += 1
            if made:
                shot_made_after_made += 1

        previous_made = made


def shot_pct_after_made():
    session = Session()
    query = session.query(ShotRecord.player_id).distinct()
    for player_id in query.all():
        process_player(player_id[0], session, three=False, commit=True)


if __name__ == '__main__':
    shot_pct_after_made()

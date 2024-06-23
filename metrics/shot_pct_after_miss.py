from db.models import Game, ShotRecord, PlayerSeasonMetrics, engine
from sqlalchemy.orm import sessionmaker

import concurrent.futures

Session = sessionmaker(bind=engine)

SHOT_PCT_AFTER_MISS_SQL = '''
SELECT 
    full_name,
    PlayerSeasonMetrics.player_id,
    team_id,
    season,
    three_pointer_made as m,
    three_pointer_attempt as a,
    three_pointer_made/three_pointer_attempt AS pct,
    three_pointer_made_after_one_miss as m_1m,
    three_pointer_attempt_after_one_miss as a_1m,
    three_pointer_made_after_one_miss / three_pointer_attempt_after_one_miss AS pct_one_miss,
    three_pointer_made_after_two_miss as m_2m,
    three_pointer_attempt_after_two_miss as a_2m,
    three_pointer_made_after_two_miss/three_pointer_attempt_after_two_miss AS pct_two_miss,
    three_pointer_made_after_one_miss / three_pointer_attempt_after_one_miss-three_pointer_made/three_pointer_attempt as diff
FROM
    PlayerSeasonMetrics
        LEFT JOIN
    Player ON Player.player_id = PlayerSeasonMetrics.player_id
WHERE
    three_pointer_attempt_after_one_miss > 100
ORDER BY diff DESC;
'''


def process_player(player_id, sess=None, three=False, commit=False):
    if sess is None:
        sess = Session()

    query = (sess.query(ShotRecord.player_id, ShotRecord.team_id, ShotRecord.season,
                        ShotRecord.shot_type, ShotRecord.shot_made)
             .filter(ShotRecord.player_id == player_id)
             .order_by(ShotRecord.season, ShotRecord.team_id))

    # Conditionally modify the query for 3-point field goals
    if three:
        query = query.filter(ShotRecord.shot_type == '3PT Field Goal')

    goal_made = 0
    goal_attempted = 0
    goal_made_after_one_miss = 0
    goal_attempted_after_one_miss = 0
    goal_made_after_two_miss = 0
    goal_attempted_after_two_miss = 0
    current_missing_straight = 0
    current_season = None
    current_team_id = None

    all_data = list(query.all())
    all_data.append((None, None, None, None, None))

    for _, team_id, season, shot_type, made in all_data:
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
                record.three_pointer_made = goal_made
                record.three_pointer_attempt = goal_attempted
                record.three_pointer_made_after_one_miss = goal_made_after_one_miss
                record.three_pointer_attempt_after_one_miss = goal_attempted_after_one_miss
                record.three_pointer_made_after_two_miss = goal_made_after_two_miss
                record.three_pointer_attempt_after_two_miss = goal_attempted_after_two_miss
                sess.add(record)

                if commit:
                    sess.commit()

            if season is None:
                break

            current_season = season
            current_team_id = team_id
            current_missing_straight = 0
            goal_made = 0
            goal_attempted = 0
            goal_made_after_one_miss = 0
            goal_attempted_after_one_miss = 0
            goal_made_after_two_miss = 0
            goal_attempted_after_two_miss = 0

        goal_attempted += 1
        if current_missing_straight == 1:
            goal_attempted_after_one_miss += 1
        elif current_missing_straight == 2:
            goal_attempted_after_two_miss += 1

        if made:
            goal_made += 1
            if current_missing_straight == 1:
                goal_made_after_one_miss += 1
            elif current_missing_straight == 2:
                goal_made_after_two_miss += 1
            current_missing_straight = 0
        else:
            current_missing_straight += 1


def shot_pct_after_miss():
    session = Session()
    query = session.query(ShotRecord.player_id).distinct()
    for player_id in query.all():
        process_player(player_id[0], session, three=True, commit=True)


if __name__ == '__main__':
    shot_pct_after_miss()

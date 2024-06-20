from db.models import Game, GamePlayByPlay, engine
from sqlalchemy.orm import sessionmaker
from static_numbers.event_msg_type import EventMsgType
from sqlalchemy import text

import concurrent.futures

Session = sessionmaker(bind=engine)


PITY_LOSS_PER_SEASON_SQL='''
WITH A AS (
    SELECT 
        season,
        CASE
            WHEN home_team_score > road_team_score THEN road_team_id
            ELSE road_team_id
        END AS pity_loss_team_id,
        wining_team_id as close_win_team_id,
        game_id
    FROM
        Game
    WHERE
        pity_loss = 1
), 
B AS (
    SELECT 
        A.*,
        full_name
    FROM 
        A 
    LEFT JOIN 
        Team 
    ON 
        A.pity_loss_team_id = Team.team_id
)
SELECT 
    full_name, 
    season, 
    COUNT(*) AS c
FROM 
    B
GROUP BY 
    full_name, 
    season 
ORDER BY 
    c DESC;

'''


def get_pity_loss_count():
    session = Session()
    return session.execute(text(PITY_LOSS_PER_SEASON_SQL)).all()


def check_and_set_pity_loss(game_id, sess=None):
    if sess is None:
        sess = Session()

    game = sess.query(Game).filter(Game.game_id==game_id).first()

    pity = False
    previous_margin = None
    for pbp in sess.query(GamePlayByPlay).filter(GamePlayByPlay.game_id==game.game_id).order_by(GamePlayByPlay.id.asc()):
        if pbp.score_margin is None:
            continue

        if pbp.period in [1, 2, 3]:
            continue

        if pbp.event_msg_type not in [EventMsgType.FREE_THROW, EventMsgType.FIELD_GOAL_MADE]:
            continue

        current_margin = pbp.score_margin
        if current_margin == 'TIE':
            current_margin = 0
        else:
            current_margin = int(current_margin)

        if previous_margin is None:
            previous_margin = current_margin
            continue

        if pbp.pc_time <= '0:24':
            if current_margin == 0 or previous_margin == 0 or current_margin * previous_margin < 0:
                pity = True
                break

        previous_margin = current_margin

    game.pity_loss = pity
    sess.commit()

    return pity


def pity_loss():
    session = Session()

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        for game in session.query(Game).filter(Game.pity_loss.is_(None)):
            executor.submit(check_and_set_pity_loss, game.game_id, None)


if __name__ == '__main__':
    pity_loss()

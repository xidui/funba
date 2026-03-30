import requests

from nba_api.stats.library.http import STATS_HEADERS
from sqlalchemy.orm import sessionmaker
from db.models import Game, GamePlayByPlay, engine
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log, RetryError
from requests.exceptions import ConnectionError, Timeout
from static_numbers.event_msg_type import EventMsgType

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _parse_period_clock(clock):
    # V3 clock format is ISO-like, e.g. PT11M34.00S -> 11:34
    if not clock:
        return None
    try:
        body = clock.replace('PT', '').replace('S', '')
        min_part, sec_part = body.split('M')
        seconds = int(float(sec_part))
        return f"{int(min_part)}:{seconds:02d}"
    except Exception:
        return None


def _event_msg_type_from_action(action):
    action_type = action.get('actionType')
    sub_type = action.get('subType')

    if action_type == 'Made Shot':
        return EventMsgType.FIELD_GOAL_MADE
    if action_type == 'Missed Shot':
        return EventMsgType.FIELD_GOAL_MISSED
    if action_type == 'Free Throw':
        return EventMsgType.FREE_THROW
    if action_type == 'Rebound':
        return EventMsgType.REBOUND
    if action_type == 'Turnover':
        return EventMsgType.TURNOVER
    if action_type == 'Foul':
        return EventMsgType.FOUL
    if action_type == 'Violation':
        return EventMsgType.VIOLATION
    if action_type == 'Substitution':
        return EventMsgType.SUBSTITUTION
    if action_type == 'Timeout':
        return EventMsgType.TIMEOUT
    if action_type == 'Jump Ball':
        return EventMsgType.JUMP_BALL
    if action_type == 'Ejection':
        return EventMsgType.EJECTION
    if action_type == 'period':
        if sub_type == 'start':
            return EventMsgType.PERIOD_BEGIN
        if sub_type == 'end':
            return EventMsgType.PERIOD_END
    if action_type == 'Instant Replay':
        return EventMsgType.INSTANT_REPLAY
    return None


def _build_score_and_margin(action):
    score_home = action.get('scoreHome')
    score_away = action.get('scoreAway')
    if score_home in (None, '') or score_away in (None, ''):
        return None, None
    try:
        home = int(score_home)
        away = int(score_away)
    except (TypeError, ValueError):
        return None, None

    score = f"{home} - {away}"
    diff = home - away
    margin = 'TIE' if diff == 0 else str(diff)
    return score, margin


def _normalize_pbp(raw):
    game = raw.get('game') or {}
    actions = game.get('actions') or []

    rows = []
    for action in actions:
        location = action.get('location')
        description = action.get('description')
        home_description = description if location == 'h' else None
        visitor_description = description if location == 'v' else None
        neutral_description = description if location not in ('h', 'v') else None

        score, score_margin = _build_score_and_margin(action)

        rows.append({
            'EVENTNUM': action.get('actionNumber') or action.get('actionId'),
            'EVENTMSGTYPE': _event_msg_type_from_action(action),
            'EVENTMSGACTIONTYPE': None,
            'PERIOD': action.get('period'),
            'WCTIMESTRING': action.get('clock'),
            'PCTIMESTRING': _parse_period_clock(action.get('clock')),
            'HOMEDESCRIPTION': home_description,
            'NEUTRALDESCRIPTION': neutral_description,
            'VISITORDESCRIPTION': visitor_description,
            'SCORE': score,
            'SCOREMARGIN': score_margin,
            # Keep player refs empty for V3: many rows point to coaches/officials,
            # which violates Player FK and is not used by current analytics.
            'PLAYER1_ID': 0,
            'PLAYER2_ID': 0,
            'PLAYER3_ID': 0,
            'ACTIONTYPE': action.get('actionType'),
            'SUBTYPE': action.get('subType'),
        })

    # Reorder by period ASC, clock DESC (12:00→0:00), event_num ASC
    # This fixes misplaced events (e.g., early-quarter plays with high event_num)
    def _clock_sort_key(row):
        pc = row.get('PCTIMESTRING') or '0:00'
        try:
            parts = pc.split(':')
            return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, IndexError):
            return 0

    rows.sort(key=lambda r: (r.get('PERIOD') or 0, -_clock_sort_key(r), r.get('EVENTNUM') or 0))

    # Dedup by (PERIOD, EVENTNUM) — keep first occurrence
    seen = set()
    deduped = []
    for r in rows:
        key = (r.get('PERIOD'), r.get('EVENTNUM'))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    # Reassign sequential event_num so DB ordering matches corrected order
    for i, r in enumerate(deduped):
        r['EVENTNUM'] = i + 1

    return {'PlayByPlay': deduped}


@retry(
    wait=wait_exponential(multiplier=1, max=4),  # Wait 1, 2, 4 seconds
    stop=stop_after_attempt(5),  # Retry up to 5 times
    retry=retry_if_exception_type((ConnectionError, Timeout)),  # Retry only network issues
    before_sleep=before_sleep_log(logger, logging.INFO)  # Log before sleep
)
def fetch_game_play_by_play(game_id):
    try:
        response = requests.get(
            "https://stats.nba.com/stats/playbyplayv3",
            params={
                "GameID": str(game_id),
                "StartPeriod": 1,
                "EndPeriod": 10,
            },
            headers=STATS_HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        raw = response.json()
        actions = ((raw.get("game") or {}).get("actions") or [])
        if not actions:
            raise ValueError(f"No PBP actions returned for game {game_id}")
        return _normalize_pbp(raw)
    except Exception as e:
        logger.error(f"Failed to fetch game pbp for {game_id}, error: {e}")
        raise e


def is_game_pbp_back_filled(game_id, sess):
    if sess.query(GamePlayByPlay).filter_by(game_id=game_id).count():
        return True
    return False


def back_fill_pbp(game_id, sess, commit, force=False):
    if not force and is_game_pbp_back_filled(game_id, sess):
        logger.info(f'skip back filling game pbp for game id {game_id}')
        return

    if force:
        deleted = sess.query(GamePlayByPlay).filter_by(game_id=str(game_id)).delete()
        if deleted:
            logger.info(f'Deleted {deleted} existing PBP rows for {game_id}')

    pbp = fetch_game_play_by_play(game_id)

    for event in pbp['PlayByPlay']:
        sess.add(GamePlayByPlay(
            game_id=str(game_id),
            event_num=event['EVENTNUM'],
            event_msg_type=event['EVENTMSGTYPE'],
            event_msg_action_type=event['EVENTMSGACTIONTYPE'],
            period=event['PERIOD'],
            wc_time=event['WCTIMESTRING'],
            pc_time=event['PCTIMESTRING'],
            home_description=event['HOMEDESCRIPTION'],
            neutral_description=event['NEUTRALDESCRIPTION'],
            visitor_description=event['VISITORDESCRIPTION'],
            score=event['SCORE'],
            score_margin=event['SCOREMARGIN'],
            player1_id=str(event['PLAYER1_ID']) if event['PLAYER1_ID'] else None,
            player2_id=str(event['PLAYER2_ID']) if event['PLAYER2_ID'] else None,
            player3_id=str(event['PLAYER3_ID']) if event['PLAYER3_ID'] else None,
        ))

    if commit:
        try:
            sess.commit()
            logger.info(f'Backfilled {len(pbp["PlayByPlay"])} PBP rows for {game_id}')
        except Exception as e:
            logger.info(f"Failed to insert game pbp {game_id}: {e}")
            sess.rollback()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill game play-by-play data (V3)")
    parser.add_argument("--game", type=str, help="Single game_id to backfill")
    parser.add_argument("--force", action="store_true", help="Delete existing PBP and re-fetch from API")
    args = parser.parse_args()

    Session = sessionmaker(bind=engine)
    session = Session()

    if args.game:
        back_fill_pbp(args.game, session, True, force=args.force)
    else:
        for game in session.query(Game):
            back_fill_pbp(game.game_id, session, True, force=args.force)

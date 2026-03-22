import os

from sqlalchemy import BLOB, DATE, Boolean, Column, Computed, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import declarative_base

from db.config import get_database_url

# Workers set DB_POOL_SIZE=1 (one connection per forked process is enough).
# Web app uses the default of 5.
_pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))

engine = create_engine(get_database_url(), pool_size=_pool_size, max_overflow=_max_overflow)

# Create a base class for declarative class definitions
Base = declarative_base()


class Team(Base):
    __tablename__ = 'Team'

    id = Column(Integer, primary_key=True)
    team_id = Column(String(50), unique=True)
    full_name = Column(String(100))
    abbr = Column(String(5))
    nick_name = Column(String(100))
    city = Column(String(50))
    state = Column(String(50))
    year_founded = Column(Integer)
    active = Column(Boolean)
    is_legacy = Column(Boolean)
    canonical_team_id = Column(String(50), ForeignKey('Team.team_id'), index=True)
    start_season = Column(String(10))
    end_season = Column(String(10))


class Player(Base):
    __tablename__ = 'Player'

    player_id = Column(String(50), unique=True, primary_key=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    full_name = Column(String(100))
    nick_name = Column(String(50), default="")
    is_active = Column(Boolean)
    is_team = Column(Boolean, default=False)

    # Bio / demographics (from commonplayerinfo endpoint)
    height = Column(String(10), nullable=True)        # e.g. "6-6"
    weight = Column(Integer, nullable=True)            # lbs
    birth_date = Column(DATE, nullable=True)
    country = Column(String(50), nullable=True)
    school = Column(String(100), nullable=True)
    draft_year = Column(Integer, nullable=True)
    draft_round = Column(Integer, nullable=True)
    draft_number = Column(Integer, nullable=True)
    jersey = Column(String(10), nullable=True)
    position = Column(String(30), nullable=True)       # e.g. "Guard"
    from_year = Column(Integer, nullable=True)         # first NBA season
    to_year = Column(Integer, nullable=True)           # latest NBA season


class Award(Base):
    __tablename__ = 'Award'

    id = Column(Integer, primary_key=True, autoincrement=True)
    award_type = Column(String(50), nullable=False)
    season = Column(Integer, nullable=False)
    player_id = Column(String(50), ForeignKey('Player.player_id'), nullable=True)
    team_id = Column(String(50), ForeignKey('Team.team_id'), nullable=True)
    notes = Column(Text, nullable=True)
    entity_key = Column(
        String(64),
        Computed(
            "CASE WHEN player_id IS NOT NULL THEN CONCAT('P:', player_id) "
            "WHEN team_id IS NOT NULL THEN CONCAT('T:', team_id) "
            "ELSE NULL END",
            persisted=True,
        ),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint('award_type', 'season', 'entity_key', name='uq_Award_type_season_entity'),
        Index('ix_Award_type_season', 'award_type', 'season'),
        Index('ix_Award_player_type', 'player_id', 'award_type'),
        Index('ix_Award_team_type', 'team_id', 'award_type'),
    )


class Game(Base):
    __tablename__ = 'Game'

    game_id = Column(String(50), primary_key=True)
    season = Column(String(50))
    game_date = Column(DATE)
    highlight = Column(BLOB, default=None)

    home_team_id = Column(String(50), ForeignKey('Team.team_id'))
    road_team_id = Column(String(50), ForeignKey('Team.team_id'))
    wining_team_id = Column(String(50), ForeignKey('Team.team_id'))
    home_team_score = Column(Integer)
    road_team_score = Column(Integer)
    pity_loss = Column(Boolean)
    backfill_mismatch = Column(Boolean, nullable=False, default=False)
    backfill_mismatch_note = Column(Text)
    backfill_mismatch_updated_at = Column(DateTime)


class TeamGameStats(Base):
    __tablename__ = 'TeamGameStats'

    game_id = Column(String(50), ForeignKey('Game.game_id'), primary_key=True)
    team_id = Column(String(50), ForeignKey('Team.team_id'), primary_key=True)
    on_road = Column(Boolean)
    win = Column(Boolean)
    min = Column(Integer)
    pts = Column(Integer)
    fgm = Column(Integer)
    fga = Column(Integer)
    fg_pct = Column(Float)
    fg3m = Column(Integer)
    fg3a = Column(Integer)
    fg3_pct = Column(Float)
    ftm = Column(Integer)
    fta = Column(Integer)
    ft_pct = Column(Float)
    oreb = Column(Integer)
    dreb = Column(Integer)
    reb = Column(Integer)
    ast = Column(Integer)
    stl = Column(Integer)
    blk = Column(Integer)
    tov = Column(Integer)
    pf = Column(Integer)


class PlayerGameStats(Base):
    __tablename__ = 'PlayerGameStats'

    game_id = Column(String(50), ForeignKey('Game.game_id'), primary_key=True)
    team_id = Column(String(50), ForeignKey('Team.team_id'), primary_key=True)
    player_id = Column(String(50), ForeignKey('Player.player_id'), primary_key=True)
    comment = Column(String(500))
    min = Column(Integer)
    sec = Column(Integer)
    starter = Column(Boolean)
    position = Column(String(5))
    pts = Column(Integer)
    fgm = Column(Integer)
    fga = Column(Integer)
    fg_pct = Column(Float)
    fg3m = Column(Integer)
    fg3a = Column(Integer)
    fg3_pct = Column(Float)
    ftm = Column(Integer)
    fta = Column(Integer)
    ft_pct = Column(Float)
    oreb = Column(Integer)
    dreb = Column(Integer)
    reb = Column(Integer)
    ast = Column(Integer)
    stl = Column(Integer)
    blk = Column(Integer)
    tov = Column(Integer)
    pf = Column(Integer)
    plus = Column(Integer)

    __table_args__ = (
        Index('ix_PlayerGameStats_player_id', 'player_id'),
        Index('ix_PlayerGameStats_game_id', 'game_id'),
        Index('ix_PlayerGameStats_team_id', 'team_id'),
    )


class GamePlayByPlay(Base):
    __tablename__ = 'GamePlayByPlay'

    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), ForeignKey('Game.game_id'))
    event_num = Column(Integer)
    event_msg_type = Column(Integer)
    event_msg_action_type = Column(Integer)
    period = Column(Integer)
    wc_time = Column(String(20))
    pc_time = Column(String(20))
    home_description = Column(String(200))
    neutral_description = Column(String(200))
    visitor_description = Column(String(200))
    score = Column(String(20))
    score_margin = Column(String(20))
    player1_id = Column(String(50), ForeignKey('Player.player_id'))
    player2_id = Column(String(50), ForeignKey('Player.player_id'))
    player3_id = Column(String(50), ForeignKey('Player.player_id'))


Index(
    'ix_GamePlayByPlay_game_id_period_pc_time',
    GamePlayByPlay.game_id.asc(),
    GamePlayByPlay.period.asc(),
    GamePlayByPlay.pc_time.desc(),
)


class ShotRecord(Base):
    __tablename__ = 'ShotRecord'

    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), ForeignKey('Game.game_id'))
    team_id = Column(String(50), ForeignKey('Team.team_id'))
    player_id = Column(String(50), ForeignKey('Player.player_id'))
    season = Column(String(50), nullable=False)
    period = Column(Integer)
    min = Column(Integer)
    sec = Column(Integer)
    event_type = Column(String(50))
    action_type = Column(String(50))
    shot_type = Column(String(50))
    shot_zone_basic = Column(String(50))
    shot_zone_area = Column(String(50))
    shot_zone_range = Column(String(50))
    shot_distance = Column(Integer)
    loc_x = Column(Integer)
    loc_y = Column(Integer)
    shot_attempted = Column(Boolean)
    shot_made = Column(Boolean)


Index('ix_ShotRecord_game_id', ShotRecord.game_id)
Index('ix_ShotRecord_player_id', ShotRecord.player_id)
Index('ix_ShotRecord_team_id', ShotRecord.team_id)
Index('ix_ShotRecord_season', ShotRecord.season)
Index('ix_ShotRecord_player_id_season_team_id', ShotRecord.player_id, ShotRecord.season, ShotRecord.team_id)
Index('ix_ShotRecord_player_id_season', ShotRecord.player_id, ShotRecord.season)
Index('ix_ShotRecord_season_zone', ShotRecord.season, ShotRecord.shot_zone_area)


class GameLineScore(Base):
    __tablename__ = 'GameLineScore'

    game_id = Column(String(50), ForeignKey('Game.game_id'), primary_key=True)
    team_id = Column(String(50), ForeignKey('Team.team_id'), primary_key=True)
    on_road = Column(Boolean, nullable=False)
    q1_pts = Column(Integer, nullable=True)
    q2_pts = Column(Integer, nullable=True)
    q3_pts = Column(Integer, nullable=True)
    q4_pts = Column(Integer, nullable=True)
    ot1_pts = Column(Integer, nullable=True)
    ot2_pts = Column(Integer, nullable=True)
    ot3_pts = Column(Integer, nullable=True)
    ot_extra_json = Column(Text, nullable=True)
    first_half_pts = Column(Integer, nullable=True)
    second_half_pts = Column(Integer, nullable=True)
    regulation_total_pts = Column(Integer, nullable=True)
    total_pts = Column(Integer, nullable=False)
    source = Column(String(64), nullable=False, default='nba_api_boxscoresummaryv3')
    fetched_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)



class MetricResult(Base):
    __tablename__ = 'MetricResult'

    id = Column(Integer, primary_key=True)
    metric_key = Column(String(64), nullable=False, index=True)
    entity_type = Column(String(16), nullable=False)   # player | team | game | league
    entity_id = Column(String(50), nullable=True)      # player_id or team_id
    season = Column(String(16), nullable=True)
    rank_group = Column(String(64), nullable=True)
    game_id = Column(String(20), nullable=True)
    value_num = Column(Float, nullable=True)
    value_str = Column(String(255), nullable=True)
    context_json = Column(Text, nullable=True)         # JSON string
    noteworthiness = Column(Float, nullable=True)      # 0.0–1.0, AI-scored
    notable_reason = Column(Text, nullable=True)
    computed_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('uq_MetricResult_key_entity_season', 'metric_key', 'entity_type', 'entity_id', 'season', unique=True),
        Index('ix_MetricResult_entity', 'entity_type', 'entity_id', 'season'),
        Index('ix_MetricResult_ranking', 'metric_key', 'season', 'rank_group', 'value_num'),
    )


class MetricDefinition(Base):
    __tablename__ = 'MetricDefinition'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(64), nullable=False, unique=True, index=True)
    family_key = Column(String(64), nullable=False, index=True)
    variant = Column(String(16), nullable=False, default='season')
    base_metric_key = Column(String(64), nullable=True, index=True)
    managed_family = Column(Boolean, nullable=False, default=False)
    name = Column(String(128), nullable=False)
    description = Column(Text, nullable=True)
    scope = Column(String(16), nullable=False)        # player | team | game
    category = Column(String(32), nullable=True)
    group_key = Column(String(64), nullable=True, index=True)
    source_type = Column(String(16), nullable=False, default='rule')  # rule | code
    status = Column(String(16), nullable=False, default='draft')      # draft | published | archived
    definition_json = Column(Text, nullable=True)     # JSON rule spec (source_type='rule')
    code_python = Column(Text, nullable=True)         # generated Python code (source_type='code')
    context_label_template = Column(String(256), nullable=True)  # format string for context label
    expression = Column(Text, nullable=True)          # original plain-English input
    min_sample = Column(Integer, nullable=False, default=1)
    created_by_user_id = Column(String(36), ForeignKey('User.id'), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('uq_MetricDefinition_family_variant', 'family_key', 'variant', unique=True),
    )


class MetricRunLog(Base):
    __tablename__ = 'MetricRunLog'

    game_id       = Column(String(20), primary_key=True)
    metric_key    = Column(String(64), primary_key=True)
    entity_type   = Column(String(16), primary_key=True)   # player | team | game | league
    entity_id     = Column(String(50), primary_key=True)   # player_id / team_id / game_id
    season        = Column(String(16), primary_key=True)   # e.g. "22025" or "all_regular" for career
    computed_at   = Column(DateTime, nullable=False)
    produced_result = Column(Boolean, nullable=False, default=True)
    delta_json    = Column(Text, nullable=True)             # per-game delta for reprocessing
    qualified     = Column(Boolean, nullable=True)          # True if entity met qualifying criteria

    __table_args__ = (
        Index('ix_MetricRunLog_metric_key_computed_at', 'metric_key', 'computed_at'),
        Index('ix_MetricRunLog_qualifying', 'metric_key', 'entity_id', 'qualified'),
    )


class MetricJobClaim(Base):
    """Atomic task-claim table — prevents concurrent duplicate metric computation.

    One row per (game_id, metric_key). The worker that successfully INSERTs
    via INSERT IGNORE owns the job. Concurrent workers get rowcount=0 and skip.

    Status lifecycle:
      'in_progress' — claimed, computation underway
      'done'        — computation committed successfully; future tasks skip

    On transient failure the worker deletes the row so a retry can reclaim.
    On success the worker updates status to 'done'.
    On worker crash the row stays as 'in_progress' and must be cleared manually
    (or via --force in dispatch) before the game can be reprocessed.
    """
    __tablename__ = "MetricJobClaim"

    game_id    = Column(String(20), primary_key=True)
    metric_key = Column(String(64), primary_key=True)
    claimed_at = Column(DateTime, nullable=False)
    worker_id  = Column(String(255), nullable=True)   # celery task id for tracing
    status     = Column(String(16), nullable=False, default="in_progress")  # in_progress | done

    __table_args__ = (
        Index('ix_MetricJobClaim_metric_status_game', 'metric_key', 'status', 'game_id'),
    )


class User(Base):
    """User account (Google OAuth or email magic-link)."""
    __tablename__ = 'User'

    id = Column(String(36), primary_key=True)          # UUID, server-generated
    google_id = Column(String(128), nullable=True, unique=True, index=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    display_name = Column(String(255), nullable=False)
    avatar_url = Column(String(1024), nullable=True)
    is_admin = Column(Boolean, default=False, nullable=False)
    stripe_customer_id = Column(String(255), nullable=True, unique=True, index=True)
    subscription_tier = Column(String(16), nullable=False, default='free')
    subscription_status = Column(String(32), nullable=True)
    subscription_expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False)
    last_login_at = Column(DateTime, nullable=False)


class MagicToken(Base):
    """Passwordless email login tokens."""
    __tablename__ = 'MagicToken'

    id = Column(Integer, primary_key=True, autoincrement=True)
    token = Column(String(64), nullable=False, unique=True, index=True)
    email = Column(String(255), nullable=False, index=True)
    expires_at = Column(DateTime, nullable=False)
    used = Column(Boolean, nullable=False, default=False)
    next_url = Column(String(1024), nullable=True)
    created_at = Column(DateTime, nullable=False)


class Feedback(Base):
    """User-submitted feedback."""
    __tablename__ = 'Feedback'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(36), ForeignKey('User.id'), nullable=False, index=True)
    content = Column(Text, nullable=False)
    page_url = Column(String(500), nullable=True)   # page the user was on
    created_at = Column(DateTime, nullable=False)


class PageView(Base):
    __tablename__ = 'PageView'

    id = Column(Integer, primary_key=True, autoincrement=True)
    visitor_id = Column(String(36), nullable=False, index=True)  # UUID from cookie
    path = Column(String(500), nullable=False)
    referrer = Column(String(1000), nullable=True)
    user_agent = Column(String(500), nullable=True)
    ip_address = Column(String(45), nullable=True)   # supports IPv6
    created_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('ix_PageView_created_at', 'created_at'),
        Index('ix_PageView_visitor_id_created_at', 'visitor_id', 'created_at'),
    )


def init_db() -> None:
    """Create tables for local bootstrap/dev if they do not exist."""
    Base.metadata.create_all(engine)

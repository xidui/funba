import os

from sqlalchemy import BLOB, DATE, Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String, Text, create_engine
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



class MetricResult(Base):
    __tablename__ = 'MetricResult'

    id = Column(Integer, primary_key=True)
    metric_key = Column(String(64), nullable=False, index=True)
    entity_type = Column(String(16), nullable=False)   # player | team | game | league
    entity_id = Column(String(50), nullable=True)      # player_id or team_id
    season = Column(String(10), nullable=True)
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
    name = Column(String(128), nullable=False)
    description = Column(Text, nullable=True)
    scope = Column(String(16), nullable=False)        # player | team | game
    category = Column(String(32), nullable=True)
    group_key = Column(String(64), nullable=True, index=True)
    source_type = Column(String(16), nullable=False, default='rule')  # rule | builtin
    status = Column(String(16), nullable=False, default='draft')      # draft | published | archived
    definition_json = Column(Text, nullable=True)     # JSON rule spec
    expression = Column(Text, nullable=True)          # original plain-English input
    min_sample = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class MetricRunLog(Base):
    __tablename__ = 'MetricRunLog'

    game_id       = Column(String(20), primary_key=True)
    metric_key    = Column(String(64), primary_key=True)
    entity_type   = Column(String(16), primary_key=True)   # player | team | game | league
    entity_id     = Column(String(50), primary_key=True)   # player_id / team_id / game_id
    season        = Column(String(10), primary_key=True)   # e.g. "22025" or "all" for career
    computed_at   = Column(DateTime, nullable=False)
    produced_result = Column(Boolean, nullable=False, default=True)
    delta_json    = Column(Text, nullable=True)             # per-game delta for reprocessing

    __table_args__ = (
        Index('ix_MetricRunLog_metric_key_computed_at', 'metric_key', 'computed_at'),
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


def init_db() -> None:
    """Create tables for local bootstrap/dev if they do not exist."""
    Base.metadata.create_all(engine)

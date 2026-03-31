import os

from sqlalchemy import BLOB, DATE, Boolean, Column, Computed, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import declarative_base

from db.config import get_database_url

# Workers set DB_POOL_SIZE=1 (one connection per forked process is enough).
# Web app uses the default of 5.
_pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))

engine = create_engine(get_database_url(), pool_size=_pool_size, max_overflow=_max_overflow, pool_pre_ping=True)

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
    season_exp = Column(Integer, nullable=True)        # NBA seasons played
    greatest_75_flag = Column(Boolean, nullable=True)  # NBA 75 greatest list


class PlayerSalary(Base):
    __tablename__ = "PlayerSalary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(String(50), ForeignKey("Player.player_id"), nullable=False, index=True)
    season = Column(Integer, nullable=False)
    salary_usd = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("player_id", "season", name="uq_PlayerSalary_player_season"),
    )


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
    data_source = Column(String(64), nullable=False, default='nba_api_box_scores')
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
    data_source = Column(String(64), nullable=False, default='nba_api_box_scores')
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
    data_source = Column(String(64), nullable=False, default='nba_api_box_scores')
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
    max_results_per_season = Column(Integer, nullable=True)
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
        Index('ix_MetricRunLog_computed_at', 'computed_at'),
        Index('ix_MetricRunLog_metric_key_computed_at', 'metric_key', 'computed_at'),
        Index('ix_MetricRunLog_metric_game', 'metric_key', 'game_id'),
        Index('ix_MetricRunLog_qualifying', 'metric_key', 'entity_id', 'qualified'),
        Index('ix_MetricRunLog_reduce', 'metric_key', 'season', 'entity_type', 'entity_id'),
        Index('ix_MetricRunLog_season', 'season'),
    )


class MetricPerfLog(Base):
    __tablename__ = "MetricPerfLog"

    id = Column(Integer, primary_key=True, autoincrement=True)
    metric_key = Column(String(64), nullable=False)
    recorded_at = Column(DateTime, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    db_reads = Column(Integer, nullable=True)
    db_writes = Column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_MetricPerfLog_metric_key_recorded_at", "metric_key", "recorded_at"),
    )


class MetricComputeRun(Base):
    """Coarse-grained orchestration state for one metric compute/backfill run."""
    __tablename__ = "MetricComputeRun"

    id = Column(String(36), primary_key=True)
    metric_key = Column(String(64), nullable=False)
    status = Column(String(16), nullable=False, default="mapping")  # mapping | reducing | complete | failed
    target_season = Column(String(16), nullable=True)
    target_date_from = Column(DATE, nullable=True)
    target_date_to = Column(DATE, nullable=True)
    target_game_count = Column(Integer, nullable=False)
    done_game_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime, nullable=False)
    reduce_enqueued_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    failed_at = Column(DateTime, nullable=True)
    error_text = Column(Text, nullable=True)

    __table_args__ = (
        Index('ix_MetricComputeRun_metric_status', 'metric_key', 'status'),
        Index('ix_MetricComputeRun_status_created', 'status', 'created_at'),
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


class Setting(Base):
    __tablename__ = 'Setting'

    key = Column(String(64), primary_key=True)
    value = Column(String(255), nullable=False)
    updated_at = Column(DateTime, nullable=False)


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
    created_at = Column(DateTime, nullable=False, index=True)


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


class AiUsageLog(Base):
    __tablename__ = "AiUsageLog"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False)
    user_id = Column(String(36), ForeignKey("User.id"), nullable=True, index=True)
    visitor_id = Column(String(36), nullable=True, index=True)
    feature = Column(String(32), nullable=False)
    operation = Column(String(32), nullable=False)
    endpoint = Column(String(128), nullable=False)
    provider = Column(String(32), nullable=False)
    model = Column(String(64), nullable=False)
    prompt_tokens = Column(Integer, nullable=True)
    completion_tokens = Column(Integer, nullable=True)
    total_tokens = Column(Integer, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    success = Column(Boolean, nullable=False, default=True)
    error_code = Column(String(64), nullable=True)
    http_status = Column(Integer, nullable=True)
    conversation_id = Column(String(36), nullable=True, index=True)
    metadata_json = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_AiUsageLog_created_at", "created_at"),
        Index("ix_AiUsageLog_feature_created_at", "feature", "created_at"),
        Index("ix_AiUsageLog_user_created_at", "user_id", "created_at"),
        Index("ix_AiUsageLog_visitor_created_at", "visitor_id", "created_at"),
        Index("ix_AiUsageLog_conversation_created_at", "conversation_id", "created_at"),
    )


# ---------------------------------------------------------------------------
# Content pipeline: SocialPost → SocialPostVariant → SocialPostDelivery
# ---------------------------------------------------------------------------

class SocialPost(Base):
    """A topic/theme for social media content (e.g. "blowout rate rankings")."""
    __tablename__ = 'SocialPost'

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(255), nullable=False)
    source_date = Column(DATE, nullable=False)
    source_metrics = Column(Text, nullable=True)       # JSON list of metric keys
    source_game_ids = Column(Text, nullable=True)      # JSON list of game IDs
    status = Column(String(16), nullable=False, default='draft')  # draft|in_review|approved|archived
    admin_comments = Column(Text, nullable=True)       # JSON array [{text, timestamp, from}]
    priority = Column(Integer, nullable=False, default=50)
    llm_model = Column(String(64), nullable=True)
    paperclip_issue_id = Column(String(64), nullable=True)
    paperclip_issue_identifier = Column(String(64), nullable=True)
    paperclip_issue_status = Column(String(16), nullable=True)
    paperclip_assignee_agent_id = Column(String(64), nullable=True)
    paperclip_assignee_user_id = Column(String(64), nullable=True)
    paperclip_last_comment_id = Column(String(64), nullable=True)
    paperclip_last_synced_at = Column(DateTime, nullable=True)
    paperclip_sync_error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('ix_SocialPost_source_date', 'source_date'),
        Index('ix_SocialPost_status', 'status'),
        Index('ix_SocialPost_source_date_status', 'source_date', 'status'),
        Index('ix_SocialPost_paperclip_issue_id', 'paperclip_issue_id'),
    )


class SocialPostVariant(Base):
    """Audience-specific content variant of a SocialPost."""
    __tablename__ = 'SocialPostVariant'

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey('SocialPost.id', ondelete='CASCADE'), nullable=False)
    title = Column(String(255), nullable=False)
    content_raw = Column(Text, nullable=False)         # content with placeholders
    audience_hint = Column(String(128), nullable=True) # e.g. "thunder fans", "general nba"
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('ix_SocialPostVariant_post_id', 'post_id'),
    )


class SocialPostDelivery(Base):
    """Per-destination publishing record for a variant."""
    __tablename__ = 'SocialPostDelivery'

    id = Column(Integer, primary_key=True, autoincrement=True)
    variant_id = Column(Integer, ForeignKey('SocialPostVariant.id', ondelete='CASCADE'), nullable=False)
    platform = Column(String(32), nullable=False)      # hupu|reddit|discord|twitter|facebook
    forum = Column(String(64), nullable=True)           # platform-specific target (e.g. "thunder", "r/nba")
    is_enabled = Column(Boolean, nullable=False, default=True)
    status = Column(String(16), nullable=False, default='pending')  # pending|publishing|published|failed
    content_final = Column(Text, nullable=True)        # platform-rendered content
    published_url = Column(String(1024), nullable=True)
    published_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('ix_SocialPostDelivery_variant_id', 'variant_id'),
        Index('ix_SocialPostDelivery_status', 'status'),
    )


def init_db() -> None:
    """Create tables for local bootstrap/dev if they do not exist."""
    Base.metadata.create_all(engine)

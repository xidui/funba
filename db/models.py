from sqlalchemy import create_engine, Index, text
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DATE, BLOB, Integer, ForeignKey, Boolean, Float


# Create an engine to connect to your MySQL database
engine = create_engine('mysql+pymysql://root:xixihaha@localhost/nba_data')

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
    canonical_team_id = Column(String(50), ForeignKey('Team.team_id'))
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


class TeamGameStats(Base):
    __tablename__ = 'TeamGameStats'

    game_id = Column(String(50), ForeignKey('Game.game_id'), primary_key=True)
    team_id = Column(String(50), ForeignKey('Team.team_id'), primary_key=True)
    on_road = Column(Boolean)
    win = Column(Boolean)  # win or lose
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
    tov = Column(Integer)  # turn over
    pf = Column(Integer)  # personal foul


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
    tov = Column(Integer)  # turn over
    pf = Column(Integer)  # personal foul
    plus = Column(Integer)

    __table_args__ = (
        Index('ix_PlayerGameStats_player_id', 'player_id'),
        Index('ix_PlayerGameStats_game_id', 'game_id'),
        Index('ix_PlayerGameStats_team_id', 'team_id'),
    )


class GamePlayByPlay(Base):
    __tablename__ = 'GamePlayByPlay'

    id = Column(Integer, primary_key=True)  # Unique ID for each record
    game_id = Column(String(50), ForeignKey('Game.game_id'))  # Assuming a 'games' table exists
    event_num = Column(Integer)  # Event number in the game
    event_msg_type = Column(Integer)  # Type of event
    event_msg_action_type = Column(Integer)  # Subtype of event
    period = Column(Integer)  # Game period
    wc_time = Column(String(20))  # Wall-clock time
    pc_time = Column(String(20))  # Play-clock time
    home_description = Column(String(200))  # Description of event for home team
    neutral_description = Column(String(200))  # Neutral description of event
    visitor_description = Column(String(200))  # Description of event for visitor team
    score = Column(String(20))  # Score after event
    score_margin = Column(String(20))  # Score margin after event
    player1_id = Column(String(50), ForeignKey('Player.player_id'))
    player2_id = Column(String(50), ForeignKey('Player.player_id'))
    player3_id = Column(String(50), ForeignKey('Player.player_id'))


Index('ix_PlayerGameStats_game_id', 'game_id'),
Index('ix_PlayerGameStats_game_id_time',
      GamePlayByPlay.game_id.asc(),
      GamePlayByPlay.period.asc(),
      GamePlayByPlay.pc_time.desc(),
      ),


class ShotRecord(Base):
    __tablename__ = 'ShotRecord'
    id = Column(Integer, primary_key=True)  # Unique ID for each record
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


Index('ix_ShotRecord_game_id', 'game_id'),
Index('ix_ShotRecord_player_id', 'player_id'),
Index('ix_ShotRecord_team_id', 'team_id'),
Index('ix_ShotRecord_season', 'season'),
Index('ix_ShotRecord_player_id_season', 'player_id', 'season'),
Index('ix_ShotRecord_season_zone', 'season', 'shot_zone_area'),


Base.metadata.create_all(engine)

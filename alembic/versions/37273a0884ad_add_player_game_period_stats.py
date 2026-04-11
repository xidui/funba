"""add_player_game_period_stats

Revision ID: 37273a0884ad
Revises: 5f68073ae20d
Create Date: 2026-04-11 00:22:17.532726

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '37273a0884ad'
down_revision: Union[str, None] = '5f68073ae20d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "PlayerGamePeriodStats",
        sa.Column("game_id", sa.String(50), sa.ForeignKey("Game.game_id"), primary_key=True),
        sa.Column("team_id", sa.String(50), sa.ForeignKey("Team.team_id"), primary_key=True),
        sa.Column("player_id", sa.String(50), sa.ForeignKey("Player.player_id"), primary_key=True),
        sa.Column("period", sa.Integer, primary_key=True),
        sa.Column("min", sa.Integer),
        sa.Column("sec", sa.Integer),
        sa.Column("pts", sa.Integer),
        sa.Column("fgm", sa.Integer),
        sa.Column("fga", sa.Integer),
        sa.Column("fg3m", sa.Integer),
        sa.Column("fg3a", sa.Integer),
        sa.Column("ftm", sa.Integer),
        sa.Column("fta", sa.Integer),
        sa.Column("oreb", sa.Integer),
        sa.Column("dreb", sa.Integer),
        sa.Column("reb", sa.Integer),
        sa.Column("ast", sa.Integer),
        sa.Column("stl", sa.Integer),
        sa.Column("blk", sa.Integer),
        sa.Column("tov", sa.Integer),
        sa.Column("pf", sa.Integer),
        sa.Column("plus_minus", sa.Integer),
    )
    op.create_index("ix_PlayerGamePeriodStats_game_id", "PlayerGamePeriodStats", ["game_id"])
    op.create_index("ix_PlayerGamePeriodStats_player_id", "PlayerGamePeriodStats", ["player_id"])


def downgrade() -> None:
    op.drop_index("ix_PlayerGamePeriodStats_player_id", "PlayerGamePeriodStats")
    op.drop_index("ix_PlayerGamePeriodStats_game_id", "PlayerGamePeriodStats")
    op.drop_table("PlayerGamePeriodStats")

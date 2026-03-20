"""add game line score table

Revision ID: 7a4f0d5b61c2
Revises: 365bd0dc2039
Create Date: 2026-03-20 13:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7a4f0d5b61c2"
down_revision: Union[str, None] = "365bd0dc2039"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "GameLineScore",
        sa.Column("game_id", sa.String(length=50), nullable=False),
        sa.Column("team_id", sa.String(length=50), nullable=False),
        sa.Column("on_road", sa.Boolean(), nullable=False),
        sa.Column("q1_pts", sa.Integer(), nullable=True),
        sa.Column("q2_pts", sa.Integer(), nullable=True),
        sa.Column("q3_pts", sa.Integer(), nullable=True),
        sa.Column("q4_pts", sa.Integer(), nullable=True),
        sa.Column("ot1_pts", sa.Integer(), nullable=True),
        sa.Column("ot2_pts", sa.Integer(), nullable=True),
        sa.Column("ot3_pts", sa.Integer(), nullable=True),
        sa.Column("ot_extra_json", sa.Text(), nullable=True),
        sa.Column("first_half_pts", sa.Integer(), nullable=True),
        sa.Column("second_half_pts", sa.Integer(), nullable=True),
        sa.Column("regulation_total_pts", sa.Integer(), nullable=True),
        sa.Column("total_pts", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False, server_default=sa.text("'nba_api_boxscoresummaryv3'")),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["game_id"], ["Game.game_id"]),
        sa.ForeignKeyConstraint(["team_id"], ["Team.team_id"]),
        sa.PrimaryKeyConstraint("game_id", "team_id"),
    )


def downgrade() -> None:
    op.drop_table("GameLineScore")

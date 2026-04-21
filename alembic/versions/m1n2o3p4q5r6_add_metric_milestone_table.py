"""add metric milestone table

Revision ID: m1n2o3p4q5r6
Revises: t2u3v4w5x6y7
Create Date: 2026-04-21 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "m1n2o3p4q5r6"
down_revision: Union[str, None] = "t2u3v4w5x6y7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_Game_season_game_date", "Game", ["season", "game_date"])
    op.create_table(
        "MetricMilestone",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("metric_key", sa.String(length=128), nullable=False),
        sa.Column("entity_type", sa.String(length=16), nullable=False),
        sa.Column("entity_id", sa.String(length=50), nullable=False),
        sa.Column("season", sa.String(length=16), nullable=False),
        sa.Column("game_id", sa.String(length=50), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("event_key", sa.String(length=128), nullable=False),
        sa.Column("prev_rank", sa.Integer(), nullable=True),
        sa.Column("new_rank", sa.Integer(), nullable=True),
        sa.Column("prev_value", sa.Float(), nullable=True),
        sa.Column("new_value", sa.Float(), nullable=True),
        sa.Column("value_delta", sa.Float(), nullable=True),
        sa.Column("thresholds_json", sa.Text(), nullable=True),
        sa.Column("passed_json", sa.Text(), nullable=True),
        sa.Column("target_json", sa.Text(), nullable=True),
        sa.Column("context_json", sa.Text(), nullable=True),
        sa.Column("severity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("computed_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "metric_key",
            "entity_type",
            "entity_id",
            "season",
            "game_id",
            "event_type",
            "event_key",
            name="uq_milestone",
        ),
    )
    op.create_index("ix_MetricMilestone_game", "MetricMilestone", ["game_id"])
    op.create_index("ix_MetricMilestone_metric_season", "MetricMilestone", ["metric_key", "season"])
    op.create_index("ix_MetricMilestone_entity", "MetricMilestone", ["entity_type", "entity_id", "season"])
    op.create_index(
        "ix_MetricMilestone_event_lookup",
        "MetricMilestone",
        ["metric_key", "entity_type", "entity_id", "season", "event_type", "event_key"],
    )
    op.create_index("ix_MetricMilestone_severity", "MetricMilestone", ["severity"])


def downgrade() -> None:
    op.drop_index("ix_MetricMilestone_severity", table_name="MetricMilestone")
    op.drop_index("ix_MetricMilestone_event_lookup", table_name="MetricMilestone")
    op.drop_index("ix_MetricMilestone_entity", table_name="MetricMilestone")
    op.drop_index("ix_MetricMilestone_metric_season", table_name="MetricMilestone")
    op.drop_index("ix_MetricMilestone_game", table_name="MetricMilestone")
    op.drop_table("MetricMilestone")
    op.drop_index("ix_Game_season_game_date", table_name="Game")

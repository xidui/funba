"""add story_score_bonus and story_cluster to metric definition

Revision ID: g7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-04-30 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "g7b8c9d0e1f2"
down_revision: Union[str, None] = "f6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "MetricDefinition",
        sa.Column("story_score_bonus", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "MetricDefinition",
        sa.Column("story_cluster", sa.String(64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("MetricDefinition", "story_cluster")
    op.drop_column("MetricDefinition", "story_score_bonus")

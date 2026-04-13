"""add embedding columns to metric definition

Revision ID: c1f2e3d4a5b6
Revises: z0a1b2c3d4e5
Create Date: 2026-04-12 22:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c1f2e3d4a5b6"
down_revision: Union[str, None] = "ab12cd34ef56"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "MetricDefinition",
        sa.Column("embedding", sa.LargeBinary(length=64 * 1024), nullable=True),
    )
    op.add_column(
        "MetricDefinition",
        sa.Column("embedding_model", sa.String(64), nullable=True),
    )
    op.add_column(
        "MetricDefinition",
        sa.Column("embedding_text_hash", sa.String(64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("MetricDefinition", "embedding_text_hash")
    op.drop_column("MetricDefinition", "embedding_model")
    op.drop_column("MetricDefinition", "embedding")

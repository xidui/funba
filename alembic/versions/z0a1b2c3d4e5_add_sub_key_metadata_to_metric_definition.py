"""add sub_key metadata to metric definition

Revision ID: z0a1b2c3d4e5
Revises: y9z0a1b2c3d4
Create Date: 2026-04-11 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "z0a1b2c3d4e5"
down_revision: Union[str, None] = "y9z0a1b2c3d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "MetricDefinition",
        sa.Column("sub_key_type", sa.String(16), nullable=True),
    )
    op.add_column(
        "MetricDefinition",
        sa.Column("sub_key_label", sa.String(64), nullable=True),
    )
    op.add_column(
        "MetricDefinition",
        sa.Column("sub_key_label_zh", sa.String(64), nullable=True),
    )
    op.add_column(
        "MetricDefinition",
        sa.Column("sub_key_rank_scope", sa.String(16), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("MetricDefinition", "sub_key_rank_scope")
    op.drop_column("MetricDefinition", "sub_key_label_zh")
    op.drop_column("MetricDefinition", "sub_key_label")
    op.drop_column("MetricDefinition", "sub_key_type")

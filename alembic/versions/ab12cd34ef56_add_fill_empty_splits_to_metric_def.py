"""add fill_missing_sub_keys_with_zero to metric definition

Revision ID: ab12cd34ef56
Revises: z0a1b2c3d4e5
Create Date: 2026-04-12 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "ab12cd34ef56"
down_revision: Union[str, None] = "z0a1b2c3d4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "MetricDefinition",
        sa.Column(
            "fill_missing_sub_keys_with_zero",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("MetricDefinition", "fill_missing_sub_keys_with_zero")

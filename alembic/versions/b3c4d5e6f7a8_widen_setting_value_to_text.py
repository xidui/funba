"""widen Setting.value to TEXT

Revision ID: b3c4d5e6f7a8
Revises: w3x4y5z6a7b8
Create Date: 2026-04-26 22:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b3c4d5e6f7a8"
down_revision: Union[str, None] = "w3x4y5z6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "Setting",
        "value",
        existing_type=sa.String(255),
        type_=sa.Text(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "Setting",
        "value",
        existing_type=sa.Text(),
        type_=sa.String(255),
        existing_nullable=False,
    )

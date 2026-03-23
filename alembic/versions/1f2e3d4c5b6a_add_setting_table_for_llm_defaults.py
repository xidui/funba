"""add_setting_table_for_llm_defaults

Revision ID: 1f2e3d4c5b6a
Revises: f4a5b6c7d8e9
Create Date: 2026-03-23

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '1f2e3d4c5b6a'
down_revision: Union[str, None] = 'f4a5b6c7d8e9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'Setting',
        sa.Column('key', sa.String(length=64), nullable=False),
        sa.Column('value', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('key'),
    )


def downgrade() -> None:
    op.drop_table('Setting')

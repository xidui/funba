"""add_player_bio_columns

Revision ID: d2e3f4a5b6c7
Revises: a1b2c3d4e5f7
Create Date: 2026-03-18

"""
from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = 'd2e3f4a5b6c7'
down_revision: Union[str, None] = 'a1b2c3d4e5f7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('Player', sa.Column('height', sa.String(10), nullable=True))
    op.add_column('Player', sa.Column('weight', sa.Integer(), nullable=True))
    op.add_column('Player', sa.Column('birth_date', sa.DATE(), nullable=True))
    op.add_column('Player', sa.Column('country', sa.String(50), nullable=True))
    op.add_column('Player', sa.Column('school', sa.String(100), nullable=True))
    op.add_column('Player', sa.Column('draft_year', sa.Integer(), nullable=True))
    op.add_column('Player', sa.Column('draft_round', sa.Integer(), nullable=True))
    op.add_column('Player', sa.Column('draft_number', sa.Integer(), nullable=True))
    op.add_column('Player', sa.Column('jersey', sa.String(10), nullable=True))
    op.add_column('Player', sa.Column('position', sa.String(30), nullable=True))
    op.add_column('Player', sa.Column('from_year', sa.Integer(), nullable=True))
    op.add_column('Player', sa.Column('to_year', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('Player', 'to_year')
    op.drop_column('Player', 'from_year')
    op.drop_column('Player', 'position')
    op.drop_column('Player', 'jersey')
    op.drop_column('Player', 'draft_number')
    op.drop_column('Player', 'draft_round')
    op.drop_column('Player', 'draft_year')
    op.drop_column('Player', 'school')
    op.drop_column('Player', 'country')
    op.drop_column('Player', 'birth_date')
    op.drop_column('Player', 'weight')
    op.drop_column('Player', 'height')

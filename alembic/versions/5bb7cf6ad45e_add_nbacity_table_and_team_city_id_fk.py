"""Add NbaCity table and Team.city_id FK

Revision ID: 5bb7cf6ad45e
Revises: v5w6x7y8z9a0
Create Date: 2026-04-10 13:03:38.865828

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '5bb7cf6ad45e'
down_revision: Union[str, None] = 'v5w6x7y8z9a0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('NbaCity',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('state', sa.String(length=50), nullable=True),
        sa.Column('country', sa.String(length=50), nullable=False),
        sa.Column('latitude', sa.Float(), nullable=False),
        sa.Column('longitude', sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'state', name='uq_NbaCity_name_state'),
    )
    op.add_column('Team', sa.Column('city_id', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_Team_city_id'), 'Team', ['city_id'], unique=False)
    op.create_foreign_key('fk_Team_city_id', 'Team', 'NbaCity', ['city_id'], ['id'])


def downgrade() -> None:
    op.drop_constraint('fk_Team_city_id', 'Team', type_='foreignkey')
    op.drop_index(op.f('ix_Team_city_id'), table_name='Team')
    op.drop_column('Team', 'city_id')
    op.drop_table('NbaCity')

"""add_pageview_table

Revision ID: f4a5b6c7d8e9
Revises: e3f4a5b6c7d8
Create Date: 2026-03-15

"""
from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = 'f4a5b6c7d8e9'
down_revision: Union[str, None] = 'e3f4a5b6c7d8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'PageView',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('visitor_id', sa.String(36), nullable=False),
        sa.Column('path', sa.String(500), nullable=False),
        sa.Column('referrer', sa.String(1000), nullable=True),
        sa.Column('user_agent', sa.String(500), nullable=True),
        sa.Column('ip_address', sa.String(45), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_PageView_visitor_id', 'PageView', ['visitor_id'])
    op.create_index('ix_PageView_created_at', 'PageView', ['created_at'])
    op.create_index('ix_PageView_visitor_id_created_at', 'PageView', ['visitor_id', 'created_at'])


def downgrade() -> None:
    op.drop_index('ix_PageView_visitor_id_created_at', table_name='PageView')
    op.drop_index('ix_PageView_created_at', table_name='PageView')
    op.drop_index('ix_PageView_visitor_id', table_name='PageView')
    op.drop_table('PageView')

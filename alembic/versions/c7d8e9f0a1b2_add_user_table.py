"""add_user_table

Revision ID: c7d8e9f0a1b2
Revises: f4a5b6c7d8e9
Create Date: 2026-03-15

"""
from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = 'c7d8e9f0a1b2'
down_revision: Union[str, None] = 'f4a5b6c7d8e9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'User',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('google_id', sa.String(128), nullable=False),
        sa.Column('email', sa.String(255), nullable=False),
        sa.Column('display_name', sa.String(255), nullable=False),
        sa.Column('avatar_url', sa.String(1024), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('last_login_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('google_id'),
        sa.UniqueConstraint('email'),
    )
    op.create_index('ix_User_google_id', 'User', ['google_id'])
    op.create_index('ix_User_email', 'User', ['email'])


def downgrade() -> None:
    op.drop_index('ix_User_email', table_name='User')
    op.drop_index('ix_User_google_id', table_name='User')
    op.drop_table('User')

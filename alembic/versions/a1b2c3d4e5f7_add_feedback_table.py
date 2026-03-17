"""add_feedback_table

Revision ID: a1b2c3d4e5f7
Revises: b9a8c7d6e5f4
Create Date: 2026-03-17

"""
from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = 'a1b2c3d4e5f7'
down_revision: Union[str, None] = 'b9a8c7d6e5f4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'Feedback',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.String(36), sa.ForeignKey('User.id'), nullable=False),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('page_url', sa.String(500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_Feedback_user_id', 'Feedback', ['user_id'])
    op.create_index('ix_Feedback_created_at', 'Feedback', ['created_at'])


def downgrade() -> None:
    op.drop_index('ix_Feedback_created_at', table_name='Feedback')
    op.drop_index('ix_Feedback_user_id', table_name='Feedback')
    op.drop_table('Feedback')

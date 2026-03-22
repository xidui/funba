"""add magic link auth

Revision ID: 30b6729a23b7
Revises: da6b6933fc31
Create Date: 2026-03-21 22:01:47.215942

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '30b6729a23b7'
down_revision: Union[str, None] = 'da6b6933fc31'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Make google_id nullable for email-only users
    op.alter_column('User', 'google_id', existing_type=sa.String(128), nullable=True)

    # Create MagicToken table
    op.create_table(
        'MagicToken',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('token', sa.String(64), nullable=False, unique=True, index=True),
        sa.Column('email', sa.String(255), nullable=False, index=True),
        sa.Column('expires_at', sa.DateTime, nullable=False),
        sa.Column('used', sa.Boolean, nullable=False, default=False),
        sa.Column('next_url', sa.String(1024), nullable=True),
        sa.Column('created_at', sa.DateTime, nullable=False),
    )


def downgrade() -> None:
    op.drop_table('MagicToken')
    op.alter_column('User', 'google_id', existing_type=sa.String(128), nullable=False)

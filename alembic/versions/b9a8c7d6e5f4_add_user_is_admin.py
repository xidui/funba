"""add_user_is_admin

Revision ID: b9a8c7d6e5f4
Revises: c7d8e9f0a1b2
Create Date: 2026-03-16

"""
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b9a8c7d6e5f4'
down_revision: Union[str, None] = 'c7d8e9f0a1b2'
branch_labels = None
depends_on = None


user_table = sa.table(
    'User',
    sa.column('email', sa.String(length=255)),
    sa.column('is_admin', sa.Boolean()),
)


def upgrade() -> None:
    op.add_column('User', sa.Column('is_admin', sa.Boolean(), nullable=False, server_default=sa.false()))
    op.execute(
        user_table.update()
        .where(user_table.c.email == 'yuewang9269@gmail.com')
        .values(is_admin=True)
    )
    op.alter_column('User', 'is_admin', server_default=None)


def downgrade() -> None:
    op.drop_column('User', 'is_admin')

"""add signed_using and guaranteed_at_sign to PlayerContract

Revision ID: v2w3x4y5z6a7
Revises: u1v2w3x4y5z6
Create Date: 2026-04-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "v2w3x4y5z6a7"
down_revision: Union[str, None] = "u1v2w3x4y5z6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("PlayerContract", sa.Column("guaranteed_at_sign_usd", sa.BigInteger, nullable=True))
    op.add_column("PlayerContract", sa.Column("signed_using", sa.String(64), nullable=True))


def downgrade() -> None:
    op.drop_column("PlayerContract", "signed_using")
    op.drop_column("PlayerContract", "guaranteed_at_sign_usd")

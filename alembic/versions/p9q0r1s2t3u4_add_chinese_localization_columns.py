"""add chinese localization columns

Revision ID: p9q0r1s2t3u4
Revises: o8p9q0r1s2t3
Create Date: 2026-03-31 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "p9q0r1s2t3u4"
down_revision: Union[str, None] = "o8p9q0r1s2t3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("MetricDefinition", sa.Column("name_zh", sa.String(length=128), nullable=True))
    op.add_column("MetricDefinition", sa.Column("description_zh", sa.Text(), nullable=True))
    op.add_column("Team", sa.Column("full_name_zh", sa.String(length=100), nullable=True))
    op.add_column("Player", sa.Column("full_name_zh", sa.String(length=100), nullable=True))


def downgrade() -> None:
    op.drop_column("Player", "full_name_zh")
    op.drop_column("Team", "full_name_zh")
    op.drop_column("MetricDefinition", "description_zh")
    op.drop_column("MetricDefinition", "name_zh")

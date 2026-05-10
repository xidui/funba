"""add eligibility notes to metric definition

Revision ID: i8j9k0l1m2n3
Revises: h8i9j0k1l2m3
Create Date: 2026-05-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "i8j9k0l1m2n3"
down_revision: Union[str, None] = "h8i9j0k1l2m3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "MetricDefinition",
        sa.Column("eligibility_note", sa.String(length=512), nullable=True),
    )
    op.add_column(
        "MetricDefinition",
        sa.Column("eligibility_note_zh", sa.String(length=512), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("MetricDefinition", "eligibility_note_zh")
    op.drop_column("MetricDefinition", "eligibility_note")

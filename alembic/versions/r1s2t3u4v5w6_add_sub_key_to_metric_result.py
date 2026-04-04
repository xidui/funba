"""add sub_key to metric result

Revision ID: r1s2t3u4v5w6
Revises: q0r1s2t3u4v5
Create Date: 2026-04-03 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "r1s2t3u4v5w6"
down_revision: Union[str, None] = "q0r1s2t3u4v5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "MetricResult",
        sa.Column("sub_key", sa.String(64), nullable=False, server_default=""),
    )
    op.drop_index("uq_MetricResult_key_entity_season", table_name="MetricResult")
    op.create_index(
        "uq_MetricResult_key_entity_season_subkey",
        "MetricResult",
        ["metric_key", "entity_type", "entity_id", "season", "sub_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_MetricResult_key_entity_season_subkey", table_name="MetricResult")
    op.create_index(
        "uq_MetricResult_key_entity_season",
        "MetricResult",
        ["metric_key", "entity_type", "entity_id", "season"],
        unique=True,
    )
    op.drop_column("MetricResult", "sub_key")

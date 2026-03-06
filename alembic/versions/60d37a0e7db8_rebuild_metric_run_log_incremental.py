"""rebuild_metric_run_log_incremental

Revision ID: 60d37a0e7db8
Revises: fb97452a2ed6
Create Date: 2026-03-06 06:06:30.401501

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '60d37a0e7db8'
down_revision: Union[str, None] = 'fb97452a2ed6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # MetricRunLog: drop and recreate with new PK (entity_type, entity_id, season)
    # and delta_json column. Old rows are incompatible — clear MetricResult too
    # since context_json now stores running totals and needs a clean start.
    op.execute("TRUNCATE TABLE MetricResult")
    op.drop_table("MetricRunLog")
    op.create_table(
        "MetricRunLog",
        sa.Column("game_id",        sa.String(20),  nullable=False),
        sa.Column("metric_key",     sa.String(64),  nullable=False),
        sa.Column("entity_type",    sa.String(16),  nullable=False),
        sa.Column("entity_id",      sa.String(50),  nullable=False),
        sa.Column("season",         sa.String(10),  nullable=False),
        sa.Column("computed_at",    sa.DateTime(),  nullable=False),
        sa.Column("produced_result",sa.Boolean(),   nullable=False, server_default=sa.true()),
        sa.Column("delta_json",     sa.Text(),      nullable=True),
        sa.PrimaryKeyConstraint("game_id", "metric_key", "entity_type", "entity_id", "season"),
    )


def downgrade() -> None:
    op.drop_table("MetricRunLog")
    op.create_table(
        "MetricRunLog",
        sa.Column("game_id",        sa.String(20),  nullable=False),
        sa.Column("metric_key",     sa.String(64),  nullable=False),
        sa.Column("computed_at",    sa.DateTime(),  nullable=False),
        sa.Column("produced_result",sa.Boolean(),   nullable=False, server_default=sa.true()),
        sa.PrimaryKeyConstraint("game_id", "metric_key"),
    )

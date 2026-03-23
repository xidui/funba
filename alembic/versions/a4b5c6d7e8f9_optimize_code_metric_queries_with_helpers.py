"""optimize code metric queries with helpers

Revision ID: a4b5c6d7e8f9
Revises: f9a0b1c2d3e4
Create Date: 2026-03-22 21:40:00.000000
"""
from __future__ import annotations

from datetime import datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from metrics.framework.code_optimizer import optimize_metric_code


revision: str = "a4b5c6d7e8f9"
down_revision: Union[str, Sequence[str], None] = "f9a0b1c2d3e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    metadata = sa.MetaData()
    metric_definition = sa.Table(
        "MetricDefinition",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("key", sa.String(64)),
        sa.Column("source_type", sa.String(16)),
        sa.Column("code_python", sa.Text),
        sa.Column("updated_at", sa.DateTime),
    )

    rows = list(
        bind.execute(
            sa.select(
                metric_definition.c.id,
                metric_definition.c.code_python,
            ).where(
                metric_definition.c.source_type == "code",
                metric_definition.c.code_python.isnot(None),
            )
        )
    )
    now = datetime.utcnow()
    for row in rows:
        optimized = optimize_metric_code(row.code_python)
        if optimized == row.code_python:
            continue
        bind.execute(
            metric_definition.update()
            .where(metric_definition.c.id == row.id)
            .values(code_python=optimized, updated_at=now)
        )


def downgrade() -> None:
    # Irreversible optimization pass; keep optimized code in place.
    pass

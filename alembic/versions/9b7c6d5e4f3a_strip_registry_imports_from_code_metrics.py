"""strip registry imports from code metrics

Revision ID: 9b7c6d5e4f3a
Revises: 8c1b2d3e4f5a
Create Date: 2026-03-21 13:25:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9b7c6d5e4f3a"
down_revision = "8c1b2d3e4f5a"
branch_labels = None
depends_on = None


def _clean_code(code: str | None) -> str | None:
    if not code:
        return code
    cleaned_lines = []
    for line in code.splitlines():
        stripped = line.strip()
        if stripped.startswith("from metrics.framework.registry import register"):
            continue
        if stripped.startswith("import metrics.framework.registry"):
            continue
        if stripped.startswith("register("):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).rstrip() + "\n"


def upgrade() -> None:
    bind = op.get_bind()
    metadata = sa.MetaData()
    metric_definition = sa.Table(
        "MetricDefinition",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("source_type", sa.String(16)),
        sa.Column("code_python", sa.Text),
    )

    rows = list(
        bind.execute(
            sa.select(metric_definition.c.id, metric_definition.c.code_python)
            .where(metric_definition.c.source_type == "code")
        ).mappings()
    )
    for row in rows:
        cleaned = _clean_code(row["code_python"])
        if cleaned != row["code_python"]:
            bind.execute(
                metric_definition.update()
                .where(metric_definition.c.id == row["id"])
                .values(code_python=cleaned)
            )


def downgrade() -> None:
    # Irreversible cleanup. Old registry imports are intentionally not restored.
    return None

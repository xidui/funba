"""Upgrade saved OpenAI defaults while preserving their workload tiers.

Revision ID: j8k9l0m1n2o3
Revises: i8j9k0l1m2n3
Create Date: 2026-09-06
"""

from alembic import op
import sqlalchemy as sa


revision = "j8k9l0m1n2o3"
down_revision = "i8j9k0l1m2n3"
branch_labels = None
depends_on = None

MODEL_KEYS = (
    "default_llm_model",
    "llm_model_search",
    "llm_model_generate",
    "llm_model_curator",
)


def _replace_models(replacements: dict[str, str]) -> None:
    settings = sa.table(
        "Setting",
        sa.column("key", sa.String),
        sa.column("value", sa.String),
        sa.column("updated_at", sa.DateTime),
    )
    for old, new in replacements.items():
        op.execute(
            settings.update()
            .where(settings.c.key.in_(MODEL_KEYS), settings.c.value == old)
            .values(value=new, updated_at=sa.func.now())
        )


def upgrade() -> None:
    _replace_models({
        "gpt-5.5": "gpt-6-astra",
        "gpt-5.4": "gpt-6-astra",
        "gpt-5.4-mini": "gpt-5.6-terra",
        "gpt-5.4-nano": "gpt-5.6-luna",
    })


def downgrade() -> None:
    # Both previous flagship defaults return to the supported GPT-5.4 tier.
    _replace_models({
        "gpt-6-astra": "gpt-5.4",
        "gpt-5.6-terra": "gpt-5.4-mini",
        "gpt-5.6-luna": "gpt-5.4-nano",
    })

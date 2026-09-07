"""Use GPT-6 for every default tier, with task-specific reasoning.

Revision ID: k8l9m0n1o2p3
Revises: j8k9l0m1n2o3
"""
from alembic import op
import sqlalchemy as sa

revision = "k8l9m0n1o2p3"
down_revision = "j8k9l0m1n2o3"
branch_labels = None
depends_on = None

settings = sa.table(
    "Setting",
    sa.column("key", sa.String),
    sa.column("value", sa.String),
    sa.column("updated_at", sa.DateTime),
)


def upgrade() -> None:
    op.execute(
        settings.update().where(
            settings.c.key.in_(("default_llm_model", "llm_model_search", "llm_model_generate", "llm_model_curator")),
            settings.c.value.in_(("gpt-5.6-terra", "gpt-5.6-luna")),
        ).values(value="gpt-6-astra", updated_at=sa.func.now())
    )
    # The preceding deployment saved 'none'; Astra curation now defaults to medium.
    connection = op.get_bind()
    curator = connection.execute(sa.select(settings.c.value).where(settings.c.key == "llm_model_curator")).scalar()
    if curator == "gpt-6-astra":
        op.execute(settings.update().where(
            settings.c.key == "llm_reasoning_curator", settings.c.value == "none",
        ).values(value="medium", updated_at=sa.func.now()))


def downgrade() -> None:
    # Restore the preceding deployment's small and medium model defaults.
    for key, model in (("default_llm_model", "gpt-5.6-terra"), ("llm_model_search", "gpt-5.6-luna")):
        op.execute(settings.update().where(
            settings.c.key == key, settings.c.value == "gpt-6-astra",
        ).values(value=model, updated_at=sa.func.now()))

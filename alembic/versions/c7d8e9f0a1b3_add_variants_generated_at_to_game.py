"""add variants_generated_at to Game

Idempotency stamp for the hero-card variant + poster pipeline. Once
run_curator_for_game has successfully generated the variants, the column
is set and a re-trigger of the same game's curator becomes a fast no-op
on this stage. Without it, every curator re-run (manual admin trigger,
worker SIGKILL during deploy, OOM-redelivery, …) produces a duplicate
SocialPost row per metric.

Revision ID: c7d8e9f0a1b3
Revises: b3c4d5e6f7a8
Create Date: 2026-04-27 00:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c7d8e9f0a1b3"
down_revision: Union[str, None] = "b3c4d5e6f7a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "Game",
        sa.Column("variants_generated_at", sa.DateTime(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("Game", "variants_generated_at")

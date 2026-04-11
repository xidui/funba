"""add_slug_to_player

Revision ID: d4fc0aa933d0
Revises: w6x7y8z9a0b
Create Date: 2026-04-10 23:43:24.228702

"""
from typing import Sequence, Union
import re
import unicodedata

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = 'd4fc0aa933d0'
down_revision: Union[str, None] = 'w6x7y8z9a0b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _slugify(name: str) -> str:
    """Convert a player name to a URL slug."""
    # Normalize unicode (accents → base chars)
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    # Replace non-alphanumeric with hyphens
    s = re.sub(r"[^a-z0-9]+", "-", s)
    # Strip leading/trailing hyphens
    s = s.strip("-")
    return s or "player"


def upgrade() -> None:
    op.add_column("Player", sa.Column("slug", sa.String(150), nullable=True))

    # Backfill slugs
    conn = op.get_bind()
    rows = conn.execute(text("SELECT player_id, full_name, from_year FROM Player WHERE full_name IS NOT NULL")).fetchall()

    # Build slugs and detect duplicates
    slug_counts: dict[str, list] = {}
    for player_id, full_name, from_year in rows:
        slug = _slugify(full_name)
        slug_counts.setdefault(slug, []).append((player_id, from_year))

    used_slugs: set[str] = set()
    for slug, players in slug_counts.items():
        if len(players) == 1:
            pid, _ = players[0]
            used_slugs.add(slug)
            conn.execute(text("UPDATE Player SET slug = :slug WHERE player_id = :pid"), {"slug": slug, "pid": pid})
        else:
            for pid, from_year in players:
                candidate = f"{slug}-{from_year}" if from_year else f"{slug}-{pid}"
                if candidate in used_slugs:
                    candidate = f"{slug}-{pid}"
                used_slugs.add(candidate)
                conn.execute(text("UPDATE Player SET slug = :slug WHERE player_id = :pid"), {"slug": candidate, "pid": pid})

    # Players without full_name get slug from player_id
    conn.execute(text("UPDATE Player SET slug = CONCAT('player-', player_id) WHERE slug IS NULL"))

    op.create_unique_constraint("uq_player_slug", "Player", ["slug"])
    op.create_index("ix_player_slug", "Player", ["slug"])


def downgrade() -> None:
    op.drop_index("ix_player_slug", "Player")
    op.drop_constraint("uq_player_slug", "Player", type_="unique")
    op.drop_column("Player", "slug")

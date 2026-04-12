"""add_slug_to_team

Revision ID: y9z0a1b2c3d4
Revises: x7y8z9a0b1c2
Create Date: 2026-04-11

"""
from typing import Sequence, Union
import re
import unicodedata

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


revision: str = "y9z0a1b2c3d4"
down_revision: Union[str, None] = "x7y8z9a0b1c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _slugify(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "team"


def upgrade() -> None:
    op.add_column("Team", sa.Column("slug", sa.String(150), nullable=True))

    conn = op.get_bind()
    rows = conn.execute(
        text(
            "SELECT team_id, full_name, start_season, COALESCE(is_legacy, 0) AS is_legacy "
            "FROM Team WHERE full_name IS NOT NULL"
        )
    ).fetchall()

    slug_groups: dict[str, list] = {}
    for team_id, full_name, start_season, is_legacy in rows:
        slug = _slugify(full_name)
        slug_groups.setdefault(slug, []).append((team_id, start_season, bool(is_legacy)))

    used_slugs: set[str] = set()
    for slug, teams in slug_groups.items():
        if len(teams) == 1:
            tid, _, _ = teams[0]
            used_slugs.add(slug)
            conn.execute(
                text("UPDATE Team SET slug = :slug WHERE team_id = :tid"),
                {"slug": slug, "tid": tid},
            )
        else:
            # Active (non-legacy) teams get the clean slug; legacy teams get suffixes.
            teams_sorted = sorted(teams, key=lambda row: (row[2], row[0]))
            primary_assigned = False
            for tid, start_season, is_legacy in teams_sorted:
                if not primary_assigned:
                    candidate = slug
                    primary_assigned = True
                else:
                    candidate = f"{slug}-{start_season}" if start_season else f"{slug}-{tid}"
                    if candidate in used_slugs:
                        candidate = f"{slug}-{tid}"
                used_slugs.add(candidate)
                conn.execute(
                    text("UPDATE Team SET slug = :slug WHERE team_id = :tid"),
                    {"slug": candidate, "tid": tid},
                )

    conn.execute(
        text("UPDATE Team SET slug = CONCAT('team-', team_id) WHERE slug IS NULL")
    )

    op.create_unique_constraint("uq_team_slug", "Team", ["slug"])
    op.create_index("ix_team_slug", "Team", ["slug"])


def downgrade() -> None:
    op.drop_index("ix_team_slug", "Team")
    op.drop_constraint("uq_team_slug", "Team", type_="unique")
    op.drop_column("Team", "slug")

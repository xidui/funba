"""add_slug_to_game

Revision ID: 5f68073ae20d
Revises: d4fc0aa933d0
Create Date: 2026-04-10 23:56:22.501922

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = '5f68073ae20d'
down_revision: Union[str, None] = 'd4fc0aa933d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("Game", sa.Column("slug", sa.String(100), nullable=True))

    conn = op.get_bind()

    # Build team_id → abbr map
    team_rows = conn.execute(text("SELECT team_id, abbr FROM Team")).fetchall()
    team_abbr = {r[0]: (r[1] or "UNK").lower() for r in team_rows}

    # Backfill slugs: YYYYMMDD-away-home
    games = conn.execute(text(
        "SELECT game_id, game_date, home_team_id, road_team_id FROM Game"
    )).fetchall()

    used: set[str] = set()
    for game_id, game_date, home_id, road_id in games:
        if game_date and home_id and road_id:
            date_str = game_date.strftime("%Y%m%d") if hasattr(game_date, "strftime") else str(game_date).replace("-", "")
            slug = f"{date_str}-{team_abbr.get(road_id, 'unk')}-{team_abbr.get(home_id, 'unk')}"
        else:
            slug = f"game-{game_id}"
        if slug in used:
            slug = f"{slug}-{game_id}"
        used.add(slug)
        conn.execute(text("UPDATE Game SET slug = :slug WHERE game_id = :gid"), {"slug": slug, "gid": game_id})

    op.create_unique_constraint("uq_game_slug", "Game", ["slug"])
    op.create_index("ix_game_slug", "Game", ["slug"])


def downgrade() -> None:
    op.drop_index("ix_game_slug", "Game")
    op.drop_constraint("uq_game_slug", "Game", type_="unique")
    op.drop_column("Game", "slug")

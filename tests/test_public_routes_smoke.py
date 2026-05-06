"""Smoke tests for public-facing routes.

Loads the real Flask app, picks one player/team/game/metric out of the
configured DB, and hits every major public URL with the test client.
A 5xx (or unhandled exception with TESTING=True) fails the test —
catching the kind of regression where a refactor breaks a page that
nobody actively unit-tests.

Skips the entire module when no usable DB is reachable, so CI without
MySQL stays green. To run locally:

    NBA_DB_URL=mysql+pymysql://root@localhost/nba_data \\
        .venv/bin/pytest tests/test_public_routes_smoke.py -v
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

import web.app as web_app
from db.models import Game, MetricDefinition, Player, Team


def _db_reachable() -> bool:
    try:
        with web_app.SessionLocal() as session:
            session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_reachable(),
    reason="DB unavailable — set NBA_DB_URL to a populated MySQL to run smoke tests",
)


@pytest.fixture(scope="module")
def client():
    web_app.app.config["TESTING"] = True
    return web_app.app.test_client()


@pytest.fixture(scope="module")
def sample_ids():
    """Pick one slug/key per detail route from the live DB."""
    with web_app.SessionLocal() as s:
        player_slug = (
            s.query(Player.slug)
            .filter(Player.slug.isnot(None), Player.is_active.is_(True))
            .order_by(Player.player_id)
            .limit(1)
            .scalar()
        )
        team_slug = (
            s.query(Team.slug)
            .filter(Team.slug.isnot(None))
            .order_by(Team.team_id)
            .limit(1)
            .scalar()
        )
        game_slug = (
            s.query(Game.slug)
            .filter(Game.slug.isnot(None))
            .order_by(Game.game_date.desc())
            .limit(1)
            .scalar()
        )
        metric_key = (
            s.query(MetricDefinition.key)
            .order_by(MetricDefinition.id)
            .limit(1)
            .scalar()
        )
    if not all([player_slug, team_slug, game_slug, metric_key]):
        pytest.skip("DB has no fixture rows for player/team/game/metric")
    return {
        "player_slug": player_slug,
        "team_slug": team_slug,
        "game_slug": game_slug,
        "metric_key": metric_key,
    }


STATIC_PATHS = [
    "/",
    "/metrics",
    "/players",
    "/teams",
    "/games",
    "/news",
    "/awards",
    "/players/compare",
    # Chinese mirrors
    "/cn/",
    "/cn/metrics",
    "/cn/players",
    "/cn/teams",
    "/cn/games",
    "/cn/news",
    "/cn/awards",
    "/cn/players/compare",
]


@pytest.mark.parametrize("path", STATIC_PATHS)
def test_static_route_renders(client, path):
    resp = client.get(path)
    assert resp.status_code in (200, 302), (
        f"GET {path} -> {resp.status_code}\n"
        f"body[:600]: {resp.data[:600].decode('utf-8', errors='replace')}"
    )


def _detail_paths(ids):
    return [
        f"/players/{ids['player_slug']}",
        f"/teams/{ids['team_slug']}",
        f"/games/{ids['game_slug']}",
        f"/metrics/{ids['metric_key']}",
        f"/cn/players/{ids['player_slug']}",
        f"/cn/teams/{ids['team_slug']}",
        f"/cn/games/{ids['game_slug']}",
        f"/cn/metrics/{ids['metric_key']}",
    ]


def test_detail_routes_render(client, sample_ids):
    failures = []
    for path in _detail_paths(sample_ids):
        resp = client.get(path)
        if resp.status_code not in (200, 302):
            failures.append(
                f"GET {path} -> {resp.status_code}: "
                f"{resp.data[:300].decode('utf-8', errors='replace')}"
            )
    assert not failures, "\n".join(failures)

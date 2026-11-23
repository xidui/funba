"""Tests for the opt-in "Next" UI (web/next_ui.py + templates/next/).

Pure helpers run everywhere. The route smoke tests load the real app and
need a reachable DB, so they follow the same skip rules as
test_public_routes_smoke.py.
"""
from __future__ import annotations

import os

import pytest

from web import next_ui


# ── Pure helpers ─────────────────────────────────────────────────────────────

def test_analyze_flow_counts_lead_changes_ties_runs_and_largest_leads():
    progression = [
        {"t": 0, "home": 0, "road": 0},
        {"t": 1, "home": 2, "road": 0},   # home leads
        {"t": 2, "home": 2, "road": 3},   # road leads → change 1
        {"t": 3, "home": 3, "road": 3},   # tie
        {"t": 4, "home": 6, "road": 3},   # home leads → change 2, run 3 continues (1+3)
        {"t": 5, "home": 8, "road": 3},   # home run reaches 6 (1 + 3 + 2)
        {"t": 6, "home": 8, "road": 5},
    ]
    flow = next_ui.analyze_flow(progression)
    assert flow["lead_changes"] == 2
    assert flow["times_tied"] == 1
    assert flow["largest_lead"] == {"home": 5, "road": 1}
    assert flow["best_run"]["team"] == "home"
    assert flow["best_run"]["pts"] == 6


def test_matchup_colors_are_distinguishable_for_similar_primaries():
    # PHI and NYK share #006BB6 as their primary blue.
    road, home = next_ui.matchup_colors("1610612752", "1610612755")
    assert road != home
    assert next_ui._color_distance(road, home) >= 170


def test_matchup_colors_unknown_teams_fall_back_to_neutral():
    road, home = next_ui.matchup_colors("legacy-1", "legacy-2")
    assert home == next_ui.NEUTRAL_COLOR


def test_ink_on_picks_dark_text_for_light_colors():
    assert next_ui.ink_on("#FDBB30") == "#141414"   # CLE gold
    assert next_ui.ink_on("#1D428A") == "#ffffff"   # GSW blue


def test_milestone_sentence_rank_crossing_and_threshold():
    team_lookup = lambda _tid: {"name": "Spurs", "abbr": "SAS"}
    en, zh = next_ui._milestone_sentence(
        {
            "event_type": "rank_crossing",
            "metric_name": "25-10 Games",
            "metric_name_zh": "25+10比赛",
            "season": "all_regular",
            "rank": 23,
            "value_num": 224.0,
            "passed": [{"name_en": "Dirk Nowitzki", "name_zh": "诺维茨基"}],
            "context_json": {"player": {"name_en": "Joel Embiid", "name_zh": "恩比德"}},
        },
        team_lookup,
    )
    assert en == "Joel Embiid climbs to No. 23 in 25-10 Games (career), passing Dirk Nowitzki"
    assert "超越诺维茨基" in zh

    en, _ = next_ui._milestone_sentence(
        {
            "event_type": "absolute_threshold",
            "metric_name": "Wins By 10+",
            "season": "22026",
            "rank": 1,
            "value_num": 10.0,
            "entity_id": "1610612759",
            "context_json": {"count_reached_before_this_game": 0},
        },
        team_lookup,
    )
    assert en == "Spurs reaches 10 Wins By 10+ this season — first in the league"


def test_margin_series_downsamples_pbp_scores():
    class Row:
        def __init__(self, period, clock, score):
            self.period, self.pc_time, self.score = period, clock, score

    rows = [Row(1, "11:00", "2 - 0"), Row(2, "6:00", "30 - 35"), Row(4, "0:00", "100 - 98")]
    series = next_ui._margin_series(rows, buckets=8)
    assert len(series) == 9
    assert series[0] == 0
    assert series[-1] == 2           # final margin (home - road)
    assert min(series) == -5         # road led by 5 in Q2


def test_reset_after_fork_clears_inflight_refresh_markers():
    # Simulate a fork that happened while a refresh thread held the lock and
    # had its key marked in flight.
    next_ui._refreshing.add("metric_count")
    held = next_ui._cache_lock
    held.acquire()
    try:
        next_ui._reset_after_fork()
        assert "metric_count" not in next_ui._refreshing
        assert next_ui._cache_lock is not held
        assert next_ui._cache_lock.acquire(blocking=False)
        next_ui._cache_lock.release()
    finally:
        held.release()


def test_swr_cache_refreshes_after_fork_reset():
    key = "test-swr-fork"
    next_ui._cache.pop(key, None)
    next_ui._refreshing.add(key)          # stale marker inherited from a parent
    next_ui._reset_after_fork()
    assert next_ui._cached_swr(key, 60, lambda: 42) == 42


# ── Route smoke tests (need the real DB) ─────────────────────────────────────

web_app = pytest.importorskip("web.app")


def _db_reachable() -> bool:
    try:
        from sqlalchemy import text

        with web_app.SessionLocal() as session:
            session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


needs_db = pytest.mark.skipif(
    os.getenv("CI", "").lower() == "true" or not _db_reachable(),
    reason="Skipped in CI / when NBA_DB_URL points at no reachable MySQL",
)


def _db_models_stubbed() -> bool:
    """Other test modules swap ``db.models`` for MagicMock stubs at import time,
    which breaks any real-DB rendering later in the same session."""
    import sys

    models = sys.modules.get("db.models")
    return models is None or not hasattr(getattr(models, "Game", None), "__table__")


@pytest.fixture(scope="module")
def client():
    if _db_models_stubbed():
        pytest.skip("db.models is stubbed by another test module in this session")
    web_app.app.config["TESTING"] = True
    return web_app.app.test_client()


@needs_db
def test_classic_is_default_and_next_is_opt_in(client, monkeypatch):
    monkeypatch.delenv("FUNBA_UI_DEFAULT", raising=False)
    client.delete_cookie(next_ui.UI_COOKIE)
    classic = client.get("/")
    assert classic.status_code == 200
    assert b'class="masthead"' not in classic.data

    nxt = client.get("/?ui=next")
    assert nxt.status_code == 200
    assert b'class="masthead"' in nxt.data
    assert f"{next_ui.UI_COOKIE}=next" in nxt.headers.get("Set-Cookie", "")


@needs_db
def test_ui_switch_route_sets_cookie_and_redirects_same_site(client):
    resp = client.get("/ui/next?next=/games%3Fui%3Dclassic")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/games")
    assert f"{next_ui.UI_COOKIE}=next" in resp.headers.get("Set-Cookie", "")

    offsite = client.get("/ui/classic?next=https://evil.example/x")
    assert offsite.status_code == 302
    assert "evil.example" not in offsite.headers["Location"]


@needs_db
def test_next_search_api_returns_grouped_results(client):
    resp = client.get("/api/next/search?q=war")
    assert resp.status_code == 200
    payload = resp.get_json()
    keys = [g["key"] for g in payload["groups"]]
    assert "ask" in keys
    for group in payload["groups"]:
        for item in group["items"]:
            assert item["url"].startswith("/")


NEXT_PATHS = ["/", "/cn/", "/games", "/players", "/teams", "/metrics", "/cn/metrics"]


@needs_db
@pytest.mark.parametrize("path", NEXT_PATHS)
def test_next_list_pages_render(client, path):
    sep = "&" if "?" in path else "?"
    resp = client.get(f"{path}{sep}ui=next")
    assert resp.status_code in (200, 302), resp.data[:600]


@needs_db
def test_next_detail_pages_render(client):
    from db.models import Game, MetricDefinition, Player, Team

    with web_app.SessionLocal() as s:
        game_slug = (
            s.query(Game.slug)
            .filter(Game.slug.isnot(None), Game.game_status == "completed")
            .order_by(Game.game_date.desc())
            .limit(1)
            .scalar()
        )
        player_slug = s.query(Player.slug).filter(Player.slug.isnot(None), Player.is_active.is_(True)).limit(1).scalar()
        team_slug = s.query(Team.slug).filter(Team.slug.isnot(None), Team.active.is_(True)).limit(1).scalar()
        metric_key = s.query(MetricDefinition.key).filter(MetricDefinition.status == "published").limit(1).scalar()
    paths = [
        f"/games/{game_slug}",
        f"/games/{game_slug}/fragment/metrics",
        f"/players/{player_slug}",
        f"/teams/{team_slug}",
        f"/metrics/{metric_key}",
        f"/cn/games/{game_slug}",
    ]
    failures = []
    for path in paths:
        resp = client.get(path + ("&" if "?" in path else "?") + "ui=next")
        if resp.status_code >= 500:
            failures.append((path, resp.status_code))
    assert not failures, failures

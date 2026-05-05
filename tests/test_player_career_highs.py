from datetime import date
from types import SimpleNamespace

from web.detail_routes import _build_player_career_highs


def _stat(game_id, **overrides):
    base = {
        "game_id": game_id,
        "team_id": "TEAM",
        "pts": 0,
        "reb": 0,
        "ast": 0,
        "stl": 0,
        "blk": 0,
        "fg3m": 0,
        "fgm": 0,
        "fga": 0,
        "fg3a": 0,
        "ftm": 0,
        "fta": 0,
        "min": 0,
        "sec": 0,
        "plus": 0,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _game(game_id, **overrides):
    base = {
        "game_id": game_id,
        "slug": f"game-{game_id}",
        "season": "22025",
        "game_date": date(2026, 1, int(game_id[-1])),
        "home_team_id": "TEAM",
        "road_team_id": "OPP",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_build_player_career_highs_adds_common_box_score_highs():
    rows = [
        (
            _stat(
                "g1",
                pts=30,
                reb=4,
                ast=3,
                fgm=10,
                fga=17,
                min=29,
                sec=5,
                plus=4,
            ),
            _game("g1"),
        ),
        (
            _stat(
                "g2",
                pts=20,
                reb=15,
                ast=8,
                stl=3,
                blk=2,
                fg3m=4,
                fgm=9,
                fga=16,
                fg3a=7,
                ftm=6,
                fta=8,
                min=35,
                sec=12,
                plus=12,
            ),
            _game("g2"),
        ),
    ]

    highs = _build_player_career_highs(
        rows,
        teams={"OPP": SimpleNamespace(abbr="OPP")},
        team_abbr_fn=lambda teams, tid: teams[tid].abbr,
        fmt_date_fn=lambda value: value.isoformat(),
        localized_url_for_fn=lambda endpoint, **kwargs: f"/{endpoint}/{kwargs['slug']}",
    )

    by_field = {item["field"]: item for item in highs}

    assert by_field["pts"]["value_display"] == "30"
    assert by_field["pts"]["href"] == "/game_page/game-g1"
    assert by_field["reb"]["value_display"] == "15"
    assert by_field["fgm"]["value_display"] == "10"
    assert by_field["fg3a"]["value_display"] == "7"
    assert by_field["fta"]["value_display"] == "8"
    assert by_field["min"]["value_display"] == "35:12"
    assert by_field["plus"]["value_display"] == "12"
    assert by_field["plus"]["opponent_abbr"] == "OPP"


def test_build_player_career_highs_skips_zero_only_stats():
    rows = [
        (
            _stat("g1", pts=8, min=12),
            _game("g1"),
        )
    ]

    highs = _build_player_career_highs(
        rows,
        teams={"OPP": SimpleNamespace(abbr="OPP")},
        team_abbr_fn=lambda teams, tid: teams[tid].abbr,
        fmt_date_fn=lambda value: value.isoformat(),
        localized_url_for_fn=lambda endpoint, **kwargs: f"/{endpoint}/{kwargs['slug']}",
    )

    fields = {item["field"] for item in highs}
    assert "pts" in fields
    assert "blk" not in fields
    assert "plus" not in fields

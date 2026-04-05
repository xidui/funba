from metrics.framework.runtime import load_code_metric

from db.bulk_insert_metrics_xix651 import (
    DEFAULT_LIMIT,
    UNSUPPORTED_CANDIDATES,
    build_metric_specs,
    validate_specs,
)
from types import SimpleNamespace


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, rows_by_entity_name):
        self._rows_by_entity_name = rows_by_entity_name

    def query(self, *entities):
        if not entities:
            raise AssertionError("query() requires at least one entity")
        key = tuple(getattr(entity, "__name__", str(entity)) for entity in entities)
        if len(key) == 1:
            rows = self._rows_by_entity_name.get(key[0], [])
        else:
            rows = self._rows_by_entity_name.get(key, [])
        return _FakeQuery(rows)


def test_bulk_insert_catalog_is_large_and_capped():
    specs = build_metric_specs()

    assert len(specs) >= 200
    assert sum(1 for spec in specs if spec.supports_career) >= 180
    assert all(spec.max_results_per_season == DEFAULT_LIMIT for spec in specs)
    assert len({spec.key for spec in specs}) == len(specs)


def test_bulk_insert_catalog_loads_all_generated_metrics():
    specs = build_metric_specs()

    validate_specs(specs)


def test_bulk_insert_catalog_tracks_unsupported_candidates():
    assert len(UNSUPPORTED_CANDIDATES) >= 3
    assert all(item["key"] and item["reason"] for item in UNSUPPORTED_CANDIDATES)


def test_player_box_metrics_define_required_career_aggregation_keys():
    specs_by_key = {spec.key: spec for spec in build_metric_specs()}

    ten_plus_points = load_code_metric(specs_by_key["ten_plus_point_games"].code_python)
    assert ten_plus_points.career_sum_keys == ("count", "games")

    points_per_game = load_code_metric(specs_by_key["points_per_game"].code_python)
    assert points_per_game.career_sum_keys == ("total", "games")

    points_per_36 = load_code_metric(specs_by_key["points_per_36"].code_python)
    assert points_per_36.career_sum_keys == ("total", "seconds", "games")

    field_goal_pct = load_code_metric(specs_by_key["field_goal_pct"].code_python)
    assert field_goal_pct.career_sum_keys == ("numerator", "denominator", "games")

    win_rate_as_starter = load_code_metric(specs_by_key["win_rate_as_starter"].code_python)
    assert win_rate_as_starter.career_sum_keys == ("wins", "starts")

    scoring_streak = load_code_metric(specs_by_key["max_consecutive_20pt_games"].code_python)
    assert scoring_streak.career_max_keys == ("best_streak",)

    best_single_game_reb = load_code_metric(specs_by_key["best_single_game_reb"].code_python)
    assert best_single_game_reb.career_max_keys == ("best_value",)


def test_threshold_metric_emits_qualifying_games():
    specs_by_key = {spec.key: spec for spec in build_metric_specs()}
    metric = load_code_metric(specs_by_key["fifteen_plus_rebound_games"].code_python)
    session = _FakeSession(
        {
            "Game": [
                SimpleNamespace(game_id="g1", season="22025", game_date="2025-01-01", home_team_id="t1", road_team_id="t2", wining_team_id="t1"),
                SimpleNamespace(game_id="g2", season="22025", game_date="2025-01-03", home_team_id="t2", road_team_id="t1", wining_team_id="t2"),
                SimpleNamespace(game_id="g3", season="22025", game_date="2025-01-05", home_team_id="t1", road_team_id="t3", wining_team_id="t1"),
            ],
            "PlayerGameStats": [
                SimpleNamespace(game_id="g1", player_id="p1", team_id="t1", reb=16, pts=8, ast=1, stl=0, blk=0, fgm=3, fga=7, fg3m=0, fg3a=0, ftm=2, fta=2, min=30, sec=0, starter=True),
                SimpleNamespace(game_id="g2", player_id="p1", team_id="t1", reb=14, pts=7, ast=2, stl=0, blk=0, fgm=2, fga=6, fg3m=0, fg3a=0, ftm=3, fta=4, min=28, sec=0, starter=True),
                SimpleNamespace(game_id="g3", player_id="p1", team_id="t1", reb=18, pts=11, ast=1, stl=0, blk=0, fgm=5, fga=10, fg3m=0, fg3a=0, ftm=1, fta=2, min=34, sec=0, starter=True),
                SimpleNamespace(game_id="g3", player_id="p2", team_id="t3", reb=15, pts=6, ast=0, stl=0, blk=0, fgm=2, fga=5, fg3m=0, fg3a=0, ftm=2, fta=2, min=26, sec=0, starter=True),
            ],
        }
    )

    results = metric.compute_season(session, "22025")
    qualifications = metric.compute_qualifications(session, "22025")

    counts = {result.entity_id: int(result.value_num) for result in results}
    assert counts["p1"] == 2
    assert counts["p2"] == 1
    assert qualifications == [
        {"entity_id": "p1", "game_id": "g1", "qualified": True},
        {"entity_id": "p1", "game_id": "g3", "qualified": True},
        {"entity_id": "p2", "game_id": "g3", "qualified": True},
    ]


def test_streak_metric_emits_best_streak_games_only():
    specs_by_key = {spec.key: spec for spec in build_metric_specs()}
    metric = load_code_metric(specs_by_key["max_consecutive_20pt_games"].code_python)
    session = _FakeSession(
        {
            "Game": [
                SimpleNamespace(game_id="g1", season="22025", game_date="2025-01-01", home_team_id="t1", road_team_id="t2", wining_team_id="t1"),
                SimpleNamespace(game_id="g2", season="22025", game_date="2025-01-03", home_team_id="t1", road_team_id="t3", wining_team_id="t1"),
                SimpleNamespace(game_id="g3", season="22025", game_date="2025-01-05", home_team_id="t2", road_team_id="t1", wining_team_id="t2"),
                SimpleNamespace(game_id="g4", season="22025", game_date="2025-01-07", home_team_id="t1", road_team_id="t4", wining_team_id="t1"),
                SimpleNamespace(game_id="g5", season="22025", game_date="2025-01-09", home_team_id="t1", road_team_id="t5", wining_team_id="t1"),
            ],
            "PlayerGameStats": [
                SimpleNamespace(game_id="g1", player_id="p1", team_id="t1", pts=22, reb=3, ast=1, stl=0, blk=0, fgm=8, fga=15, fg3m=2, fg3a=5, ftm=4, fta=4, min=32, sec=0, starter=True),
                SimpleNamespace(game_id="g2", player_id="p1", team_id="t1", pts=21, reb=2, ast=3, stl=0, blk=0, fgm=7, fga=14, fg3m=1, fg3a=4, ftm=6, fta=7, min=31, sec=0, starter=True),
                SimpleNamespace(game_id="g3", player_id="p1", team_id="t1", pts=9, reb=4, ast=2, stl=0, blk=0, fgm=3, fga=10, fg3m=0, fg3a=2, ftm=3, fta=4, min=30, sec=0, starter=True),
                SimpleNamespace(game_id="g4", player_id="p1", team_id="t1", pts=25, reb=5, ast=2, stl=0, blk=0, fgm=9, fga=16, fg3m=3, fg3a=6, ftm=4, fta=5, min=33, sec=0, starter=True),
                SimpleNamespace(game_id="g5", player_id="p1", team_id="t1", pts=26, reb=4, ast=2, stl=0, blk=0, fgm=9, fga=18, fg3m=2, fg3a=6, ftm=6, fta=6, min=34, sec=0, starter=True),
            ],
        }
    )

    results = metric.compute_season(session, "22025")
    qualifications = metric.compute_qualifications(session, "22025")

    assert {result.entity_id: int(result.value_num) for result in results}["p1"] == 2
    assert qualifications == [
        {"entity_id": "p1", "game_id": "g1", "qualified": True},
        {"entity_id": "p1", "game_id": "g2", "qualified": True},
    ]


def test_single_game_record_metric_emits_best_game_only():
    specs_by_key = {spec.key: spec for spec in build_metric_specs()}
    metric = load_code_metric(specs_by_key["best_single_game_reb"].code_python)
    session = _FakeSession(
        {
            "Game": [
                SimpleNamespace(game_id="g1", season="22025", game_date="2025-01-01", home_team_id="t1", road_team_id="t2", wining_team_id="t1"),
                SimpleNamespace(game_id="g2", season="22025", game_date="2025-01-03", home_team_id="t1", road_team_id="t3", wining_team_id="t1"),
                SimpleNamespace(game_id="g3", season="22025", game_date="2025-01-05", home_team_id="t2", road_team_id="t1", wining_team_id="t2"),
            ],
            "PlayerGameStats": [
                SimpleNamespace(game_id="g1", player_id="p1", team_id="t1", reb=12, pts=8, ast=1, stl=0, blk=0, fgm=3, fga=7, fg3m=0, fg3a=0, ftm=2, fta=2, min=30, sec=0, starter=True, plus=5),
                SimpleNamespace(game_id="g2", player_id="p1", team_id="t1", reb=18, pts=10, ast=2, stl=0, blk=0, fgm=4, fga=9, fg3m=0, fg3a=1, ftm=2, fta=2, min=32, sec=0, starter=True, plus=8),
                SimpleNamespace(game_id="g3", player_id="p1", team_id="t1", reb=15, pts=9, ast=1, stl=0, blk=0, fgm=3, fga=8, fg3m=0, fg3a=0, ftm=3, fta=4, min=29, sec=0, starter=True, plus=-1),
            ],
        }
    )

    results = metric.compute_season(session, "22025")
    qualifications = metric.compute_qualifications(session, "22025")

    assert {result.entity_id: int(result.value_num) for result in results}["p1"] == 18
    assert qualifications == [{"entity_id": "p1", "game_id": "g2", "qualified": True}]


def test_team_count_and_streak_metrics_emit_qualifying_games():
    specs_by_key = {spec.key: spec for spec in build_metric_specs()}
    wins_by_ten = load_code_metric(specs_by_key["wins_by_10_plus"].code_python)
    longest_win_streak = load_code_metric(specs_by_key["longest_win_streak"].code_python)
    session = _FakeSession(
        {
            "Game": [
                SimpleNamespace(game_id="g1", season="22025", game_date="2025-01-01", home_team_id="t1", road_team_id="t2", home_team_score=110, road_team_score=95, wining_team_id="t1"),
                SimpleNamespace(game_id="g2", season="22025", game_date="2025-01-03", home_team_id="t3", road_team_id="t1", home_team_score=99, road_team_score=112, wining_team_id="t1"),
                SimpleNamespace(game_id="g3", season="22025", game_date="2025-01-05", home_team_id="t1", road_team_id="t4", home_team_score=101, road_team_score=98, wining_team_id="t1"),
                SimpleNamespace(game_id="g4", season="22025", game_date="2025-01-07", home_team_id="t5", road_team_id="t1", home_team_score=120, road_team_score=100, wining_team_id="t5"),
            ],
            "TeamGameStats": [
                SimpleNamespace(game_id="g1", team_id="t1", pts=110, reb=40, ast=22, stl=7, blk=5, tov=11, fgm=40, fga=80, fg_pct=0.5, fg3m=12, fg3a=30, fg3_pct=0.4, ftm=18, fta=21, ft_pct=0.857, win=True),
                SimpleNamespace(game_id="g1", team_id="t2", pts=95, reb=38, ast=18, stl=5, blk=4, tov=13, fgm=35, fga=79, fg_pct=0.443, fg3m=10, fg3a=29, fg3_pct=0.345, ftm=15, fta=19, ft_pct=0.789, win=False),
                SimpleNamespace(game_id="g2", team_id="t1", pts=112, reb=41, ast=24, stl=6, blk=6, tov=10, fgm=39, fga=78, fg_pct=0.5, fg3m=11, fg3a=28, fg3_pct=0.393, ftm=19, fta=23, ft_pct=0.826, win=True),
                SimpleNamespace(game_id="g2", team_id="t3", pts=99, reb=37, ast=20, stl=4, blk=3, tov=12, fgm=36, fga=77, fg_pct=0.468, fg3m=9, fg3a=27, fg3_pct=0.333, ftm=18, fta=22, ft_pct=0.818, win=False),
                SimpleNamespace(game_id="g3", team_id="t1", pts=101, reb=39, ast=21, stl=5, blk=4, tov=9, fgm=38, fga=79, fg_pct=0.481, fg3m=10, fg3a=31, fg3_pct=0.323, ftm=15, fta=18, ft_pct=0.833, win=True),
                SimpleNamespace(game_id="g3", team_id="t4", pts=98, reb=35, ast=19, stl=4, blk=3, tov=12, fgm=37, fga=81, fg_pct=0.457, fg3m=8, fg3a=26, fg3_pct=0.308, ftm=16, fta=20, ft_pct=0.8, win=False),
                SimpleNamespace(game_id="g4", team_id="t1", pts=100, reb=36, ast=18, stl=4, blk=3, tov=14, fgm=35, fga=82, fg_pct=0.427, fg3m=9, fg3a=30, fg3_pct=0.3, ftm=21, fta=24, ft_pct=0.875, win=False),
                SimpleNamespace(game_id="g4", team_id="t5", pts=120, reb=42, ast=27, stl=8, blk=6, tov=10, fgm=44, fga=83, fg_pct=0.53, fg3m=13, fg3a=33, fg3_pct=0.394, ftm=19, fta=22, ft_pct=0.864, win=True),
            ],
        }
    )

    count_results = wins_by_ten.compute_season(session, "22025")
    count_qualifications = wins_by_ten.compute_qualifications(session, "22025")
    streak_results = longest_win_streak.compute_season(session, "22025")
    streak_qualifications = longest_win_streak.compute_qualifications(session, "22025")

    assert {result.entity_id: int(result.value_num) for result in count_results}["t1"] == 2
    assert [item for item in count_qualifications if item["entity_id"] == "t1"] == [
        {"entity_id": "t1", "entity_type": "team", "game_id": "g1", "qualified": True},
        {"entity_id": "t1", "entity_type": "team", "game_id": "g2", "qualified": True},
    ]
    assert {result.entity_id: int(result.value_num) for result in streak_results}["t1"] == 3
    assert [item for item in streak_qualifications if item["entity_id"] == "t1"] == [
        {"entity_id": "t1", "entity_type": "team", "game_id": "g1", "qualified": True},
        {"entity_id": "t1", "entity_type": "team", "game_id": "g2", "qualified": True},
        {"entity_id": "t1", "entity_type": "team", "game_id": "g3", "qualified": True},
    ]

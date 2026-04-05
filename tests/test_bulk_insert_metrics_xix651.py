from metrics.framework.runtime import load_code_metric

from db.bulk_insert_metrics_xix651 import (
    DEFAULT_LIMIT,
    UNSUPPORTED_CANDIDATES,
    build_metric_specs,
    validate_specs,
)


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

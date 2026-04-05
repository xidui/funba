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

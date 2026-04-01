from pathlib import Path
import sys
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from metrics.framework.runtime import _aggregated_career_qualification_game_ids


class _FakeCareerMetric:
    career = True
    career_aggregate_mode = "season_results"
    career_sum_keys = ("games",)
    career_max_keys = ("max_points",)
    career_min_keys = ()

    def __init__(self):
        self.calls: list[tuple[object, str]] = []

    def compute_qualifications(self, session, season):
        self.calls.append((session, season))
        return [
            {"entity_id": "893", "game_id": "0048509102", "qualified": True},
            {"entity_id": "893", "game_id": "0048509102", "qualified": True},
            {"entity_id": "893", "game_id": "0049100022", "qualified": True},
            {"entity_id": "893", "game_id": "0049200074", "qualified": False},
            {"entity_id": "947", "game_id": "0048600014", "qualified": True},
            {"entity_id": "893", "game_id": None, "qualified": True},
        ]


class _FakeSeasonMetric:
    career = False
    career_aggregate_mode = "season_results"
    career_sum_keys = ("games",)
    career_max_keys = ("max_points",)
    career_min_keys = ()

    def compute_qualifications(self, session, season):
        raise AssertionError("compute_qualifications should not be called")


class _FakeCareerMetricNoQuals:
    key = "fastest_double_double_career"
    career = True
    career_aggregate_mode = "season_results"
    career_sum_keys = ()
    career_max_keys = ()
    career_min_keys = ("elapsed_seconds",)

    def compute_qualifications(self, session, season):
        return []


class _FakeMetricResultModel:
    context_json = "context_json"
    metric_key = "metric_key"
    entity_id = "entity_id"
    season = "season"


class _FakeQuery:
    def __init__(self, row):
        self._row = row

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self._row


class _FakeSessionWithContext:
    def __init__(self, row):
        self._row = row

    def query(self, *args, **kwargs):
        return _FakeQuery(self._row)


def test_aggregated_career_qualification_game_ids_filters_entity_and_dedupes():
    metric = _FakeCareerMetric()
    session = object()

    game_ids = _aggregated_career_qualification_game_ids(
        metric,
        session,
        "all_playoffs",
        entity_id="893",
    )

    assert game_ids == ["0048509102", "0049100022"]
    assert metric.calls == [(session, "all_playoffs")]


def test_aggregated_career_qualification_game_ids_returns_none_for_non_career_metric():
    metric = _FakeSeasonMetric()

    game_ids = _aggregated_career_qualification_game_ids(
        metric,
        object(),
        "all_playoffs",
        entity_id="893",
    )

    assert game_ids is None


def test_aggregated_career_qualification_game_ids_falls_back_to_career_result_context():
    metric = _FakeCareerMetricNoQuals()
    session = _FakeSessionWithContext(('{"game_id":"0021200764","game_ids":["0021200764","0021200765"]}',))

    with patch("metrics.framework.runtime.MetricResultModel", _FakeMetricResultModel):
        game_ids = _aggregated_career_qualification_game_ids(
            metric,
            session,
            "all_regular",
            entity_id="201965",
        )

    assert game_ids == ["0021200764", "0021200765"]

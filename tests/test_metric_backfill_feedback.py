"""Tests for metric publish backfill feedback and status polling support."""
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from sqlalchemy.sql import column

from tests.db_model_stubs import install_fake_db_module

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app_module():
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    install_fake_db_module(
        REPO_ROOT,
        user_cls=fake_user_cls,
        engine=fake_engine,
    )

    fake_backfill = types.ModuleType("db.backfill_nba_player_shot_detail")
    fake_backfill.back_fill_game_shot_record = MagicMock()
    fake_backfill.back_fill_game_shot_record_from_api = MagicMock()
    fake_backfill.is_game_shot_back_filled = MagicMock(return_value=False)
    sys.modules["db.backfill_nba_player_shot_detail"] = fake_backfill

    fake_line = types.ModuleType("db.backfill_nba_game_line_score")
    fake_line.back_fill_game_line_score = MagicMock()
    fake_line.has_game_line_score = MagicMock(return_value=False)
    fake_line.normalize_game_line_score_payload = MagicMock()
    sys.modules["db.backfill_nba_game_line_score"] = fake_line

    for key in list(sys.modules):
        if key == "web.app" or key.startswith("web.app."):
            del sys.modules[key]

    import web.app as web_app

    web_app.app.config["TESTING"] = True
    return web_app


def _session_ctx(session):
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


def _query_mock(*, first=None, scalar=None):
    query = MagicMock()
    query.filter.return_value.first.return_value = first
    query.filter.return_value.scalar.return_value = scalar
    return query


class TestMetricBackfillFeedback(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()
        self.client = self.web_app.app.test_client()

    def test_publish_uses_shared_backfill_dispatch(self):
        metric = SimpleNamespace(status="draft", updated_at=None)
        session = _session_ctx(MagicMock())
        session.query.return_value.filter.return_value.first.return_value = metric

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(self.web_app, "_dispatch_metric_backfill") as dispatch:
            response = self.client.post(
                "/api/metrics/custom_metric/publish",
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "published")
        self.assertEqual(metric.status, "published")
        dispatch.assert_called_once_with("custom_metric")
        session.commit.assert_called_once()

    def test_publish_returns_warning_when_dispatch_fails(self):
        metric = SimpleNamespace(status="draft", updated_at=None)
        session = _session_ctx(MagicMock())
        session.query.return_value.filter.return_value.first.return_value = metric

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(self.web_app, "_dispatch_metric_backfill", side_effect=RuntimeError("broker down")):
            response = self.client.post(
                "/api/metrics/custom_metric/publish",
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["ok"])
        self.assertIn("warning", body)
        self.assertEqual(metric.status, "published")

    def test_update_rebackfill_uses_shared_dispatch_helper(self):
        metric = SimpleNamespace(status="published", updated_at=None)
        session = _session_ctx(MagicMock())
        queries = [
            _query_mock(first=metric),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        ]
        session.query.side_effect = queries
        for query in queries[1:]:
            query.filter.return_value.delete.return_value = None

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(self.web_app, "_dispatch_metric_backfill") as dispatch:
            response = self.client.post(
                "/api/metrics/custom_metric/update",
                json={"rebackfill": True},
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])
        dispatch.assert_called_once_with("custom_metric")

    def test_update_preserves_user_edits_while_syncing_code_metric_metadata(self):
        metric = SimpleNamespace(
            key="single_quarter_team_scoring",
            name="test",
            description="old desc",
            scope="team",
            category="old",
            min_sample=99,
            status="published",
            source_type="code",
            code_python="old code",
            definition_json="{}",
            updated_at=None,
        )
        session = _session_ctx(MagicMock())
        session.query.return_value.filter.return_value.first.return_value = metric

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(
                 self.web_app,
                 "_code_metric_metadata_from_code",
                 return_value={
                     "key": "single_quarter_team_scoring",
                     "name": "Single-Quarter Team Scoring",
                     "description": "Per-quarter team points.",
                     "scope": "game",
                     "category": "scoring",
                     "min_sample": 1,
                     "career_min_sample": None,
                     "supports_career": False,
                     "career": False,
                     "incremental": False,
                     "rank_order": "desc",
                     "code_python": "normalized code",
                 },
            ) as metadata_from_code:
            response = self.client.post(
                "/api/metrics/single_quarter_team_scoring/update",
                json={"code": "new code", "name": "User Edited Name", "rank_order": "asc"},
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(metric.name, "User Edited Name")
        self.assertEqual(metric.description, "Per-quarter team points.")
        self.assertEqual(metric.scope, "game")
        self.assertEqual(metric.category, "scoring")
        self.assertEqual(metric.min_sample, 1)
        self.assertEqual(metric.code_python, "normalized code")
        self.assertEqual(metric.source_type, "code")
        self.assertIsNone(metric.definition_json)
        self.assertEqual(metadata_from_code.call_args.kwargs["rank_order_override"], "asc")

    def test_code_metric_metadata_normalizes_key_to_expected_key(self):
        metadata = self.web_app._code_metric_metadata_from_code(
            """
from metrics.framework.base import MetricDefinition


class LowestRegulationQuarterScore(MetricDefinition):
    key = "lowest_regulation_quarter_score"
    name = "Lowest Regulation Quarter Score"
    description = "Lowest score in a regulation quarter."
    scope = "game"
    category = "record"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
""",
            expected_key="lowest_quarter_score",
        )

        self.assertEqual(metadata["key"], "lowest_quarter_score")
        self.assertIn("key = 'lowest_quarter_score'", metadata["code_python"])

    def test_code_metric_metadata_applies_rank_order_override(self):
        metadata = self.web_app._code_metric_metadata_from_code(
            """
from metrics.framework.base import MetricDefinition


class LowestQuarterScore(MetricDefinition):
    key = "lowest_quarter_score"
    name = "Lowest Quarter Score"
    description = "Lowest score in any quarter."
    scope = "game"
    category = "record"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
""",
            rank_order_override="asc",
        )

        self.assertEqual(metadata["rank_order"], "asc")
        self.assertIn('rank_order = "asc"', metadata["code_python"])

    def test_code_metric_metadata_defaults_season_types_to_all_supported_types(self):
        metadata = self.web_app._code_metric_metadata_from_code(
            """
from metrics.framework.base import MetricDefinition


class DemoMetric(MetricDefinition):
    key = "demo_metric"
    name = "Demo Metric"
    description = "Demo."
    scope = "player"
    category = "aggregate"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
"""
        )

        self.assertEqual(metadata["season_types"], ["regular", "playoffs", "playin"])

    def test_code_metric_metadata_applies_season_types_override(self):
        metadata = self.web_app._code_metric_metadata_from_code(
            """
from metrics.framework.base import MetricDefinition


class PlayoffMetric(MetricDefinition):
    key = "playoff_metric"
    name = "Playoff Metric"
    description = "Playoff-only demo."
    scope = "player"
    category = "aggregate"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
""",
            season_types_override=["playoffs"],
        )

        self.assertEqual(metadata["season_types"], ["playoffs"])
        self.assertIn("season_types = ('playoffs',)", metadata["code_python"])

    def test_catalog_prefers_code_metric_name_over_stale_db_name(self):
        row = SimpleNamespace(
            key="single_quarter_team_scoring",
            name="test",
            description="old desc",
            scope="team",
            category="old",
            status="published",
            source_type="code",
            group_key=None,
            min_sample=1,
            expression="",
            code_python="fake code",
            created_at=1,
        )

        counts_query = MagicMock()
        counts_query.group_by.return_value.all.return_value = [
            SimpleNamespace(metric_key="single_quarter_team_scoring", count=12)
        ]
        db_query = MagicMock()
        db_query.filter.return_value = db_query
        db_query.order_by.return_value.all.return_value = [row]

        session = MagicMock()
        session.query.side_effect = [db_query, counts_query]

        with patch.object(
            self.web_app,
            "_safe_code_metric_metadata",
            return_value={
                "key": "single_quarter_team_scoring",
                "name": "Single-Quarter Team Scoring",
                "description": "Per-quarter team points.",
                "scope": "game",
                "category": "scoring",
                "min_sample": 1,
                "career_min_sample": None,
                "supports_career": False,
                "career": False,
                "incremental": False,
                "rank_order": "desc",
            },
        ), patch.object(self.web_app, "is_admin", return_value=False):
            catalog = self.web_app._catalog_metrics(session)

        metric = next(m for m in catalog if m["key"] == "single_quarter_team_scoring")
        self.assertEqual(metric["name"], "Single-Quarter Team Scoring")
        self.assertEqual(metric["description"], "Per-quarter team points.")
        self.assertEqual(metric["scope"], "game")
        self.assertEqual(metric["category"], "scoring")

    def test_catalog_preserves_localized_name_and_description_over_search_field_english(self):
        row = SimpleNamespace(
            key="single_quarter_team_scoring",
            name="英文名称",
            description="英文描述",
            name_zh="数据库中文名",
            description_zh="数据库中文描述",
            scope="team",
            category="old",
            status="published",
            source_type="code",
            group_key=None,
            min_sample=1,
            expression="",
            code_python="fake code",
            created_at=1,
            created_by_user_id=None,
        )

        counts_query = MagicMock()
        counts_query.group_by.return_value.all.return_value = [
            SimpleNamespace(metric_key="single_quarter_team_scoring", count=12)
        ]
        db_query = MagicMock()
        db_query.filter.return_value = db_query
        db_query.order_by.return_value.all.return_value = [row]

        session = MagicMock()
        session.query.side_effect = [db_query, counts_query]

        with patch.object(
            self.web_app,
            "_safe_code_metric_metadata",
            return_value={
                "key": "single_quarter_team_scoring",
                "name": "Single-Quarter Team Scoring",
                "name_zh": "单节球队得分",
                "description": "Per-quarter team points.",
                "description_zh": "球队每节得分表现。",
                "scope": "game",
                "category": "scoring",
                "min_sample": 1,
                "career_min_sample": None,
                "supports_career": False,
                "career": False,
                "incremental": False,
                "rank_order": "desc",
            },
        ), patch.object(self.web_app, "_is_zh", return_value=True), patch.object(
            self.web_app, "is_admin", return_value=False
        ):
            catalog = self.web_app._catalog_metrics(session)

        metric = next(m for m in catalog if m["key"] == "single_quarter_team_scoring")
        self.assertEqual(metric["name"], "单节球队得分")
        self.assertEqual(metric["description"], "球队每节得分表现。")
        self.assertEqual(metric["name_zh"], "单节球队得分")
        self.assertEqual(metric["description_zh"], "球队每节得分表现。")

    def test_catalog_falls_back_to_db_localization_when_code_metadata_lacks_zh(self):
        row = SimpleNamespace(
            key="largest_in_game_lead",
            name="Largest In-Game Lead",
            description="Largest lead held in a game.",
            name_zh="比赛中最大领先分差",
            description_zh="球队在比赛任意时刻保持的最大领先分差，无论最终结果。",
            scope="team",
            category="record",
            status="published",
            source_type="code",
            group_key=None,
            min_sample=1,
            expression="",
            code_python="fake code",
            created_at=1,
            created_by_user_id=None,
        )
        counts_query = MagicMock()
        counts_query.group_by.return_value.all.return_value = [
            SimpleNamespace(metric_key="largest_in_game_lead", count=12),
            SimpleNamespace(metric_key="largest_in_game_lead_career", count=8),
        ]
        db_query = MagicMock()
        db_query.filter.return_value = db_query
        db_query.order_by.return_value.all.return_value = [row]

        session = MagicMock()
        session.query.side_effect = [db_query, counts_query]

        with patch.object(
            self.web_app,
            "_safe_code_metric_metadata",
            return_value={
                "key": "largest_in_game_lead",
                "name": "Largest In-Game Lead",
                "name_zh": "",
                "description": "Largest lead held in a game.",
                "description_zh": "",
                "scope": "team",
                "category": "record",
                "min_sample": 1,
                "career_min_sample": None,
                "supports_career": True,
                "career": False,
                "incremental": False,
                "rank_order": "desc",
                "career_name_suffix": " (All-Time)",
            },
        ), patch.object(
            self.web_app, "_is_zh", return_value=True
        ), patch.object(
            self.web_app, "is_admin", return_value=False
        ):
            catalog = self.web_app._catalog_metrics(session)

        base_metric = next(m for m in catalog if m["key"] == "largest_in_game_lead")
        career_metric = next(m for m in catalog if m["key"] == "largest_in_game_lead_career")

        self.assertEqual(base_metric["name"], "比赛中最大领先分差")
        self.assertEqual(base_metric["description"], "球队在比赛任意时刻保持的最大领先分差，无论最终结果。")
        self.assertEqual(base_metric["name_zh"], "比赛中最大领先分差")
        self.assertEqual(base_metric["description_zh"], "球队在比赛任意时刻保持的最大领先分差，无论最终结果。")
        self.assertEqual(career_metric["name"], "比赛中最大领先分差（队史）")
        self.assertEqual(career_metric["description"], "队史范围内球队在比赛任意时刻保持的最大领先分差，无论最终结果。")
        self.assertEqual(career_metric["name_zh"], "比赛中最大领先分差（队史）")
        self.assertEqual(career_metric["description_zh"], "队史范围内球队在比赛任意时刻保持的最大领先分差，无论最终结果。")

    def test_catalog_displays_persisted_team_career_as_franchise_history(self):
        row = SimpleNamespace(
            key="team_three_attempts_per_game_career",
            name="Team 3PA Per Game (Career)",
            description=(
                "Average three-point attempts per game. "
                "Computed across seasons of the same type (regular season, playoffs, or play-in)."
            ),
            name_zh="球队场均三分出手（生涯）",
            description_zh="生涯统计球队场均三分出手数。",
            scope="team",
            category="shooting",
            status="published",
            source_type="rule",
            group_key=None,
            min_sample=1,
            expression="",
            code_python="",
            definition_json='{"time_scope": "career"}',
            created_at=1,
            created_by_user_id=None,
        )

        with patch.object(self.web_app, "_is_zh", return_value=False):
            entries = self.web_app._catalog_metric_entries_for_row(
                row,
                existing_keys={"team_three_attempts_per_game_career"},
                counts={},
            )

        metric = entries[0]
        self.assertEqual(metric["name"], "Team 3PA Per Game (Franchise History)")
        self.assertEqual(
            metric["description"],
            "Average three-point attempts per game. Computed across each franchise's seasons of the selected type.",
        )
        self.assertEqual(metric["name_zh"], "球队场均三分出手（队史）")
        self.assertEqual(metric["description_zh"], "队史范围内统计球队场均三分出手数。")

    def test_create_uses_latest_code_metric_key(self):
        class FakeMetricDefinitionModel:
            key = MagicMock()
            status = MagicMock()

            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        session = _session_ctx(MagicMock())
        session.query.return_value.filter.return_value.first.return_value = None

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(self.web_app, "MetricDefinitionModel", FakeMetricDefinitionModel), \
             patch.object(
                 self.web_app,
                 "_code_metric_metadata_from_code",
                 return_value={
                     "key": "lowest_regulation_quarter_score",
                     "name": "Lowest Regulation Quarter Score",
                     "description": "Lowest score in a regulation quarter.",
                     "scope": "game",
                     "category": "record",
                     "min_sample": 1,
                     "career_min_sample": None,
                     "supports_career": False,
                     "career": False,
                     "incremental": False,
                     "rank_order": "asc",
                     "code_python": "normalized code",
                 },
             ) as metadata_from_code:
            response = self.client.post(
                "/api/metrics",
                json={
                    "key": "lowest_quarter_score",
                    "name": "Old Name",
                    "scope": "game",
                    "code": "latest code",
                    "rank_order": "asc",
                },
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        body = response.get_json()
        self.assertEqual(response.status_code, 201)
        self.assertEqual(body["key"], "lowest_regulation_quarter_score")
        created = session.add.call_args.args[0]
        self.assertEqual(created.key, "lowest_regulation_quarter_score")
        self.assertEqual(created.name, "Lowest Regulation Quarter Score")
        self.assertEqual(created.code_python, "normalized code")
        self.assertEqual(metadata_from_code.call_args.kwargs["rank_order_override"], "asc")

    def test_metric_rank_order_uses_runtime_metric(self):
        with patch("metrics.framework.runtime.get_metric", return_value=SimpleNamespace(rank_order="asc")):
            rank_order = self.web_app._metric_rank_order(MagicMock(), "low_quarter_score")
        self.assertEqual(rank_order, "asc")

    def test_catalog_excludes_drafts_by_default_but_allows_explicit_draft_filter(self):
        class FakeColumn:
            def __init__(self, name):
                self.name = name

            def __ne__(self, other):
                return ("ne", self.name, other)

            def __eq__(self, other):
                return ("eq", self.name, other)

            def in_(self, values):
                return ("in", self.name, tuple(values))

            def desc(self):
                return ("desc", self.name)

        class FakeMetricDefinitionModel:
            status = FakeColumn("status")
            scope = FakeColumn("scope")
            created_at = FakeColumn("created_at")

        class RecordingCountsQuery:
            def group_by(self, *args, **kwargs):
                return self

            def all(self):
                return []

        class RecordingMetricQuery:
            def __init__(self, rows):
                self.rows = rows
                self.filters = []

            def filter(self, *conditions):
                self.filters.extend(conditions)
                return self

            def order_by(self, *args, **kwargs):
                return self

            def all(self):
                return self.rows

        row = SimpleNamespace(
            key="draft_metric",
            name="Draft Metric",
            description="Hidden until published.",
            scope="player",
            category="custom",
            status="draft",
            source_type="code",
            group_key=None,
            min_sample=1,
            expression="",
            code_python="fake code",
            created_at=1,
            created_by_user_id=None,
        )

        def build_session(metric_query):
            session = MagicMock()
            session.query.side_effect = [metric_query, RecordingCountsQuery()]
            return session

        with patch.object(self.web_app, "MetricDefinitionModel", FakeMetricDefinitionModel), \
             patch.object(self.web_app, "_safe_code_metric_metadata", return_value={}), \
             patch.object(self.web_app, "is_admin", return_value=False):
            default_query = RecordingMetricQuery([row])
            self.web_app._catalog_metrics(build_session(default_query), include_result_counts=False)

            explicit_draft_query = RecordingMetricQuery([row])
            self.web_app._catalog_metrics(
                build_session(explicit_draft_query),
                status_filter="draft",
                include_result_counts=False,
            )

        self.assertEqual(
            default_query.filters,
            [("ne", "status", "archived"), ("ne", "status", "draft"), ("ne", "status", "disabled")],
        )
        self.assertEqual(
            explicit_draft_query.filters,
            [("ne", "status", "archived"), ("eq", "status", "draft")],
        )

    def test_catalog_page_slices_after_virtual_career_expansion(self):
        row_a = SimpleNamespace(key="metric_a")
        row_b = SimpleNamespace(key="metric_b")
        row_c = SimpleNamespace(key="metric_c")

        base_query = MagicMock()
        base_query.with_entities.return_value.all.return_value = [("metric_a",), ("metric_b",), ("metric_c",)]

        ordered_query = MagicMock()
        ordered_query.offset.return_value.limit.return_value.all.side_effect = [
            [row_a, row_b],
            [row_c],
        ]

        with patch.object(self.web_app, "_catalog_metric_base_query", return_value=base_query), \
             patch.object(self.web_app, "_catalog_metric_ordered_query", return_value=ordered_query), \
             patch.object(
                 self.web_app,
                 "_catalog_metric_entries_for_row",
                 side_effect=[
                     [{"key": "metric_a"}, {"key": "metric_a_career"}],
                     [{"key": "metric_b"}],
                     [{"key": "metric_c"}],
                 ],
             ):
            page, has_more = self.web_app._catalog_metrics_page(
                MagicMock(),
                offset=1,
                limit=2,
            )

        self.assertEqual(page, [{"key": "metric_a_career"}, {"key": "metric_b"}])
        self.assertTrue(has_more)

    def test_team_logo_url_helper_builds_nba_cdn_path(self):
        self.assertEqual(
            self.web_app._team_logo_url("1610612743"),
            "https://cdn.nba.com/logos/nba/1610612743/global/L/logo.svg",
        )
        self.assertIsNone(self.web_app._team_logo_url(None))

    def test_asc_metric_keys_uses_runtime_catalog(self):
        with patch(
            "metrics.framework.runtime.get_all_metrics",
            return_value=[
                SimpleNamespace(key="low_quarter_score", rank_order="asc"),
                SimpleNamespace(key="combined_score", rank_order="desc"),
                SimpleNamespace(key="true_shooting_pct", rank_order="asc"),
            ],
        ):
            asc_keys = self.web_app._asc_metric_keys(MagicMock())

        self.assertEqual(asc_keys, {"low_quarter_score", "true_shooting_pct"})

    def test_game_entity_filter_matches_base_and_composite_ids(self):
        expr = self.web_app._game_entity_filter(column("entity_id"), "0022500870")
        compiled = str(expr.compile(compile_kwargs={"literal_binds": True}))

        self.assertIn("entity_id = '0022500870'", compiled)
        self.assertIn("entity_id LIKE '0022500870:%'", compiled)

    def test_backfill_status_endpoint_returns_combined_payload(self):
        backfill = {
            "status": "queued",
            "total_games": 120,
            "done_games": 0,
            "active_games": 0,
            "pending_games": 120,
            "progress_pct": 0.0,
            "latest_run_at": None,
            "components": [],
            "is_multi_component": False,
        }
        session = _session_ctx(MagicMock())

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(
                 self.web_app,
                 "_build_metric_backfill_status",
                 return_value=(SimpleNamespace(key="custom_metric"), backfill),
             ):
            response = self.client.get("/api/metrics/custom_metric/backfill-status")

        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(body["metric_key"], "custom_metric")
        self.assertEqual(body["backfill"]["status"], "queued")

    def test_build_metric_backfill_status_formats_component_timestamps(self):
        db_metric = SimpleNamespace(
            key="custom_metric",
            name="Custom Metric",
            description="",
            scope="player",
            category="Shooting",
            status="published",
            source_type="rule",
            min_sample=1,
        )
        runtime_metric = SimpleNamespace(
            key="custom_metric",
            name="Custom Metric",
            description="",
            scope="player",
            category="Shooting",
            supports_career=True,
            career=False,
        )
        career_metric = SimpleNamespace(
            key="custom_metric_career",
            name="Custom Metric Career",
            description="",
            scope="player",
            category="Shooting",
            supports_career=False,
            career=True,
        )

        session = MagicMock()
        session.query.side_effect = [
            _query_mock(first=db_metric),
            _query_mock(scalar=100),
        ]

        fake_runtime = types.ModuleType("metrics.framework.runtime")
        fake_runtime.get_metric = MagicMock(side_effect=[runtime_metric, career_metric])

        component_results = [
            {
                "metric_key": "custom_metric",
                "status": "running",
                "done_games": 30,
                "active_games": 5,
                "pending_games": 65,
                "total_games": 100,
                "progress_pct": 30.0,
                "reduce_done_seasons": 0,
                "reduce_total_seasons": 0,
                "latest_run_at": datetime(2026, 3, 19, 12, 34, 56),
            },
            {
                "metric_key": "custom_metric_career",
                "status": "complete",
                "done_games": 100,
                "active_games": 0,
                "pending_games": 0,
                "total_games": 100,
                "progress_pct": 100.0,
                "reduce_done_seasons": 0,
                "reduce_total_seasons": 0,
                "latest_run_at": datetime(2026, 3, 19, 13, 0, 0),
            },
        ]

        with patch.dict(sys.modules, {"metrics.framework.runtime": fake_runtime}), \
             patch.object(self.web_app, "_metric_backfill_component", side_effect=component_results):
            metric_def, backfill = self.web_app._build_metric_backfill_status(session, "custom_metric")

        self.assertEqual(metric_def.key, "custom_metric")
        self.assertEqual(backfill["status"], "running")
        self.assertEqual(backfill["done_games"], 130)
        self.assertEqual(backfill["active_games"], 5)
        self.assertEqual(backfill["pending_games"], 65)
        self.assertEqual(backfill["latest_run_at"], "2026-03-19 13:00:00")
        self.assertEqual(backfill["components"][0]["latest_run_at"], "2026-03-19 12:34:56")
        self.assertEqual(backfill["components"][1]["label"], "Career")

    def test_metric_backfill_component_uses_compute_run_progress_when_failed(self):
        latest_compute_run = SimpleNamespace(
            status="failed",
            target_game_count=89,
            done_game_count=38,
            started_at=datetime(2026, 3, 30, 21, 49, 17),
            reduce_enqueued_at=None,
            completed_at=None,
            failed_at=datetime(2026, 3, 30, 21, 57, 13),
            created_at=datetime(2026, 3, 30, 21, 49, 17),
        )

        latest_run_query = MagicMock()
        latest_run_query.filter.return_value.order_by.return_value.first.return_value = latest_compute_run

        latest_log_query = MagicMock()
        latest_log_query.filter.return_value.order_by.return_value.limit.return_value.scalar.return_value = datetime(2026, 3, 30, 21, 57, 31)

        latest_result_query = MagicMock()
        latest_result_query.filter.return_value.order_by.return_value.limit.return_value.scalar.return_value = None

        session = MagicMock()
        session.query.side_effect = [latest_run_query, latest_log_query, latest_result_query]

        self.web_app.MetricComputeRun.created_at = column("created_at")
        self.web_app.MetricRunLog.computed_at = column("computed_at")
        self.web_app.MetricResultModel.computed_at = column("computed_at")
        self.web_app.MetricResultModel.id = column("id")
        self.web_app.MetricComputeRun.metric_key = column("metric_key")
        self.web_app.MetricRunLog.metric_key = column("metric_key")
        self.web_app.MetricResultModel.metric_key = column("metric_key")

        component = self.web_app._metric_backfill_component(session, "bench_high_score", 10570)

        self.assertEqual(component["status"], "failed")
        self.assertEqual(component["done_games"], 38)
        self.assertEqual(component["pending_games"], 51)
        self.assertEqual(component["total_games"], 89)
        self.assertEqual(component["progress_pct"], round(38 / 89 * 100.0, 1))

    def test_admin_compute_run_display_status_marks_stalled_when_reduce_never_started(self):
        run = SimpleNamespace(
            status="reducing",
            reduce_enqueued_at=datetime(2026, 3, 23, 8, 14, 55),
            target_game_count=102,
        )

        status, detail = self.web_app._admin_compute_run_display_status(
            run,
            scope_done_games=102,
            scope_active_games=0,
            metric_seasons=1,
            fresh_result_seasons=0,
            now=datetime(2026, 3, 23, 8, 25, 0),
        )

        self.assertEqual(status, "stalled")
        self.assertIn("no reduce output", detail)

    def test_admin_compute_run_display_status_marks_needs_finalize_when_reduce_output_is_fresh(self):
        run = SimpleNamespace(
            status="reducing",
            reduce_enqueued_at=datetime(2026, 3, 23, 8, 14, 55),
            target_game_count=102,
        )

        status, detail = self.web_app._admin_compute_run_display_status(
            run,
            scope_done_games=102,
            scope_active_games=0,
            metric_seasons=2,
            fresh_result_seasons=2,
            now=datetime(2026, 3, 23, 8, 16, 0),
        )

        self.assertEqual(status, "needs_finalize")
        self.assertIn("never recorded completion", detail)

    def test_load_admin_compute_runs_panel_reclassifies_stalled_reducing_runs(self):
        run = SimpleNamespace(
            id="run-1",
            metric_key="forty_plus_scoring_games_career",
            status="reducing",
            target_game_count=102,
            created_at=datetime(2026, 3, 23, 6, 7, 57),
            completed_at=None,
            failed_at=None,
            reduce_enqueued_at=datetime(2026, 3, 23, 8, 14, 55),
        )

        counts_query = MagicMock()
        counts_query.group_by.return_value.all.return_value = [
            ("reducing", 1),
            ("complete", 2),
        ]

        reducing_query = MagicMock()
        reducing_query.filter.return_value.all.return_value = [run]

        active_query = MagicMock()
        ordered_active_query = active_query.filter.return_value.order_by.return_value
        ordered_active_query.count.return_value = 1
        ordered_active_query.offset.return_value.limit.return_value.all.return_value = [run]

        session = MagicMock()
        session.query.side_effect = [counts_query, reducing_query, active_query]

        with patch.object(
            self.web_app,
            "_admin_compute_run_activity",
            return_value={
                "scope_done_games": 102,
                "scope_active_games": 0,
                "metric_seasons": 1,
                "fresh_result_seasons": 0,
            },
        ):
            panel = self.web_app._load_admin_compute_runs_panel(session, runs_page=1, runs_page_size=25)

        self.assertEqual(panel["compute_run_counts"]["reducing"], 0)
        self.assertEqual(panel["compute_run_counts"]["stalled"], 1)
        self.assertEqual(panel["compute_run_counts"]["complete"], 2)
        self.assertEqual(panel["compute_runs"][0]["status"], "stalled")
        self.assertEqual(panel["compute_runs"][0]["raw_status"], "reducing")


if __name__ == "__main__":
    unittest.main()

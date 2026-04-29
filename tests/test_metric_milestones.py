from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from db.models import Base, Game, MetricMilestone, Player, PlayerGameStats
from metrics.framework.milestones import (
    BoxScoreSliceProvider,
    InMemoryBatchProvider,
    aggregate_pool_as_of,
    detect_milestones_for_metric,
)


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _metric():
    return SimpleNamespace(
        key="season_total_assists_career",
        scope="player",
        metric_kind="season_total",
        value_field="ast",
        split_key=None,
        rank_order="desc",
        additive_accumulator=True,
        approaching_thresholds=[1, 3, 10, 30],
        absolute_thresholds=[],
        absolute_approach_thresholds=[],
        min_sample=1,
        supports_career=False,
    )


def _add_stat(session, game_id, player_id, ast):
    session.add(
        PlayerGameStats(
            game_id=game_id,
            team_id="t1",
            player_id=player_id,
            min=30,
            sec=0,
            pts=0,
            reb=0,
            ast=ast,
            stl=0,
            blk=0,
            fgm=0,
            fga=0,
            fg3m=0,
            fg3a=0,
            ftm=0,
            fta=0,
        )
    )


def test_box_score_slice_matches_in_memory_batch_provider_for_crossing_event():
    session = _session()
    session.add_all(
        [
            Player(player_id="p1", full_name="Player One"),
            Player(player_id="p2", full_name="Player Two"),
            Game(game_id="g1", season="42024", game_date=date(2024, 4, 1), home_team_score=100, road_team_score=90),
            Game(game_id="g2", season="42024", game_date=date(2024, 4, 2), home_team_score=101, road_team_score=91),
        ]
    )
    _add_stat(session, "g1", "p1", 8)
    _add_stat(session, "g1", "p2", 10)
    _add_stat(session, "g2", "p1", 5)
    _add_stat(session, "g2", "p2", 0)
    session.commit()

    metric = _metric()
    statements = []

    def _record_sql(*args):
        statements.append(args[2])

    event.listen(session.bind, "before_cursor_execute", _record_sql)
    pre_g2_pool = aggregate_pool_as_of(session, metric, "all_playoffs", date(2024, 4, 2), "g2")
    event.remove(session.bind, "before_cursor_execute", _record_sql)
    assert pre_g2_pool == {"p1": 8.0, "p2": 10.0}
    assert len(statements) == 1

    batch_provider = InMemoryBatchProvider(event_lookup_authoritative=True)
    with patch("metrics.framework.milestones.get_metric", return_value=metric):
        detect_milestones_for_metric(
            session,
            "g1",
            metric.key,
            "all_playoffs",
            prev_values_provider=batch_provider,
        )
        batch_events = detect_milestones_for_metric(
            session,
            "g2",
            metric.key,
            "all_playoffs",
            prev_values_provider=batch_provider,
        )
        session.flush()

        g2 = session.query(Game).filter(Game.game_id == "g2").one()
        slice_events = detect_milestones_for_metric(
            session,
            "g2",
            metric.key,
            "all_playoffs",
            prev_values_provider=BoxScoreSliceProvider(g2),
        )

    batch_crossings = [
        (e["event_type"], e["event_key"], e["prev_value"], e["new_value"], e["prev_rank"], e["new_rank"])
        for e in batch_events
        if e["event_type"] == "rank_crossing"
    ]
    slice_crossings = [
        (e["event_type"], e["event_key"], e["prev_value"], e["new_value"], e["prev_rank"], e["new_rank"])
        for e in slice_events
        if e["event_type"] == "rank_crossing"
    ]
    assert batch_crossings == slice_crossings == [("rank_crossing", "cross_p2", 8.0, 13.0, 2, 1)]

    rows = session.query(MetricMilestone).filter(MetricMilestone.game_id == "g2").all()
    assert {(row.event_type, row.event_key) for row in rows} == {("rank_crossing", "cross_p2")}


def test_milestone_detection_is_idempotent_and_stat_corrections_update_row():
    session = _session()
    session.add_all(
        [
            Player(player_id="p1", full_name="Player One"),
            Player(player_id="p2", full_name="Player Two"),
            Game(game_id="g1", season="42024", game_date=date(2024, 4, 1), home_team_score=100, road_team_score=90),
            Game(game_id="g2", season="42024", game_date=date(2024, 4, 2), home_team_score=101, road_team_score=91),
        ]
    )
    _add_stat(session, "g1", "p1", 8)
    _add_stat(session, "g1", "p2", 10)
    _add_stat(session, "g2", "p1", 5)
    _add_stat(session, "g2", "p2", 0)
    session.commit()

    metric = _metric()
    g2 = session.query(Game).filter(Game.game_id == "g2").one()
    with patch("metrics.framework.milestones.get_metric", return_value=metric):
        detect_milestones_for_metric(
            session,
            "g2",
            metric.key,
            "all_playoffs",
            prev_values_provider=BoxScoreSliceProvider(g2),
        )
        session.commit()
        first = session.query(MetricMilestone).filter_by(game_id="g2", event_key="cross_p2").one()
        assert first.new_value == 13.0
        row_count = session.query(MetricMilestone).filter(MetricMilestone.game_id == "g2").count()

        detect_milestones_for_metric(
            session,
            "g2",
            metric.key,
            "all_playoffs",
            prev_values_provider=BoxScoreSliceProvider(g2),
        )
        session.commit()
        assert session.query(MetricMilestone).filter(MetricMilestone.game_id == "g2").count() == row_count

        corrected = (
            session.query(PlayerGameStats)
            .filter(PlayerGameStats.game_id == "g2", PlayerGameStats.player_id == "p1")
            .one()
        )
        corrected.ast = 6
        session.commit()
        detect_milestones_for_metric(
            session,
            "g2",
            metric.key,
            "all_playoffs",
            prev_values_provider=BoxScoreSliceProvider(g2),
        )
        session.commit()

    updated = session.query(MetricMilestone).filter_by(game_id="g2", event_key="cross_p2").one()
    assert updated.prev_value == 8.0
    assert updated.new_value == 14.0
    assert session.query(MetricMilestone).filter(MetricMilestone.game_id == "g2").count() == row_count


def test_season_scope_crossing_suppressed_when_target_below_value_floor():
    """count_threshold / season_total 等 metric 在 concrete season 上，
    target 的 prev_value 小于 floor 时,不 emit crossing event —— 避免早
    playoff 阶段一堆球员并列低值触发的级联噪音。Career-scope 仍保持原行为。"""
    session = _session()
    session.add_all(
        [
            Player(player_id="hero", full_name="Hero"),
            Player(player_id="t1", full_name="T1"),
            Player(player_id="t2", full_name="T2"),
            Game(game_id="g1", season="42024", game_date=date(2024, 4, 1), home_team_score=100, road_team_score=90),
            Game(game_id="g2", season="42024", game_date=date(2024, 4, 2), home_team_score=100, road_team_score=90),
        ]
    )
    # 英雄 g1: 0 次助攻 (不 qualify, 忽略)
    # t1 g1: 1 次助攻, t2 g1: 1 次助攻 (都是 1, 作为被穿越的低值目标)
    # 英雄 g2: 5 次助攻 → 累计 5 (crossing value floor 对 count_threshold=3 是 3, 对 season_total=30 是 30)
    _add_stat(session, "g1", "t1", 1)
    _add_stat(session, "g1", "t2", 1)
    _add_stat(session, "g2", "hero", 5)
    session.commit()

    # season_total 场景: floor=30, target prev_value=1 < 30 → 不 emit
    metric_season_total = _metric()
    metric_season_total.key = "season_total_assists"
    metric_season_total.absolute_thresholds = []
    provider = InMemoryBatchProvider(event_lookup_authoritative=True)
    with patch("metrics.framework.milestones.get_metric", return_value=metric_season_total):
        events_concrete = detect_milestones_for_metric(
            session, "g1", metric_season_total.key, "42024",
            prev_values_provider=provider,
        )
        events_concrete_g2 = detect_milestones_for_metric(
            session, "g2", metric_season_total.key, "42024",
            prev_values_provider=provider,
        )
    # g2 应该没有 rank_crossing 事件（target prev=1 < 30 floor）
    assert [e for e in events_concrete_g2 if e["event_type"] == "rank_crossing"] == [], \
        f"concrete season 不应 emit crossing: {events_concrete_g2}"

    # Career 场景: 同样的数据, 但 season=all_playoffs, floor=0 (career) → emit
    # 清掉前一次测试写入的行, 换用新 provider
    session.query(MetricMilestone).delete()
    session.commit()
    metric_career = _metric()  # metric_kind=season_total, supports career
    provider_career = InMemoryBatchProvider(event_lookup_authoritative=True)
    with patch("metrics.framework.milestones.get_metric", return_value=metric_career):
        detect_milestones_for_metric(
            session, "g1", metric_career.key, "all_playoffs",
            prev_values_provider=provider_career,
        )
        events_career_g2 = detect_milestones_for_metric(
            session, "g2", metric_career.key, "all_playoffs",
            prev_values_provider=provider_career,
        )
    # Career scope 应该仍 emit crossing (2 个 target 都被穿过)
    career_crossings = [e for e in events_career_g2 if e["event_type"] == "rank_crossing"]
    assert len(career_crossings) == 2, \
        f"career scope 应 emit crossing: {events_career_g2}"


def test_absolute_threshold_and_approaching_absolute_events_emit_once():
    session = _session()
    session.add_all(
        [
            Player(player_id="p1", full_name="Player One"),
            Game(game_id="g1", season="42024", game_date=date(2024, 4, 1), home_team_score=100, road_team_score=90),
            Game(game_id="g2", season="42024", game_date=date(2024, 4, 2), home_team_score=101, road_team_score=91),
        ]
    )
    _add_stat(session, "g1", "p1", 8)
    _add_stat(session, "g2", "p1", 5)
    session.commit()

    metric = _metric()
    metric.absolute_thresholds = [10, 15]
    metric.absolute_approach_thresholds = [1, 3]
    provider = InMemoryBatchProvider(event_lookup_authoritative=True)
    with patch("metrics.framework.milestones.get_metric", return_value=metric):
        g1_events = detect_milestones_for_metric(
            session,
            "g1",
            metric.key,
            "all_playoffs",
            prev_values_provider=provider,
        )
        g2_events = detect_milestones_for_metric(
            session,
            "g2",
            metric.key,
            "all_playoffs",
            prev_values_provider=provider,
        )

    assert [
        (event["event_type"], event["event_key"])
        for event in g1_events
        if event["event_type"] == "approaching_absolute"
    ] == [("approaching_absolute", "approach_abs_10_thr3")]
    assert [
        (event["event_type"], event["event_key"])
        for event in g2_events
        if event["event_type"] == "absolute_threshold"
    ] == [("absolute_threshold", "reach_10")]


# ---------------------------------------------------------------------------
# Regression: window-pair walking and per-window key normalization.
# Pre-fix bug: _metric_season_pairs walked only [game.season, career_season],
# so milestones for last3/last5 windows never fired even though the matching
# *_last3 / *_last5 metric variants were computed in MetricResult. Both fixes
# below must hold simultaneously, otherwise the curator's last-N candidate
# bucket stays empty.
# ---------------------------------------------------------------------------


def test_metric_season_pairs_walks_all_four_windows():
    from metrics.framework.milestones import _metric_season_pairs

    metric = SimpleNamespace(
        key="season_total_assists",
        scope="player",
        career=False,
        supports_career=True,
        trigger="season",
    )
    game = SimpleNamespace(season="42025")  # current playoffs (4-prefix)

    def fake_get_metric(key, session=None):
        return SimpleNamespace(
            key=key,
            scope="player",
            career=False,
            supports_career=False,
            trigger="season",
        )

    with patch("metrics.framework.milestones.get_metric", side_effect=fake_get_metric):
        pairs = _metric_season_pairs(None, game, [metric], None)

    seasons = {season for _metric, season in pairs}
    assert "42025" in seasons
    assert "all_playoffs" in seasons
    assert "last3_playoffs" in seasons
    assert "last5_playoffs" in seasons


def test_metric_season_pairs_skips_unsupported_windows():
    """Game-scope metric (supports_career=False) must NOT be paired with any
    window season — only its concrete this-season."""
    from metrics.framework.milestones import _metric_season_pairs

    metric = SimpleNamespace(
        key="game_total_blocks",
        scope="game",
        career=False,
        supports_career=False,
        trigger="season",
    )
    game = SimpleNamespace(season="42025")

    pairs = _metric_season_pairs(None, game, [metric], None)
    seasons = {season for _metric, season in pairs}
    assert seasons == {"42025"}


def test_normalize_metric_key_for_season_dispatches_per_window():
    from metrics.framework.milestones import _normalize_metric_key_for_season

    # Base key + window season → matching variant suffix.
    assert _normalize_metric_key_for_season("wins_by_10_plus", "all_playoffs") == "wins_by_10_plus_career"
    assert _normalize_metric_key_for_season("wins_by_10_plus", "all_regular") == "wins_by_10_plus_career"
    assert _normalize_metric_key_for_season("wins_by_10_plus", "last3_playoffs") == "wins_by_10_plus_last3"
    assert _normalize_metric_key_for_season("wins_by_10_plus", "last5_regular") == "wins_by_10_plus_last5"
    # Concrete season has no window — leave unchanged.
    assert _normalize_metric_key_for_season("wins_by_10_plus", "42025") == "wins_by_10_plus"
    # Already a variant — never double-suffix.
    assert _normalize_metric_key_for_season("wins_by_10_plus_career", "all_playoffs") == "wins_by_10_plus_career"
    # Old buggy code mapped *_last5 + all_playoffs to *_career; ensure we DON'T
    # mutate a variant key based on a mismatched season.
    assert _normalize_metric_key_for_season("wins_by_10_plus_last5", "all_playoffs") == "wins_by_10_plus_last5"

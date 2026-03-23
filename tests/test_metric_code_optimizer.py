from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from metrics.framework.code_optimizer import optimize_metric_code


def test_optimize_metric_code_rewrites_player_game_stats_lookup():
    code = """
\"\"\"Example metric.\"\"\"
from metrics.framework.base import MetricDefinition
from db.models import PlayerGameStats


class Example(MetricDefinition):
    key = "example"
    name = "Example"
    description = "desc"
    scope = "player"
    category = "efficiency"
    incremental = True

    def compute_delta(self, session, entity_id, game_id):
        row = (
            session.query(PlayerGameStats)
            .filter(
                PlayerGameStats.player_id == entity_id,
                PlayerGameStats.game_id == game_id,
            )
            .first()
        )
        return None if row is None else {"ast": int(row.ast or 0)}
""".strip()

    optimized = optimize_metric_code(code)

    assert "from metrics.helpers import player_game_stat" in optimized
    assert "row = player_game_stat(session, game_id, entity_id)" in optimized
    assert "session.query(PlayerGameStats)" not in optimized


def test_optimize_metric_code_preserves_future_import_position():
    code = """
\"\"\"Example metric.\"\"\"
from __future__ import annotations

from metrics.framework.base import MetricDefinition
from db.models import PlayerGameStats


class Example(MetricDefinition):
    key = "example"
    name = "Example"
    description = "desc"
    scope = "player"
    category = "efficiency"
    incremental = True

    def compute_delta(self, session, entity_id, game_id):
        row = (
            session.query(PlayerGameStats)
            .filter(
                PlayerGameStats.player_id == entity_id,
                PlayerGameStats.game_id == game_id,
            )
            .first()
        )
        return None if row is None else {"ast": int(row.ast or 0)}
""".strip()

    optimized = optimize_metric_code(code)
    lines = optimized.splitlines()
    assert lines[0] == '"""Example metric."""'
    assert lines[1] == "from __future__ import annotations"
    assert lines[3].startswith("from metrics.helpers import ")


def test_optimize_metric_code_rewrites_shot_record_lookup():
    code = """
from metrics.framework.base import MetricDefinition
from db.models import ShotRecord


class Example(MetricDefinition):
    key = "example"
    name = "Example"
    description = "desc"
    scope = "player"
    category = "conditional"
    incremental = True

    def compute_delta(self, session, entity_id, game_id):
        shots = (
            session.query(ShotRecord)
            .filter(
                ShotRecord.player_id == entity_id,
                ShotRecord.game_id == game_id,
                ShotRecord.shot_attempted.is_(True),
            )
            .order_by(ShotRecord.period, ShotRecord.min.desc(), ShotRecord.sec.desc())
            .all()
        )
        return {"shots": len(shots)}
""".strip()

    optimized = optimize_metric_code(code)

    assert "from metrics.helpers import player_attempted_shots" in optimized
    assert "shots = player_attempted_shots(session, game_id, entity_id)" in optimized
    assert "session.query(ShotRecord)" not in optimized

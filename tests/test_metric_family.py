from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from metrics.framework.family import build_career_code_variant
from metrics.framework.runtime import load_code_metric


def test_build_career_code_variant_preserves_valid_python():
    base_code = """
from metrics.framework.base import MetricDefinition, MetricResult


class AssistToTurnoverRatio(MetricDefinition):
    key = "assist_to_turnover_ratio"
    name = "Assist/Turnover Ratio"
    description = "Assists per turnover this season — higher is better; elite playmakers exceed 3.0."
    scope = "player"
    category = "efficiency"
    min_sample = 20
    incremental = True
    supports_career = True

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        return {"ast": 1, "tov": 1}

    def compute_value(self, totals, season, entity_id):
        return MetricResult(metric_key=self.key, entity_type="player", entity_id=entity_id, season=season, game_id=None, value_num=1.0)
""".strip() + "\n"
    career_code = build_career_code_variant(
        base_code,
        base_key="assist_to_turnover_ratio",
        name="Assist/Turnover Ratio (Career)",
        description="Assists per turnover this season — higher is better; elite playmakers exceed 3.0. Computed across all seasons.",
        min_sample=20,
    )

    metric = load_code_metric(career_code)

    assert metric.key == "assist_to_turnover_ratio_career"
    assert metric.name == "Assist/Turnover Ratio (Career)"
    assert metric.description.endswith("Computed across all seasons.")
    assert metric.min_sample == 20
    assert metric.career is True
    assert metric.supports_career is False

from web.metric_detail_routes import (
    _metric_dataset_description,
    _metric_window_label,
    _team_career_metric_description,
    _team_career_metric_name,
)


class _Deps:
    @staticmethod
    def t():
        return lambda en, zh: en


class _Metric:
    def __init__(self, name="Game Final Margin", description="Final scoring margin."):
        self.name = name
        self.description = description


def test_team_career_window_label_uses_franchise_history():
    assert _metric_window_label(_Deps, "career", "team") == "Franchise History"
    assert _metric_window_label(_Deps, "career", "player") == "Career"


def test_team_career_metric_name_rewrites_old_suffixes():
    assert _team_career_metric_name("Team 3PA Per Game") == "Team 3PA Per Game (Franchise History)"
    assert _team_career_metric_name("Team 3PA Per Game (Career)") == "Team 3PA Per Game (Franchise History)"
    assert _team_career_metric_name("球队场均三分出手（生涯）", zh=True) == "球队场均三分出手（队史）"


def test_team_career_metric_description_rewrites_old_scope_text():
    assert _team_career_metric_description("Average 3PA. Computed across franchise history.") == (
        "Average 3PA. Computed across each franchise's seasons of the selected type."
    )
    assert _team_career_metric_description("生涯统计球队场均三分出手数。", zh=True) == "队史范围内统计球队场均三分出手数。"


def test_metric_dataset_description_expands_short_descriptions():
    description = _metric_dataset_description(_Metric())

    assert len(description) >= 50
    assert description.startswith("Final scoring margin.")
    assert "Game Final Margin" in description
    assert "FUNBA" in description


def test_metric_dataset_description_keeps_long_descriptions():
    source = "This NBA metric ranks player scoring consistency across qualifying games in the selected season."

    assert _metric_dataset_description(_Metric(description=source)) == source


def test_metric_dataset_description_has_chinese_fallback():
    description = _metric_dataset_description(_Metric(name="胜场", description="胜场。"), zh=True)

    assert len(description) >= 50
    assert description.startswith("胜场。")
    assert "FUNBA" in description

from web.metric_detail_routes import (
    _metric_window_label,
    _team_career_metric_description,
    _team_career_metric_name,
)


class _Deps:
    @staticmethod
    def t():
        return lambda en, zh: en


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

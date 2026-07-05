from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock


WEB_APP_DB_MODEL_NAMES = (
    "Award",
    "Feedback",
    "Game",
    "GameContentAnalysisIssue",
    "GameContentAnalysisIssuePost",
    "GameLineScore",
    "GamePlayByPlay",
    "MagicToken",
    "MetricComputeRun",
    "MetricDefinition",
    "MetricMilestone",
    "MetricPerfLog",
    "MetricResult",
    "MetricRunLog",
    "NewsArticle",
    "NewsArticlePlayer",
    "NewsArticleTeam",
    "NewsCluster",
    "PageView",
    "Player",
    "PlayerGamePeriodStats",
    "PlayerGameStats",
    "PlayerSalary",
    "ShotRecord",
    "SocialPost",
    "SocialPostDelivery",
    "SocialPostImage",
    "SocialPostVariant",
    "Team",
    "TeamCoachStint",
    "TeamGameStats",
    "TeamRosterStint",
)


def install_fake_db_module(
    repo_root: Path,
    *,
    user_cls=None,
    engine=None,
    extra_model_names=(),
):
    fake_models = types.ModuleType("db.models")
    model_names = tuple(dict.fromkeys((*WEB_APP_DB_MODEL_NAMES, *extra_model_names)))
    for name in model_names:
        setattr(fake_models, name, MagicMock())

    def _fake_model_attr(name):
        # Fallback for model names web.app starts importing after this list
        # was written — memoized so repeated lookups return the same mock.
        model = MagicMock()
        model.__name__ = name
        setattr(fake_models, name, model)
        return model

    fake_models.__getattr__ = _fake_model_attr

    if user_cls is None:
        user_cls = MagicMock()
        user_cls.__name__ = "User"
    if engine is None:
        engine = MagicMock()

    fake_models.User = user_cls
    fake_models.engine = engine
    sys.modules["db.models"] = fake_models

    fake_db = types.ModuleType("db")
    fake_db.__path__ = [str(repo_root / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db

    return fake_models

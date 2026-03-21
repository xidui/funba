from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from metrics.framework import runtime


def test_get_all_metrics_prefers_db_over_registry_for_same_key():
    db_metric = SimpleNamespace(key="lead_changes", name="DB Lead Changes")
    builtin_metric = SimpleNamespace(key="lead_changes", name="Builtin Lead Changes")

    with patch.object(runtime, "_load_all_db_metrics", return_value=[db_metric]), \
         patch.object(runtime.registry, "get_all", return_value=[builtin_metric]):
        metrics = runtime.get_all_metrics(session=object())

    assert len(metrics) == 1
    assert metrics[0].name == "DB Lead Changes"


def test_get_metric_prefers_db_over_registry():
    db_metric = SimpleNamespace(key="combined_score", name="DB Combined Score")
    builtin_metric = SimpleNamespace(key="combined_score", name="Builtin Combined Score")

    with patch.object(runtime, "_load_all_db_metrics", return_value=[db_metric]), \
         patch.object(runtime.registry, "get", return_value=builtin_metric):
        metric = runtime.get_metric("combined_score", session=object())

    assert metric.name == "DB Combined Score"

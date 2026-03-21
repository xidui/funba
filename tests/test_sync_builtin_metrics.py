from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db import sync_builtin_metrics_to_db as sync_mod


class FakeMetricDefinitionModel:
    key = MagicMock()

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_sync_builtin_metrics_inserts_published_code_rows():
    metric = SimpleNamespace(
        key="lead_changes",
        name="Lead Changes",
        description="Count of lead changes.",
        scope="game",
        category="aggregate",
        min_sample=1,
        career=False,
        group_key=None,
    )
    loaded = SimpleNamespace(
        key="lead_changes",
        name="Lead Changes",
        description="Count of lead changes.",
        scope="game",
        category="aggregate",
        min_sample=1,
    )

    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None

    with patch.object(sync_mod.registry, "get_all", return_value=[metric]), \
         patch.object(sync_mod, "_clean_module_source", return_value=("print('code')\n", "metrics/definitions/game/lead_changes.py")), \
         patch.object(sync_mod, "load_code_metric", return_value=loaded), \
         patch.object(sync_mod, "MetricDefinitionModel", FakeMetricDefinitionModel):
        result = sync_mod.sync_builtin_metrics_to_db(session, overwrite=False)

    created = session.add.call_args.args[0]
    assert result == {"inserted": 1, "updated": 0, "skipped": 0}
    assert created.key == "lead_changes"
    assert created.source_type == "code"
    assert created.status == "published"
    assert created.code_python == "print('code')\n"
    assert created.expression == "[seed_builtin] metrics/definitions/game/lead_changes.py"
    session.commit.assert_called_once()

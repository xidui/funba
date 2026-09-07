"""Model routing, persisted defaults, and request compatibility regressions."""
import importlib.util
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.orm import declarative_base, Session

from db import llm_models
from metrics.framework.generator import _call_llm, _call_llm_with_system
from metrics.framework.search import rank_metrics


@pytest.fixture
def settings_session(monkeypatch):
    base = declarative_base()

    class Setting(base):
        __tablename__ = "Setting"
        key = sa.Column(sa.String(64), primary_key=True)
        value = sa.Column(sa.String(255), nullable=False)
        updated_at = sa.Column(sa.DateTime, default=datetime.now)

    engine = sa.create_engine("sqlite://")
    base.metadata.create_all(engine)
    monkeypatch.setattr(llm_models, "_setting_model", lambda: Setting)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    with Session(engine) as session:
        yield session, Setting
    engine.dispose()


def test_workload_defaults_and_explicit_overrides(settings_session):
    session, _ = settings_session
    for purpose, expected in [("search", "gpt-6-astra"), ("generate", "gpt-6-astra"), ("curator", "gpt-6-astra")]:
        assert llm_models.get_llm_model_for_purpose(session, purpose) == expected
        assert llm_models.resolve_llm_model(session, purpose=purpose) == expected

    llm_models.set_default_llm_model(session, "gpt-5.6-terra")
    assert llm_models.resolve_llm_model(session, purpose="search") == "gpt-5.6-terra"
    llm_models.set_llm_model_for_purpose(session, "search", "gpt-5.6-luna")
    assert llm_models.get_llm_model_for_purpose(session, "search") == "gpt-5.6-luna"
    assert llm_models.resolve_llm_model(session, "gpt-5.4", "search") == "gpt-5.4"


def test_curator_reasoning_tracks_selected_model(settings_session):
    session, _ = settings_session
    llm_models.set_llm_model_for_purpose(session, "curator", "gpt-5.4")
    llm_models.set_curator_reasoning_effort(session, "none")
    llm_models.set_llm_model_for_purpose(session, "curator", "gpt-6-astra")
    assert llm_models.get_curator_reasoning_effort(session) == "medium"
    assert llm_models.set_curator_reasoning_effort(session, "none") == "medium"
    assert llm_models.set_curator_reasoning_effort(session, "high") == "high"
    assert llm_models.get_curator_reasoning_effort(session) == "high"
    meta = {item["id"]: item for item in llm_models.available_llm_models_meta()}
    assert "none" not in meta["gpt-6-astra"]["reasoning_efforts"]
    assert "max" in meta["gpt-6-astra"]["reasoning_efforts"]
    assert "none" in meta["gpt-5.6-luna"]["reasoning_efforts"]
    assert llm_models.set_curator_reasoning_effort(session, "max") == "max"


def test_anthropic_only_configuration_keeps_provider_fallback(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    assert llm_models.env_default_llm_model("search") == "claude-opus-4-7"
    with pytest.raises(ValueError, match="API key not configured"):
        llm_models.ensure_model_available("gpt-6-astra")


@pytest.mark.parametrize("model,effort,expected", [
    ("gpt-6-astra", None, "low"),
    ("gpt-6-astra", "none", "low"),
    ("gpt-6-astra", "high", "high"),
    ("gpt-5.6-terra", None, "none"),
    ("gpt-5.6-luna", "medium", "medium"),
    ("gpt-5.4", None, "none"),
])
def test_generator_request_preserves_compatible_reasoning(monkeypatch, model, effort, expected):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    with patch("openai.OpenAI") as client_cls:
        client = client_cls.return_value
        client.chat.completions.create.return_value = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"ok":true}'))],
        )
        assert _call_llm_with_system("Return JSON", [], model=model, reasoning_effort=effort) == '{"ok":true}'
        params = client.chat.completions.create.call_args.kwargs
        assert params["reasoning_effort"] == expected
        assert ("temperature" in params) == (expected == "none")
        assert params["max_completion_tokens"] == 4096


@pytest.mark.parametrize("model", [None, "gpt-6-astra", "gpt-5.6-luna"])
def test_search_default_and_astra_override_have_valid_token_budgets(monkeypatch, model):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    with patch("openai.OpenAI") as client_cls:
        client = client_cls.return_value
        client.chat.completions.create.return_value = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='[{"key":"points","reason":"Scoring"}]'))],
        )
        assert rank_metrics("points", [{"key": "points"}], model=model)[0]["key"] == "points"
        params = client.chat.completions.create.call_args.kwargs
        assert params["model"] == (model or "gpt-6-astra")
        if params["model"] == "gpt-6-astra":
            assert "temperature" not in params
            assert params["reasoning_effort"] == "low"
            assert params["max_completion_tokens"] >= 4096
        else:
            assert params["reasoning_effort"] == "none"
            assert params["max_completion_tokens"] == 400


def test_metric_generation_uses_xhigh_reasoning_with_room_for_code(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    with patch("metrics.framework.generator._build_system_prompt", return_value="Return JSON"), \
         patch("openai.OpenAI") as client_cls:
        client = client_cls.return_value
        client.chat.completions.create.return_value = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"ok":true}'))],
        )
        _call_llm([{"role": "user", "content": "Build a scoring metric"}])
        params = client.chat.completions.create.call_args.kwargs
        assert params["model"] == "gpt-6-astra"
        assert params["reasoning_effort"] == "xhigh"
        assert params["max_completion_tokens"] == 16384
        assert "temperature" not in params


@pytest.mark.parametrize("curator_model,old_effort,new_effort", [
    ("gpt-6-astra", "none", "medium"),
    ("gpt-6-astra", "high", "high"),
    ("claude-opus-4-7", "none", "none"),
])
def test_all_gpt6_migration_preserves_explicit_reasoning(settings_session, curator_model, old_effort, new_effort):
    session, setting = settings_session
    rows = {
        "default_llm_model": "gpt-5.6-terra",
        "llm_model_search": "gpt-5.6-luna",
        "llm_model_generate": "gpt-6-astra",
        "llm_model_curator": curator_model,
        "llm_reasoning_curator": old_effort,
        "unrelated": "gpt-5.6-terra",
    }
    session.add_all(setting(key=key, value=value) for key, value in rows.items())
    session.commit()
    path = Path(__file__).resolve().parents[1] / "alembic/versions/k8l9m0n1o2p3_use_gpt6_for_all_default_tiers.py"
    spec = importlib.util.spec_from_file_location("all_gpt6_upgrade", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with Operations.context(MigrationContext.configure(session.connection())):
        migration.upgrade()
    session.expire_all()
    expected = {**rows, "default_llm_model": "gpt-6-astra", "llm_model_search": "gpt-6-astra", "llm_reasoning_curator": new_effort}
    assert {row.key: row.value for row in session.query(setting)} == expected


def test_saved_defaults_migration_preserves_other_settings(settings_session):
    session, setting = settings_session
    rows = {
        "default_llm_model": "gpt-5.4-mini",
        "llm_model_search": "gpt-5.4-nano",
        "llm_model_generate": "gpt-5.5",
        "llm_model_curator": "claude-opus-4-7",
        "llm_reasoning_curator": "high",
        "unrelated": "gpt-5.4",
    }
    session.add_all(setting(key=key, value=value) for key, value in rows.items())
    session.commit()
    path = Path(__file__).resolve().parents[1] / "alembic/versions/j8k9l0m1n2o3_upgrade_openai_model_defaults.py"
    spec = importlib.util.spec_from_file_location("model_upgrade", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    context = MigrationContext.configure(session.connection())
    with Operations.context(context):
        migration.upgrade()
        migration.upgrade()  # Reapplying cannot overwrite new/custom choices.
    session.expire_all()
    expected = {**rows, "default_llm_model": "gpt-5.6-terra", "llm_model_search": "gpt-5.6-luna", "llm_model_generate": "gpt-6-astra"}
    assert {row.key: row.value for row in session.query(setting)} == expected
    with Operations.context(context):
        migration.downgrade()
    session.expire_all()
    assert {row.key: row.value for row in session.query(setting)} == {**rows, "llm_model_generate": "gpt-5.4"}

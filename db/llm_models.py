from __future__ import annotations

import os
from datetime import datetime

AVAILABLE_LLM_MODELS = (
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.4-nano",
    "claude-opus-4-7",
    "claude-sonnet-4-6",
    "claude-haiku-4-5",
)
AVAILABLE_REASONING_EFFORTS = ("none", "low", "medium", "high", "xhigh")
DEFAULT_LLM_MODEL_SETTING_KEY = "default_llm_model"
SEARCH_LLM_MODEL_SETTING_KEY = "llm_model_search"
GENERATE_LLM_MODEL_SETTING_KEY = "llm_model_generate"
CURATOR_LLM_MODEL_SETTING_KEY = "llm_model_curator"
CURATOR_REASONING_SETTING_KEY = "llm_reasoning_curator"

_MODEL_PROVIDER = {
    "gpt-5.5": "openai",
    "gpt-5.4": "openai",
    "gpt-5.4-mini": "openai",
    "gpt-5.4-nano": "openai",
    "claude-opus-4-7": "anthropic",
    "claude-sonnet-4-6": "anthropic",
    "claude-haiku-4-5": "anthropic",
}

_PROVIDER_ENV_KEY = {
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
}

_PROVIDER_LABEL = {
    "openai": "OpenAI",
    "anthropic": "Anthropic",
}


def _setting_model():
    from db import models as db_models

    return getattr(db_models, "Setting", None)


def available_llm_models() -> list[str]:
    return list(AVAILABLE_LLM_MODELS)


def available_llm_models_meta() -> list[dict]:
    return [
        {
            "id": model,
            "provider": _MODEL_PROVIDER[model],
            "provider_label": _PROVIDER_LABEL[_MODEL_PROVIDER[model]],
            "available": provider_is_configured(model),
        }
        for model in AVAILABLE_LLM_MODELS
    ]


def validate_llm_model(model: str) -> str:
    normalized = str(model or "").strip()
    if normalized not in _MODEL_PROVIDER:
        raise ValueError(f"Unsupported model: {normalized or '(empty)'}")
    return normalized


def provider_for_model(model: str) -> str:
    return _MODEL_PROVIDER[validate_llm_model(model)]


def provider_is_configured(model: str) -> bool:
    provider = provider_for_model(model)
    return bool(os.getenv(_PROVIDER_ENV_KEY[provider]))


def ensure_model_available(model: str) -> str:
    normalized = validate_llm_model(model)
    if provider_is_configured(normalized):
        return normalized
    provider = provider_for_model(normalized)
    raise ValueError(
        f"Model unavailable — API key not configured for {_PROVIDER_LABEL[provider]}"
    )


def env_default_llm_model() -> str | None:
    if os.getenv("OPENAI_API_KEY"):
        return "gpt-5.4"
    if os.getenv("ANTHROPIC_API_KEY"):
        return "claude-opus-4-7"
    return None


def _get_setting(session, key: str) -> str | None:
    setting_model = _setting_model()
    if setting_model is None:
        return None
    row = session.get(setting_model, key)
    if row is None:
        return None
    value = str(row.value or "").strip()
    if not value:
        return None
    return validate_llm_model(value)


def _set_setting(session, key: str, model: str) -> str:
    normalized = ensure_model_available(model)
    setting_model = _setting_model()
    if setting_model is None:
        raise RuntimeError("Setting model is unavailable")
    row = session.get(setting_model, key)
    if row is None:
        row = setting_model(key=key, value=normalized, updated_at=datetime.utcnow())
        session.add(row)
    else:
        row.value = normalized
        row.updated_at = datetime.utcnow()
    return normalized


def get_stored_default_llm_model(session) -> str | None:
    return _get_setting(session, DEFAULT_LLM_MODEL_SETTING_KEY)


def get_default_llm_model_for_ui(session) -> str:
    stored = get_stored_default_llm_model(session)
    if stored:
        return stored
    return env_default_llm_model() or AVAILABLE_LLM_MODELS[0]


_PURPOSE_KEYS = {
    "search": SEARCH_LLM_MODEL_SETTING_KEY,
    "generate": GENERATE_LLM_MODEL_SETTING_KEY,
    "curator": CURATOR_LLM_MODEL_SETTING_KEY,
}


def get_llm_model_for_purpose(session, purpose: str) -> str:
    """Get the configured model for a specific purpose.

    Falls back to the global default if no purpose-specific setting exists.
    """
    key = _PURPOSE_KEYS.get(purpose, GENERATE_LLM_MODEL_SETTING_KEY)
    stored = _get_setting(session, key)
    if stored:
        return stored
    return get_default_llm_model_for_ui(session)


def set_llm_model_for_purpose(session, purpose: str, model: str) -> str:
    key = _PURPOSE_KEYS.get(purpose, GENERATE_LLM_MODEL_SETTING_KEY)
    return _set_setting(session, key, model)


def get_curator_reasoning_effort(session) -> str:
    """Return the configured reasoning_effort for the highlight curator.

    Defaults to 'none' when no setting is stored.
    """
    setting_model = _setting_model()
    if setting_model is None:
        return "none"
    row = session.get(setting_model, CURATOR_REASONING_SETTING_KEY)
    if row is None:
        return "none"
    value = str(row.value or "").strip().lower()
    return value if value in AVAILABLE_REASONING_EFFORTS else "none"


def set_curator_reasoning_effort(session, effort: str) -> str:
    normalized = str(effort or "").strip().lower()
    if normalized not in AVAILABLE_REASONING_EFFORTS:
        raise ValueError(f"Unsupported reasoning_effort: {effort}")
    setting_model = _setting_model()
    if setting_model is None:
        raise RuntimeError("Setting model is unavailable")
    row = session.get(setting_model, CURATOR_REASONING_SETTING_KEY)
    if row is None:
        row = setting_model(key=CURATOR_REASONING_SETTING_KEY, value=normalized, updated_at=datetime.utcnow())
        session.add(row)
    else:
        row.value = normalized
        row.updated_at = datetime.utcnow()
    return normalized


def resolve_llm_model(session, requested_model: str | None = None, purpose: str | None = None) -> str:
    if requested_model:
        return ensure_model_available(requested_model)

    if purpose:
        key = _PURPOSE_KEYS.get(purpose)
        if key:
            stored = _get_setting(session, key)
            if stored:
                return ensure_model_available(stored)

    stored = get_stored_default_llm_model(session)
    if stored:
        return ensure_model_available(stored)

    fallback = env_default_llm_model()
    if fallback:
        return fallback

    raise ValueError("No AI API key set — set OPENAI_API_KEY or ANTHROPIC_API_KEY.")


def set_default_llm_model(session, model: str) -> str:
    return _set_setting(session, DEFAULT_LLM_MODEL_SETTING_KEY, model)

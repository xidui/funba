from __future__ import annotations

import os
from datetime import datetime

AVAILABLE_LLM_MODELS = ("gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano")
DEFAULT_LLM_MODEL_SETTING_KEY = "default_llm_model"

_MODEL_PROVIDER = {
    "gpt-5.4": "openai",
    "gpt-5.4-mini": "openai",
    "gpt-5.4-nano": "openai",
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
    return None


def get_stored_default_llm_model(session) -> str | None:
    setting_model = _setting_model()
    if setting_model is None:
        return None

    row = session.get(setting_model, DEFAULT_LLM_MODEL_SETTING_KEY)
    if row is None:
        return None
    value = str(row.value or "").strip()
    if not value:
        return None
    return validate_llm_model(value)


def get_default_llm_model_for_ui(session) -> str:
    stored = get_stored_default_llm_model(session)
    if stored:
        return stored
    return env_default_llm_model() or AVAILABLE_LLM_MODELS[0]


def resolve_llm_model(session, requested_model: str | None = None) -> str:
    if requested_model:
        return ensure_model_available(requested_model)

    stored = get_stored_default_llm_model(session)
    if stored:
        return ensure_model_available(stored)

    fallback = env_default_llm_model()
    if fallback:
        return fallback

    raise ValueError("No AI API key set — set ANTHROPIC_API_KEY or OPENAI_API_KEY.")


def set_default_llm_model(session, model: str) -> str:
    normalized = ensure_model_available(model)
    setting_model = _setting_model()
    if setting_model is None:
        raise RuntimeError("Setting model is unavailable")

    row = session.get(setting_model, DEFAULT_LLM_MODEL_SETTING_KEY)
    if row is None:
        row = setting_model(
            key=DEFAULT_LLM_MODEL_SETTING_KEY,
            value=normalized,
            updated_at=datetime.utcnow(),
        )
        session.add(row)
    else:
        row.value = normalized
        row.updated_at = datetime.utcnow()
    return normalized

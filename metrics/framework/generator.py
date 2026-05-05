"""AI-powered metric generator: converts plain-English descriptions into executable Python code.

Uses OpenAI GPT-5.4 when available, with Anthropic as a fallback, to generate a
MetricDefinition subclass that the runner can execute directly. Returns a spec
dict with metadata + Python code.
"""
from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable

from db.ai_usage import extract_provider_usage
from db.llm_models import ensure_model_available, env_default_llm_model, provider_for_model

logger = logging.getLogger(__name__)

# ── Prompt template lives in DB (Setting key=METRIC_GENERATOR_PROMPT_KEY) ──

METRIC_GENERATOR_PROMPT_KEY = "metric_generator_prompt"


def _load_example_metrics() -> str:
    """Load curated DB-backed metric source examples for the prompt."""
    from sqlalchemy.orm import sessionmaker

    from db.models import MetricDefinition as MetricDefinitionModel, engine

    SessionLocal = sessionmaker(bind=engine)

    curated_keys = [
        "combined_score",
        "lead_changes",
        "top_scorer",            # game-scope: window function (ROW_NUMBER) pattern
        "multi_20pt_game",       # game-scope: GROUP BY aggregate pattern
        "win_pct_leading_at_half",
        "road_win_pct",
        "bench_scoring_share",
        "comeback_win_pct",
        "hot_hand",
        "clutch_fg_pct",
        "double_double_rate",
        "true_shooting_pct",
        "scoring_consistency",
        "fastest_double_double", # career Mode B: career_min_keys (extrema) pattern
    ]

    examples = []
    with SessionLocal() as session:
        rows = (
            session.query(MetricDefinitionModel)
            .filter(MetricDefinitionModel.key.in_(curated_keys))
            .all()
        )
        rows_by_key = {row.key: row for row in rows}
        for key in curated_keys:
            row = rows_by_key.get(key)
            if row is None or not row.code_python:
                continue
            cleaned = "\n".join(
                line
                for line in row.code_python.rstrip().split("\n")
                if not line.strip().startswith("register(")
            )
            examples.append(f"### {key}\n```python\n{cleaned.strip()}\n```")

    return "\n\n".join(examples) if examples else "(no examples found)"


def get_metric_generator_prompt_template() -> str:
    """Read the raw prompt template (with {EXAMPLES_PLACEHOLDER}) from Setting table.

    No fallback: if the Setting row is missing or empty, raise. The prompt is
    DB-resident by design so admins can edit it without redeploys.
    """
    from sqlalchemy.orm import sessionmaker

    from db.models import Setting, engine

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        row = session.get(Setting, METRIC_GENERATOR_PROMPT_KEY)
        if row is None or not (row.value or "").strip():
            raise RuntimeError(
                f"Setting row {METRIC_GENERATOR_PROMPT_KEY!r} is missing or empty. "
                "Seed it before invoking the metric generator."
            )
        return row.value


def set_metric_generator_prompt_template(value: str) -> str:
    """Persist a new prompt template to the Setting table. Empty input is rejected."""
    from datetime import datetime

    from sqlalchemy.orm import sessionmaker

    from db.models import Setting, engine

    text = (value or "").strip()
    if not text:
        raise ValueError("Prompt template must be non-empty")

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        row = session.get(Setting, METRIC_GENERATOR_PROMPT_KEY)
        if row is None:
            session.add(Setting(key=METRIC_GENERATOR_PROMPT_KEY, value=text, updated_at=datetime.utcnow()))
        else:
            row.value = text
            row.updated_at = datetime.utcnow()
        session.commit()
    return text


def _build_system_prompt() -> str:
    """Build the full system prompt with dynamically loaded examples."""
    template = get_metric_generator_prompt_template()
    examples = _load_example_metrics()
    return template.replace("{EXAMPLES_PLACEHOLDER}", examples)


def _initial_user_message(expression: str) -> str:
    return (
        "Handle this metric-builder chat message. "
        "If it is a metric creation/modification request, return the metric-spec JSON. "
        "If it is a clarification question, return the clarification JSON.\n\n"
        f"{expression}"
    )


def _call_llm_with_system(
    system_prompt: str,
    messages: list[dict],
    model: str | None = None,
    max_tokens: int | None = 4096,
    usage_recorder: Callable[[dict], None] | None = None,
    reasoning_effort: str | None = None,
) -> str:
    """Call OpenAI or Anthropic with an explicit system prompt.

    Pass ``max_tokens=None`` to omit the cap entirely (useful for reasoning
    models where the caller would rather the provider default limit apply).
    """
    selected_model = model or env_default_llm_model()
    if not selected_model:
        raise ValueError("No AI API key set — set OPENAI_API_KEY.")

    selected_model = ensure_model_available(selected_model)
    provider = provider_for_model(selected_model)

    if provider == "openai":
        import openai
        # max_retries=0 — avoid silent retries that re-pay full reasoning
        # token cost on reasoning models if the first attempt times out.
        client = openai.OpenAI(max_retries=0, timeout=300)
        kwargs: dict = {
            "model": selected_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                *messages,
            ],
        }
        if max_tokens is not None:
            kwargs["max_completion_tokens"] = max_tokens
        if reasoning_effort and reasoning_effort.lower() != "none":
            # Reasoning-enabled GPT-5.4 models reject temperature=0, so skip it.
            kwargs["reasoning_effort"] = reasoning_effort
        else:
            kwargs["temperature"] = 0
        response = client.chat.completions.create(**kwargs)
        if usage_recorder:
            usage_recorder(extract_provider_usage(provider, response, selected_model))
        return response.choices[0].message.content.strip()
    elif provider == "anthropic":
        import anthropic
        client = anthropic.Anthropic(max_retries=0, timeout=600)
        # Anthropic requires max_tokens; pick a sane default when caller passes None.
        anthropic_max_tokens = max_tokens if max_tokens is not None else 16384
        kwargs: dict = {
            "model": selected_model,
            "max_tokens": anthropic_max_tokens,
            "system": system_prompt,
            "messages": messages,
        }
        # Map our reasoning_effort to Anthropic adaptive thinking + effort. Haiku
        # does not support the `effort` knob, so just toggle adaptive thinking.
        effort = (reasoning_effort or "").strip().lower()
        if effort and effort != "none":
            kwargs["thinking"] = {"type": "adaptive"}
            if "haiku" not in selected_model:
                ant_effort = effort
                if "sonnet" in selected_model and ant_effort in ("xhigh", "max"):
                    ant_effort = "high"  # Sonnet caps at high
                elif "opus-4-6" in selected_model and ant_effort == "xhigh":
                    ant_effort = "max"  # xhigh is Opus 4.7-only
                kwargs["output_config"] = {"effort": ant_effort}
        message = client.messages.create(**kwargs)
        if usage_recorder:
            usage_recorder(extract_provider_usage(provider, message, selected_model))
        text_block = next((b for b in message.content if getattr(b, "type", None) == "text"), None)
        if text_block is None:
            raise ValueError(f"Anthropic response had no text block (model={selected_model})")
        return text_block.text.strip()
    else:
        raise ValueError(f"Unsupported provider: {provider}")


def _call_llm(
    messages: list[dict],
    model: str | None = None,
    usage_recorder: Callable[[dict], None] | None = None,
) -> str:
    """Call LLM with the metric-generator system prompt."""
    return _call_llm_with_system(
        _build_system_prompt(),
        messages,
        model=model,
        usage_recorder=usage_recorder,
    )


def generate(
    expression: str,
    history: list[dict] | None = None,
    existing: dict | None = None,
    model: str | None = None,
    usage_recorder: Callable[[dict], None] | None = None,
) -> dict:
    """Convert a plain-English expression into a metric spec with Python code.

    Args:
        expression: The user's current message (initial description or followup).
        history: Previous conversation turns as [{"role": "user"|"assistant", "content": "..."}].
                 None for first-time generation.
        existing: Current metric info (key, name, description, scope, category, rank_order, code) for edit mode.
                  When provided, the AI should only modify the code and keep metadata unchanged.

    Returns either:
    - {"responseType": "code", ...metric spec fields...}
    - {"responseType": "clarification", "message": "..."}

    Raises ValueError if generation fails or output is unparseable.
    """
    edit_prefix = ""
    if existing:
        edit_prefix = (
            "You are EDITING an existing metric. Keep the key, name, description, scope, "
            "category, and rank_order exactly as provided below — only modify the code.\n"
            "The MetricDefinition subclass in the code must keep the same key value and rank_order.\n\n"
            f"Current metric:\n"
            f"  key: {existing.get('key', '')}\n"
            f"  name: {existing.get('name', '')}\n"
            f"  description: {existing.get('description', '')}\n"
            f"  scope: {existing.get('scope', '')}\n"
            f"  category: {existing.get('category', '')}\n"
            f"  rank_order: {existing.get('rank_order', '')}\n"
            f"  season_types: {existing.get('season_types', '')}\n"
            f"\nCurrent code:\n```python\n{existing.get('code', '')}\n```\n\n"
            "User's requested change:\n"
        )

    if history:
        # Multi-turn: append the new user message to existing conversation
        messages = list(history) + [{"role": "user", "content": edit_prefix + expression}]
    else:
        # First turn
        if existing:
            messages = [{"role": "user", "content": edit_prefix + expression}]
        else:
            messages = [{"role": "user", "content": _initial_user_message(expression)}]

    raw = _call_llm(messages, model=model, usage_recorder=usage_recorder)

    # Strip markdown code fences if the model wrapped the response
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        spec = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Generator returned invalid JSON: %s\nRaw: %s", exc, raw)
        raise ValueError(f"AI returned invalid JSON: {exc}") from exc

    response_type = str(spec.get("responseType") or "code").strip().lower()

    if response_type == "clarification":
        message = str(spec.get("message") or "").strip()
        if not message:
            raise ValueError("AI clarification response missing 'message'")
        return {
            "responseType": "clarification",
            "message": message,
        }

    if response_type != "code":
        raise ValueError(f"AI returned unsupported responseType: {response_type!r}")

    spec["responseType"] = "code"

    # Backward-compat: older prompts/tests may omit the Chinese fields.
    if existing:
        spec.setdefault("name_zh", existing.get("name_zh") or existing.get("name") or spec.get("name", ""))
        spec.setdefault(
            "description_zh",
            existing.get("description_zh") or existing.get("description") or spec.get("description", ""),
        )
    else:
        spec.setdefault("name_zh", spec.get("name", ""))
        spec.setdefault("description_zh", spec.get("description", ""))
    spec.setdefault("season_types", ["regular", "playoffs", "playin"])

    # Validate required keys
    for key in ("name", "name_zh", "description", "description_zh", "scope", "code"):
        if key not in spec:
            raise ValueError(f"AI response missing required key: {key!r}")

    if not str(spec["code"]).strip():
        raise ValueError("AI returned empty code")

    # In edit mode, override metadata with the existing values
    if existing:
        for field in ("key", "name", "name_zh", "description", "description_zh", "scope", "category", "rank_order", "season_types"):
            if field in existing:
                spec[field] = existing[field]

    return spec


def generate_rule(expression: str) -> dict:
    """Legacy: generate a JSON rule definition. Kept for backwards compatibility."""
    from metrics.framework._generator_rule import generate as _gen_rule
    return _gen_rule(expression)

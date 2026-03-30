from __future__ import annotations

import json
from datetime import datetime, timedelta

from sqlalchemy import func


def _usage_model():
    from db import models as db_models

    return getattr(db_models, "AiUsageLog", None)


def _user_model():
    from db import models as db_models

    return getattr(db_models, "User", None)


def _coerce_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def extract_provider_usage(provider: str, response, model: str | None = None) -> dict:
    payload = {
        "provider": str(provider or "").strip() or "unknown",
        "model": str(model or "").strip() or "",
        "prompt_tokens": None,
        "completion_tokens": None,
        "total_tokens": None,
    }

    if provider == "openai":
        usage = getattr(response, "usage", None)
        if usage is not None:
            payload["prompt_tokens"] = _coerce_int(getattr(usage, "prompt_tokens", None))
            payload["completion_tokens"] = _coerce_int(getattr(usage, "completion_tokens", None))
            payload["total_tokens"] = _coerce_int(getattr(usage, "total_tokens", None))
    elif provider == "anthropic":
        usage = getattr(response, "usage", None)
        if usage is not None:
            payload["prompt_tokens"] = _coerce_int(getattr(usage, "input_tokens", None))
            payload["completion_tokens"] = _coerce_int(getattr(usage, "output_tokens", None))

    if payload["total_tokens"] is None:
        prompt_tokens = payload["prompt_tokens"] or 0
        completion_tokens = payload["completion_tokens"] or 0
        if prompt_tokens or completion_tokens:
            payload["total_tokens"] = prompt_tokens + completion_tokens

    return payload


def log_ai_usage_event(
    session,
    *,
    user_id: str | None,
    visitor_id: str | None,
    feature: str,
    operation: str,
    endpoint: str,
    provider: str,
    model: str,
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
    total_tokens: int | None = None,
    latency_ms: int | None = None,
    success: bool,
    error_code: str | None = None,
    http_status: int | None = None,
    conversation_id: str | None = None,
    metadata: dict | None = None,
):
    usage_model = _usage_model()
    if usage_model is None:
        return None

    row = usage_model(
        created_at=datetime.utcnow(),
        user_id=user_id,
        visitor_id=visitor_id,
        feature=str(feature or "").strip(),
        operation=str(operation or "").strip(),
        endpoint=str(endpoint or "").strip(),
        provider=str(provider or "").strip() or "unknown",
        model=str(model or "").strip() or "unknown",
        prompt_tokens=_coerce_int(prompt_tokens),
        completion_tokens=_coerce_int(completion_tokens),
        total_tokens=_coerce_int(total_tokens),
        latency_ms=_coerce_int(latency_ms),
        success=bool(success),
        error_code=(str(error_code).strip() if error_code else None),
        http_status=_coerce_int(http_status),
        conversation_id=(str(conversation_id).strip() if conversation_id else None),
        metadata_json=json.dumps(metadata, ensure_ascii=True, sort_keys=True) if metadata else None,
    )
    session.add(row)
    return row


def _aggregate_window(session, usage_model, cutoff: datetime) -> dict:
    row = (
        session.query(
            func.count(usage_model.id),
            func.coalesce(func.sum(usage_model.prompt_tokens), 0),
            func.coalesce(func.sum(usage_model.completion_tokens), 0),
            func.coalesce(func.sum(usage_model.total_tokens), 0),
        )
        .filter(usage_model.created_at >= cutoff)
        .one()
    )
    return {
        "calls": int(row[0] or 0),
        "prompt_tokens": int(row[1] or 0),
        "completion_tokens": int(row[2] or 0),
        "total_tokens": int(row[3] or 0),
    }


def get_ai_usage_dashboard(session, *, recent_limit: int = 20) -> dict:
    usage_model = _usage_model()
    if usage_model is None:
        return {
            "window_24h": {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            "window_7d": {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            "by_feature": [],
            "top_actors": [],
            "recent": [],
        }

    now = datetime.utcnow()
    cutoff_24h = now - timedelta(hours=24)
    cutoff_7d = now - timedelta(days=7)

    by_feature_rows = (
        session.query(
            usage_model.feature,
            usage_model.operation,
            func.count(usage_model.id),
            func.coalesce(func.sum(usage_model.total_tokens), 0),
        )
        .filter(usage_model.created_at >= cutoff_7d)
        .group_by(usage_model.feature, usage_model.operation)
        .order_by(func.coalesce(func.sum(usage_model.total_tokens), 0).desc(), func.count(usage_model.id).desc())
        .all()
    )

    actor_rows = (
        session.query(
            usage_model.user_id,
            usage_model.visitor_id,
            func.count(usage_model.id),
            func.coalesce(func.sum(usage_model.total_tokens), 0),
        )
        .filter(usage_model.created_at >= cutoff_7d)
        .group_by(usage_model.user_id, usage_model.visitor_id)
        .order_by(func.coalesce(func.sum(usage_model.total_tokens), 0).desc(), func.count(usage_model.id).desc())
        .limit(10)
        .all()
    )

    recent_rows = (
        session.query(usage_model)
        .order_by(usage_model.created_at.desc(), usage_model.id.desc())
        .limit(recent_limit)
        .all()
    )

    user_model = _user_model()
    user_ids = {row[0] for row in actor_rows if row[0]}
    user_ids.update({row.user_id for row in recent_rows if getattr(row, "user_id", None)})
    users_by_id = {}
    if user_model is not None and user_ids:
        users = session.query(user_model).filter(user_model.id.in_(sorted(user_ids))).all()
        users_by_id = {user.id: user for user in users}

    def actor_label(user_id: str | None, visitor_id: str | None) -> str:
        if user_id:
            user = users_by_id.get(user_id)
            if user is not None:
                return getattr(user, "display_name", None) or getattr(user, "email", None) or f"User {user_id[:8]}"
            return f"User {user_id[:8]}"
        if visitor_id:
            return f"Visitor {visitor_id[:8]}"
        return "Unknown"

    def metadata_dict(row) -> dict:
        raw = getattr(row, "metadata_json", None)
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    return {
        "window_24h": _aggregate_window(session, usage_model, cutoff_24h),
        "window_7d": _aggregate_window(session, usage_model, cutoff_7d),
        "by_feature": [
            {
                "feature": row[0],
                "operation": row[1],
                "calls": int(row[2] or 0),
                "total_tokens": int(row[3] or 0),
            }
            for row in by_feature_rows
        ],
        "top_actors": [
            {
                "user_id": row[0],
                "visitor_id": row[1],
                "label": actor_label(row[0], row[1]),
                "calls": int(row[2] or 0),
                "total_tokens": int(row[3] or 0),
            }
            for row in actor_rows
        ],
        "recent": [
            {
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "label": actor_label(getattr(row, "user_id", None), getattr(row, "visitor_id", None)),
                "feature": row.feature,
                "operation": row.operation,
                "model": row.model,
                "total_tokens": int(row.total_tokens or 0),
                "prompt_tokens": int(row.prompt_tokens or 0),
                "completion_tokens": int(row.completion_tokens or 0),
                "success": bool(row.success),
                "error_code": row.error_code,
                "input_preview": (metadata_dict(row).get("query_text") or metadata_dict(row).get("input_text")),
            }
            for row in recent_rows
        ],
    }

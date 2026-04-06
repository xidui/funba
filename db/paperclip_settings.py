from __future__ import annotations

from datetime import datetime
from urllib.parse import urlparse

PAPERCLIP_ISSUE_BASE_URL_SETTING_KEY = "paperclip_issue_base_url"


def _setting_model():
    from db import models as db_models

    return getattr(db_models, "Setting", None)


def normalize_paperclip_issue_base_url(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Paperclip issue base URL must be an absolute http(s) URL")
    normalized = raw.rstrip("/")
    return normalized


def get_paperclip_issue_base_url(session) -> str | None:
    setting_model = _setting_model()
    if setting_model is None:
        return None
    row = session.get(setting_model, PAPERCLIP_ISSUE_BASE_URL_SETTING_KEY)
    if row is None:
        return None
    return normalize_paperclip_issue_base_url(getattr(row, "value", None))


def set_paperclip_issue_base_url(session, value: str | None) -> str | None:
    normalized = normalize_paperclip_issue_base_url(value)
    setting_model = _setting_model()
    if setting_model is None:
        raise RuntimeError("Setting model is unavailable")
    row = session.get(setting_model, PAPERCLIP_ISSUE_BASE_URL_SETTING_KEY)
    if normalized is None:
        if row is not None:
            session.delete(row)
        return None
    if row is None:
        row = setting_model(
            key=PAPERCLIP_ISSUE_BASE_URL_SETTING_KEY,
            value=normalized,
            updated_at=datetime.utcnow(),
        )
        session.add(row)
    else:
        row.value = normalized
        row.updated_at = datetime.utcnow()
    return normalized


def build_paperclip_issue_url(identifier: str | None, base_url: str | None) -> str | None:
    issue_identifier = str(identifier or "").strip()
    normalized_base = normalize_paperclip_issue_base_url(base_url)
    if not issue_identifier or not normalized_base:
        return None
    return f"{normalized_base}/{issue_identifier}"

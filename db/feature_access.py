from __future__ import annotations

from datetime import datetime

ACCESS_LEVELS = ("anonymous", "logged_in", "pro", "admin")

ACCESS_LEVEL_LABELS = {
    "anonymous": "Not signed in",
    "logged_in": "Signed in",
    "pro": "Pro",
    "admin": "Admin",
}

_FEATURE_ACCESS_DESCRIPTORS = (
    {
        "key": "metric_search",
        "label": "Find Metrics",
        "description": "Natural-language metric search from the public catalog.",
        "default_level": "logged_in",
        "allowed_levels": ACCESS_LEVELS,
    },
    {
        "key": "metric_create",
        "label": "Create / Edit Metrics",
        "description": "Create, edit, and publish custom metrics.",
        "default_level": "pro",
        "allowed_levels": ("logged_in", "pro", "admin"),
    },
)

_FEATURE_ACCESS_DESCRIPTOR_BY_KEY = {
    descriptor["key"]: descriptor for descriptor in _FEATURE_ACCESS_DESCRIPTORS
}

_FEATURE_ACCESS_SETTING_KEYS = {
    descriptor["key"]: f"feature_access_{descriptor['key']}"
    for descriptor in _FEATURE_ACCESS_DESCRIPTORS
}


def _setting_model():
    from db import models as db_models

    return getattr(db_models, "Setting", None)


def access_level_label(level: str) -> str:
    normalized = validate_access_level(level)
    return ACCESS_LEVEL_LABELS[normalized]


def feature_access_descriptors() -> list[dict]:
    return [dict(descriptor) for descriptor in _FEATURE_ACCESS_DESCRIPTORS]


def validate_access_level(level: str) -> str:
    normalized = str(level or "").strip()
    if normalized not in ACCESS_LEVELS:
        raise ValueError(f"Unsupported access level: {normalized or '(empty)'}")
    return normalized


def validate_feature_key(feature: str) -> str:
    normalized = str(feature or "").strip()
    if normalized not in _FEATURE_ACCESS_DESCRIPTOR_BY_KEY:
        raise ValueError(f"Unsupported feature key: {normalized or '(empty)'}")
    return normalized


def allowed_feature_access_levels(feature: str) -> tuple[str, ...]:
    descriptor = _FEATURE_ACCESS_DESCRIPTOR_BY_KEY[validate_feature_key(feature)]
    return tuple(descriptor["allowed_levels"])


def validate_feature_access_level(feature: str, level: str) -> str:
    normalized_feature = validate_feature_key(feature)
    normalized_level = validate_access_level(level)
    if normalized_level not in allowed_feature_access_levels(normalized_feature):
        raise ValueError(
            f"Unsupported access level for {normalized_feature}: {normalized_level}"
        )
    return normalized_level


def default_feature_access_level(feature: str) -> str:
    descriptor = _FEATURE_ACCESS_DESCRIPTOR_BY_KEY[validate_feature_key(feature)]
    return descriptor["default_level"]


def _setting_key(feature: str) -> str:
    return _FEATURE_ACCESS_SETTING_KEYS[validate_feature_key(feature)]


def get_feature_access_level(session, feature: str) -> str:
    normalized_feature = validate_feature_key(feature)
    setting_model = _setting_model()
    if setting_model is None:
        return default_feature_access_level(normalized_feature)
    row = session.get(setting_model, _setting_key(normalized_feature))
    if row is None:
        return default_feature_access_level(normalized_feature)
    value = str(getattr(row, "value", "") or "").strip()
    if not value:
        return default_feature_access_level(normalized_feature)
    return validate_feature_access_level(normalized_feature, value)


def get_feature_access_config(session) -> dict[str, str]:
    return {
        descriptor["key"]: get_feature_access_level(session, descriptor["key"])
        for descriptor in _FEATURE_ACCESS_DESCRIPTORS
    }


def set_feature_access_level(session, feature: str, level: str) -> str:
    normalized_feature = validate_feature_key(feature)
    normalized_level = validate_feature_access_level(normalized_feature, level)
    setting_model = _setting_model()
    if setting_model is None:
        raise RuntimeError("Setting model is unavailable")
    row = session.get(setting_model, _setting_key(normalized_feature))
    if row is None:
        row = setting_model(
            key=_setting_key(normalized_feature),
            value=normalized_level,
            updated_at=datetime.utcnow(),
        )
        session.add(row)
    else:
        row.value = normalized_level
        row.updated_at = datetime.utcnow()
    return normalized_level

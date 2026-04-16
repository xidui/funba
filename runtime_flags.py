from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


DEFAULT_RUNTIME_FLAGS: dict[str, bool] = {
    "legacy_game_metric_fanout": False,
    "ingest_block_shot": False,
    "ingest_block_line_score": False,
    "ingest_block_period_stats": False,
    "platform_hupu": True,
    "platform_xiaohongshu": True,
    "platform_reddit": True,
}


def _runtime_flags_path() -> Path:
    override = (os.getenv("FUNBA_RUNTIME_FLAGS_FILE") or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parent / ".runtime_flags.json"


def load_runtime_flags() -> dict[str, bool]:
    path = _runtime_flags_path()
    flags = dict(DEFAULT_RUNTIME_FLAGS)
    if not path.exists():
        return flags
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return flags
    if not isinstance(payload, dict):
        return flags
    for key, default in DEFAULT_RUNTIME_FLAGS.items():
        value = payload.get(key, default)
        flags[key] = bool(value)
    return flags


def get_runtime_flag(key: str) -> bool:
    if key not in DEFAULT_RUNTIME_FLAGS:
        raise KeyError(key)
    return load_runtime_flags()[key]


_PLATFORM_FLAG_PREFIX = "platform_"

KNOWN_PLATFORMS = ["hupu", "xiaohongshu", "reddit"]


def get_enabled_platforms() -> list[str]:
    flags = load_runtime_flags()
    return [p for p in KNOWN_PLATFORMS if flags.get(f"{_PLATFORM_FLAG_PREFIX}{p}", True)]


def set_runtime_flag(key: str, value: Any) -> dict[str, bool]:
    if key not in DEFAULT_RUNTIME_FLAGS:
        raise KeyError(key)
    flags = load_runtime_flags()
    flags[key] = bool(value)

    path = _runtime_flags_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(path.parent), prefix=path.name + ".tmp.") as tmp:
        json.dump(flags, tmp, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
    return flags

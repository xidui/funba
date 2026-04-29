"""Pipeline × platform × {generate, autopublish} configuration.

Funba runs multiple content pipelines (game analysis, metric analysis,
hero card …). Each pipeline writes variants for a subset of platforms and
each platform may auto-publish or wait for human review. This module
holds:

  - The registry of known pipelines and which platforms each supports.
  - Read/write helpers that store one row per (pipeline, platform, action)
    in the Setting table.
  - A serializer that produces the full matrix for the admin UI.

Storage convention
------------------
Setting key = "pipeline.{pipeline_key}.platform.{platform_key}.{action}"
              where action is "generate" or "autopublish".
Setting value is the literal "true" or "false". Missing rows fall back to
the package default declared in the registry below.

To add a new pipeline:
  1. Append a Pipeline entry to PIPELINES.
  2. Make the pipeline's code call is_generate_enabled() / is_autopublish_enabled()
     at the right decision points.
  3. The admin matrix UI picks it up automatically.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy.orm import Session

from db.models import Setting


GENERATE_ACTION = "generate"
AUTOPUBLISH_ACTION = "autopublish"
_VALID_ACTIONS = (GENERATE_ACTION, AUTOPUBLISH_ACTION)

# Platforms whose publish wrapper is fully self-contained (script-level
# preflight + retry + status writeback) and don't need Paperclip's Delivery
# Publisher agent in the loop. When admin approves a variant carrying any of
# these platforms, the system enqueues publish_social_delivery_task directly.
_DIRECT_PUBLISH_PLATFORMS = frozenset({"twitter", "funba"})


def direct_publish_platforms() -> frozenset[str]:
    """Return the set of platforms eligible for direct script-driven publish."""
    return _DIRECT_PUBLISH_PLATFORMS


@dataclass(frozen=True)
class PlatformDefault:
    """Default toggle values for one (pipeline, platform) cell."""
    key: str            # platform identifier — e.g. "twitter", "funba"
    label: str          # admin-UI label — e.g. "Twitter"
    generate: bool      # default for "generate variant?"
    autopublish: bool   # default for "auto-publish after creation?"
    notes: str = ""     # short admin-UI hint


@dataclass(frozen=True)
class Pipeline:
    """A content pipeline + the platforms it supports."""
    key: str                            # e.g. "hero_card"
    label: str                          # e.g. "Hero Card"
    platforms: tuple[PlatformDefault, ...]
    description: str = ""

    def platform_keys(self) -> tuple[str, ...]:
        return tuple(p.key for p in self.platforms)

    def platform(self, platform_key: str) -> PlatformDefault | None:
        for p in self.platforms:
            if p.key == platform_key:
                return p
        return None


# ---------------------------------------------------------------------------
# Registry — declare every pipeline + its supported platforms here
# ---------------------------------------------------------------------------

PIPELINES: tuple[Pipeline, ...] = (
    Pipeline(
        key="game_analysis",
        label="Game Analysis",
        description="Agent-driven recap posts written from one game's curated highlights.",
        platforms=(
            PlatformDefault("hupu", "Hupu", generate=True, autopublish=False, notes="虎扑"),
            PlatformDefault("xiaohongshu", "Xiaohongshu", generate=True, autopublish=False, notes="小红书"),
            PlatformDefault("reddit", "Reddit", generate=True, autopublish=False, notes="r/nba + team subs"),
        ),
    ),
    Pipeline(
        key="metric_analysis",
        label="Metric Analysis",
        description="Agent-driven deep-dive posts about one metric.",
        platforms=(
            PlatformDefault("hupu", "Hupu", generate=True, autopublish=False, notes="虎扑"),
            PlatformDefault("xiaohongshu", "Xiaohongshu", generate=True, autopublish=False, notes="小红书"),
            PlatformDefault("reddit", "Reddit", generate=True, autopublish=False, notes="r/nba + team subs"),
        ),
    ),
    Pipeline(
        key="hero_card",
        label="Hero Card",
        description="Deterministic per-game hero highlight social posts (Funba home feed + Twitter).",
        platforms=(
            PlatformDefault("twitter", "Twitter", generate=True, autopublish=False, notes="X / Twitter — needs human review by default"),
            PlatformDefault("funba", "Funba (home feed)", generate=True, autopublish=True, notes="自家首页瀑布流 — 默认自动发布"),
            PlatformDefault("instagram", "Instagram", generate=False, autopublish=False, notes="IG — 额外生成方版海报，目前手动发布"),
        ),
    ),
)


_PIPELINES_BY_KEY: dict[str, Pipeline] = {p.key: p for p in PIPELINES}


def known_pipelines() -> tuple[Pipeline, ...]:
    return PIPELINES


def get_pipeline(pipeline_key: str) -> Pipeline | None:
    return _PIPELINES_BY_KEY.get(pipeline_key)


def _setting_key(pipeline_key: str, platform_key: str, action: str) -> str:
    return f"pipeline.{pipeline_key}.platform.{platform_key}.{action}"


def _normalize_bool(value: str | bool | None, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if raw in {"true", "1", "yes", "on", "y"}:
        return True
    if raw in {"false", "0", "no", "off", "n", ""}:
        return False
    return default


def _platform_default(pipeline: Pipeline, platform_key: str, action: str) -> bool:
    pf = pipeline.platform(platform_key)
    if pf is None:
        return False
    return pf.generate if action == GENERATE_ACTION else pf.autopublish


def _read_cell(session: Session, pipeline: Pipeline, platform_key: str, action: str) -> bool:
    if action not in _VALID_ACTIONS:
        raise ValueError(f"Invalid action: {action}")
    if pipeline.platform(platform_key) is None:
        # Platform isn't declared for this pipeline — caller should not be
        # asking. Treat as "off" to avoid producing variants for an
        # unsupported destination.
        return False
    row = session.get(Setting, _setting_key(pipeline.key, platform_key, action))
    default = _platform_default(pipeline, platform_key, action)
    if row is None:
        return default
    return _normalize_bool(row.value, default)


def is_generate_enabled(session: Session, pipeline_key: str, platform_key: str) -> bool:
    pipeline = get_pipeline(pipeline_key)
    if pipeline is None:
        return False
    return _read_cell(session, pipeline, platform_key, GENERATE_ACTION)


def is_autopublish_enabled(session: Session, pipeline_key: str, platform_key: str) -> bool:
    pipeline = get_pipeline(pipeline_key)
    if pipeline is None:
        return False
    return _read_cell(session, pipeline, platform_key, AUTOPUBLISH_ACTION)


def enabled_generate_platforms(session: Session, pipeline_key: str) -> list[str]:
    pipeline = get_pipeline(pipeline_key)
    if pipeline is None:
        return []
    return [p.key for p in pipeline.platforms if _read_cell(session, pipeline, p.key, GENERATE_ACTION)]


def autopublish_platforms(session: Session, pipeline_key: str) -> set[str]:
    pipeline = get_pipeline(pipeline_key)
    if pipeline is None:
        return set()
    return {p.key for p in pipeline.platforms if _read_cell(session, pipeline, p.key, AUTOPUBLISH_ACTION)}


def set_cell(
    session: Session,
    pipeline_key: str,
    platform_key: str,
    action: str,
    value: bool,
) -> bool:
    """Persist one cell of the pipeline×platform matrix. Caller commits."""
    pipeline = get_pipeline(pipeline_key)
    if pipeline is None:
        raise ValueError(f"Unknown pipeline: {pipeline_key}")
    if pipeline.platform(platform_key) is None:
        raise ValueError(f"Pipeline {pipeline_key!r} does not support platform {platform_key!r}")
    if action not in _VALID_ACTIONS:
        raise ValueError(f"Invalid action: {action}")

    key = _setting_key(pipeline_key, platform_key, action)
    text = "true" if value else "false"
    now = datetime.utcnow()
    row = session.get(Setting, key)
    if row is None:
        session.add(Setting(key=key, value=text, updated_at=now))
    else:
        row.value = text
        row.updated_at = now
    return value


def get_publishing_matrix(session: Session) -> dict:
    """Snapshot of the full matrix for admin UI."""
    payload: list[dict] = []
    for pipeline in PIPELINES:
        platforms_payload: list[dict] = []
        for platform in pipeline.platforms:
            platforms_payload.append({
                "key": platform.key,
                "label": platform.label,
                "notes": platform.notes,
                "default_generate": platform.generate,
                "default_autopublish": platform.autopublish,
                "generate": _read_cell(session, pipeline, platform.key, GENERATE_ACTION),
                "autopublish": _read_cell(session, pipeline, platform.key, AUTOPUBLISH_ACTION),
            })
        payload.append({
            "key": pipeline.key,
            "label": pipeline.label,
            "description": pipeline.description,
            "platforms": platforms_payload,
        })
    return {"pipelines": payload}


def update_publishing_matrix(session: Session, updates: Iterable[dict]) -> dict:
    """Apply a batch of updates: each item = {pipeline, platform, action, value}.

    Caller commits after this returns. Unknown cells raise.
    """
    applied: list[dict] = []
    for item in updates:
        pipeline_key = str(item.get("pipeline") or "").strip()
        platform_key = str(item.get("platform") or "").strip()
        action = str(item.get("action") or "").strip()
        value = bool(item.get("value"))
        set_cell(session, pipeline_key, platform_key, action, value)
        applied.append({
            "pipeline": pipeline_key,
            "platform": platform_key,
            "action": action,
            "value": value,
        })
    return {"applied": applied, "matrix": get_publishing_matrix(session)}

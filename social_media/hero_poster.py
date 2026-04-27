"""Generate a Funba hero card poster (PNG) for one curated highlight.

Pipeline:
    HeroHighlightCard
      ─► build prompt context (metric def, top 10, trigger row, game)
      ─► render prompt template (Jinja-style {placeholders})
      ─► call gpt-image-2 via social_media.funba_imagegen.generate_image
      ─► save to media/hero_posters/{game_id}/{stable_key}.png

The prompt template lives in the Setting table (key = HERO_POSTER_PROMPT_TEMPLATE_KEY)
so admins can edit it without a deploy. A package default template is shipped
below and used as a fallback when the Setting row is missing or empty.

The generator is idempotent: if the target path already exists and is
non-empty, the call returns it without regenerating.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy.orm import Session

from db.models import (
    Game,
    MetricDefinition,
    MetricResult,
    Player,
    PlayerGameStats,
    Setting,
    Team,
)

logger = logging.getLogger(__name__)

HERO_POSTER_PROMPT_TEMPLATE_KEY = "hero_poster_prompt_template"
HERO_POSTER_MODEL_KEY = "hero_poster_model"
HERO_POSTERS_SUBDIR = "hero_posters"
HERO_POSTER_DEFAULT_MODEL = "gpt-image-2"
HERO_POSTER_DEFAULT_SIZE = "1024x1536"
HERO_POSTER_DEFAULT_QUALITY = "high"
HERO_POSTER_TOP_N = 10

# Default prompt template — admin-editable via the Setting row above. Placeholders
# in {curly_braces} are substituted via str.format with the context dict produced
# by build_prompt_context(). Conditional blocks use a tiny Jinja-style syntax
# implemented in render_prompt(): {% if FLAG %}...{% endif %}.
DEFAULT_HERO_POSTER_PROMPT_TEMPLATE = """\
You are designing a vertical 1024x1536 social-media-ready NBA infographic
poster, formatted like a leaderboard, about ONE specific metric and the
TOP {top_n} standings of that metric this season — with the row triggered by
tonight's game visually highlighted.

================ METRIC ================
Key:           {metric_key}
Name:          {metric_name}
Description:   {metric_description}
Scope:         {metric_scope}
Category:      {metric_category}
Season frame:  {season_label}

================ TRIGGERING GAME ================
Tonight, {game_score_line} ({game_date} · {game_stage}) produced an entry
on this leaderboard.

Trigger entity: {trigger_label}
Trigger team:   {trigger_team_full}
Trigger value:  {trigger_value_str}
Trigger rank:   #{trigger_rank} ({trigger_window})

{% if trigger_full_line %}Trigger full game line: {trigger_full_line}
{% endif %}================ TOP {top_n} ================
Render the leaderboard as {top_n} horizontal rows, ordered top to bottom.

Row anatomy:
  - rank number on the far left
  - the entity's primary visual: a player headshot for player metrics, a
    team logo for team or game metrics
  - the entity's display name as text (full team name for team rows;
    player name plus optional jersey number and three-letter team abbr
    for player rows). DO NOT show both the full team name AND the
    three-letter abbreviation in the same team row — that's redundant
    when the logo and full name are already there.
  - the metric value right-aligned

{% if trigger_in_topn %}The triggering row (rank {trigger_rank}, {trigger_label})
was produced by tonight's game — render that row noticeably taller,
brighter, with a glowing silver-white border and a small "TRIGGERED
TONIGHT" badge on the row. The other rows are slimmer and darker.
DO NOT also append a duplicate appended row for the trigger at the
bottom — it is already highlighted in place above.{% endif %}

Use these EXACT entries in this EXACT order; do not invent or substitute:

{top_n_table}

{% if not trigger_in_topn %}APPEND ONE EXTRA ROW BELOW THE TOP {top_n}, separated
by a thin divider, labelled "TRIGGERED TONIGHT — outside top {top_n}":

  {trigger_appendix_row}

{% endif %}================ LAYOUT ================
- Header (top ~15%):
    Top-left  : white rounded pill "FUNBA" in bold sans-serif
    Top-right : subtle pill "{game_stage_pill}"
    Centered  : two-line title in big chrome-silver bevelled type with soft glow:
                  {title_line_1}
                  {title_line_2}

- Leaderboard (middle ~70%): the rows described above.

- Footer (bottom ~15%):
    Centered "FUNBA.APP" in clean uppercase white
    Subtitle "MORE STATS · MORE INSIGHTS"

================ VISUAL ASSETS ================
- Player headshots / team imagery: render real, recognizable likenesses
  in the player's or team's current identity. Each headshot or team mark
  should be a clean circular crop.
- Team logos: render accurate official-style NBA team logos next to each
  row. Each logo small and circular.
- Do NOT include the NBA league logo or any league mark.
- Do NOT include any broadcaster watermarks.

================ TYPOGRAPHY RULES ================
- All numerical values must render EXACTLY as written above.
- All three-letter team abbreviations must be spelled correctly.
- All player or team names must be spelled correctly as listed above.

================ AESTHETIC ================
Adopt the visual identity of {trigger_team_full} as the dominant palette
(infer the team's primary, secondary, and accent colours from your
knowledge of the NBA). Dark cinematic background with subtle metallic
accents and a single warm highlight ray behind the triggering row.
Premium broadcast graphics energy, ESPN / NBA Studios / Bleacher Report
editorial feel, high contrast, clean grid alignment, social-media share
friendly (1024x1536 vertical safe zone).
"""


# ---------------------------------------------------------------------------
# Setting helpers
# ---------------------------------------------------------------------------

def get_hero_poster_prompt_template(session: Session) -> str:
    row = session.get(Setting, HERO_POSTER_PROMPT_TEMPLATE_KEY)
    if row and row.value and row.value.strip():
        return row.value
    return DEFAULT_HERO_POSTER_PROMPT_TEMPLATE


def set_hero_poster_prompt_template(session: Session, value: str) -> str:
    text = (value or "").strip() or DEFAULT_HERO_POSTER_PROMPT_TEMPLATE
    row = session.get(Setting, HERO_POSTER_PROMPT_TEMPLATE_KEY)
    if row is None:
        row = Setting(
            key=HERO_POSTER_PROMPT_TEMPLATE_KEY,
            value=text,
            updated_at=datetime.utcnow(),
        )
        session.add(row)
    else:
        row.value = text
        row.updated_at = datetime.utcnow()
    return text


def get_hero_poster_model(session: Session) -> str:
    row = session.get(Setting, HERO_POSTER_MODEL_KEY)
    if row and row.value and row.value.strip():
        return row.value.strip()
    return HERO_POSTER_DEFAULT_MODEL


# ---------------------------------------------------------------------------
# Prompt template rendering — supports {var} substitution + tiny if blocks
# ---------------------------------------------------------------------------

_IF_BLOCK_RE = re.compile(
    r"\{%\s*if\s+(?P<negate>not\s+)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*%\}"
    r"(?P<body>.*?)"
    r"\{%\s*endif\s*%\}",
    re.DOTALL,
)


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return bool(value)


def render_prompt(template: str, context: dict[str, Any]) -> str:
    """Render the template string against the context.

    Two substitutions:
      1. `{% if NAME %}...{% endif %}` and `{% if not NAME %}...{% endif %}`
         where NAME is a key in `context`.
      2. `{var}` style str.format substitution against `context`.

    Unknown `{var}` placeholders are left literal so a typo in the template
    does not blow up the worker — the resulting prompt will visibly contain
    the placeholder, which surfaces the bug to the admin reviewer.
    """
    def _resolve_block(match: re.Match[str]) -> str:
        name = match.group("name")
        body = match.group("body")
        flag = _truthy(context.get(name))
        if match.group("negate"):
            flag = not flag
        return body if flag else ""

    rendered = _IF_BLOCK_RE.sub(_resolve_block, template)
    try:
        return rendered.format_map(_SafeDict(context))
    except Exception:
        logger.exception("hero poster prompt rendering failed; falling back to raw template")
        return rendered


class _SafeDict(dict):
    """Dict subclass that keeps unknown placeholders literal during str.format."""

    def __missing__(self, key: str) -> str:  # pragma: no cover - trivial
        return "{" + key + "}"


# ---------------------------------------------------------------------------
# Context builder — pulls metric def, top N, trigger details from DB
# ---------------------------------------------------------------------------

_DEFAULT_TOP_N = HERO_POSTER_TOP_N


def _safe_str(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    s = str(value).strip()
    return s if s else fallback


def _player_label(session: Session, entity_id: str) -> tuple[str, Player | None]:
    p = session.query(Player).filter(Player.player_id == entity_id).first()
    if p is None:
        return entity_id, None
    return p.full_name or entity_id, p


def _team_label(session: Session, team_id: str) -> tuple[str, Team | None]:
    t = session.query(Team).filter(Team.team_id == team_id).first()
    if t is None:
        return team_id, None
    return (t.abbr or t.full_name or team_id), t


def _team_for_player_in_game(session: Session, player_id: str, game_id: str) -> Team | None:
    pgs = (
        session.query(PlayerGameStats)
        .filter(PlayerGameStats.player_id == player_id, PlayerGameStats.game_id == game_id)
        .first()
    )
    if not pgs:
        return None
    return session.query(Team).filter(Team.team_id == pgs.team_id).first()


def _format_top_row(rank: int, name: str, team_abbr: str | None, team_full: str | None, jersey: str | None, value_str: str) -> str:
    pieces = [f"{rank:>2}.", name]
    if jersey:
        pieces.append(f"#{jersey}")
    if team_abbr:
        pieces.append(team_abbr)
    if team_full and team_full != team_abbr:
        pieces.append(f"({team_full})")
    pieces.append("—")
    pieces.append(value_str)
    return " ".join(pieces)


def _full_player_line(pgs: PlayerGameStats | None) -> str:
    if pgs is None:
        return ""
    bits: list[str] = []
    if pgs.pts is not None:
        bits.append(f"{int(pgs.pts)} PTS")
    if pgs.reb is not None:
        bits.append(f"{int(pgs.reb)} REB")
    if pgs.ast is not None:
        bits.append(f"{int(pgs.ast)} AST")
    if pgs.stl is not None and pgs.stl:
        bits.append(f"{int(pgs.stl)} STL")
    if pgs.blk is not None and pgs.blk:
        bits.append(f"{int(pgs.blk)} BLK")
    if pgs.plus is not None:
        sign = "+" if pgs.plus >= 0 else ""
        bits.append(f"{sign}{int(pgs.plus)} +/-")
    return " · ".join(bits)


def _season_stage(season: str | None) -> tuple[str, str]:
    """Return (stage_word, stage_pill_word) — recognises virtual season keys
    like all_playoffs / last3_playoffs / all_regular as well as numeric
    single-season ids (2xxxx, 4xxxx, 5xxxx)."""
    raw = str(season or "")
    if raw[:1] == "4" or "_playoffs" in raw:
        return "playoffs", "PLAYOFFS"
    if raw[:1] == "5" or "_playin" in raw:
        return "play-in", "PLAY-IN"
    return "regular season", "REGULAR SEASON"


def _season_label(season: str | None) -> str:
    """Year/scope label suitable for prefixing 'NBA <stage>' to."""
    raw = str(season or "")
    if raw.startswith("all_"):
        return "All-Time"
    if raw.startswith("last3_"):
        return "Last 3 Seasons"
    if raw.startswith("last5_"):
        return "Last 5 Seasons"
    if len(raw) == 5 and raw.isdigit():
        year = raw[1:]
        try:
            return f"{year}-{str(int(year) + 1)[-2:]}"
        except ValueError:
            return raw
    return raw or "season"


def build_prompt_context(
    session: Session,
    *,
    card: dict[str, Any],
    game: Game,
    top_n: int = _DEFAULT_TOP_N,
) -> dict[str, Any]:
    """Build the substitution context for the prompt template.

    `card` is the bundled view of one curated hero entry. It can be either:
      - a raw entry from highlights_curated_*_json (with `metric_key`,
        `entity_id`, `value_snapshot`, `rank_snapshot`, etc.), OR
      - a HeroHighlightCard dataclass converted via dataclasses.asdict.

    The triggering game is `game`. Top N is the leaderboard size to
    surface to GPT.
    """
    metric_key = _safe_str(card.get("metric_key") or card.get("ranking_metric_key"))
    md = (
        session.query(MetricDefinition)
        .filter(MetricDefinition.key == metric_key)
        .first()
    )
    metric_name = (
        _safe_str(card.get("metric_name"))
        or (md.name if md and md.name else metric_key.replace("_", " ").title())
    )
    metric_description = (md.description if md and md.description else "") or ""
    metric_scope = (md.scope if md and md.scope else _safe_str(card.get("scope"), "game"))
    metric_category = (md.category if md and md.category else "general")

    season = _safe_str(card.get("ranking_season") or card.get("season") or game.season)
    stage_word, stage_pill_word = _season_stage(season)
    season_label = f"{_season_label(season)} NBA {stage_word.title()}"

    # ---- top N ----
    rank_order = "desc"
    try:
        from metrics.framework.runtime import get_metric

        m = get_metric(metric_key, session=session)
        rank_order = "asc" if str(getattr(m, "rank_order", "desc")).lower() == "asc" else "desc"
    except Exception:
        pass
    order_col = MetricResult.value_num.asc() if rank_order == "asc" else MetricResult.value_num.desc()
    rows: Iterable[MetricResult] = (
        session.query(MetricResult)
        .filter(
            MetricResult.metric_key == metric_key,
            MetricResult.season == season,
            MetricResult.value_num.isnot(None),
        )
        .order_by(order_col, MetricResult.entity_id.asc())
        .limit(top_n)
        .all()
    )

    trigger_entity_id = _safe_str(card.get("entity_id"))
    trigger_game_id = str(game.game_id)

    # Locate the trigger row inside the top-N. Two-pass match: first try
    # entity_id + game_id (single-game metrics where the same player has
    # multiple top rows); fall back to entity_id alone (season aggregates
    # often have row.game_id == NULL, and team-scope metrics never carry a
    # game_id).
    trigger_rank: int | None = None
    if trigger_entity_id:
        for idx, row in enumerate(rows, start=1):
            if str(row.entity_id) == trigger_entity_id and str(row.game_id or "") == trigger_game_id:
                trigger_rank = idx
                break
        if trigger_rank is None:
            for idx, row in enumerate(rows, start=1):
                if str(row.entity_id) == trigger_entity_id:
                    trigger_rank = idx
                    break
    trigger_in_topn = trigger_rank is not None

    top_lines: list[str] = []
    for idx, row in enumerate(rows, start=1):
        entity_id = _safe_str(row.entity_id)
        is_trigger = trigger_in_topn and idx == trigger_rank
        if metric_scope == "player":
            name, p = _player_label(session, entity_id)
            team = _team_for_player_in_game(session, entity_id, str(row.game_id or ""))
            jersey = p.jersey if p else None
            line = _format_top_row(
                idx,
                name,
                team.abbr if team else None,
                team.full_name if team else None,
                jersey,
                _safe_str(row.value_str, "?"),
            )
        elif metric_scope == "team":
            tm = session.query(Team).filter(Team.team_id == entity_id).first()
            # For team scope show only the full name (no abbr appended). The
            # logo + full name carries everything; doubling up made GPT print
            # "Boston Celtics | BOS" with the abbr column redundant.
            line = _format_top_row(
                idx,
                tm.full_name if tm else entity_id,
                None,
                None,
                None,
                _safe_str(row.value_str, "?"),
            )
        else:  # game scope
            g = session.query(Game).filter(Game.game_id == _safe_str(row.game_id or row.entity_id)).first()
            label = ""
            if g:
                home, _ = _team_label(session, str(g.home_team_id or ""))
                road, _ = _team_label(session, str(g.road_team_id or ""))
                label = f"{road} @ {home}"
            line = _format_top_row(idx, label or _safe_str(row.entity_id), None, None, None, _safe_str(row.value_str, "?"))
        if is_trigger:
            line += "    ← TRIGGERED TONIGHT"
        top_lines.append(line)

    # If trigger isn't in top N, query its actual rank.
    trigger_appendix_row = ""
    if not trigger_in_topn and trigger_entity_id:
        trigger_value_num = card.get("value_snapshot")
        if isinstance(trigger_value_num, (int, float)):
            better = (
                session.query(MetricResult)
                .filter(
                    MetricResult.metric_key == metric_key,
                    MetricResult.season == season,
                    MetricResult.value_num.isnot(None),
                    MetricResult.value_num
                    > trigger_value_num
                    if rank_order == "desc"
                    else MetricResult.value_num < trigger_value_num,
                )
                .count()
            )
            trigger_rank = better + 1

    # Trigger row labels
    trigger_label = _safe_str(card.get("entity_label") or card.get("player_name") or card.get("team_abbr") or trigger_entity_id)
    trigger_team_full = ""
    trigger_team_abbr = ""
    trigger_full_line = ""
    if metric_scope == "player" and trigger_entity_id:
        name, p = _player_label(session, trigger_entity_id)
        if not trigger_label:
            trigger_label = name
        team = _team_for_player_in_game(session, trigger_entity_id, str(game.game_id))
        if team:
            trigger_team_full = team.full_name or ""
            trigger_team_abbr = team.abbr or ""
        pgs = (
            session.query(PlayerGameStats)
            .filter(PlayerGameStats.player_id == trigger_entity_id, PlayerGameStats.game_id == str(game.game_id))
            .first()
        )
        trigger_full_line = _full_player_line(pgs)
    elif metric_scope == "team" and trigger_entity_id:
        tm = session.query(Team).filter(Team.team_id == trigger_entity_id).first()
        if tm:
            trigger_label = tm.full_name or trigger_label
            trigger_team_full = tm.full_name or ""
            trigger_team_abbr = tm.abbr or ""
    else:
        # game scope — pick the home or road team perspective
        home, _ = _team_label(session, str(game.home_team_id or ""))
        road, _ = _team_label(session, str(game.road_team_id or ""))
        winner = session.query(Team).filter(Team.team_id == game.wining_team_id).first() if game.wining_team_id else None
        if winner:
            trigger_team_full = winner.full_name or ""
            trigger_team_abbr = winner.abbr or ""
        if not trigger_label:
            trigger_label = f"{road} @ {home}"

    # Build the appendix row (used when rank > top_n). Team scope prints
    # only full name; player scope prints player name + team abbr (no full
    # name) for compact context.
    if not trigger_in_topn and trigger_rank:
        if metric_scope == "team":
            trigger_appendix_row = _format_top_row(
                trigger_rank,
                trigger_label,
                None,
                None,
                None,
                _safe_str(card.get("value_str_snapshot") or card.get("value_text"), "?"),
            )
        else:
            trigger_appendix_row = _format_top_row(
                trigger_rank,
                trigger_label,
                trigger_team_abbr or None,
                None,
                None,
                _safe_str(card.get("value_str_snapshot") or card.get("value_text"), "?"),
            )

    # Game line + scoreline
    home_team = session.query(Team).filter(Team.team_id == game.home_team_id).first()
    road_team = session.query(Team).filter(Team.team_id == game.road_team_id).first()
    home_abbr = home_team.abbr if home_team else "HOME"
    road_abbr = road_team.abbr if road_team else "ROAD"
    home_score = game.home_team_score if game.home_team_score is not None else "?"
    road_score = game.road_team_score if game.road_team_score is not None else "?"
    game_score_line = f"{road_abbr} {road_score} @ {home_abbr} {home_score}"

    game_date_str = ""
    if game.game_date:
        game_date_str = game.game_date.strftime("%b %-d, %Y") if hasattr(game.game_date, "strftime") else str(game.game_date)

    title_line_1 = metric_name.upper()
    title_line_2 = f"{_season_label(season).upper()} NBA {stage_pill_word} · TOP {top_n}"

    # entity_kind: for player metrics this is "player"; for team/game it's "team".
    if metric_scope == "player":
        entity_kind_word = "player"
    else:
        entity_kind_word = "team"

    return {
        "metric_key": metric_key,
        "metric_name": metric_name,
        "metric_description": metric_description.strip() or "(no description on file)",
        "metric_scope": metric_scope,
        "metric_category": metric_category,
        "season_label": season_label,
        "game_score_line": game_score_line,
        "game_date": game_date_str,
        "game_stage": stage_word,
        "game_stage_pill": f"{stage_pill_word} · {game_date_str.upper()}" if game_date_str else stage_pill_word,
        "trigger_label": trigger_label,
        "trigger_team_full": trigger_team_full or trigger_team_abbr or "the team",
        "trigger_team_abbr": trigger_team_abbr,
        "trigger_value_str": _safe_str(card.get("value_str_snapshot") or card.get("value_text"), "?"),
        "trigger_rank": trigger_rank or 0,
        "trigger_window": _safe_str(card.get("rank_window"), "season"),
        "trigger_full_line": trigger_full_line,
        "trigger_in_topn": trigger_in_topn,
        "trigger_appendix_row": trigger_appendix_row,
        "top_n_table": "\n".join(f"  {line}" for line in top_lines) if top_lines else "  (no top-N rows available)",
        "top_n": top_n,
        "title_line_1": title_line_1,
        "title_line_2": title_line_2,
        "entity_kind": entity_kind_word,
    }


# ---------------------------------------------------------------------------
# File-system helpers + idempotent generator
# ---------------------------------------------------------------------------

def _media_root() -> Path:
    # Deploy worktree puts media/ at the repo root. Allow override via env for tests.
    override = os.getenv("FUNBA_MEDIA_ROOT")
    if override:
        return Path(override).expanduser()
    # Walk up from this file: social_media/hero_poster.py → social_media/ → repo
    return Path(__file__).resolve().parent.parent / "media"


def _safe_segment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "")).strip("._-")
    return cleaned or "x"


def poster_path_for(card: dict[str, Any], game: Game) -> Path:
    metric_key = _safe_str(card.get("metric_key") or card.get("ranking_metric_key"), "metric")
    entity_id = _safe_str(card.get("entity_id"), "game")
    scope = _safe_str(card.get("scope"), "game")
    file_stem = f"{_safe_segment(scope)}.{_safe_segment(metric_key)}.{_safe_segment(entity_id)}"
    return _media_root() / HERO_POSTERS_SUBDIR / _safe_segment(str(game.game_id)) / f"{file_stem}.png"


def _try_claim_poster_file(target: Path) -> bool:
    """Atomically reserve `target` so concurrent callers don't all hit the API.

    Uses os.open with O_CREAT|O_EXCL: the first caller wins and creates the
    file as a 0-byte placeholder, subsequent callers get FileExistsError and
    return False. The winner is responsible for filling the file with real
    bytes; on exceptions the caller must remove the placeholder so a retry
    can claim it again.
    """
    try:
        fd = os.open(str(target), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    except Exception:
        logger.exception("hero_poster: failed to claim placeholder %s", target)
        return False
    os.close(fd)
    return True


def _wait_for_poster_completion(target: Path, *, timeout_seconds: float = 300.0, poll_interval: float = 2.0) -> bool:
    """Poll until the target file has non-zero size (winner finished writing)."""
    import time as _time

    deadline = _time.time() + timeout_seconds
    while _time.time() < deadline:
        try:
            if target.exists() and target.stat().st_size > 0:
                return True
        except Exception:
            pass
        _time.sleep(poll_interval)
    return False


def generate_hero_poster(
    session: Session,
    *,
    card: dict[str, Any],
    game: Game,
    model: str | None = None,
    force: bool = False,
) -> Path | None:
    """Generate (or reuse) one hero card poster. Returns the file path or
    None when generation was skipped (no metric_key, no entity_id, etc).

    Idempotent at the file system level: if the destination file already
    exists and is non-empty, returns it without calling the API. Concurrent
    callers race-safely via os.O_CREAT|O_EXCL — only the first one calls the
    paid API, others wait for the file to materialise then return the path.
    Pass `force=True` to bypass both checks and regenerate.
    """
    metric_key = _safe_str(card.get("metric_key") or card.get("ranking_metric_key"))
    if not metric_key:
        logger.info("hero_poster: skipping card with no metric_key (game=%s)", game.game_id)
        return None

    target = poster_path_for(card, game)
    if not force and target.exists() and target.stat().st_size > 0:
        logger.info("hero_poster: reusing existing poster %s", target)
        return target

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.exception("hero_poster: failed to mkdir %s", target.parent)
        return None

    if force and target.exists():
        # Force: drop any existing file (placeholder or real) so we re-claim cleanly.
        try:
            target.unlink()
        except Exception:
            pass

    # Race-safe claim: only one concurrent caller crosses this barrier and
    # actually calls the (paid) API. Losers poll until the winner finishes.
    if not _try_claim_poster_file(target):
        logger.info("hero_poster: another worker is generating %s — waiting", target)
        if _wait_for_poster_completion(target):
            return target
        logger.warning("hero_poster: timeout waiting for %s; returning None", target)
        return None

    template = get_hero_poster_prompt_template(session)
    context = build_prompt_context(session, card=card, game=game)
    prompt = render_prompt(template, context)

    chosen_model = model or get_hero_poster_model(session)

    # Late import to keep module importable without OpenAI client at startup.
    from social_media.funba_imagegen import generate_image

    try:
        out = generate_image(
            prompt=prompt,
            output_path=target,
            model=chosen_model,
            size=HERO_POSTER_DEFAULT_SIZE,
            quality=HERO_POSTER_DEFAULT_QUALITY,
            output_format="png",
            background="opaque",
        )
    except Exception:
        logger.exception("hero_poster: generate_image failed (game=%s metric=%s)", game.game_id, metric_key)
        # Remove the empty placeholder so a future call can retry instead of
        # forever-waiting on a 0-byte file.
        try:
            if target.exists() and target.stat().st_size == 0:
                target.unlink()
        except Exception:
            pass
        return None

    return Path(out) if out else None


def backfill_posters_into_existing_posts(
    session: Session,
    game_id: str,
    *,
    add_funba_variant: bool = True,
) -> dict[str, Any]:
    """One-off: attach already-generated hero poster files to SocialPost rows
    that were created BEFORE the curator-side hookup landed. New games get
    posters attached automatically via _create_post_for_card; this helper is
    only for catch-up on legacy posts.
    """
    import json as _json
    from datetime import UTC as _UTC, datetime as _dt

    from db.models import (
        SocialPost,
        SocialPostDelivery,
        SocialPostImage,
        SocialPostVariant,
    )

    posts = (
        session.query(SocialPost)
        .filter(SocialPost.source_game_ids.like(f"%{game_id}%"))
        .filter(SocialPost.status != "archived")
        .all()
    )
    attached = 0
    funba_variants_added = 0
    skipped = 0
    for post in posts:
        # Topic is "Hero Highlight — {game_id} — {scope} — {metric_key} — {entity_id}"
        parts = [p.strip() for p in str(post.topic or "").split("—")]
        if len(parts) < 5 or parts[0] != "Hero Highlight" or parts[1] != game_id:
            skipped += 1
            continue
        scope, metric_key, entity_id = parts[2], parts[3], parts[4]
        # Filenames go through _safe_segment which strips ":" etc, so compare
        # the sanitized versions on both sides.
        safe_entity = _safe_segment(entity_id)
        candidates = list_hero_posters_for_game(game_id)
        match = next(
            (c for c in candidates if c["scope"] == scope and c["metric_key"] == metric_key and c["entity_id"] == safe_entity),
            None,
        )
        if not match:
            # Fallback: match by metric_key + entity_id only (scope mismatch tolerable)
            match = next(
                (c for c in candidates if c["metric_key"] == metric_key and c["entity_id"] == safe_entity),
                None,
            )
        if not match:
            skipped += 1
            continue

        # Attach SocialPostImage if not already there
        existing_img = (
            session.query(SocialPostImage)
            .filter(SocialPostImage.post_id == post.id, SocialPostImage.slot == "poster")
            .first()
        )
        if existing_img is None:
            from social_media.images import store_prepared_image

            stored = store_prepared_image(match["path"], post_id=int(post.id), slot="poster")
            spec = {
                "source": "hero_poster",
                "metric_key": metric_key,
                "entity_id": entity_id,
                "scope": scope,
                "model": "gpt-image-2",
            }
            session.add(SocialPostImage(
                post_id=int(post.id),
                slot="poster",
                image_type="ai_generated",
                spec=_json.dumps(spec, ensure_ascii=False),
                note=f"Hero card poster — {metric_key}",
                file_path=stored,
                is_enabled=True,
                created_at=_dt.now(_UTC).replace(tzinfo=None),
            ))
            attached += 1

        # Add funba variant + auto-publish delivery if missing
        if add_funba_variant:
            from content_pipeline.hero_highlight_variants import (
                FUNBA_INTERNAL_PLATFORM,
                HERO_HIGHLIGHT_RENDERERS,
                _variant_title,
                collect_hero_highlight_cards,
            )

            existing_funba = (
                session.query(SocialPostVariant)
                .join(SocialPostDelivery, SocialPostDelivery.variant_id == SocialPostVariant.id)
                .filter(
                    SocialPostVariant.post_id == post.id,
                    SocialPostDelivery.platform == FUNBA_INTERNAL_PLATFORM,
                )
                .first()
            )
            if existing_funba is None:
                from db.models import Game as _Game

                game = session.query(_Game).filter(_Game.game_id == game_id).first()
                if game is None:
                    continue
                cards = collect_hero_highlight_cards(session, game)
                # Match the card by (scope, metric_key, entity_id)
                card = next(
                    (c for c in cards if c.scope == scope and (c.ranking_metric_key == metric_key or c.metric_key == metric_key)),
                    None,
                )
                if card is None:
                    continue
                renderer = HERO_HIGHLIGHT_RENDERERS[FUNBA_INTERNAL_PLATFORM]
                now = _dt.now(_UTC).replace(tzinfo=None)
                variant = SocialPostVariant(
                    post_id=int(post.id),
                    title=_variant_title(card, FUNBA_INTERNAL_PLATFORM),
                    content_raw=renderer(card),
                    audience_hint=f"deterministic hero highlight / {FUNBA_INTERNAL_PLATFORM}",
                    created_at=now,
                    updated_at=now,
                )
                session.add(variant)
                session.flush()
                session.add(SocialPostDelivery(
                    variant_id=int(variant.id),
                    platform=FUNBA_INTERNAL_PLATFORM,
                    forum=None,
                    is_enabled=True,
                    status="published",
                    content_final=variant.content_raw,
                    published_at=now,
                    created_at=now,
                    updated_at=now,
                ))
                funba_variants_added += 1

    session.commit()
    return {
        "game_id": game_id,
        "posts_seen": len(posts),
        "posters_attached": attached,
        "funba_variants_added": funba_variants_added,
        "skipped": skipped,
    }


def list_hero_posters_for_game(game_id: str) -> list[dict[str, str]]:
    """Return all generated hero card poster files for a given game_id.

    Useful for the game-analysis pipeline: when the analyst is assembling
    the image pool for a game-recap post, it can offer these as
    pre-generated candidates instead of asking GPT to draw a poster again.
    Each dict carries `path`, `metric_key`, `entity_id`, `scope` (decoded
    from the file stem) so the caller can pick by topic.
    """
    base = _media_root() / HERO_POSTERS_SUBDIR / _safe_segment(str(game_id))
    if not base.exists() or not base.is_dir():
        return []
    out: list[dict[str, str]] = []
    for p in sorted(base.glob("*.png")):
        stem = p.stem
        # Stem format: "{scope}.{metric_key}.{entity_id}"
        parts = stem.split(".", 2)
        scope = parts[0] if len(parts) > 0 else ""
        metric_key = parts[1] if len(parts) > 1 else ""
        entity_id = parts[2] if len(parts) > 2 else ""
        out.append({
            "path": str(p),
            "scope": scope,
            "metric_key": metric_key,
            "entity_id": entity_id,
        })
    return out


# Convenience used by the curator hook: generate posters for every hero entry
# across the three curated JSONs. Errors per-card are swallowed so a single
# poster failure doesn't block the rest of the pipeline. Image gen calls run
# in parallel — they are pure IO-bound (waiting on the OpenAI API), so a small
# ThreadPoolExecutor cuts wall time roughly to the slowest single call.
def generate_posters_for_curated_game(
    session: Session,
    game: Game,
    *,
    model: str | None = None,
    force: bool = False,
    max_workers: int = 6,
) -> list[Path]:
    from concurrent.futures import ThreadPoolExecutor

    blobs = (
        ("game", game.highlights_curated_json),
        ("player", game.highlights_curated_player_json),
        ("team", game.highlights_curated_team_json),
    )
    cards: list[dict[str, Any]] = []
    for scope, blob in blobs:
        try:
            parsed = json.loads(blob) if blob else {}
        except Exception:
            continue
        for entry in (parsed.get("hero") or []):
            if not isinstance(entry, dict) or not entry.get("metric_key"):
                continue
            entry = dict(entry)
            entry.setdefault("scope", scope)
            cards.append(entry)

    if not cards:
        return []

    # Resolve template + model + context UP FRONT in the calling thread, since
    # session/SQLAlchemy work isn't safe to share across threads. Each worker
    # then only does the file write + HTTP call.
    chosen_model = model or get_hero_poster_model(session)
    template = get_hero_poster_prompt_template(session)
    plans: list[tuple[dict[str, Any], Path, str]] = []
    for entry in cards:
        target = poster_path_for(entry, game)
        if not force and target.exists() and target.stat().st_size > 0:
            plans.append((entry, target, ""))  # empty prompt => reuse path
            continue
        try:
            ctx = build_prompt_context(session, card=entry, game=game)
            prompt = render_prompt(template, ctx)
        except Exception:
            logger.exception("hero_poster: prompt build failed game=%s metric=%s", game.game_id, entry.get("metric_key"))
            continue
        plans.append((entry, target, prompt))

    def _run(plan: tuple[dict[str, Any], Path, str]) -> Path | None:
        entry, target, prompt = plan
        if not prompt:
            return target  # already exists, reused
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            logger.exception("hero_poster: failed to mkdir %s", target.parent)
            return None

        # Race-safe claim across processes. Loser of the race waits for the
        # winner's bytes to land.
        if not _try_claim_poster_file(target):
            logger.info("hero_poster: another worker generating %s — waiting", target)
            if _wait_for_poster_completion(target):
                return target
            return None

        from social_media.funba_imagegen import generate_image

        try:
            out = generate_image(
                prompt=prompt,
                output_path=target,
                model=chosen_model,
                size=HERO_POSTER_DEFAULT_SIZE,
                quality=HERO_POSTER_DEFAULT_QUALITY,
                output_format="png",
                background="opaque",
            )
            return Path(out) if out else None
        except Exception:
            logger.exception("hero_poster: generate_image failed game=%s metric=%s", game.game_id, entry.get("metric_key"))
            try:
                if target.exists() and target.stat().st_size == 0:
                    target.unlink()
            except Exception:
                pass
            return None

    paths: list[Path] = []
    workers = max(1, min(max_workers, len(plans)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(_run, plans):
            if result is not None:
                paths.append(result)
    return paths

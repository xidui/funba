"""Canonical decoder for ``MetricResult.entity_id``.

The shape of ``entity_id`` depends on ``entity_type``. Centralizing the
parsing here means there is exactly one place to read (and update) when
shapes change. Inline ``entity_id.split(":")`` calls drift from the catalog
and silently mis-attribute when the field takes a shape the caller didn't
think of — e.g. anchoring a poster on the winning team when the trigger
player was on the losing side, the bug that originally motivated this
module.

**Code that consumes ``MetricResult.entity_id`` MUST call ``decode`` here
instead of splitting the string itself.** Adding a new shape means adding
it here once; every consumer picks it up automatically.

If you suspect a new shape has appeared, re-run the survey query::

    SELECT entity_type, entity_id
    FROM MetricResult
    WHERE entity_id LIKE '%:%'
    GROUP BY entity_type, entity_id
    ORDER BY entity_type;

------------------------------------------------------------------------
Canonical shape catalog
------------------------------------------------------------------------

``entity_type='player'``
    ``"<player_id>"`` — e.g. ``"1641705"``. Never compound.

``entity_type='team'``
    ``"<team_id>"`` — e.g. ``"1610612759"``. Never compound in current data.
    Some legacy / cross-scope code paths still defensively parse a colon
    out of team-scope entity_ids; for those, use ``team_id_best_effort``.

``entity_type='season'``
    ``"<season_token>"`` — e.g. ``"22025"`` or ``"all_playoffs"``.
    Never compound.

``entity_type='player_franchise'``
    ``"<player_id>:<team_id>"`` — one row per player+team tenure. Always
    has the colon when there's a team to attribute to.

``entity_type='game'`` — four shapes:

    1. ``"<game_id>"`` — the row is the whole game (e.g. ``lead_changes``).
    2. ``"<game_id>:<player_id>"`` — a player-attributable game stat
       (e.g. ``stocks``, ``first_half_blocks_game``).
    3. ``"<game_id>:<team_id>"`` — a team-attributable game stat
       (e.g. ``max_scoring_run``, ``most_rebounds``,
       ``team_threes_made_ranking``, ``dominant_run_window``,
       ``highest/lowest_*_quarter_fg_pct``).
    4. ``"<game_id>:<team_id>:<segment>"`` — a team stat scoped to a game
       segment, e.g. ``"...:Q3"`` (``low_quarter_score``,
       ``single_quarter_team_scoring``).

For game shapes 2 vs 3, the second part is either a player_id or a
team_id. NBA team_ids share the prefix ``1610`` (current league spans
``1610610xxx`` for historical Indianapolis Olympians up through
``1610612766`` for current franchises) and player_ids never collide with
that prefix, so a literal prefix check disambiguates reliably. When a
``Session`` is available, ``decode`` will instead query the ``Team``
table for an authoritative answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# NBA team_ids all start with this prefix. Player_ids never do. Used as
# the disambiguation heuristic when no DB session is available.
_NBA_TEAM_ID_PREFIX = "1610"


@dataclass(frozen=True)
class EntityRef:
    """Decoded form of a ``(entity_type, entity_id)`` pair.

    Only the fields meaningful for the entity_type are populated. Callers
    pick the field they need; absent fields are ``None``. The ``raw``
    field always carries the original entity_id string for logging /
    error messages.

    Convention: when a game-scope row's second compound part is a team_id,
    ``team_id`` is set; when it's a player_id, ``player_id`` is set; both
    are never set simultaneously for ``entity_type='game'``. For
    ``entity_type='player_franchise'``, both ``player_id`` and ``team_id``
    are set.
    """

    entity_type: str
    raw: str
    player_id: Optional[str] = None
    team_id: Optional[str] = None
    game_id: Optional[str] = None
    season: Optional[str] = None
    segment: Optional[str] = None  # game-scope only, e.g. "Q1"–"Q4", "OT1"

    @property
    def base_id(self) -> Optional[str]:
        """The primary id for this entity_type — the value used as a URL
        slug or join key. For game scope this is ``game_id``; for player
        scope, ``player_id``; etc. ``None`` when no field decoded.
        """
        if self.entity_type == "game":
            return self.game_id
        if self.entity_type == "player":
            return self.player_id
        if self.entity_type == "team":
            return self.team_id
        if self.entity_type == "season":
            return self.season
        if self.entity_type == "player_franchise":
            return self.player_id
        return None


def decode(entity_type: str, entity_id: Optional[str], *, session=None) -> EntityRef:
    """Decode ``(entity_type, entity_id)`` into typed parts.

    For ``entity_type='game'`` compounds where the second part is either
    a team_id or a player_id, ``session`` (if given) is used to query the
    ``Team`` table for an authoritative answer. Without a session, the
    ``1610`` team_id prefix is used as a heuristic — fine for current
    NBA data but won't survive a future team_id renumbering.

    Returns an ``EntityRef`` even for unrecognised shapes — the ``raw``
    field is always populated so callers can fall back to displaying
    the original string. Decoded fields stay ``None`` for unknowns.
    """
    raw = (entity_id or "").strip()
    if not raw:
        return EntityRef(entity_type=entity_type, raw=raw)

    if entity_type == "player":
        return EntityRef(entity_type=entity_type, raw=raw, player_id=raw)
    if entity_type == "team":
        return EntityRef(entity_type=entity_type, raw=raw, team_id=raw)
    if entity_type == "season":
        return EntityRef(entity_type=entity_type, raw=raw, season=raw)

    if entity_type == "player_franchise":
        if ":" in raw:
            pid, _, tid = raw.partition(":")
            return EntityRef(
                entity_type=entity_type,
                raw=raw,
                player_id=pid or None,
                team_id=tid or None,
            )
        # Legacy / partial row without the team part — keep the player_id.
        return EntityRef(entity_type=entity_type, raw=raw, player_id=raw)

    if entity_type == "game":
        parts = raw.split(":")
        gid = parts[0]
        rest = parts[1:]
        if not rest:
            return EntityRef(entity_type=entity_type, raw=raw, game_id=gid)
        candidate = rest[0]
        is_team = _looks_like_team_id(candidate, session=session)
        if is_team:
            segment = rest[1] if len(rest) > 1 else None
            return EntityRef(
                entity_type=entity_type,
                raw=raw,
                game_id=gid,
                team_id=candidate,
                segment=segment,
            )
        # candidate is a player_id; current data has no 3-part player shape.
        return EntityRef(
            entity_type=entity_type,
            raw=raw,
            game_id=gid,
            player_id=candidate,
        )

    # Unknown entity_type — return raw only so the caller can degrade
    # to displaying the original string instead of crashing.
    return EntityRef(entity_type=entity_type, raw=raw)


def _looks_like_team_id(candidate: str, *, session=None) -> bool:
    """Return True if ``candidate`` is an NBA team_id.

    Prefers a Team-table lookup when ``session`` is given (most reliable);
    falls back to the ``1610`` prefix convention when not.
    """
    if not candidate:
        return False
    if session is not None:
        # Local import keeps this module dependency-light when callers
        # don't need DB-backed disambiguation.
        from db.models import Team as _Team

        return (
            session.query(_Team.team_id)
            .filter(_Team.team_id == candidate)
            .first()
            is not None
        )
    return candidate.startswith(_NBA_TEAM_ID_PREFIX)


# ----------------------------------------------------------------------
# Defensive cross-scope helpers
# ----------------------------------------------------------------------
#
# A handful of legacy code paths render entity_ids without trusting the
# declared scope — e.g. ``team_id_best_effort`` is called on what the
# caller believes is a team-scope entity_id but might in fact be a
# game-scope ``"<game_id>:<team_id>"`` or ``"<game_id>:<team_id>:<segment>"``
# leaked through a cross-scope rendering path. Real data has no such
# leak today, but the defense was added historically and removing it
# silently is risky. Use these helpers in those exact spots so the
# defense is named and centralized rather than re-implemented inline.
#
# If you find yourself reaching for one of these in NEW code, stop and
# call ``decode`` with the correct entity_type instead — these helpers
# only exist to capture pre-existing defensive parsing.


def team_id_best_effort(entity_id: Optional[str]) -> Optional[str]:
    """Return the team_id for an id that is *expected* to be team-scope
    but might have slipped in as ``"<x>:<team_id>"`` or
    ``"<x>:<team_id>:<seg>"``. Returns the raw id when no ``:``.

    Encodes the historical inline pattern::

        entity_id.split(":")[1] if ":" in entity_id else entity_id
    """
    raw = (entity_id or "").strip()
    if not raw:
        return None
    if ":" not in raw:
        return raw
    parts = raw.split(":")
    return parts[1] if len(parts) >= 2 else raw


def team_id_from_trailing(entity_id: Optional[str]) -> Optional[str]:
    """Return the trailing team_id for ``"<season>:<team_id>"`` shape.
    Returns the raw id when no ``:``.

    Encodes the historical inline pattern::

        entity_id.split(":")[-1]

    Currently used only for team-scope news rendering (``db/news_internal.py``);
    real data does not produce ``<season>:<team_id>`` rows today.
    """
    raw = (entity_id or "").strip()
    if not raw:
        return None
    return raw.split(":")[-1]

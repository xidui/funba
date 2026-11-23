"""Next UI: awards page extras — "most wins" leaderboards per award section."""
from __future__ import annotations

from collections import Counter


def enrich(ctx: dict, api) -> dict:
    sections = ctx.get("award_sections") or []
    boards = {}
    for section in sections:
        counts: Counter = Counter()
        meta: dict = {}
        latest: dict = {}
        for group in section.get("groups") or []:
            for entry in group.get("entries") or []:
                if section.get("is_team_award"):
                    key = entry.get("team_id") or entry.get("team_name")
                else:
                    key = entry.get("player_id") or entry.get("player_name")
                if not key:
                    continue
                counts[key] += 1
                meta.setdefault(key, entry)
                latest.setdefault(key, group.get("season_label"))
        top = []
        for key, n in counts.most_common(8):
            if n < 2 and len(top) >= 3:
                break
            entry = meta[key]
            top.append({
                "count": n,
                "player_id": entry.get("player_id"),
                "player_name": entry.get("player_name"),
                "team_id": entry.get("team_id"),
                "team_name": entry.get("team_name"),
                "team_abbr": entry.get("team_abbr"),
                "last": latest.get(key),
            })
        boards[section.get("award_type")] = top
    return {"boards": boards}

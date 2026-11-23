"""Next UI — players browse enrichment: season leaders strip + per-player team map."""
from __future__ import annotations


def enrich(ctx: dict, api) -> dict:
    season = str(ctx.get("selected_season") or "")
    out: dict = {"leaders": [], "season": season}
    if not season.startswith("2"):
        return out
    try:
        out["leaders"] = api.league_leaders(season, per_cat=3)
    except Exception:
        api.app.logger.warning("next-ui players leaders failed", exc_info=True)
    return out

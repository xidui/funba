"""Next UI — teams list enrichment: current conferences → divisions → teams with records."""
from __future__ import annotations


def _records_for(records_by_year: dict, year) -> dict:
    if not records_by_year:
        return {}
    return records_by_year.get(year) or records_by_year.get(str(year)) or (
        records_by_year.get(int(year)) if str(year).isdigit() else None
    ) or {}


def enrich(ctx: dict, api) -> dict:
    divisions_map = ctx.get("divisions_map") or {}
    groups = ctx.get("divisions_groups") or []
    year = ctx.get("default_year")
    records = _records_for(ctx.get("records_by_year") or {}, year)

    def rec(team_id: str) -> dict:
        r = records.get(team_id) or records.get(str(team_id)) or {}
        w, l = int(r.get("w") or 0), int(r.get("l") or 0)
        return {"w": w, "l": l, "pct": (w / (w + l)) if (w + l) else None}

    # conference ranks by win pct
    conf_rank: dict = {}
    for conf in ("E", "W"):
        ids = [tid for tid, (c, _d) in ((k, tuple(v)[:2]) for k, v in divisions_map.items()) if c == conf]
        ranked = sorted(ids, key=lambda tid: (-(rec(tid)["pct"] or 0), -rec(tid)["w"]))
        for i, tid in enumerate(ranked, 1):
            conf_rank[tid] = i

    confs = []
    for conf, label_en, label_zh in (("E", "East", "东部"), ("W", "West", "西部")):
        divisions = []
        for grp in groups:
            g = list(grp)
            if g[0] != conf:
                continue
            name = g[1]
            name_zh = g[2] if len(g) > 2 else name
            teams = []
            for tid, v in divisions_map.items():
                v = list(v)
                if v[0] != conf or v[1] != name:
                    continue
                info = api.team_info(tid)
                r = rec(tid)
                teams.append({
                    "team_id": tid,
                    "abbr": info.get("abbr"),
                    "name": info.get("name"),
                    "name_zh": info.get("name_zh"),
                    "nick": info.get("nick"),
                    "slug": info.get("slug"),
                    "color": api.team_color(tid),
                    "color2": api.team_color(tid, 1),
                    "rank": conf_rank.get(tid),
                    **r,
                })
            teams.sort(key=lambda x: (-(x["pct"] or 0), -x["w"], x["abbr"] or ""))
            divisions.append({"name": name, "name_zh": name_zh, "teams": teams})
        confs.append({"conf": conf, "label_en": label_en, "label_zh": label_zh, "divisions": divisions})
    has_records = any(t["w"] or t["l"] for c in confs for d in c["divisions"] for t in d["teams"])
    return {"confs": confs, "has_records": has_records, "year": year}

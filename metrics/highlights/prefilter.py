"""Pre-filter raw game metric candidates before handing to the LLM curator.

Cheap rules-based pass that trims 30+ raw metric rows down to ~15 candidates
worth the LLM's attention. The LLM then picks and writes narrative for the
final 5-8.

Candidate dict shape (subset of _build_game_season_metrics_list output):
    metric_key, metric_name, entity_id, value_num, value_str, context,
    rank, total, all_games_rank, all_games_total,
    last3_rank, last3_total, last5_rank, last5_total
"""
from __future__ import annotations

from typing import Iterable

MAX_CANDIDATES = 15
NOTABLE_RATIO = 0.25


def _best_ratio(entry: dict) -> float:
    """Smallest rank/total across windows — lower = more noteworthy."""
    ratios: list[float] = []
    for r_key, t_key in (
        ("all_games_rank", "all_games_total"),
        ("rank", "total"),
        ("last3_rank", "last3_total"),
        ("last5_rank", "last5_total"),
    ):
        r = entry.get(r_key)
        t = entry.get(t_key)
        if r is not None and t:
            ratios.append(r / t)
    return min(ratios) if ratios else 1.0


def _best_rank_total(entry: dict) -> tuple[int | None, int | None]:
    best: tuple[int | None, int | None] = (None, None)
    best_ratio: float = 2.0
    for r_key, t_key in (
        ("all_games_rank", "all_games_total"),
        ("rank", "total"),
        ("last3_rank", "last3_total"),
        ("last5_rank", "last5_total"),
    ):
        r = entry.get(r_key)
        t = entry.get(t_key)
        if r is not None and t:
            ratio = r / t
            if ratio < best_ratio:
                best_ratio = ratio
                best = (r, t)
    return best


def prefilter_candidates(
    entries: Iterable[dict],
    *,
    max_candidates: int = MAX_CANDIDATES,
    notable_ratio: float = NOTABLE_RATIO,
    session=None,
    tied_drop_threshold: int = 3,
) -> list[dict]:
    """Keep only reasonably noteworthy entries, sorted by best rank ratio.

    Drops:
    - Entries whose best rank window has ratio > notable_ratio
    - Entries with no rank data at all
    - Entries where the top rank is meaningless due to ties (when `session`
      is provided): if the candidate sits at rank <= 3 and at least
      `tied_drop_threshold` other entities share its exact value in the
      same pool, the rank is an artifact (e.g., "1 loss, rank 1" early in
      season with 20 teams also at 1 loss).

    Caps at max_candidates after sorting.
    """
    entries_list = [e for e in entries]

    tied_counts: dict[tuple, int] = {}
    if session is not None:
        from db.models import MetricResult as MetricResultModel

        tie_keys: set[tuple] = set()
        for e in entries_list:
            ranks = [
                e.get("rank"), e.get("all_games_rank"),
                e.get("last3_rank"), e.get("last5_rank"),
            ]
            ranks = [r for r in ranks if r is not None]
            best_rank = min(ranks) if ranks else None
            if best_rank is not None and best_rank <= 3 and e.get("value_num") is not None and e.get("season"):
                tie_keys.add((e["metric_key"], "game", e["season"], float(e["value_num"])))
        for mk, et, season, val in tie_keys:
            cnt = (
                session.query(MetricResultModel)
                .filter(
                    MetricResultModel.metric_key == mk,
                    MetricResultModel.entity_type == et,
                    MetricResultModel.season == season,
                    MetricResultModel.value_num == val,
                )
                .count()
            )
            tied_counts[(mk, et, season, val)] = cnt

    scored: list[tuple[float, dict]] = []
    for e in entries_list:
        ratio = _best_ratio(e)
        if ratio > notable_ratio:
            continue
        if tied_counts:
            ranks = [
                e.get("rank"), e.get("all_games_rank"),
                e.get("last3_rank"), e.get("last5_rank"),
            ]
            ranks = [r for r in ranks if r is not None]
            best_rank = min(ranks) if ranks else None
            if best_rank is not None and best_rank <= 3 and e.get("value_num") is not None and e.get("season"):
                key = (e["metric_key"], "game", e["season"], float(e["value_num"]))
                if tied_counts.get(key, 0) >= tied_drop_threshold:
                    continue
        scored.append((ratio, e))
    scored.sort(key=lambda pair: pair[0])
    return [e for _, e in scored[:max_candidates]]


def build_llm_input(entries: Iterable[dict]) -> list[dict]:
    """Flatten pre-filtered entries to a compact list for the LLM prompt.

    Only the fields the LLM needs to reason about the highlight — leaves out
    raw SQL rows, computed_at timestamps, etc.
    """
    out = []
    for e in entries:
        best_rank, best_total = _best_rank_total(e)
        out.append({
            "metric_key": e["metric_key"],
            "metric_name": e.get("metric_name") or e["metric_key"],
            "entity_id": e.get("entity_id"),
            "value": e.get("value_str") or (
                str(e.get("value_num")) if e.get("value_num") is not None else None
            ),
            "value_num": e.get("value_num"),
            "rank": best_rank,
            "total": best_total,
            "season_rank": e.get("rank"),
            "season_total": e.get("total"),
            "alltime_rank": e.get("all_games_rank"),
            "alltime_total": e.get("all_games_total"),
            "context": e.get("context") or {},
            "context_label": e.get("context_label"),
        })
    return out

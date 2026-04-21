"""LLM-based curator for game highlights.

Takes a game + pre-filtered metric candidates, asks an LLM to pick the top
5-8 with a short Chinese narrative for each, and returns a structured result
ready to be stored as Game.highlights_curated_json.

The curator writes narrative text using the rank/value at curation time,
freezing the perspective of the game as it looked then. Later record-breakers
don't rewrite history — the page falls back to these snapshots.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from metrics.framework.generator import _call_llm_with_system
from metrics.highlights.prefilter import build_llm_input

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DEFAULT_MODEL = "gpt-5.4-nano"
MAX_HERO = 2
MAX_NOTABLE = 6


SYSTEM_PROMPT = """你是 NBA 比赛精华编辑。你的工作是从候选指标（metric）里挑出最值得告诉读者的本场"看点"，并为每条写一句简短的中文解说。

## 输入
- 本场比赛的基本信息（队伍、比分、胜负、赛季）
- 候选指标列表（已按排名百分位预筛过，约 10-15 条）。每条带 metric_key、指标名、实体（球队或球员）、数值、排名、排名池总量、可选的 context。

## 你的任务
从候选里选出：
- `hero`：本场最亮眼、最能概括比赛故事的 1-2 条。
- `notable`：其余确实值得提的 3-6 条。
- 其余候选全部丢弃。

## 筛选原则
1. **真正的稀有性**。比如 "球队 1 场输球排第一" 这种早期赛季的假排名，必须丢。排名第 1 但是值非常小、明显是多队并列的，也要丢。
2. **不重复**。同一概念不同包装只保留一条（例如 "最终分差" 和 "胜分差"；"最高得分高潮" 出现两次）。
3. **不要普通 box score**。半场分差、领先变换次数、总失误数等本来就是每场都有的统计，除非值确实出色（队史/赛季头部），否则不要。
4. **偏好叙事钩子**。宁可选一个能讲故事的（"赛季第 X"、"球员生涯第 Y"），也不要一个光有数字没语境的。
5. **胜负平衡**。尽量挑能概括比赛的一组（赢球方的亮点 + 输球方的亮点），不要全是一边的。

## narrative 规则（非常重要）
- **中文**，一句话，10-25 字为佳，不超过 40 字。
- 不加 "本场"、"这场比赛" 这种赘词，页面上下文已经交代。
- **不重复指标本身的定义**。例如指标叫 "N-0 得分潮"，你写 "打出 17-0 得分潮" 就够了，不要再加 "对手 0 分"，那是 N-0 的定义。
- **不用引号、感叹号、夸张词**。不要写 "创下'xxx'的纪录"、"神级表现"、"史诗级" 等。
- 优先用具体的排名表达（"赛季第 2"、"队史第 3"、"近 5 场最高"），而不是模糊的 "最"、"极高"。
- 只使用候选数据里给出的信息。**不要编造** "前一档"、"上一场"、"生涯第 X" 这种候选里没出现过的语境。
- 如果一个候选的语境不够写出合格的 narrative（只剩数值没故事），可以把它丢掉，不要硬写。

## narrative 示例（好 vs 差）
好：
- "猛龙打出17-0得分潮，把分差拉到38分。"
- "文班亚马35分，赛季单场得分第2高。"
- "马刺命中15记三分，赛季三分命中第3高。"

差（不要这么写）：
- "猛龙打出17-0得分潮，期间对手0分。"（"对手0分"是 N-0 定义的重复）
- "创下'输球最多'的纪录。"（引号 + 夸张）
- "文班亚马35分，赛季仅次于前一档表现。"（"前一档"候选里没给）
- "命中数赛季排名第94且赢球。"（"赛季排名第94" 不算 highlight；并列条件拼接生硬）

## 输出格式（严格 JSON，不要 markdown 代码块）
{
  "hero": [
    {"metric_key": "<key>", "entity_id": "<id>", "narrative": "<中文>"}
  ],
  "notable": [
    {"metric_key": "<key>", "entity_id": "<id>", "narrative": "<中文>"}
  ]
}

其中 metric_key 和 entity_id 必须精确来自候选列表里的值。不要编造新 key。
如果候选全部不合格（早期赛季数据太稀疏），可以返回 `{"hero": [], "notable": []}`，不要硬凑。
"""


def _build_user_message(game_ctx: dict, candidates: list[dict]) -> str:
    payload = {
        "game": game_ctx,
        "candidates": candidates,
    }
    return "候选数据如下，按照 system prompt 里的原则选出 hero + notable 并输出 JSON：\n\n" + json.dumps(
        payload, ensure_ascii=False, indent=2
    )


_JSON_BLOCK = re.compile(r"\{.*\}", re.DOTALL)


def _parse_llm_json(text: str) -> dict:
    stripped = text.strip()
    if stripped.startswith("```"):
        # strip code fence
        stripped = stripped.strip("`")
        if stripped.lower().startswith("json"):
            stripped = stripped[4:].lstrip("\n")
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        match = _JSON_BLOCK.search(stripped)
        if match:
            return json.loads(match.group(0))
        raise


def _snapshot_entry(entry: dict, narrative: str) -> dict:
    """Freeze rank/value from the raw candidate at curation time."""
    return {
        "metric_key": entry["metric_key"],
        "entity_id": entry.get("entity_id"),
        "narrative": narrative,
        "value_snapshot": entry.get("value_num"),
        "value_str_snapshot": entry.get("value_str"),
        "rank_snapshot": {
            "season": entry.get("rank"),
            "season_total": entry.get("total"),
            "alltime": entry.get("all_games_rank"),
            "alltime_total": entry.get("all_games_total"),
            "last3": entry.get("last3_rank"),
            "last3_total": entry.get("last3_total"),
            "last5": entry.get("last5_rank"),
            "last5_total": entry.get("last5_total"),
        },
        "context_label_snapshot": entry.get("context_label"),
    }


def curate_game_highlights(
    *,
    game_context: dict,
    candidates: list[dict],
    model: str | None = None,
) -> dict:
    """Run the LLM curator for a single game.

    Returns a JSON-serializable dict ready to stash on Game.highlights_curated_json.
    Raises on LLM/JSON failure — callers decide whether to fall back.
    """
    if not candidates:
        return {
            "version": SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model": None,
            "hero": [],
            "notable": [],
            "note": "no_candidates",
        }

    llm_candidates = build_llm_input(candidates)
    by_key: dict[tuple[str, str | None], dict] = {}
    for raw, llm in zip(candidates, llm_candidates):
        by_key[(llm["metric_key"], llm.get("entity_id"))] = raw

    user_message = _build_user_message(game_context, llm_candidates)
    selected_model = model or DEFAULT_MODEL
    raw_response = _call_llm_with_system(
        SYSTEM_PROMPT,
        [{"role": "user", "content": user_message}],
        model=selected_model,
        max_tokens=2048,
    )

    parsed = _parse_llm_json(raw_response)

    def _take(section: str, limit: int) -> list[dict]:
        out = []
        for pick in (parsed.get(section) or [])[:limit]:
            key = pick.get("metric_key")
            entity = pick.get("entity_id")
            narrative = (pick.get("narrative") or "").strip()
            if not key or not narrative:
                continue
            raw = by_key.get((key, entity))
            if raw is None:
                logger.warning("LLM returned unknown metric_key=%s entity=%s", key, entity)
                continue
            out.append(_snapshot_entry(raw, narrative))
        return out

    return {
        "version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": selected_model,
        "hero": _take("hero", MAX_HERO),
        "notable": _take("notable", MAX_NOTABLE),
    }


def run_curator_for_game(session, game, *, model: str | None = None) -> dict:
    """End-to-end: build candidates, call LLM, persist result on the Game row.

    Returns the curated dict written to Game.highlights_curated_json.
    """
    from db.models import Team
    from web.app import _build_game_raw_metric_candidates

    raw = _build_game_raw_metric_candidates(session, game.game_id, game.season)
    from metrics.highlights.prefilter import prefilter_candidates

    candidates = prefilter_candidates(raw)
    team_lookup = {t.team_id: t.full_name for t in session.query(Team).all()}
    ctx = build_game_context(game, team_lookup)
    curated = curate_game_highlights(
        game_context=ctx,
        candidates=candidates,
        model=model,
    )
    game.highlights_curated_json = json.dumps(curated, ensure_ascii=False)
    game.highlights_curated_at = datetime.now(timezone.utc)
    game.highlights_curated_model = curated.get("model")
    session.commit()
    try:
        from web.app import _delete_game_metrics_payload_cache

        _delete_game_metrics_payload_cache(game.game_id)
    except Exception:
        logger.exception("failed to invalidate game metrics cache for %s", game.game_id)
    return curated


def build_game_context(game, team_name_lookup: dict[str, str]) -> dict:
    """Build the minimal game context passed to the LLM.

    `game` is a db.models.Game row (read-only use).
    `team_name_lookup` maps team_id → display name.
    """
    home = team_name_lookup.get(game.home_team_id, game.home_team_id)
    road = team_name_lookup.get(game.road_team_id, game.road_team_id)
    winner = game.wining_team_id
    winner_name = team_name_lookup.get(winner, winner) if winner else None
    return {
        "game_id": game.game_id,
        "season": game.season,
        "date": str(game.game_date) if game.game_date else None,
        "home_team": home,
        "home_team_id": game.home_team_id,
        "home_score": game.home_team_score,
        "road_team": road,
        "road_team_id": game.road_team_id,
        "road_score": game.road_team_score,
        "winner_team": winner_name,
        "winner_team_id": winner,
    }

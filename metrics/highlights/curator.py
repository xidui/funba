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
DEFAULT_MODEL = "gpt-5.4"
DEFAULT_REASONING_EFFORT = "xhigh"
MAX_HERO = 2
MAX_NOTABLE = 6
MAX_TRIGGERED_HERO = 2
MAX_TRIGGERED_NOTABLE = 8


def _resolve_curator_settings(session, model_override: str | None = None) -> tuple[str, str]:
    """Return (model, reasoning_effort) for the curator.

    Admin settings take precedence; CLI --model override wins over settings.
    Falls back to package defaults when nothing is stored.
    """
    from db.llm_models import (
        get_curator_reasoning_effort,
        get_llm_model_for_purpose,
    )

    model = model_override
    if not model:
        try:
            model = get_llm_model_for_purpose(session, "curator")
        except Exception:
            model = DEFAULT_MODEL
    effort: str
    try:
        effort = get_curator_reasoning_effort(session)
    except Exception:
        effort = DEFAULT_REASONING_EFFORT
    return model or DEFAULT_MODEL, effort or DEFAULT_REASONING_EFFORT


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
每条 highlight 必须同时输出 **中文 (narrative_zh)** 和 **英文 (narrative_en)** 两个版本，指向同一个事实。
- 中文：一句话，10-25 字为佳，不超过 40 字。
- 英文：one short sentence, 6-18 words, no trailing period is fine.
- 不加 "本场"、"这场比赛" / "this game", "tonight" 之类赘词。
- **不重复指标本身的定义**。例如指标叫 "N-0 得分潮"，写 "打出 17-0 得分潮" 就够，别加 "对手 0 分"。
- **不用引号、感叹号、夸张词**。不写 "创下'xxx'的纪录"、"神级"、"史诗级"、"insane"、"historic"。
- **禁止空洞形容词 / 套话**：不要写 "火力全开"、"高强度对攻"、"主旋律"、"定型"、"制造杀伤"、"拉满"、"奠定差距"、"fully unleashed"、"dominant showing"、"set the tone"。narrative 必须是具体数字+具体事实。
- 优先具体排名（"赛季第 2" / "2nd-best this season"），别用模糊的 "最高" / "record-high"。
- 只使用候选数据里给出的信息。**严禁编造**以下这类候选里没给的表达：
  - "前一档 / 上一档 / 仅次于前一档"（候选里根本没有"前一档"这个字段，不要凭空写）
  - "前一场 / 上一场"
  - "生涯第 X / career-high"（除非候选的指标本身就是生涯级）
  - "队史第 X"（除非候选就是队史级）
  - "近 X 场最高"（除非候选里确实有 last3/last5 rank）
- 看到 "rank: 2" 就写 "赛季第 2"/"2nd-best this season"，不要脑补 "仅次于前面那个"。
- 写不出合格 narrative 的候选就丢掉，不要硬写。

## narrative 示例（好 vs 差）
好：
- zh: "猛龙打出17-0得分潮，把分差拉到38分。" en: "Raptors uncorked a 17-0 run to push the lead to 38."
- zh: "文班亚马35分，赛季单场得分第2高。" en: "Wembanyama dropped 35 — his 2nd-highest total this season."
- zh: "马刺命中15记三分，赛季三分命中第3高。" en: "Spurs buried 15 threes, their 3rd-most in a game this season."

差（不要这么写）：
- "猛龙打出17-0得分潮，期间对手0分。" ("对手0分" 是 N-0 定义的重复)
- "创下'输球最多'的纪录。" (引号 + 夸张)
- "文班亚马35分，赛季仅次于前一档表现。" ("前一档"候选里没给)
- "20记三分全中。" (20 makes ≠ 全中/perfect shooting，幻觉)

## 输出格式（严格 JSON，不要 markdown 代码块）
{
  "hero": [
    {"metric_key": "<key>", "entity_id": "<id>", "narrative_zh": "<中文>", "narrative_en": "<English>"}
  ],
  "notable": [
    {"metric_key": "<key>", "entity_id": "<id>", "narrative_zh": "<中文>", "narrative_en": "<English>"}
  ]
}

其中 metric_key 和 entity_id 必须精确来自候选列表里的值。不要编造新 key。
如果候选全部不合格（早期赛季数据太稀疏），可以返回 `{"hero": [], "notable": []}`，不要硬凑。
"""


PLAYER_SYSTEM_PROMPT = """你是 NBA 比赛精华编辑。任务：从本场比赛里"被触发"的球员级指标（metric）候选里挑出最亮眼的若干条，并为每条写一句中英文解说。

## 输入
- 比赛基本信息（队伍、比分、胜负）
- 候选球员级指标列表（每条带 metric_key、指标名、player_id、player_name、球队、数值、赛季排名、全时排名）

## 你的任务
- `hero`：本场最亮眼的 1-2 位球员表现（例：全场得分王、创生涯记录的表现）
- `notable`：其余值得提的 4-8 条（不同球员或同一球员的不同维度）
- 其余候选全部丢弃

## 筛选原则
1. 样本池稳定度优先顺序：生涯 > 近5场 > 近3场 > 当前赛季。尤其当前赛季刚开始时，concrete-season 的数值都很小，基本都是噪音，要丢掉。
2. 不重复：同一球员的同类指标（例如"得分最高"和"赛季得分最高"），只保留最能讲故事的一条。
3. **同一球员最多输出 2 条**（总共 hero + notable 合计），即使他触发了 10 个 metric 也只选最能代表他本场的 2 条。**必须 spread 到不同的球员**，覆盖比赛里多方面的亮点。
4. 球员要有**具体的名字**出现在 narrative 里。
5. 真实稀有性：rank 1 但值（value）很小（比如得分、助攻 < 5）多半是并列第一，不是真正突出，丢掉。rank/total > 0.2 不够 hero。
6. `source=milestone` 的候选代表本场发生了排名跨越或逼近目标事件，优先作为 hero；叙事必须优先使用候选里的 milestone_context/passed/target/thresholds，不要再脑补外部事实。
7. milestone 的 `event_type=absolute_threshold` 代表本场跨越绝对里程碑（如万分、100 次三双）。narrative 必须引用 threshold_label_zh / threshold_label_en 和 count_reached_before_this_game。
8. milestone 的 `event_type=approaching_absolute` 代表逼近绝对里程碑。narrative 用 threshold_label + new_gap 表达，例如"距万分仅差 47 分"。

## narrative 规则
每条输出 narrative_zh + narrative_en：
- 中文 10-25 字，英文 6-18 词。
- 必须提到球员名字（中文：威斯布鲁克/文班亚马这样的译名；英文：直接用候选里给出的姓名）。
- 不用引号、感叹号、夸张词。
- 具体排名（"赛季第 2"/"2nd-best this season"）优于模糊的"最高"。
- 只使用候选里给出的信息，**不要编造**生涯/职业/历史排名（除非候选数据就是生涯范围）。
- milestone 候选可以直接写"超越 X"或"距离 X 还差 Y"，但 X/Y 必须来自 target/passed 字段。

## 示例
好：
- zh: "文班亚马砍下35分，赛季单场得分排名第2。" en: "Wembanyama dropped 35, his 2nd-best scoring game this season."
- zh: "东契奇生涯首次 40+10+10 三双。" en: "Doncic's first career 40-point triple-double."

差：
- "文班亚马表现出色" (无数字)
- "35分创生涯新高" (候选里没给生涯数据就不要写)

## 输出格式（严格 JSON，不要 markdown）
{
  "hero": [
    {"metric_key": "<key>", "entity_id": "<player_id>", "narrative_zh": "...", "narrative_en": "..."}
  ],
  "notable": [
    {"metric_key": "<key>", "entity_id": "<player_id>", "narrative_zh": "...", "narrative_en": "..."}
  ]
}

entity_id 必须是候选里的 player_id。候选不够好就返回空列表，不要硬凑。
"""


TEAM_SYSTEM_PROMPT = """你是 NBA 比赛精华编辑。任务：从本场比赛里"被触发"的球队级指标候选里挑出最亮眼的若干条，并为每条写一句中英文解说。

## 输入
- 比赛基本信息（队伍、比分、胜负）
- 候选球队级指标列表（每条带 metric_key、指标名、team_id、team_abbr、数值、赛季排名、全时排名）

## 你的任务
- `hero`：本场最亮眼的 1-2 条球队层面表现（例：赛季单场最高三分、队史最大分差）
- `notable`：其余 3-6 条值得提的
- 其余候选全部丢弃

## 筛选原则
1. 排名越前（尤其全时 / 队史）越优先。
2. 不重复：同一球队同一维度只保留最强一条。**同一球队最多输出 3 条**（hero + notable 合计）；优先 spread 到两支球队。
3. 真实稀有性：rank 1 但值（value）很小（比如 "1 场输球" / "1 场胜利"）几乎肯定是赛季初和大片队伍并列第一，不算亮点，丢掉。
4. rank/total > 0.2 不够 hero。
5. 避免仅反映"赛后结果"的冗余（final margin、combined score 等），除非数值确实出色。
6. `source=milestone` 的候选代表本场发生了排名跨越或逼近目标事件，优先作为 hero；叙事只引用候选里的 milestone_context/passed/target/thresholds。
7. milestone 的 `event_type=absolute_threshold` 代表本场跨越绝对里程碑；`event_type=approaching_absolute` 代表逼近绝对里程碑。叙事必须使用候选里的 threshold_label / new_gap / count_reached_before_this_game。

## narrative 规则
每条输出 narrative_zh + narrative_en：
- 中文 10-25 字，英文 6-18 词。
- 必须提到球队（中文用球队简称或名字，例：马刺、猛龙；英文：Spurs、Raptors、用候选里给出的 abbr）。
- 不用引号、感叹号、夸张词。
- 具体排名优于模糊的"最高"。
- 只使用候选里给出的信息，不要编造队史/历史数据（除非候选就是那个范围）。

## 示例
好：
- zh: "马刺命中15记三分，赛季三分命中第3高。" en: "Spurs buried 15 threes, their 3rd-most in a game this season."
- zh: "猛龙最大领先38分，近5场最大分差。" en: "Raptors led by as many as 38 — biggest lead over their last 5."

差：
- "马刺打得不错" (无数字)
- "队史最佳" (候选里没给队史排名就不要写)

## 输出格式（严格 JSON，不要 markdown）
{
  "hero": [
    {"metric_key": "<key>", "entity_id": "<team_id>", "narrative_zh": "...", "narrative_en": "..."}
  ],
  "notable": [
    {"metric_key": "<key>", "entity_id": "<team_id>", "narrative_zh": "...", "narrative_en": "..."}
  ]
}

entity_id 必须是候选里的 team_id。候选不够好就返回空列表。
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
    stripped = (text or "").strip()
    if not stripped:
        logger.warning("LLM returned empty content (likely ran out of tokens during reasoning)")
        raise ValueError("empty LLM response")
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
        logger.warning("LLM returned non-JSON content (first 500 chars): %r", stripped[:500])
        raise


def _snapshot_entry(entry: dict, narrative_zh: str, narrative_en: str) -> dict:
    """Freeze rank/value from the raw candidate at curation time."""
    return {
        "metric_key": entry["metric_key"],
        "entity_id": entry.get("entity_id"),
        "narrative_zh": narrative_zh,
        "narrative_en": narrative_en,
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
    reasoning_effort: str | None = None,
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
        max_tokens=None,
        reasoning_effort=reasoning_effort,
    )

    parsed = _parse_llm_json(raw_response)

    def _take(section: str, limit: int) -> list[dict]:
        out = []
        for pick in (parsed.get(section) or [])[:limit]:
            key = pick.get("metric_key")
            entity = pick.get("entity_id")
            narrative_zh = (pick.get("narrative_zh") or pick.get("narrative") or "").strip()
            narrative_en = (pick.get("narrative_en") or "").strip()
            if not key or not narrative_zh or not narrative_en:
                continue
            raw = by_key.get((key, entity))
            if raw is None:
                logger.warning("LLM returned unknown metric_key=%s entity=%s", key, entity)
                continue
            out.append(_snapshot_entry(raw, narrative_zh, narrative_en))
        return out

    return {
        "version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": selected_model,
        "hero": _take("hero", MAX_HERO),
        "notable": _take("notable", MAX_NOTABLE),
    }


def _build_triggered_llm_input(kind: str, cards: list[dict]) -> list[dict]:
    """Flatten triggered player/team cards into compact LLM input.

    `kind` is "player" or "team". Uses the same field names the triggered
    metric builder already produces (player_name, team_abbr, ranks etc.).
    """
    out = []
    for c in cards:
        entry = {
            "metric_key": c.get("metric_key"),
            "metric_name": c.get("metric_name"),
            "source": c.get("source"),
            "event_type": c.get("event_type"),
            "entity_id": c.get("entity_id"),
            "value": c.get("value_str") or (str(c.get("value_num")) if c.get("value_num") is not None else None),
            "value_num": c.get("value_num"),
            "season_rank": c.get("rank"),
            "season_total": c.get("total"),
            "alltime_rank": c.get("all_rank"),
            "alltime_total": c.get("all_total"),
            "context_label": c.get("context_label"),
        }
        if c.get("source") == "milestone":
            entry["severity"] = c.get("severity")
            entry["passed"] = c.get("passed")
            entry["target"] = c.get("target")
            entry["thresholds"] = c.get("thresholds")
            entry["milestone_context"] = c.get("context_json")
            related = (c.get("context_json") or {}).get("related_milestones") if isinstance(c.get("context_json"), dict) else None
            if related:
                entry["related_milestones"] = related
        if kind == "player":
            entry["player_name"] = c.get("player_name")
            entry["team_abbr"] = c.get("team_abbr")
        else:
            entry["team_abbr"] = c.get("team_abbr")
        out.append(entry)
    return out


def _snapshot_triggered_entry(card: dict, narrative_zh: str, narrative_en: str) -> dict:
    snap = {
        "metric_key": card["metric_key"],
        "entity_id": card.get("entity_id"),
        "narrative_zh": narrative_zh,
        "narrative_en": narrative_en,
        "metric_name_snapshot": card.get("metric_name"),
        "value_snapshot": card.get("value_num"),
        "value_str_snapshot": card.get("value_str"),
        "rank_snapshot": {
            "season": card.get("rank"),
            "season_total": card.get("total"),
            "alltime": card.get("all_rank"),
            "alltime_total": card.get("all_total"),
            "last3": card.get("last3_rank"),
            "last3_total": card.get("last3_total"),
            "last5": card.get("last5_rank"),
            "last5_total": card.get("last5_total"),
        },
        "context_label_snapshot": card.get("context_label"),
        "season": card.get("season"),
    }
    if card.get("source") == "milestone":
        snap["source"] = "milestone"
        snap["event_type"] = card.get("event_type")
        snap["event_key"] = card.get("event_key")
        snap["fallback_narrative_zh"] = card.get("fallback_narrative_zh")
        snap["fallback_narrative_en"] = card.get("fallback_narrative_en")
        snap["milestone_context_snapshot"] = card.get("context_json")
    if card.get("player_id"):
        snap["player_id"] = card["player_id"]
        snap["player_name"] = card.get("player_name")
    if card.get("team_id"):
        snap["team_id"] = card["team_id"]
        snap["team_abbr"] = card.get("team_abbr")
    return snap


def curate_triggered_highlights(
    *,
    kind: str,
    game_context: dict,
    candidates: list[dict],
    model: str | None = None,
    reasoning_effort: str | None = None,
    max_hero: int = MAX_TRIGGERED_HERO,
    max_notable: int = MAX_TRIGGERED_NOTABLE,
) -> dict:
    """Run the LLM curator for player or team triggered cards of a single game.

    `candidates` are the card dicts produced by _get_game_triggered_entity_metrics
    (already featured-filtered). Caller should prefilter down to ~15 before
    passing in to keep the prompt compact.
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

    if kind == "player":
        system = PLAYER_SYSTEM_PROMPT
    elif kind == "team":
        system = TEAM_SYSTEM_PROMPT
    else:
        raise ValueError(f"unsupported kind: {kind}")

    llm_candidates = _build_triggered_llm_input(kind, candidates)
    by_key: dict[tuple[str, str | None], dict] = {
        (c["metric_key"], c.get("entity_id")): c for c in candidates
    }

    user_message = (
        "候选数据如下，按照 system prompt 里的原则选出 hero + notable 并输出 JSON：\n\n"
        + json.dumps({"game": game_context, "candidates": llm_candidates}, ensure_ascii=False, indent=2)
    )

    selected_model = model or DEFAULT_MODEL
    raw_response = _call_llm_with_system(
        system,
        [{"role": "user", "content": user_message}],
        model=selected_model,
        max_tokens=None,
        reasoning_effort=reasoning_effort,
    )
    parsed = _parse_llm_json(raw_response)

    def _take(section: str, limit: int) -> list[dict]:
        out = []
        for pick in (parsed.get(section) or [])[:limit]:
            key = pick.get("metric_key")
            entity = pick.get("entity_id")
            narrative_zh = (pick.get("narrative_zh") or pick.get("narrative") or "").strip()
            narrative_en = (pick.get("narrative_en") or "").strip()
            if not key or not narrative_zh or not narrative_en:
                continue
            raw = by_key.get((key, entity))
            if raw is None:
                logger.warning("LLM %s returned unknown metric=%s entity=%s", kind, key, entity)
                continue
            out.append(_snapshot_triggered_entry(raw, narrative_zh, narrative_en))
        return out

    return {
        "version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": selected_model,
        "hero": _take("hero", max_hero),
        "notable": _take("notable", max_notable),
    }


def _prefilter_triggered(
    session,
    cards: list[dict],
    *,
    max_candidates: int = 20,
    tied_drop_threshold: int = 3,
) -> list[dict]:
    """Pick the most notable cards (by best rank ratio) up to max_candidates.

    Drops cards where the top rank is meaningless due to ties: e.g., a team
    with 1 loss early in the season ranks #1 along with 15 other teams also
    at 1 loss. If `tied_count >= tied_drop_threshold` AND best rank is near
    the top (<= 3), the card is dropped.
    """
    from db.models import MetricResult as MetricResultModel

    if not cards:
        return []

    # Collect (metric_key, entity_type, season, value_num) pools that need tie lookup
    tie_keys: set[tuple] = set()
    for c in cards:
        best_rank = min(
            r for r in (c.get("rank"), c.get("all_rank"), c.get("last3_rank"), c.get("last5_rank"))
            if r is not None
        ) if any(c.get(k) is not None for k in ("rank", "all_rank", "last3_rank", "last5_rank")) else None
        if best_rank is not None and best_rank <= 3 and c.get("value_num") is not None and c.get("season") and c.get("entity_type"):
            tie_keys.add((c["metric_key"], c["entity_type"], c["season"], float(c["value_num"])))

    tied_counts: dict[tuple, int] = {}
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

    kept: list[dict] = []
    for c in cards:
        best_rank = min(
            r for r in (c.get("rank"), c.get("all_rank"), c.get("last3_rank"), c.get("last5_rank"))
            if r is not None
        ) if any(c.get(k) is not None for k in ("rank", "all_rank", "last3_rank", "last5_rank")) else None
        if best_rank is not None and best_rank <= 3 and c.get("value_num") is not None:
            key = (c["metric_key"], c.get("entity_type"), c.get("season"), float(c["value_num"]))
            if tied_counts.get(key, 0) >= tied_drop_threshold:
                continue
        kept.append(c)

    def _tier(card: dict) -> int:
        # Order narrative richness: career > last5 > last3 > concrete season > runlog.
        # Concrete-season pools in early playoff/regular-season windows are
        # noisy (a few games only, dense ties at low counts), so the career
        # and recent-form windows should outrank them for headline space.
        if card.get("source") == "milestone":
            metric_key = str(card.get("metric_key") or "")
            if metric_key.endswith("_career"):
                return 0
            if metric_key.endswith("_last5"):
                return 1
            if metric_key.endswith("_last3"):
                return 2
            return 3  # base metric in a concrete season
        return 4  # runlog

    kept.sort(key=lambda c: (_tier(c), c.get("best_ratio", 1.0), c.get("rank") or 10**9))
    return kept[:max_candidates]


def run_curator_for_game(
    session,
    game,
    *,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> dict:
    """End-to-end: build candidates for game + player + team, call LLM for each,
    persist all three on the Game row.

    `model` and `reasoning_effort` default to the admin-configured values
    (or package defaults) when not passed explicitly. Returns a dict with
    keys game/player/team each holding the curated payload.
    """
    from db.models import Team
    from web.app import _build_game_raw_metric_candidates, _get_game_triggered_entity_metrics

    resolved_model, resolved_effort = _resolve_curator_settings(session, model)
    if reasoning_effort is None:
        reasoning_effort = resolved_effort

    raw_game = _build_game_raw_metric_candidates(session, game.game_id, game.season)
    from metrics.highlights.prefilter import prefilter_candidates

    game_candidates = prefilter_candidates(raw_game, session=session)
    team_lookup = {t.team_id: t.full_name for t in session.query(Team).all()}
    ctx = build_game_context(game, team_lookup)

    triggered = _get_game_triggered_entity_metrics(session, game.game_id, game.season)
    player_candidates = _prefilter_triggered(session, triggered.get("player") or [])
    team_candidates = _prefilter_triggered(session, triggered.get("team") or [])

    # Fan out the 3 LLM calls in parallel — each is IO-bound and independent.
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=3) as pool:
        fut_game = pool.submit(
            curate_game_highlights,
            game_context=ctx,
            candidates=game_candidates,
            model=resolved_model,
            reasoning_effort=reasoning_effort,
        )
        fut_player = pool.submit(
            curate_triggered_highlights,
            kind="player",
            game_context=ctx,
            candidates=player_candidates,
            model=resolved_model,
            reasoning_effort=reasoning_effort,
        )
        fut_team = pool.submit(
            curate_triggered_highlights,
            kind="team",
            game_context=ctx,
            candidates=team_candidates,
            model=resolved_model,
            reasoning_effort=reasoning_effort,
        )
        game_curated = fut_game.result()
        player_curated = fut_player.result()
        team_curated = fut_team.result()

    now = datetime.now(timezone.utc)
    game.highlights_curated_json = json.dumps(game_curated, ensure_ascii=False)
    game.highlights_curated_player_json = json.dumps(player_curated, ensure_ascii=False)
    game.highlights_curated_team_json = json.dumps(team_curated, ensure_ascii=False)
    game.highlights_curated_at = now
    game.highlights_curated_model = game_curated.get("model")
    session.commit()

    try:
        from web.app import _delete_game_metrics_payload_cache

        _delete_game_metrics_payload_cache(game.game_id)
    except Exception:
        logger.exception("failed to invalidate game metrics cache for %s", game.game_id)

    return {"game": game_curated, "player": player_curated, "team": team_curated}


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

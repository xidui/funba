# Metric Series — Hupu Writing Playbook

Use this playbook when generating Funba **metric series** content for Hupu.

## Scope

This document is for the **metric data series** only. Do not mix with game-analysis Hupu rules (`funba-hupu-writing.md`).

## Language

Write in Chinese.

## Player Names

Always use full player names on first mention — never use English nicknames or abbreviations (KAT, AD, CP3, LBJ, SGA, etc.). After the first full-name mention, use the commonly recognized Chinese short name (唐斯, 浓眉, 詹姆斯, etc.).

## Length

Short-to-medium: 500–1000 Chinese characters. This is NOT the 1800–2000 character deep-dive format used in game analysis. Metric series posts are data-focused and concise.

## Title

Use the `智趣NBA:` prefix, followed by a clear data finding.

Good examples:
- `智趣NBA: 本赛季已出现10场140+得分的比赛，历史最多`
- `智趣NBA: 詹姆斯35岁后40+得分场次远超其他球员30岁后的表现`

## Structure

1. **开头 (1-2句)**: State the headline finding directly
2. **数据展示 (主体)**: Top 5 ranking with values, brief context for notable entries
3. **赛季/历史对比**: How this compares to recent seasons or all-time
4. **来源链接**: Brief footer with funba.app metric page link

Keep it tight. Do not pad with generic commentary or speculation.

## Images

Include 1–3 metric ranking page screenshots. Use the capture CLI:

```
python -m social_media.funba_capture metric-page --metric-key <key> --season <season> --top-n 5 --output <file>
```

No game photos, no AI-generated images, no web-sourced images.

## Forum Targeting

- `湿乎乎的话题` — always, for general NBA audience
- Team forum — when a team dominates the ranking or the finding is clearly team-relevant. Use forum names from `social_media/hupu/forums.py`.

Adjust tone per forum:
- `湿乎乎的话题`: broad league context
- Team forum: fan-facing, emphasize what this means for that team

## Variant Count

Default: 1 variant for `湿乎乎的话题`. Add team forum variant(s) when relevant teams are clearly identifiable from the top entries.

## Safety

- Write in Chinese only
- Do not fabricate stats
- Do not include game-series-style long analysis or PBP breakdowns

# Metric Series — Xiaohongshu Writing Playbook

Use this playbook when generating Funba **metric series** content for Xiaohongshu.

## Scope

This document is for the **metric data series** only. Do not mix with game-analysis Xiaohongshu rules (`funba-xiaohongshu-writing.md`).

## Language

Write in Chinese.

## Length

- Title: under 20 characters (Xiaohongshu hard limit)
- Body: under 1000 characters (Xiaohongshu hard limit for graph notes)

Target 600–900 characters for the body. Leave room for tags.

## Title

Short, punchy, data-driven. Must fit in 20 characters.

Good examples:
- `本赛季140+得分场次创纪录`
- `詹姆斯35岁后依然无敌`
- `三分球时代：赛季新高`

## Structure

1. **数据亮点 (1句)**: The headline stat
2. **排名展示**: Top 3-5 entries with values
3. **简短点评**: One sentence of context or comparison
4. **标签**: Use `[[TAGS:NBA,球员名,球队名]]` for discovery

No long analysis. No game recaps. Let the data be the content.

## Images

Include 1–2 metric ranking page screenshots. Use the capture CLI:

```
python -m social_media.funba_capture metric-page --metric-key <key> --season <season> --top-n 5 --output <file>
```

No game photos, no AI-generated images, no web-sourced images.

At least 1 image is required for Xiaohongshu graph notes.

## Safety

- Write in Chinese only
- Do not fabricate stats
- Do not include external links (Xiaohongshu penalizes external URLs)
- Do not reuse Hupu copy — write natively for Xiaohongshu's concise style

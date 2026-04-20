# Cross-Platform Writing Principles

Platform-agnostic writing discipline for any Funba content draft — Hupu, Xiaohongshu, Reddit, or any future destination. Each platform's `*-writing.md` playbook layers platform-specific rules on top of these.

Always read this file together with the relevant platform writing playbook(s) when drafting or revising copy.

When the same writing principle applies to two or more platforms, it belongs here. Do not copy it into platform playbooks. Platform playbooks should only contain rules that are genuinely platform-specific (length limits, native vocabulary, formatting capabilities, footer conventions, etc.).

## Write for the Reader, Not the Reviewer

The reader is cold — they have not seen any prior draft of this post and have not read the review thread. Reviewer feedback is a signal to you about **which angle to pick and which claims to drop**, not content to quote, dismiss, or rebut in the prose.

This applies to the **entire post** — title, opening, body paragraphs, transitions, the closing, and the related-links framing — not just the opening hook. The hook is the most visible failure mode, but the same pattern shows up mid-body and in conclusions:

- Chinese mid-body example: `上面那种说法听起来很顺，其实……`
- Chinese closing example: `所以这场不是关于X，而是Y`
- English mid-body example: `that take sounds clean, but actually...`
- English closing example: `so really this game isn't about X, it's about Y`

Hard rules:

- Do not name or paraphrase the angle a reviewer rejected, even to dismiss it. Constructions like:
  - Chinese: `"本届第一"其实意义不大` / `"X之最"这种标签噱头大过实际` / `不是X，而是Y` (when X is the rejected angle)
  - English: `"first of this playoff" doesn't really mean much` / `the "X-of-the-season" framing is overhyped` / `it's not X, it's Y` (when X is the rejected angle)

  These import the reviewer's voice into the article and create a strawman the reader did not know existed. Pick a stronger angle and write to it directly.

- Negation-then-pivot constructions (`不是X，而是Y` / `X意义不大，Y才有价值` / `it's not X, it's Y` / `forget X — the real story is Y`) are only allowed when X is a take a real reader is likely to bring with them — a widely-circulated public narrative, a common fan assumption — not when X is only the previous draft's lead.

- If multiple variants in the same post — or multiple paragraphs in the same variant — use the same negation-then-pivot frame, treat that as a defect. It is performing-the-feedback, not writing for the reader. Rewrite each lead and each pivot independently.

Self-check before submitting any draft (initial or revised): read the entire post as if you had never seen the prior draft or the review thread. If any sentence — opening, mid-body, or closing — only makes sense to someone who knows what was rejected, rewrite it.

## Choosing Which Rank Window To Cite

Each story candidate now ships with four ranking windows: `season_rank` / `last3_rank` / `last5_rank` / `all_rank` (with matching `*_total` and `*_ratio` fields). Picking the right window is part of writing well — the wrong choice produces statements that sound impressive but mean nothing.

Window-selection priority depends on the mode of the metric (the season prefix tells you which: `4*` = playoffs, `5*` = play-in, `2*` = regular season).

### Playoff mode (`season` prefix = `4`) and play-in mode (`5`)

Per-season playoff samples are inherently small — a team plays at most 16–26 playoff games in a year. Even a fully-played playoff yields a rank-vs-the-current-sample number that says little about the player's or team's actual standing. Multi-season windows are the better baseline.

Priority order (highest to lowest):

1. **`last5_rank`** — the strongest baseline. ~80–130 team-games or several thousand player-games of recent same-mode comparison.
2. **`last3_rank`** — close second. Use when the recency tightens the story (e.g. "since last finals") or when last5_total is unusually thin.
3. **`all_rank`** — career / all-history. Use when the rank itself is the story (true top-1% of all-history) and accept the era / pace / rule-change caveat for cross-decade comparisons.
4. **`season_rank`** — lowest priority **even when `season_total` looks large**. Treat the season number as a current-state fact ("they sit at X this playoff"), not as a rank-based hook. "本届季后赛第一" / "leads this playoff" almost never carries enough information to lead with, regardless of how many games are in the pool.

### Regular-season mode (`season` prefix = `2`)

Regular-season pools are large (1,200+ league-games per year, hundreds of player-game samples per metric), so `season_rank` becomes a meaningful baseline once the season is ≥ a few months in.

Priority order (highest to lowest):

1. **`season_rank`** — when `season_total` is large enough to be informative (rule of thumb: ≥ 100 rows in the pool).
2. **`last5_rank`** / **`last3_rank`** — equal-priority recency baselines, useful for "career year" / "career low" framing or for comparing against the player/team's own recent track record.
3. **`all_rank`** — for true historical hooks only.

When `season_total` is small (≤ 30 — typical of the first weeks of a regular season), demote `season_rank` the same way you would in playoff mode and lean on `last5_rank` / `last3_rank`.

### General rules (both modes)

- **Don't list every window.** Pick 1, occasionally 2, ranks per metric mention. A paragraph that recites all four windows for the same metric reads like a leaderboard dump, not analysis.
- **Cite the pool size when the rank is the hook.** "近 5 季 432 场季后赛里第 8 位" carries more weight than a bare "近 5 季第 8 位" — the reader can judge the rarity for themselves.

## Chinese-Language Conventions

These rules apply only to Chinese-language drafts (Hupu, Xiaohongshu, and any future Chinese platform). Reddit and other English destinations can ignore them.

### Player Name Lookup

`web/i18n/player_names_zh.py` is the canonical source for Chinese player names. Before writing any player's full Chinese name in a draft (title, body, image notes, related-link anchors), look up the player there — do not transliterate by ear or guess.

The reason this matters: many NBA players have multiple Chinese transliterations in active circulation that share the same Mandarin pronunciation but use different characters. The wrong character is silently wrong — the pinyin still matches, so it sounds right when read aloud, but it is not the name Funba has standardized on, and downstream search / linking / branding all break. Homophone confusables across the player pool are common; do not assume the cases you happen to remember are the only ones.

Operationally:

- For each player you reference by full Chinese name, grep `web/i18n/player_names_zh.py` for the player's `player_id` or English name and use the value found there.
- If the player is not in the file, fall back to the most widely-circulated Chinese basketball community spelling — but flag it in the draft so a human or follow-up patch can add the player to the canonical mapping.
- Player nicknames or abbreviations widely recognized in the Chinese basketball community are still allowed (per each platform playbook's own player-name guidance), but the **full** Chinese name on first mention should match the canonical source.

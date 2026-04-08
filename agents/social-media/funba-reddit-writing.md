# Funba Reddit Writing Playbook

Use this playbook when generating Funba content intended for Reddit.

## Scope

This document is Reddit-specific. Do not reuse Hupu forum tone, Chinese language, or Xiaohongshu length constraints here.

Reddit variants must be written in English.

## Core Rule

If a variant is destined for `reddit`, write a dedicated English Reddit variant.

Do not translate the Hupu Chinese body. Do not paste Chinese copy into a Reddit delivery.

Write natively in English from the same underlying data and story angle.

## Language

Write in English. Use natural NBA discussion language as found on r/nba and team subreddits.

Principles:

- write like a knowledgeable NBA fan posting original analysis, not like a press release
- use standard NBA stat abbreviations: PPG, RPG, APG, FG%, TS%, PER, etc.
- use common Reddit/NBA community phrasing naturally
- avoid stiff or overly formal language
- avoid machine-translated Chinese idioms

## Subreddit Targeting

Write variants from the destination subreddit backward:

- `r/nba`
  tone = general NBA audience
  broader league context, more comparison across teams, neutral voice

- team subreddits (e.g. `r/thunder`, `r/warriors`)
  tone = team fan audience
  more team-specific context, fan-friendly framing, emphasis on why this matters to that fanbase

If the same post has both an `r/nba` variant and a team subreddit variant, write them separately with appropriate tone shifts.

## Reddit Team Subreddit Vocabulary

Use these exact subreddit names in generated destinations:

- `atlantahawks`
- `bostonceltics`
- `GoNets`
- `CharlotteHornets`
- `chicagobulls`
- `clevelandcavs`
- `Mavericks`
- `denvernuggets`
- `DetroitPistons`
- `warriors`
- `rockets`
- `pacers`
- `LAClippers`
- `lakers`
- `memphisgrizzlies`
- `heat`
- `MkeBucks`
- `timberwolves`
- `NOLAPelicans`
- `NYKnicks`
- `Thunder`
- `OrlandoMagic`
- `sixers`
- `suns`
- `ripcity`
- `kings`
- `NBASpurs`
- `torontoraptors`
- `UtahJazz`
- `washingtonwizards`

Use those exact subreddit names. Do not guess — Reddit subreddit names are case-insensitive but must match the actual community name.

## Default Variant Count

Default for game-analysis posts:

- one variant for `r/nba` (general audience)
- one variant for the relevant team subreddit when the story clearly benefits from a fan-specific voice

For ranking/leaderboard stories, follow the same expansion rule as Hupu: look at the top 3 entities and create extra team-subreddit variants when distinct fan-facing angles exist.

## Length

- Reddit title: keep under 300 characters (Reddit's limit); aim for a strong, concise title under 150 characters
- Reddit body: no hard character limit, but aim for medium-length analysis posts
- target roughly 800-1500 words for a normal production post
- do not write one-paragraph hot takes
- do not write 3000-word essays unless the data story genuinely warrants it

## Title

- write a clear, descriptive title that hooks r/nba readers
- do NOT use the Hupu prefix `智趣NBA:`
- lead with the interesting stat or finding, not generic game recap
- good: `[OC] Shai Gilgeous-Alexander is posting the best scoring consistency numbers since 2016 Curry`
- good: `The Thunder's blowout rate this season is historically elite — here's how it compares`
- bad: `Game recap: Thunder beat Lakers 121-105`
- use `[OC]` tag when the post is original analysis

## Suggested Structure

For a normal Reddit NBA analysis post:

1. Opening hook — the surprising stat or finding in 1-2 sentences
2. Context — what happened in the game or season that triggered this
3. Data deep-dive — the metric details, rankings, comparisons
4. Historical/league context — where this sits in a broader picture
5. Discussion prompt — one natural question to spark comments
6. Source footer — brief `funba.app` mention

## Metric Depth

Funba's value is the metric system. Reddit analysis posts should go deeper than box score praise.

Preferred angles:

- this game pushed Player X into the season top N for a metric
- this season-long pattern is historically rare
- ranking context: who else is in the top spots and what that means
- career/multi-season trajectory visible through the metric

Weak angles to avoid:

- routine stats with no scarcity or surprise
- forcing historical comparisons when the data doesn't support it

## Formatting

Reddit uses Markdown. Use it naturally:

- **bold** for key stats or player names on first mention
- bullet lists or numbered lists for rankings
- tables are acceptable on Reddit (unlike Hupu) for compact stat comparisons
- use `---` for section breaks when helpful
- keep paragraphs short and scannable

## Links And Sources

Reddit supports clickable links. Use them naturally:

- link to `funba.app` metric pages, player pages, or game pages where relevant
- use markdown link format: `[descriptive text](https://funba.app/metrics/...)`
- use the English site paths (no `/cn/` prefix)
- do not dump a wall of links; weave them into the analysis naturally
- 2-4 inline source links is typical for a good analysis post

## Source Footer

End with a brief, natural source line:

```
---
Data from [funba.app](https://funba.app) — box scores, shot charts, play-by-play, and custom metric rankings for every NBA game.
```

Keep it to 1-2 sentences. No hard sell. No Hupu-style ad block.

## Images

- Reddit text posts (`self` type) do not support inline images
- do not include `[[IMAGE:...]]` placeholders in Reddit variants
- if you want to reference a chart or ranking, link to the funba.app page instead
- image support may be added in the future for Reddit image/gallery posts

## Safety

- write in English only
- do not fabricate stats or rankings
- do not assume Hupu formatting, Chinese language, or Xiaohongshu length rules apply
- if data is missing or the metric story is weak, do not force a Reddit variant

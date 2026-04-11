# Funba Hupu Writing Playbook

Use this playbook when generating Funba content intended for Hupu.

## Scope

This document is Hupu-specific. Do not assume these tone or structure rules should transfer to future platforms such as Xiaohongshu.

## Forum-First Rule

Write variants from the destination forum backward:

- `湿乎乎的话题` (`hupu/nba`)
  tone = `general nba`
  broad NBA audience, more league comparison, less team-homer language

- any NBA team forum (`hupu/<team>`)
  tone = `<team> fans`
  stronger team identity, more team-context language, stronger emphasis on why that team's fans should care

If tone and forum diverge, treat that as an exception, not the default.

## Hupu Forum Vocabulary

For Funba content generation, treat the Hupu destination vocabulary as:

User-facing forum choices should be treated in Chinese:

- `湿乎乎的话题`
- `CBA版`
- `老鹰专区`
- `凯尔特人专区`
- `篮网专区`
- `黄蜂专区`
- `公牛专区`
- `骑士专区`
- `独行侠专区`
- `掘金专区`
- `活塞专区`
- `勇士专区`
- `火箭专区`
- `步行者专区`
- `快船专区`
- `湖人专区`
- `灰熊专区`
- `热火专区`
- `雄鹿专区`
- `森林狼专区`
- `鹈鹕专区`
- `尼克斯专区`
- `雷霆专区`
- `魔术专区`
- `76人专区`
- `太阳专区`
- `开拓者专区`
- `国王专区`
- `马刺专区`
- `猛龙专区`
- `爵士专区`
- `奇才专区`

Use those exact Chinese forum labels in generated destinations. Do not invent alternative team names or historical names.
Do not emit English destination forum keys such as `hawks`, `76ers`, `spurs`, or `thunder` in generated Funba destinations when a Chinese forum label is available.

Delivery tooling support may lag behind this vocabulary. That is not a reason to suppress a valid team-forum variant during content generation.

If a team-specific idea is good and the story clearly supports it, create that team-forum variant explicitly rather than folding everything back into `NBA版`.

## Default Variant Count

Default maximum: 2 variants per post for non-ranking stories.

- one assigned variant for `湿乎乎的话题`
- one assigned variant for a relevant team forum only when the story clearly benefits from that team-fan voice

Create more only when the review value is genuinely higher.

## Ranking Story Expansion Rule

If the post is built around a ranking / leaderboard metric:

- always create one `湿乎乎的话题` league-wide summary post
- then look at the top 3 entities in that ranking
- if the top-3 teams or players each support meaningful fan-facing angles, create extra fan-voice variants for them

Interpretation rules:

- if the top-3 entity is a team
  create a team-fan variant for that team's forum

- if the top-3 entity is a player
  create a fan-voice variant for that player's current team forum

Destination rule:

- if the top-3 entity maps to one of the 30 NBA team forums above, assign that forum directly in the generated destination
- if multiple top-3 entities map to different teams, generate separate team-forum variants for each team when the stories are distinct enough
- if a player changes teams in reality, use that player's current NBA team at the time of writing rather than any hardcoded example

Do not force top-3 fan variants when the ranking is weak, redundant, or the lower-ranked entities do not produce distinct enough stories.

## Required Output Contract

1. Title
   - Every title must start with `智趣NBA:`

2. Length
   - Long-form Chinese forum post
   - Target visible正文 roughly 1800-2000 Chinese characters for normal production posts
   - Treat 1800 as the working minimum unless the operator explicitly asks for a shorter experiment
   - A little above 2000 is acceptable only when the story clearly benefits from the extra depth
   - Do not produce one-paragraph mini posts
   - Do not use markdown tables in Hupu-targeted content
   - Count visible正文, not raw `content_raw` storage length
   - Do not count markdown URL strings, `[[IMAGE:...]]` placeholders, `[[TAGS:...]]` placeholders, or line-break bookkeeping toward the target
   - This means a body with many `funba` links may need significantly more raw characters than the visible正文 target suggests

3. Structure
   - hook / framing question
   - core numbers and what they mean
   - historical / league comparison
   - when relevant, career / multi-season framing
   - basketball interpretation
   - discussion prompt
   - related metrics section
   - `funba` footer ad

4. Career / historical framing
   - If a post is built around a metric, do not stop at current-season ranking when deeper historical framing is available
   - Prefer:
     - a career variant of the metric
     - an all-time / historical leaderboard for the same metric family
     - a multi-season comparison with recognizable benchmarks
   - The post should answer both:
     - how good is this today?
     - where does this sit in a longer historical context?

5. Related metrics section
   - Include a short section near the end:
     - `你可能还想看：` or `相关数据：`
   - Add **6–8 metric links** — this section should be metric-focused
   - Choose metric links that are strongly connected to the main story:
     - same metric family
     - same player/team style profile
     - a supporting or contrasting metric
     - career vs season variants of the same metric
   - Do NOT fill this section with game pages or player pages — those belong as inline links in the body text, not here
   - Keep it concise and natural, not a raw link dump

6. Inline source linking
   - All `funba.app` links must use the Chinese site prefix `/cn/`: `https://funba.app/cn/metrics/...`, `https://funba.app/cn/players/...`, `https://funba.app/cn/games/...`. Do not link to the English paths.
   - Do not leave core metric references as unsupported plain text when a `funba` source page is available
   - In the main body, the first mention of each primary metric should use a natural inline `funba` link to the metric page
   - When a paragraph relies heavily on one game's box score or play-by-play, include a natural inline link to that game page in that paragraph or in the immediately adjacent sentence
   - When a paragraph is centered on one player, prefer linking that player's `funba` page at the first natural mention or in the nearby supporting sentence
   - Use natural Chinese anchor text, not naked raw URLs, in the main body whenever possible
   - The goal is that data-heavy claims in the article have visible nearby source paths, not only a link dump at the end
   - Because raw markdown links add many hidden URL characters, do not mistake raw storage length for visible正文 length

7. Image placeholders
   - Add placeholders when rankings, leaderboards, charts, or game visuals clearly improve the story
   - Placeholder format:
     `[[IMAGE: type=<kind>; target=<funba url>; note=<what to capture>]]`
   - Multiple placeholders are allowed when they add real value
   - Good uses:
     - current-season ranking
     - career / all-time ranking
     - game page / boxscore snapshot
     - metric detail chart/table

8. Footer ad
   - End with a short `funba` promo block
   - It should sound natural, not spammy
   - Mention that `funba.app` can查:
     - box score
     - play-by-play
     - heatmaps
     - metric rankings
     - deeper NBA data

## Formatting Constraints For Hupu

Hupu editor and delivery tooling currently handle prose, lists, bold text, and links more reliably than markdown tables.

Required rules:

- do not use markdown tables such as:
  `| 排名 | 球员 | 数值 |`
- for rankings or comparisons, use one of:
  - numbered lists
  - bullet lists
  - short comparison paragraphs
- keep spacing simple and deliberate
- prefer readable paragraph breaks over fancy markdown constructs
- when checking whether a draft is long enough, estimate the visible正文 after stripping:
  - markdown URL bodies
  - `[[IMAGE:...]]`
  - `[[TAGS:...]]`

## Player Names

Player abbreviations and nicknames are fine when they are widely recognized in the Chinese basketball community. If there is any doubt whether the average reader would immediately know who an abbreviation refers to, use the player's full Chinese name on first mention instead. When in doubt, default to the full name — it never hurts readability, while an unrecognized abbreviation does.

## Chinese Basketball Language

Write like a real Chinese basketball community poster, not like a literal translator.

Principles:

- prefer the most natural, mainstream Chinese basketball phrasing used on Hupu and similar NBA communities
- do not mechanically translate English stat terms if the direct translation sounds stiff or unnatural in Chinese
- if a term has a more common established Chinese expression, use that expression by default
- do a final language pass before submitting to replace obvious translationese with natural Chinese hoops wording

High-frequency term anchors:

- `double-double` -> `两双`
- `triple-double` -> `三双`
- `back-to-back` -> `背靠背`
- `clutch` in player/game context -> prefer natural Chinese phrasing such as `关键时刻`, `关键球`, `决胜时刻`

Self-check requirement:

- after drafting, reread the post specifically for awkward wording
- replace any phrase that feels like an English-first construction with a more natural Chinese basketball-community expression
- optimize for how a Hupu user would naturally phrase the point, not for literal faithfulness to English wording

## Style References

Reference Hupu posts:

- https://bbs.hupu.com/638153415.html
- https://bbs.hupu.com/638163441.html

Takeaways:

- open with a concrete basketball framing question
- put the key ranking or metric cluster early
- spend most of the body interpreting the numbers instead of merely repeating them
- close with a short `——` separator and a `funba` promo footer

## API Write Safety

When sending long content to Funba Content API:

- do not inline giant JSON strings directly inside `curl -d '...'`
- instead use:
  - a temp JSON file plus `--data-binary @file`
  - or Python `requests` / `json`

Long Chinese bodies, tables, links, and placeholders easily break shell-quoted JSON and cause `400 Bad Request`.

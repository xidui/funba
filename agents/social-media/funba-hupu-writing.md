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

### Destination routing (enforced)

- `湿乎乎的话题` is reserved exclusively for the `general nba` league-wide variant.
- A winning-team or losing-team variant must be routed to that team's `<team>专区` forum (for example `hupu/老鹰专区`, `hupu/尼克斯专区`), never to `hupu/湿乎乎的话题`. A team-fan variant sharing `湿乎乎的话题` collapses into the general slot and the team-fan voice is lost downstream.
- Emit the exact Chinese forum string from the Hupu Forum Vocabulary section below. Do not emit Paperclip-internal placeholders or short codes such as `hupu_nba_bbs`, `nba`, or English team keys like `hawks` — none of these are valid Hupu forums.

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
   - Hupu composer title length is `4-40` characters (`请输入标题（4-40个字）`)
   - Do not draft Hupu titles above 40 characters and do not assume the publisher should silently truncate them

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

4. Signal weighting for game-analysis posts
   - Before drafting, sort candidate signals into:
     - `P1` = most important
     - `P2` = still useful
     - `P3` = secondary
   - Treat a current-season / current-playoff `#1` or tied `#1` as a `P1` candidate by default.
   - `P1` does not mean automatic title lock. Promote it when it has real basketball meaning, fan relevance, and changes how the game should be read.
   - If a `P1` signal is not the title hook, it should usually still appear early in the post with a dedicated sentence or paragraph.
   - Use `P2` signals to sharpen the main story, not to compete with it.
   - Keep `P3` signals out unless they clarify a stronger point.
   - Freshness matters:
     - first hit / newly tied / moved up = strong reason to feature the signal only when that movement is explicitly evidenced
     - if freshness is unknown, write the signal as a current-state ranking fact, not as a movement claim
     - do not keep recycling an old leaderboard fact into new game posts unless this game made that old fact newly meaningful and you can explain why

5. Current-game fact vs season-context rule
   - Split strong signal writing into two layers:
     - layer 1 = what happened in this game
     - layer 2 = why that number sits unusually high in the current season / playoffs / history
   - Good pattern:
     - `骑士这场三分32投16中。`
     - `这16记把他们写到本届季后赛全队单场三分命中并列第一。`
   - Also good when freshness is unknown:
     - `这场之后，他在本赛季单场得分榜排并列第2。`
   - Bad pattern:
     - rewriting a season-record or leaderboard metric into a fake `今天` / `本场` stat line
     - claiming `升到第2` or `首次来到第2` without explicit evidence
   - If the sentence says `今天` / `本场` / `这场` / `G1` / `首战`, the number must come from the game facts, not from a metric ranking page

6. Career / historical framing
   - If a post is built around a metric, do not stop at current-season ranking when deeper historical framing is available
   - Prefer:
     - a career variant of the metric
     - an all-time / historical leaderboard for the same metric family
     - a multi-season comparison with recognizable benchmarks
   - The post should answer both:
     - how good is this today?
     - where does this sit in a longer historical context?

7. Related metrics section
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

8. Inline source linking
   - All `funba.app` links must use the Chinese site prefix `/cn/`: `https://funba.app/cn/metrics/...`, `https://funba.app/cn/players/...`, `https://funba.app/cn/games/...`. Do not link to the English paths.
   - Do not leave core metric references as unsupported plain text when a `funba` source page is available
   - In the main body, the first mention of each primary metric should use a natural inline `funba` link to the metric page
   - When a paragraph relies heavily on one game's box score or play-by-play, include a natural inline link to that game page in that paragraph or in the immediately adjacent sentence
   - When a paragraph is centered on one player, prefer linking that player's `funba` page at the first natural mention or in the nearby supporting sentence
   - Use natural Chinese anchor text, not naked raw URLs, in the main body whenever possible
   - The goal is that data-heavy claims in the article have visible nearby source paths, not only a link dump at the end
   - Because raw markdown links add many hidden URL characters, do not mistake raw storage length for visible正文 length

9. Image placeholders
   - Add placeholders when rankings, leaderboards, charts, or game visuals clearly improve the story
   - Placeholder format:
     `[[IMAGE: type=<kind>; target=<funba url>; note=<what to capture>]]`
   - Multiple placeholders are allowed when they add real value
   - Good uses:
     - current-season ranking
     - career / all-time ranking
     - game page / boxscore snapshot
     - metric detail chart/table

10. Footer ad
   - End with a short `funba` promo block
   - It should sound natural, not spammy
   - Mention that `funba.app` can查:
     - box score
     - play-by-play
     - heatmaps
     - metric rankings
     - deeper NBA data

## Write for the Reader, Not the Reviewer

This rule is cross-platform. See `agents/social-media/writing-principles.md`. No Hupu-specific additions.

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

Shooting and free-throw notation:

- field goals / 3-pointers: always `<attempts>投<makes>中`. Examples: `90投45中`, `三分44投16中`, `三分23投4中`.
- free throws: always `<attempts>罚<makes>中`. Examples: `17罚15中`, `6罚6中`.
- do not use `<makes>中<attempts>` (e.g. `4中23`) — it is non-standard and ambiguous.
- do not use `<attempts>中<makes>` (e.g. `23中4`, `44中16`) either — it omits the `投`/`罚` discriminator and reads inconsistently. Reviewers in earlier iterations flipped between the two reversed forms without solving the underlying ambiguity; lock to the `投/中` and `罚/中` forms instead.
- Western fraction notation (`16/44`, `15/17`) is acceptable inline when natural, but the dominant form in Hupu prose should be `投/中` and `罚/中`.

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

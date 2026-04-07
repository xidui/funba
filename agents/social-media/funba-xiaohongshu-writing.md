# Funba Xiaohongshu Writing Playbook

Use this playbook when generating Funba content intended for Xiaohongshu.

## Scope

This document is Xiaohongshu-specific. Do not reuse Hupu forum tone, length, or footer structure here.

On April 4, 2026, the live Xiaohongshu creator web graph-note composer showed a body counter of `0 / 1000` after image upload. Treat that as the current working body limit for `上传图文`.
On that same live composer, the title counter turned red at `26 / 20`. Treat `20` characters as the current working title limit.

## Core Rule

If a variant is destined for `xiaohongshu`, write a dedicated Xiaohongshu variant.

Do not reuse the Hupu long-form body and do not assume a Hupu forum variant can be delivered safely to Xiaohongshu unchanged.

## Role And Objective

Write as a professional NBA data analyst who also built `funba.app`.

Your Xiaohongshu goal is not just to retell the game. Your job is to turn the game into a strong data story that:

- feels professional and insightful
- earns likes, saves, follows, and discussion
- naturally makes readers curious about `funba.app`

Treat `funba.app` as the underlying data engine behind the post, not just a site name to drop at the end.

## Length

- treat 20 characters as the current working title limit
- treat 1000 characters as the current hard ceiling for the body
- target roughly 900-980 Chinese characters for normal production Xiaohongshu notes
- treat 900 as the working minimum unless the operator explicitly asks for a shorter experimental post
- use the available body budget; do not stop at a shallow short note when more metric depth would still fit under the limit
- stay below the ceiling with enough room for native topic anchors

## Tone

Write like a concise Chinese social note, not like a forum thread.

Prefer:

- cleaner opening hook
- tighter rhythm
- fewer repeated stats
- fewer rhetorical challenge lines
- less fandom shouting
- more readable transitions between numbers and takeaway

Avoid obvious Hupu carryover such as:

- `JR`
- `老哥们`
- `专区`
- long discussion-bait endings that read like forum bait
- the mandatory Hupu prefix `智趣NBA:`
- long `你可能还想看` link dumps
- long `funba` ad blocks

## Suggested Structure

For a normal Xiaohongshu NBA data note:

1. one-sentence hook
2. one short game-process paragraph
3. one or two metric/ranking paragraphs
4. one short interpretation paragraph
5. one concise takeaway

This can still be tighter than Hupu sentence by sentence, but it should be much richer than a short recap and should use most of the available Xiaohongshu body budget.
Funba's real product edge is the metric system, so Xiaohongshu drafts should usually surface:

- which interesting metric(s) this game or player triggered
- where the player/team currently ranks
- which screenshot in the image pool corresponds to that metric/ranking

Think of it as a compressed metric-driven note, not a generic recap.
If images do most of the explanatory work, tighten repetition and cleanup transitions, but keep normal production notes in the same 900-980 character band unless the operator explicitly asks for a shorter experimental post.

## Story Selection

For Xiaohongshu, the default writing priority is:

1. quickly establish what happened in the game
2. identify the strongest interesting data story inside that game
3. explain why that data point matters in a broader ranking / season / historical context

Do not spend most of the post on generic game recap if the real value is in the metric angle.
Do not default to the winning side.

If the losing team or a losing-side player has the stronger data story, write that story instead.
Judge the angle by signal strength, not by the final score.

Good losing-side reasons to center the post:

- a player hit a rare ranking / milestone even in a loss
- a strong individual performance reveals a meaningful season trend
- the loss still surfaced a more interesting metric story than the winner's box score
- the losing side's numbers better explain a larger league / team / player narrative

If both sides have real value, you may compare them in the same post. Do not force the winner to be the main character just because they won.

## Metric Depth Rules

Funba's biggest product advantage is the metric system. Use it.

For most Xiaohongshu posts, you should go beyond single-game box score praise and answer:

- what interesting metric(s) did this game or player trigger?
- where does that metric rank this season?
- if relevant, how does it compare across seasons or in career / historical context?
- if the ranking itself is interesting, who else is in the top few spots?

When a metric is worth using, do more than say the player is great. Place the number in context.

Preferred angles:

- this game pushed a player into season top X
- this game extended or reinforced a rare season-long pattern
- this game matches or approaches a career-best / historical-best profile
- this game shows a player's role change or a team's system change through metrics

Weak angles to avoid:

- routine triggered metrics with little scarcity or no wider context
- low-signal milestones that do not actually change how the reader understands the player or team
- forcing historical framing when the metric itself is not strong enough

If a triggered metric is not actually interesting, do not spend much space on it. Pick the strongest 1-2 data stories only.

## Ranking Context Rule

When you decide that one ranking or metric is worth highlighting:

- mention where the featured player/team sits
- when useful, also mention the most relevant names ahead of them or around them
- do not list rankings mechanically; explain why those neighbors make the ranking more meaningful

The goal is not just “Player X ranked 4th.”
The goal is “Player X ranked 4th, and the names above / below him show what tier he has entered.”

## Image Alignment

If you mention a metric or ranking in the body, try to pair it with the matching screenshot placeholder.

Good Xiaohongshu flow:

- body introduces the metric angle
- nearby screenshot shows the ranking / metric page
- next paragraph explains what the ranking means

This keeps the post visually anchored in `funba.app` data instead of feeling like unsupported praise.

## Light CTA

You may end with one short, natural `funba.app` CTA line.

Good pattern:

- add a visible divider line above it: `------`
- one sentence only
- no raw URL dump
- no hard sell
- no Hupu-style footer block

Examples:

- `想继续翻更多有意思的 NBA 数据，可以去 funba.app 看看。`
- `更多比赛页、球员页和指标榜单，都能在 funba.app 里继续挖。`

## Tags

Agent should generate suggested tags as part of the draft.

For now, encode them in one internal line at the end of `content_raw`:

`[[TAGS:#NBA #底特律活塞 #比赛复盘 #助攻]]`

Rules:

- generate 3-5 tags
- prefer league tags + team tags + one angle tag
- do not spam large tag clouds
- keep tags platform-native and readable
- the delivery tool may later turn this internal line into native Xiaohongshu topic anchors
- until native topic insertion is automated, do not rely on plain text hashtags in the final body as if they were real clickable topics

## Links And Sources

- treat Xiaohongshu as a platform where normal graph-note delivery should not depend on clickable external links
- do not dump 6-8 raw links like the Hupu footer pattern
- do not paste naked external URLs into the body as a call to action
- if a source mention is useful, rewrite it as natural prose instead of a link dump
- prioritize readable copy over aggressive source-link density

## Images

- Xiaohongshu graph notes require images; do not prepare text-only deliveries
- preserve valid `[[IMAGE:slot=...]]` placeholders in Funba variants so the delivery tool can resolve the image pool
- if the story depends on charts or rankings, make sure the copy and image order still make sense without a long explanatory footer

## Safety

- if the existing draft is Hupu-style long-form copy, rewrite it instead of trimming a few lines
- if the body would exceed the current creator limit, revise it before review or delivery
- do not silently assume Hupu discussion prompts, footers, and forum language belong on Xiaohongshu

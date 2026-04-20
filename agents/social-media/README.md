# Social Media Playbooks

Platform-specific social content rules live here instead of inside role root specs.

Scaling rule:

- keep role-level `AGENTS.md` files platform-agnostic whenever possible
- put platform-specific writing and delivery constraints only in per-platform playbooks here
- put writing rules that apply to two or more platforms in `writing-principles.md`, not duplicated into each platform playbook
- when adding a new platform, add one new writing playbook and/or one new delivery playbook instead of editing existing platform rules to special-case the new platform

Current documents:

### Cross-Platform

- `writing-principles.md` — platform-agnostic writing discipline that applies to every writing playbook (game series and metric series alike)

### Game Analysis Series (赛后系列)

- `funba-hupu-writing.md` — Hupu-oriented content generation rules for Funba
- `funba-hupu-delivery.md` — Hupu publishing and image-placeholder handling rules
- `funba-xiaohongshu-writing.md` — Xiaohongshu-oriented content generation rules for Funba
- `funba-xiaohongshu-delivery.md` — Xiaohongshu publishing rules for Funba
- `funba-reddit-writing.md` — Reddit-oriented English content generation rules for Funba
- `funba-reddit-delivery.md` — Reddit publishing rules for Funba

### Metric Data Series (数据系列)

- `metric-hupu-writing.md` — Hupu writing rules for metric data series
- `metric-xiaohongshu-writing.md` — Xiaohongshu writing rules for metric data series
- `metric-reddit-writing.md` — Reddit writing rules for metric data series (image posts)

### Delivery Playbooks (shared across series)

Delivery playbooks (`funba-*-delivery.md`) are shared across content series. The delivery publisher follows the same publishing flow regardless of which series produced the content.

If Funba adds another platform later, add a new playbook here instead of bloating the role-level `AGENTS.md`.

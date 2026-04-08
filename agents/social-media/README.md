# Social Media Playbooks

Platform-specific social content rules live here instead of inside role root specs.

Scaling rule:

- keep role-level `AGENTS.md` files platform-agnostic whenever possible
- put platform-specific writing and delivery constraints only in per-platform playbooks here
- when adding a new platform, add one new writing playbook and/or one new delivery playbook instead of editing existing platform rules to special-case the new platform

Current documents:

- `funba-hupu-writing.md` — Hupu-oriented content generation rules for Funba
- `funba-hupu-delivery.md` — Hupu publishing and image-placeholder handling rules
- `funba-xiaohongshu-writing.md` — Xiaohongshu-oriented content generation rules for Funba
- `funba-xiaohongshu-delivery.md` — Xiaohongshu publishing rules for Funba
- `funba-reddit-writing.md` — Reddit-oriented English content generation rules for Funba
- `funba-reddit-delivery.md` — Reddit publishing rules for Funba

If Funba adds another platform later, add a new playbook here instead of bloating the role-level `AGENTS.md`.

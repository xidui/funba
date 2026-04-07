---
name: funba-imagegen
description: Use when an agent needs to create an AI-generated supporting image for a Funba social post. Provides a stable CLI for image generation with optional real-game reference photos so the output stays grounded and less fake-looking.
---

# Funba Imagegen

Run these commands from the `funba` repo root.

Preferred path:

```bash
python -m social_media.funba_imagegen generate \
  --prompt "<image prompt>" \
  --reference-image "<real-game-photo-1>" \
  --reference-image "<real-game-photo-2>" \
  --output "<local-file>"
```

Rules:

- Use real game photos as `--reference-image` whenever possible.
- Prefer 1-2 strong reference images over many weak ones.
- Keep the prompt tied to the actual game story, player, and moment.
- Do not use mini models. The CLI already defaults to the strongest official OpenAI image model available in this stack.
- Save the generated file locally, then pass it into Funba through `images[].file_path`.
- Keep metadata honest:
  - `type: ai_generated`
  - `prompt`: the prompt you used
  - `note`: short Chinese reviewer-facing explanation

The CLI resolves `OPENAI_API_KEY` locally from:

1. environment
2. `.env`
3. `SECRETS.md`

So you do not need to hand-write SDK code inside the agent run.

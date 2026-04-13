# Funba Hupu Delivery Playbook

Use this playbook when publishing approved Funba posts to Hupu.

## Publishing Tool

Use the Funba repo tool:

```bash
python3 scripts/funba_hupu_publish.py --post-id <post_id> --delivery-id <delivery_id> --timeout-seconds 120
```

Rules:

- prefer the wrapper above for normal delivery work; it already runs `check`, posts, and writes delivery status back
- the wrapper runs headless by default; only add `--show-browser` when debugging a failing publish and you need to watch the live browser
- allow up to 120 seconds for a Hupu publish attempt before calling it timed out
- if session is expired, stop and mark the delivery `failed`
- do not silently skip failed destinations

Supported forum behavior:

- `湿乎乎的话题`
- `CBA版`
- any NBA team forum in Chinese user-facing form, for example:
  - `老鹰专区`
  - `76人专区`
  - `马刺专区`
  - `雷霆专区`

Tool behavior:

- `湿乎乎的话题` -> `nba`
- `CBA版` -> `cba`
- NBA team forums are selected dynamically through the Hupu forum picker
- the tool may also accept common English team aliases and normalize them to the correct Chinese forum label before selection

Think in the Chinese forum names first. Use tool keys only when the forum is truly a global board like `湿乎乎的话题` or `CBA版`.

Do not fail a Hupu delivery just because the forum is not one of a tiny hardcoded examples list.
Only fail it as unsupported if the tool or the live Hupu picker cannot resolve or select the requested forum.

## Image Placeholder Handling

Content may contain image placeholders:

`[[IMAGE: type=<kind>; target=<funba url>; note=<what to capture>]]`

When present:

1. open the target
2. capture the relevant ranking/chart/visual area
3. upload the screenshot into the Hupu post
4. remove the placeholder text from the final body

If a post contains multiple placeholders:

- process them in order
- keep all useful images, not just the first one
- only drop a placeholder when the capture is low-value, redundant, or broken

Do not leave raw placeholder text in the published post.

## Writeback

Use:

- `POST /api/content/deliveries/{id}/status`

Status rules:

- success -> `published` + `published_url`
- failure -> `failed` + concrete `error_message`
- start of attempt -> `publishing`

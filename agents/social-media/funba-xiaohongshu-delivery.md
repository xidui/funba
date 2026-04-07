# Funba Xiaohongshu Delivery Playbook

Use this playbook when publishing approved Funba posts to Xiaohongshu.

## Publishing Tool

Use the Funba repo wrapper:

```bash
python3 scripts/funba_xiaohongshu_publish.py --post-id <post_id> --delivery-id <delivery_id> --timeout-seconds 180
```

Rules:

- prefer the wrapper above for normal delivery work; it already runs `check`, posts, and writes delivery status back
- if the Xiaohongshu session is expired, stop and mark the delivery `failed`
- do not silently skip failed destinations
- do not manually rewrite the body at delivery time just to force it through

## Content Preconditions

Before publishing:

- the variant must already be written in Xiaohongshu style
- the title must fit the current title limit
- the body must fit the current graph-note limit
- the post must have at least one enabled image in the Funba image pool
- the post must not depend on clickable external links or a Hupu-style source-link footer to make sense
- if tags are present, they should be encoded through the internal `[[TAGS:...]]` line so the publisher can render them cleanly
- do not ship plain text hashtags as if they were native clickable Xiaohongshu topics unless the publisher has actually inserted them through the topic flow

If those conditions are not met, fail the delivery with a concrete error and send it back for revision rather than publishing bad copy.

## Writeback

Use:

- `POST /api/content/deliveries/{id}/status`

Status rules:

- success -> `published` with best-effort `published_url`
- failure -> `failed` + concrete `error_message`
- start of attempt -> `publishing`

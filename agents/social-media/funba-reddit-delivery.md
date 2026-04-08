# Funba Reddit Delivery Playbook

Use this playbook when publishing approved Funba posts to Reddit.

## Publishing Tool

Use the Funba repo wrapper:

```bash
python3 scripts/funba_reddit_publish.py --post-id <post_id> --delivery-id <delivery_id> --submit --timeout-seconds 120
```

Rules:

- prefer the wrapper above for normal delivery work; it already runs `check`, posts, retries (up to 3 attempts), and writes delivery status back to Funba
- allow up to 120 seconds per attempt before calling it timed out
- if the Reddit session is expired (`ERROR: Not logged in.`), stop and mark the delivery `failed`
- do not silently skip failed destinations

## Dry Run

Omit `--submit` to do a dry run:

```bash
python3 scripts/funba_reddit_publish.py --post-id <post_id> --delivery-id <delivery_id>
```

This fills the Reddit submission form but does not click submit. Use this to verify login state and form rendering before real publishes.

## Subreddit Handling

The tool normalizes subreddits automatically:

- strips leading `/`, `r/` prefix
- validates format (alphanumeric + underscores only)
- invalid subreddit names cause immediate failure (not retryable)

Pass the forum value stored in the Funba delivery directly. Do not normalize it yourself.

Common subreddits for Funba content:

- `nba` — general NBA audience
- team subreddits (e.g. `Thunder`, `warriors`, `lakers`)

## Content Preconditions

Before publishing:

- the variant must already be written in English Reddit style
- the variant must be a `reddit` platform delivery
- the post must be `approved` status and not stale (within 24 hours of `source_date`)
- Reddit posts are text-only; image placeholders in the body should have been resolved or removed before delivery

## Retryable vs Non-Retryable Failures

The wrapper handles retry logic automatically. Non-retryable failures include:

- not logged in
- already published
- invalid subreddit
- missing title/content/subreddit
- post successfully submitted (even without URL detection)

Retryable failures include:

- form element not found (Reddit UI variation)
- network errors (`net::ERR*`)
- browser context closed unexpectedly
- timeout before reaching submit phase

## Artifacts

Each attempt creates artifacts under `logs/reddit_publish/`:

- `request.json` — input parameters
- `login_state.json` — session check result
- `submit_loaded.png` — screenshot after page load
- `filled.png` — screenshot after form fill
- `submitted.png` — screenshot after submit
- `failure_state.json`, `failure.png`, `failure_page.html` — on failure

Reference these artifacts in error reports.

## Writeback

Use:

- `POST /api/content/deliveries/{id}/status`

Status rules:

- success -> `published` + `published_url`
- failure -> `failed` + concrete `error_message` (include artifact path)
- start of attempt -> `publishing`

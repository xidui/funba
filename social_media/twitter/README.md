# Twitter

Canonical X/Twitter integration directory.

Contents:

- `auth.py` - cookie import and login status checks
- `.twitter_cookies.json` - local cookie cache used by future X/Twitter automation
- `.twitter_session_meta.json` - local metadata for the saved login state
- `.twitter_browser_data/` - optional local browser/profile artifacts kept out of git

Run examples:

```bash
.venv/bin/python -m social_media.twitter.auth login --chrome-profile Default
.venv/bin/python -m social_media.twitter.auth check
.venv/bin/python -m tasks.dispatch twitter-engage --dry-run --run-now
.venv/bin/python -m tasks.dispatch twitter-engage --run-now
.venv/bin/python -m social_media.twitter.reply --tweet-url "https://x.com/user/status/..." --content "..." --headed --keep-open-seconds 60
.venv/bin/python -m social_media.twitter.reply --tweet-url "https://x.com/user/status/..." --content "..." --submit
```

Notes:

- Cookie files are local runtime state and are ignored by git.
- `login` imports cookies from local Chrome into `.twitter_cookies.json`.
- `check` validates the saved state against `https://x.com/home` using Playwright.
- `twitter-engage` uses X Recent Search with `X_BEARER_TOKEN` and creates
  `TwitterEngagementConversation` and `TwitterEngagementMessage` rows for
  external X threads. For selected inbound messages, it creates one outgoing
  `SocialPost` reply draft, a disabled `twitter_reply` delivery, and a
  Paperclip Content Analyst issue. The LLM writing work happens through
  Paperclip, not the Funba OpenAI API path. Use `--no-paperclip` only for local
  debugging. The Paperclip issue includes matched game context plus any stored
  hero/notable metric signals so the agent can choose the strongest NBA data
  analyst angle for the reply.
- When `FUNBA_TWITTER_ACCOUNT_HANDLE` is set, discovery can include mentions
  and replies to that account so follow-up work items appear on the same 30-minute
  cadence. Set `FUNBA_TWITTER_ENGAGEMENT_INCLUDE_MENTIONS=0` to disable that.
- `twitter-engage` does not publish replies; manual confirmation is required
  before any reply is sent.
- `reply` is the manual-send tool for one approved target. Without `--submit`
  it only fills a draft; with `--submit` it sends the reply you explicitly
  selected.

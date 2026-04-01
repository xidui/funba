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
```

Notes:

- Cookie files are local runtime state and are ignored by git.
- `login` imports cookies from local Chrome into `.twitter_cookies.json`.
- `check` validates the saved state against `https://x.com/home` using Playwright.

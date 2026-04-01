# Reddit

Canonical Reddit integration directory.

Contents:

- `auth.py` - cookie import and login status checks
- `.reddit_cookies.json` - local cookie cache used by future Reddit automation
- `.reddit_session_meta.json` - local metadata for the saved login state
- `.reddit_browser_data/` - optional local browser/profile artifacts kept out of git

Run examples:

```bash
.venv/bin/python -m social_media.reddit.auth login --chrome-profile Default
.venv/bin/python -m social_media.reddit.auth check
```

Notes:

- Cookie files are local runtime state and are ignored by git.
- `login` imports cookies from local Chrome into `.reddit_cookies.json`.
- `check` validates the saved state against `https://www.reddit.com/user/me/`.

# Xiaohongshu

Canonical Xiaohongshu integration directory.

Contents:

- `auth.py` - cookie import and creator login status checks
- `.xiaohongshu_cookies.json` - local cookie cache used by future Xiaohongshu automation
- `.xiaohongshu_session_meta.json` - local metadata for the saved login state
- `.xiaohongshu_browser_data/` - optional local browser/profile artifacts kept out of git

Run examples:

```bash
.venv/bin/python -m social_media.xiaohongshu.auth login --chrome-profile Default
.venv/bin/python -m social_media.xiaohongshu.auth check
```

Notes:

- Cookie files are local runtime state and are ignored by git.
- `login` imports cookies from local Chrome into `.xiaohongshu_cookies.json`.
- `check` validates the saved state against Xiaohongshu creator pages.

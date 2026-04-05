# Xiaohongshu

Canonical Xiaohongshu integration directory.

Contents:

- `auth.py` - cookie import and creator login status checks
- `post.py` - CLI entrypoint and browser automation for Xiaohongshu graph notes
- `.xiaohongshu_cookies.json` - local cookie cache used by future Xiaohongshu automation
- `.xiaohongshu_session_meta.json` - local metadata for the saved login state
- `.xiaohongshu_browser_data/` - optional local browser/profile artifacts kept out of git

Run examples:

```bash
.venv/bin/python -m social_media.xiaohongshu.auth login --chrome-profile Default
.venv/bin/python -m social_media.xiaohongshu.auth check

# Dry run: fill the composer without submitting
.venv/bin/python -m social_media.xiaohongshu.post post \
  --title "雷霆这场防守强度有点离谱" \
  --content "正文..." \
  --image /tmp/funba_asset.png

# Save a draft
.venv/bin/python -m social_media.xiaohongshu.post post \
  --title "标题" \
  --content "正文..." \
  --image /tmp/funba_asset.png \
  --save-draft

# Publish for real
.venv/bin/python -m social_media.xiaohongshu.post post \
  --title "标题" \
  --content "正文..." \
  --image /tmp/funba_asset.png \
  --submit
```

Notes:

- Cookie files are local runtime state and are ignored by git.
- `login` imports cookies from local Chrome into `.xiaohongshu_cookies.json`.
- `check` validates the saved state against Xiaohongshu creator pages.
- `post` currently targets the Xiaohongshu graph-note flow (`上传图文`) and requires at least one image.
- If you are publishing from a Funba `SocialPost`, pass `--post-id <id>` so slot-based image placeholders can resolve from the image pool.

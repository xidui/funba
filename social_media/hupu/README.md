# Hupu

Canonical Hupu integration directory.

Contents:

- `post.py` — CLI entrypoint and browser automation
- `forums.py` — forum normalization and alias handling
- `.hupu_cookies.json` — local cookie cache used by the publisher
- `.hupu_browser_data/` — optional local browser/profile artifacts kept out of git

Run examples:

```bash
python -m social_media.hupu.post check
python -m social_media.hupu.post post --title "标题" --content "正文" --forum "NBA版"
python -m social_media.hupu.post post --title "标题" --content "正文" --forum "76人专区" --submit
```

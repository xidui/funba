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
python -m social_media.hupu.post capture --target "https://funba.app/players/1642843" --output /tmp/flagg_player.png
python -m social_media.hupu.post post --title "标题" --content "正文" --forum "NBA版"
python -m social_media.hupu.post post --title "标题" --content "正文" --forum "76人专区" --submit
```

## Screenshot Helper

Use the built-in compact capture command for Funba screenshots:

```bash
python -m social_media.hupu.post capture \
  --target "https://funba.app/metrics/fifty_point_games?season=22025" \
  --output /tmp/fifty_point_games.png
```

This command uses page-type-specific cropping rules for `player`, `game`, and
`metric` pages so the result is shorter and more post-friendly than a raw full-page screenshot.

## Login State

- `check` now does a page-level validation, not just a cookie-name check.
- A session is treated as logged in only when auth cookies exist and Hupu does not still render the logged-out UI.
- This avoids false positives where `u/us/_CLT` still exist in storage but the page header still shows `登录 / 注册`.

## Cookie Refresh

Preferred path:

```bash
python -m social_media.hupu.post login --chrome-profile "Profile 1"
python -m social_media.hupu.post check
```

This imports Hupu cookies from a local Chrome profile into `.hupu_cookies.json`, then validates them against the live Hupu page.

## macOS Keychain Caveat

On macOS, `login` depends on Chrome cookie decryption via `Chrome Safe Storage` in Keychain. It can fail even when Chrome is already logged in.

Typical failure:

```text
browser_cookie3.BrowserCookieError: Unable to get key for cookie decryption
```

Common causes:

- the current terminal / automation process does not have permission to read `Chrome Safe Storage`
- Chrome cookies exist on disk, but the decryption key is blocked by Keychain access rules

Recommended recovery order:

1. In `Keychain Access`, grant the calling process access to `Chrome Safe Storage`.
2. Re-run `python -m social_media.hupu.post login --chrome-profile "Profile 1"`.
3. Re-run `python -m social_media.hupu.post check`.

If direct Chrome import still fails, use a dedicated browser session that the operator can log into interactively, then export or reuse that live session for publishing. Do not assume a personal Chrome session can always be read offline.

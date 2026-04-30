# Instagram Publishing

Instagram uses credential login instead of imported Chrome cookies.

Credentials are read in this order:

- environment: `INSTAGRAM_USER` / `INSTAGRAM_PASSWORD`
- repo-local `SECRETS.md` with the same key names

Commands:

```bash
python -m social_media.instagram.post check
python -m social_media.instagram.post login --show-browser

python -m social_media.instagram.post post \
  --content "Caption text" \
  --image /path/to/poster.png

python -m social_media.instagram.post post \
  --content "Caption text" \
  --image /path/to/poster.png \
  --submit
```

For Funba content deliveries, pass `--post-id` so enabled `SocialPostImage`
rows are used. Slot priority is `poster_ig`, then `instagram`, then `poster`,
then the remaining enabled image rows.

```bash
python -m social_media.instagram.post post --content "Caption text" --post-id 123 --submit
```

The first login may still hit an Instagram checkpoint or 2FA prompt. Run with
`--show-browser` to complete that once; the script saves local storage state
under this directory for later runs.

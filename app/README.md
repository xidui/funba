# Funba Mobile

React Native (Expo) port of [funba.app](https://funba.app). iOS + Android from one codebase.

## Stack

- Expo SDK 52 + expo-router (file-based routing)
- TypeScript, NativeWind (Tailwind on RN), TanStack Query, Zustand
- react-native-svg for the shot chart
- Talks to the Flask backend via the new JSON layer under `/api/v1/mobile/*`

## Setup

```bash
cd app
npm install
npx expo start
```

Press `i` for iOS simulator, `a` for Android, or scan the QR code with Expo Go.

## Pointing the app at your backend

The backend base URL lives in `app.json` under `extra.apiBaseUrl`. Default is `http://localhost:5001`.

Local dev checklist:

1. Run the Flask app (`python -m web.app`) with `FUNBA_WEB_PORT=5001`.
2. iOS simulator can reach `localhost` directly. On a physical device, change `extra.apiBaseUrl` to your machine's LAN IP (e.g. `http://192.168.1.12:5001`).
3. For production, set it to `https://funba.app`.

## Feature coverage

Mirrors the web app's user-facing pages:

- Home — standings (east/west), team grid, recent games
- Games list — phase + year filters, grouped by date
- Game detail — scoreboard, quarter scores, team stats, player box scores, play-by-play, notable metrics
- Player detail — stat chips, per-season table, game log, shot chart (SVG)
- Team detail — record, roster, coaches, game log, totals
- Players browse + Player search
- Compare — pick 2–4 players, side-by-side career chips
- Metrics catalog + detail with pagination/seasons + my metrics
- News list + detail (with player/team tags)
- Draft by year (round groupings)
- Awards (MVP, FMVP, DPOY, ROY, SMOY, MIP, COY, Champion)
- Account — language switcher, subscription status, feedback, sign out

### Auth

- Magic link via `POST /api/v1/mobile/auth/magic/request`
- Email contains a deep link `funba://auth?token=...` that the app handles via `expo-linking`
- Bearer token persisted in `AsyncStorage`; attached as `Authorization: Bearer …`
- Google OAuth is not reimplemented natively — a button opens the web sign-in, user can come back to the app and tap "Email me a link" instead.

### Intentionally **not** in the app (by design)

- **Pro subscription upgrade** — web-only. iOS App Store requires IAP for digital subs; since the user will keep Pro on the web, the account screen shows the current tier and links out to `/pricing` in Safari.
- **Metric create / edit** — code editor on mobile is a bad experience. The "My metrics" screen links out to `/metrics/new` on the web.
- **Admin / content pipeline** — admin tools stay on the web.
- **Push notifications** — stub only; wire up in a follow-up when Apple Developer account is ready.

## Backend changes

`web/mobile_api_routes.py` adds a self-contained JSON layer. It is registered in `web/app.py` via `register_mobile_api_routes(app, session_factory=SessionLocal, send_magic_link=_mobile_send_magic_link)`. CORS is permissive for `/api/v1/mobile/*` only.

Endpoints:

- `GET /api/v1/mobile/health`
- `GET /api/v1/mobile/me`
- `GET /api/v1/mobile/home`
- `GET /api/v1/mobile/games` (?year, ?phase, ?team, ?page)
- `GET /api/v1/mobile/games/<slug_or_id>`
- `GET /api/v1/mobile/teams`
- `GET /api/v1/mobile/teams/<slug_or_id>` (?season)
- `GET /api/v1/mobile/players` (?season, ?team)
- `GET /api/v1/mobile/players/hints` (?q)
- `GET /api/v1/mobile/players/<slug_or_id>` (?season, ?heatmap_season)
- `GET /api/v1/mobile/players/compare` (?ids=A,B,C,D)
- `GET /api/v1/mobile/news`
- `GET /api/v1/mobile/news/<cluster_id>`
- `GET /api/v1/mobile/draft/<year>`
- `GET /api/v1/mobile/awards` (?type)
- `GET /api/v1/mobile/metrics` (?scope, ?q)
- `GET /api/v1/mobile/metrics/<key>` (?season, ?page)
- `GET /api/v1/mobile/metrics/mine` (auth required)
- `POST /api/v1/mobile/auth/magic/request`
- `POST /api/v1/mobile/auth/magic/verify`
- `POST /api/v1/mobile/feedback` (auth required)

Every endpoint honors `?lang=en|zh`.

## Directory layout

```
app/
  app/              # expo-router file-based routes
    (tabs)/         # bottom tabs (home/games/metrics/account)
    games/[slug].tsx
    players/[slug].tsx
    teams/[slug].tsx
    metrics/[key].tsx, mine.tsx
    news.tsx, news/[id].tsx
    draft/[year].tsx, awards.tsx
    compare.tsx, search.tsx, login.tsx
  components/       # Card, Screen, TeamBadge, GameRow, ShotHeatmap, …
  lib/              # api client, i18n, zustand store, auth, queries
```

## Release (next session)

- Apple Developer account — needed to publish to TestFlight / App Store
- Replace `extra.apiBaseUrl` with production URL
- Add real app icon + splash under `assets/`
- Run `npx expo prebuild` + EAS Build
- Configure Universal Links for `funba.app` magic link callback

# Kaggle Historical Box Score Dataset

Selected dataset for `db/backfill_kaggle_historical.py`:

- Dataset: `eoinamoore/historical-nba-data-and-player-box-scores`
- URL: <https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores>
- Verified page version: `425`
- Verified page date: `2026-03-30`
- License: `CC0: Public Domain`
- Page-reported coverage: `1947 to the present`

## Why This Dataset

- It is the most current Kaggle candidate surfaced during implementation.
- The dataset page explicitly lists separate `PlayerStatistics.csv`, `TeamStatistics.csv`, `Games.csv`, `Players.csv`, and `TeamHistories.csv` files, which matches Funba's existing `Game`, `PlayerGameStats`, `TeamGameStats`, `Player`, and `Team` tables.
- The page description says the data is based on NBA.com data rather than a Basketball-Reference scrape, which lowers the ID-mapping risk versus older Basketball-Reference-based Kaggle dumps.

## File Contract Used By The Importer

The Kaggle UI requires login to download files, so the importer is intentionally tolerant about exact header names. It accepts the following file set and looks for these field families:

### `Games.csv`

- Game identity: `game_id`
- Date: `game_date`
- Season: `season_id` or season text/start year
- Home team: id, name, abbreviation
- Away team: id, name, abbreviation
- Final score: home score, away score

### `TeamStatistics.csv`

- Game identity: `game_id`
- Team identity: team id, team name, team abbreviation
- Box score fields: minutes, points, FGM/FGA/FG%, 3PM/3PA/3P%, FTM/FTA/FT%, OREB/DREB/REB, AST, STL, BLK, TOV, PF
- Optional line-score fields: Q1-Q4 and OT period points

### `PlayerStatistics.csv`

- Game identity: `game_id`
- Team identity: team id / abbreviation
- Player identity: player id or full name
- Box score fields: minutes, starter/position, points, shooting splits, rebounds, assists, steals, blocks, turnovers, fouls, plus/minus

### `Players.csv`

- Player identity: player id or full name
- Biographical fields when present: first/last name, height, weight, position, active flag, career span

### `TeamHistories.csv`

- Team identity: team id / name / abbreviation
- Historical metadata when present: city, nickname, founding year, start/end season

## Current Implementation Notes

- Imported records are tagged with `data_source = "kaggle_box_scores"` on `Game`, `TeamGameStats`, and `PlayerGameStats`.
- Existing NBA API box-score rows default to `data_source = "nba_api_box_scores"`.
- The importer is idempotent at the row level: it upserts by the existing primary keys.
- Missing historical fields stay `NULL` instead of being forced to zero.
- Existing `GameLineScore` already supports `Q1-Q4`, `OT1-OT3`, and overflow OT data via `ot_extra_json`, so no extra OT migration was needed for this ticket.
- Because Kaggle download is login-gated, runtime usage currently expects a manually downloaded Kaggle zip or extracted directory passed to `--source`.

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.request import urlopen

from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import MetricDefinition, Player, Team, engine
from web.i18n.metric_names_zh import METRIC_NAMES_ZH
from web.i18n.player_names_zh import PLAYER_NAMES_ZH
from web.i18n.team_names_zh import TEAM_NAMES_ZH

SessionLocal = sessionmaker(bind=engine)
NBA_CN_PLAYERLIST_URL = "https://china.nba.cn/stats2/league/playerlist.json?locale=zh_CN"


def populate_teams() -> int:
    updated = 0
    with SessionLocal() as session:
        teams = session.query(Team).filter(Team.team_id.in_(TEAM_NAMES_ZH.keys())).all()
        for team in teams:
            zh_name = TEAM_NAMES_ZH.get(team.team_id)
            if zh_name and team.full_name_zh != zh_name:
                team.full_name_zh = zh_name
                updated += 1
        session.commit()
    return updated


def _fetch_nba_cn_player_names() -> dict[str, str]:
    with urlopen(NBA_CN_PLAYERLIST_URL, timeout=30) as resp:
        payload = json.load(resp)

    players = payload.get("payload", {}).get("players", [])
    names: dict[str, str] = {}
    for player in players:
        profile = player.get("playerProfile") or {}
        player_id = str(profile.get("playerId") or "").strip()
        display_name = str(profile.get("displayName") or "").strip()
        if player_id and display_name:
            names[player_id] = display_name
    return names


def populate_players(include_remote_feed: bool = False) -> int:
    updated = 0
    remote_names: dict[str, str] = {}
    if include_remote_feed:
        try:
            remote_names = _fetch_nba_cn_player_names()
        except Exception:
            remote_names = {}

    player_names = dict(remote_names)
    player_names.update(PLAYER_NAMES_ZH)

    with SessionLocal() as session:
        players = (
            session.query(Player)
            .filter(Player.player_id.in_(player_names.keys()))
            .order_by(Player.full_name.asc())
            .all()
        )
        for player in players:
            full_name_zh = player_names.get(player.player_id)
            if not full_name_zh:
                continue
            should_update = not player.full_name_zh or player.player_id in PLAYER_NAMES_ZH
            if should_update and player.full_name_zh != full_name_zh:
                player.full_name_zh = full_name_zh
                updated += 1
        session.commit()
    return updated


def populate_metrics() -> int:
    updated = 0
    with SessionLocal() as session:
        metrics = (
            session.query(MetricDefinition)
            .filter(MetricDefinition.status != "archived")
            .order_by(MetricDefinition.key.asc())
            .all()
        )
        for metric in metrics:
            item = METRIC_NAMES_ZH.get(metric.key)
            if item is None:
                continue
            name_zh = item.get("name_zh")
            description_zh = item.get("description_zh")
            if metric.name_zh != name_zh or metric.description_zh != description_zh:
                metric.name_zh = name_zh
                metric.description_zh = description_zh
                updated += 1
        session.commit()
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate Chinese team, player, and metric names.")
    parser.add_argument("--skip-metrics", action="store_true")
    parser.add_argument("--skip-remote-player-feed", action="store_true")
    args = parser.parse_args()

    team_updates = populate_teams()
    player_updates = populate_players(include_remote_feed=not args.skip_remote_player_feed)
    metric_updates = 0 if args.skip_metrics else populate_metrics()

    print(
        json.dumps(
            {
                "team_updates": team_updates,
                "player_updates": player_updates,
                "metric_updates": metric_updates,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from pathlib import Path

import anthropic
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import MetricDefinition, Player, Team, engine
from web.i18n.metric_names_zh import METRIC_NAMES_ZH
from web.i18n.team_names_zh import TEAM_NAMES_ZH


PLAYER_BATCH_SIZE = 50
MODEL = "claude-sonnet-4-20250514"
SessionLocal = sessionmaker(bind=engine)


def _chunks(items: list[dict], size: int) -> Iterable[list[dict]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _call_json(client: anthropic.Anthropic, prompt: str) -> list[dict]:
    response = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        temperature=0,
        system=(
            "Return only valid JSON. Use established Simplified Chinese NBA names as used on Hupu/NBA中文. "
            "If there is no established Chinese name, return null for that field."
        ),
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(block.text for block in response.content if getattr(block, "type", "") == "text").strip()
    return json.loads(text)


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


def populate_players(client: anthropic.Anthropic) -> int:
    updated = 0
    with SessionLocal() as session:
        players = (
            session.query(Player)
            .filter(Player.is_active.is_(True))
            .order_by(Player.full_name.asc())
            .all()
        )
        payload = [{"player_id": player.player_id, "full_name": player.full_name} for player in players if player.full_name]
        for batch in _chunks(payload, PLAYER_BATCH_SIZE):
            prompt = (
                "For each NBA player, return JSON list entries with keys player_id and full_name_zh. "
                "Use the most common Simplified Chinese forum/media name. Return null if there is no stable common Chinese name.\n\n"
                f"{json.dumps(batch, ensure_ascii=False)}"
            )
            translated = _call_json(client, prompt)
            translated_map = {item["player_id"]: item.get("full_name_zh") for item in translated}
            for player in players:
                if player.player_id not in translated_map:
                    continue
                full_name_zh = translated_map[player.player_id]
                if player.full_name_zh != full_name_zh:
                    player.full_name_zh = full_name_zh
                    updated += 1
        session.commit()
    return updated


def populate_metrics(client: anthropic.Anthropic | None = None) -> int:
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
    parser.add_argument("--skip-players", action="store_true")
    parser.add_argument("--skip-metrics", action="store_true")
    args = parser.parse_args()

    client = None
    if not args.skip_players:
        client = anthropic.Anthropic()

    team_updates = populate_teams()
    player_updates = 0 if args.skip_players else populate_players(client)
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

from __future__ import annotations

import json
import sys
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from content_pipeline.hero_highlight_variants import (
    enabled_hero_highlight_platforms,
    generate_hero_highlight_variants_for_game,
)
from db.models import (
    Base,
    Game,
    MetricDefinition,
    MetricResult,
    Player,
    SocialPost,
    SocialPostDelivery,
    SocialPostVariant,
    Team,
)


def _curated_player_hero() -> str:
    return json.dumps(
        {
            "version": 1,
            "hero": [
                {
                    "metric_key": "best_single_game_pts",
                    "entity_id": "p1",
                    "player_id": "p1",
                    "player_name": "Hero Player",
                    "narrative_zh": "Hero Player砍下55分，赛季单场第1。",
                    "narrative_en": "Hero Player scored 55, best this season",
                    "value_snapshot": 55,
                    "value_str_snapshot": "55",
                    "rank_snapshot": {
                        "season": 1,
                        "season_total": 100,
                        "alltime": 12,
                        "alltime_total": 5000,
                    },
                    "season": "22025",
                }
            ],
            "notable": [],
        },
        ensure_ascii=False,
    )


def _curated_team_hero() -> str:
    return json.dumps(
        {
            "version": 1,
            "hero": [
                {
                    "metric_key": "wins_by_10_plus_last5",
                    "entity_id": "1610612760",
                    "team_id": "1610612760",
                    "team_abbr": "OKC",
                    "narrative_zh": "OKC季后赛10+分胜场来到75场，历史第8。",
                    "narrative_en": "OKC reached 75 playoff wins by 10+, moving to 8th all-time.",
                    "value_snapshot": 75,
                    "value_str_snapshot": "75",
                    "rank_snapshot": {"alltime": 8},
                    "season": "all_playoffs",
                }
            ],
            "notable": [],
        },
        ensure_ascii=False,
    )


class TestHeroHighlightVariants(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _seed_game(self, session):
        now = datetime.now(UTC).replace(tzinfo=None)
        session.add_all(
            [
                Team(team_id="1610612747", abbr="LAL", full_name="Los Angeles Lakers"),
                Team(team_id="1610612738", abbr="BOS", full_name="Boston Celtics"),
                Player(player_id="p1", full_name="Hero Player"),
                Player(player_id="p2", full_name="Second Player"),
                Player(player_id="p3", full_name="Third Player"),
                Player(player_id="p4", full_name="Fourth Player"),
                MetricDefinition(
                    key="best_single_game_pts",
                    family_key="best_single_game_pts",
                    name="Best Single-Game Points",
                    name_zh="单场最高得分",
                    scope="player",
                    status="published",
                    source_type="rule",
                    created_at=now,
                    updated_at=now,
                ),
                Game(
                    game_id="0022500001",
                    slug="20260422-lal-bos",
                    season="22025",
                    game_date=date.fromisoformat("2026-04-22"),
                    home_team_id="1610612738",
                    road_team_id="1610612747",
                    wining_team_id="1610612747",
                    home_team_score=110,
                    road_team_score=118,
                    game_status="final",
                    highlights_curated_player_json=_curated_player_hero(),
                    highlights_curated_json=json.dumps({"version": 1, "hero": [], "notable": []}),
                    highlights_curated_team_json=json.dumps({"version": 1, "hero": [], "notable": []}),
                    highlights_curated_at=now,
                ),
                MetricResult(
                    metric_key="best_single_game_pts",
                    entity_type="player",
                    entity_id="p2",
                    season="22025",
                    sub_key="",
                    value_num=60,
                    value_str="60",
                    computed_at=now,
                ),
                MetricResult(
                    metric_key="best_single_game_pts",
                    entity_type="player",
                    entity_id="p1",
                    season="22025",
                    sub_key="",
                    value_num=55,
                    value_str="55",
                    computed_at=now,
                ),
                MetricResult(
                    metric_key="best_single_game_pts",
                    entity_type="player",
                    entity_id="p3",
                    season="22025",
                    sub_key="",
                    value_num=53,
                    value_str="53",
                    computed_at=now,
                ),
                MetricResult(
                    metric_key="best_single_game_pts",
                    entity_type="player",
                    entity_id="p4",
                    season="22025",
                    sub_key="",
                    value_num=49,
                    value_str="49",
                    computed_at=now,
                ),
            ]
        )
        session.commit()

    def test_generate_creates_in_review_post_with_pending_delivery(self):
        with self.SessionLocal() as session:
            self._seed_game(session)

            result = generate_hero_highlight_variants_for_game(
                session,
                "0022500001",
                platforms=["twitter"],
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["hero_count"], 1)

            post = session.query(SocialPost).one()
            self.assertEqual(post.status, "in_review")
            self.assertEqual(json.loads(post.source_metrics), ["best_single_game_pts"])
            self.assertEqual(json.loads(post.source_game_ids), ["0022500001"])
            self.assertTrue(post.topic.startswith("Hero Highlight — 0022500001 — player"))

            variant = session.query(SocialPostVariant).one()
            self.assertEqual(variant.post_id, post.id)
            self.assertIn("Hero Player scored 55, best this season", variant.content_raw)
            self.assertIn("Data: Best Single-Game Points = 55", variant.content_raw)
            self.assertIn("Ranking: #12 / 5000 (All-time)", variant.content_raw)
            self.assertIn("Top 3:\n1. Second Player - 60\n2. Hero Player - 55\n3. Third Player - 53", variant.content_raw)
            self.assertIn("Source: https://funba.app/metrics/best_single_game_pts?season=22025", variant.content_raw)
            self.assertIn("Game: https://funba.app/games/20260422-lal-bos", variant.content_raw)

            delivery = session.query(SocialPostDelivery).one()
            self.assertEqual(delivery.variant_id, variant.id)
            self.assertEqual(delivery.platform, "twitter")
            self.assertEqual(delivery.status, "pending")
            self.assertTrue(delivery.is_enabled)

    def test_generate_is_idempotent_for_same_hero_identity(self):
        with self.SessionLocal() as session:
            self._seed_game(session)

            first = generate_hero_highlight_variants_for_game(session, "0022500001", platforms=["twitter"])
            second = generate_hero_highlight_variants_for_game(session, "0022500001", platforms=["twitter"])

            self.assertEqual(first["post_ids"], second["post_ids"])
            self.assertEqual(session.query(SocialPost).count(), 1)
            self.assertEqual(session.query(SocialPostVariant).count(), 1)
            self.assertEqual(session.query(SocialPostDelivery).count(), 1)

    def test_top_three_uses_metric_family_virtual_season_pool(self):
        with self.SessionLocal() as session:
            self._seed_game(session)
            now = datetime.now(UTC).replace(tzinfo=None)
            game = session.query(Game).filter(Game.game_id == "0022500001").one()
            game.highlights_curated_player_json = json.dumps({"version": 1, "hero": [], "notable": []})
            game.highlights_curated_team_json = _curated_team_hero()
            session.add_all(
                [
                    Team(team_id="1610612760", abbr="OKC", full_name="Oklahoma City Thunder"),
                    Team(team_id="1610612739", abbr="CLE", full_name="Cleveland Cavaliers"),
                    MetricDefinition(
                        key="wins_by_10_plus_last5",
                        family_key="wins_by_10_plus",
                        name="Wins By 10+",
                        name_zh="10+分胜场",
                        scope="team",
                        status="published",
                        source_type="rule",
                        created_at=now,
                        updated_at=now,
                    ),
                    MetricResult(
                        metric_key="wins_by_10_plus_last5",
                        entity_type="team",
                        entity_id="1610612760",
                        season="last5_playoffs",
                        sub_key="",
                        value_num=75,
                        value_str="75",
                        computed_at=now,
                    ),
                    MetricResult(
                        metric_key="wins_by_10_plus_last5",
                        entity_type="team",
                        entity_id="1610612738",
                        season="last5_playoffs",
                        sub_key="",
                        value_num=72,
                        value_str="72",
                        computed_at=now,
                    ),
                    MetricResult(
                        metric_key="wins_by_10_plus_last5",
                        entity_type="team",
                        entity_id="1610612739",
                        season="last5_playoffs",
                        sub_key="",
                        value_num=70,
                        value_str="70",
                        computed_at=now,
                    ),
                ]
            )
            session.commit()

            generate_hero_highlight_variants_for_game(session, "0022500001", platforms=["twitter"])

            variant = session.query(SocialPostVariant).one()
            self.assertIn("Top 3:\n1. OKC - 75\n2. BOS - 72\n3. CLE - 70", variant.content_raw)
            self.assertIn("Source: https://funba.app/metrics/wins_by_10_plus_last5?season=all_playoffs", variant.content_raw)

    def test_platform_config_is_generic_and_normalizes_x_alias(self):
        self.assertEqual(
            enabled_hero_highlight_platforms({"FUNBA_HERO_HIGHLIGHT_PLATFORMS": "x,twitter,unknown"}),
            ["twitter"],
        )


if __name__ == "__main__":
    unittest.main()

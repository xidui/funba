from __future__ import annotations

import json
import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from content_pipeline.social_publish_throttle import (  # noqa: E402
    dispatch_next_social_delivery,
    get_social_throttle_config,
    update_social_throttle_config,
)
from db.models import Base, Setting, SocialPost, SocialPostDelivery, SocialPostVariant  # noqa: E402


class TestSocialPublishThrottle(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.now = datetime.fromisoformat("2026-05-01T18:00:00")

    def tearDown(self):
        self.engine.dispose()

    def _delivery(
        self,
        session,
        *,
        platform: str = "twitter",
        status: str = "pending",
        variant_status: str = "approved",
        post_status: str = "approved",
        game_ids: list[str] | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        published_at: datetime | None = None,
        priority: int = 25,
    ) -> SocialPostDelivery:
        stamp = created_at or self.now
        post = SocialPost(
            topic=f"post {platform} {stamp.isoformat()}",
            source_date=date.fromisoformat("2026-05-01"),
            source_metrics=json.dumps(["metric_a"]),
            source_game_ids=json.dumps(game_ids or ["game-1"]),
            status=post_status,
            priority=priority,
            created_at=stamp,
            updated_at=updated_at or stamp,
        )
        session.add(post)
        session.flush()
        variant = SocialPostVariant(
            post_id=post.id,
            title="Variant",
            content_raw="copy",
            audience_hint=None,
            status=variant_status,
            created_at=stamp,
            updated_at=updated_at or stamp,
        )
        session.add(variant)
        session.flush()
        delivery = SocialPostDelivery(
            variant_id=variant.id,
            platform=platform,
            forum=None,
            is_enabled=True,
            status=status,
            content_final=None,
            published_url=None,
            published_at=published_at,
            error_message=None,
            created_at=stamp,
            updated_at=updated_at or stamp,
        )
        session.add(delivery)
        session.flush()
        return delivery

    def test_social_throttle_config_round_trips_instagram(self):
        with self.SessionLocal() as session:
            cfg = update_social_throttle_config(
                session,
                "instagram",
                {
                    "enabled": False,
                    "min_interval_minutes": 90,
                    "max_posts_per_day": 2,
                    "max_posts_per_game_per_day": 1,
                    "max_pending_age_hours": 36,
                },
            )
            session.commit()

            self.assertFalse(cfg.enabled)
            stored = get_social_throttle_config(session, "ig")
            self.assertFalse(stored.enabled)
            self.assertEqual(stored.min_interval_minutes, 90)
            self.assertEqual(stored.max_posts_per_day, 2)
            self.assertEqual(stored.max_pending_age_hours, 36)
            self.assertIsNotNone(session.get(Setting, "social.instagram.throttle.daily_max"))

    def test_dispatch_reserves_one_delivery_and_enqueues_publisher(self):
        with self.SessionLocal() as session:
            update_social_throttle_config(session, "instagram", {"min_interval_minutes": 0})
            delivery = self._delivery(session, platform="instagram")
            enqueued: list[tuple[int, int]] = []

            result = dispatch_next_social_delivery(
                session,
                platform="instagram",
                now_utc=self.now,
                enqueue_publish=lambda post_id, delivery_id: enqueued.append((post_id, delivery_id)),
            )

            self.assertEqual(result["status"], "enqueued")
            self.assertEqual(result["delivery_id"], delivery.id)
            self.assertEqual(delivery.status, "publishing")
            self.assertEqual(enqueued, [(result["post_id"], delivery.id)])

    def test_daily_cap_counts_published_activity(self):
        with self.SessionLocal() as session:
            update_social_throttle_config(
                session,
                "twitter",
                {"min_interval_minutes": 0, "max_posts_per_day": 1},
            )
            self._delivery(
                session,
                platform="twitter",
                status="published",
                published_at=self.now - timedelta(hours=1),
                updated_at=self.now - timedelta(hours=1),
            )
            self._delivery(session, platform="twitter", game_ids=["game-2"])

            result = dispatch_next_social_delivery(
                session,
                platform="x",
                now_utc=self.now,
                enqueue_publish=lambda _post_id, _delivery_id: None,
            )

            self.assertEqual(result["status"], "daily_cap_reached")
            self.assertEqual(result["published_or_reserved_today"], 1)

    def test_game_cap_skips_one_game_and_publishes_next_candidate(self):
        with self.SessionLocal() as session:
            update_social_throttle_config(
                session,
                "twitter",
                {"min_interval_minutes": 0, "max_posts_per_day": 3, "max_posts_per_game_per_day": 1},
            )
            self._delivery(
                session,
                platform="twitter",
                status="published",
                game_ids=["game-1"],
                published_at=self.now - timedelta(hours=1),
                updated_at=self.now - timedelta(hours=1),
            )
            skipped = self._delivery(
                session,
                platform="twitter",
                game_ids=["game-1"],
                created_at=self.now - timedelta(minutes=2),
            )
            chosen = self._delivery(
                session,
                platform="twitter",
                game_ids=["game-2"],
                created_at=self.now - timedelta(minutes=1),
            )
            enqueued: list[int] = []

            result = dispatch_next_social_delivery(
                session,
                platform="twitter",
                now_utc=self.now,
                enqueue_publish=lambda _post_id, delivery_id: enqueued.append(delivery_id),
            )

            self.assertEqual(result["status"], "enqueued")
            self.assertEqual(result["delivery_id"], chosen.id)
            self.assertEqual(skipped.status, "pending")
            self.assertEqual(enqueued, [chosen.id])

    def test_min_interval_counts_in_flight_publishing_delivery(self):
        with self.SessionLocal() as session:
            update_social_throttle_config(session, "instagram", {"min_interval_minutes": 60})
            self._delivery(
                session,
                platform="instagram",
                status="publishing",
                updated_at=self.now - timedelta(minutes=30),
            )
            self._delivery(session, platform="instagram", game_ids=["game-2"])

            result = dispatch_next_social_delivery(
                session,
                platform="instagram",
                now_utc=self.now,
                enqueue_publish=lambda _post_id, _delivery_id: None,
            )

            self.assertEqual(result["status"], "waiting_interval")
            self.assertIn("next_allowed_at", result)

    def test_max_pending_age_excludes_old_approved_delivery(self):
        with self.SessionLocal() as session:
            update_social_throttle_config(
                session,
                "twitter",
                {"min_interval_minutes": 0, "max_pending_age_hours": 24},
            )
            self._delivery(
                session,
                platform="twitter",
                created_at=self.now - timedelta(days=2),
                updated_at=self.now - timedelta(days=2),
            )

            result = dispatch_next_social_delivery(
                session,
                platform="twitter",
                now_utc=self.now,
                enqueue_publish=lambda _post_id, _delivery_id: None,
            )

            self.assertEqual(result["status"], "no_eligible_delivery")
            self.assertEqual(result["candidate_count"], 0)


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from db.models import Base, SocialPost, SocialPostDelivery, SocialPostVariant  # noqa: E402
from web.admin_content_routes import _advance_hero_variants_after_ai_review  # noqa: E402


class TestHeroReviewFlow(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_ai_review_pass_auto_approves_only_autopublish_hero_variants(self):
        now = datetime.utcnow()
        with self.SessionLocal() as session:
            post = SocialPost(
                topic="Hero Highlight — 0042500231 — game — stocks — 0042500231:1641705",
                source_date=date.fromisoformat("2026-05-04"),
                source_metrics='["stocks"]',
                source_game_ids='["0042500231"]',
                status="ai_review",
                priority=25,
                created_at=now,
                updated_at=now,
            )
            session.add(post)
            session.flush()

            funba_variant = SocialPostVariant(
                post_id=post.id,
                title="Funba card",
                content_raw="[[IMAGE:slot=poster]]",
                audience_hint="deterministic hero highlight / funba",
                status="ai_review",
                created_at=now,
                updated_at=now,
            )
            twitter_variant = SocialPostVariant(
                post_id=post.id,
                title="Twitter card",
                content_raw="[[IMAGE:slot=poster]]",
                audience_hint="deterministic hero highlight / twitter",
                status="ai_review",
                created_at=now,
                updated_at=now,
            )
            session.add_all([funba_variant, twitter_variant])
            session.flush()
            funba_delivery = SocialPostDelivery(
                variant_id=funba_variant.id,
                platform="funba",
                is_enabled=True,
                status="pending",
                created_at=now,
                updated_at=now,
            )
            twitter_delivery = SocialPostDelivery(
                variant_id=twitter_variant.id,
                platform="twitter",
                is_enabled=True,
                status="pending",
                created_at=now,
                updated_at=now,
            )
            session.add_all([funba_delivery, twitter_delivery])
            session.flush()

            publish_targets = _advance_hero_variants_after_ai_review(
                session,
                post,
                SocialPostVariant,
                SocialPostDelivery,
            )

            self.assertEqual(post.status, "in_review")
            self.assertEqual(funba_variant.status, "approved")
            self.assertEqual(twitter_variant.status, "in_review")
            self.assertEqual(publish_targets, [(funba_delivery.id, "funba")])


if __name__ == "__main__":
    unittest.main()

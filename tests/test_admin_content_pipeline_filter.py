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

from db.models import Base, GameContentAnalysisIssuePost, SocialPost
from web.admin_content_routes import _apply_pipeline_filter


class TestAdminContentPipelineFilter(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _post(self, *, topic: str, comments=None) -> SocialPost:
        now = datetime.now(UTC).replace(tzinfo=None)
        return SocialPost(
            topic=topic,
            source_date=date.fromisoformat("2026-04-22"),
            source_metrics=json.dumps(["m"], ensure_ascii=False),
            source_game_ids=json.dumps([], ensure_ascii=False),
            status="draft",
            priority=50,
            llm_model=None,
            admin_comments=json.dumps(comments, ensure_ascii=False) if comments is not None else None,
            created_at=now,
            updated_at=now,
        )

    def test_pipeline_filter_splits_known_types_and_keeps_null_comment_other(self):
        with self.SessionLocal() as session:
            game_post = self._post(topic="Game analysis post")
            metric_post = self._post(
                topic="Metric 数据分析",
                comments=[{"event_type": "metric_deep_dive_brief", "text": "brief"}],
            )
            hero_post = self._post(topic="Hero Highlight — 0022500001 — player — pts — p1")
            other_post = self._post(topic="Manual post", comments=None)
            session.add_all([game_post, metric_post, hero_post, other_post])
            session.flush()
            session.add(
                GameContentAnalysisIssuePost(
                    issue_record_id=1,
                    post_id=game_post.id,
                    discovered_via="api_create",
                    created_at=datetime.now(UTC).replace(tzinfo=None),
                )
            )
            session.commit()

            base = session.query(SocialPost)
            ids_by_filter = {
                key: {
                    row.id
                    for row in _apply_pipeline_filter(base, session, SocialPost, GameContentAnalysisIssuePost, key).all()
                }
                for key in ("game_analysis", "metric_deep_dive", "hero_highlight", "other")
            }

            self.assertEqual(ids_by_filter["game_analysis"], {game_post.id})
            self.assertEqual(ids_by_filter["metric_deep_dive"], {metric_post.id})
            self.assertEqual(ids_by_filter["hero_highlight"], {hero_post.id})
            self.assertEqual(ids_by_filter["other"], {other_post.id})


if __name__ == "__main__":
    unittest.main()

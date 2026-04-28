"""Backfill: mirror existing published SocialPost rows as NewsArticle (source='funba').

The publish flow now calls mirror_published_social_post automatically for new
hero-highlight posts. This script catches up on the ~200+ posts that were
created before the wiring landed.

Usage:
    .venv/bin/python -m scripts.backfill_funba_news [--dry-run]

Eligibility: a SocialPost qualifies if its status is approved/published AND it
has at least one SocialPostDelivery on platform='funba' with status='published'.
mirror_published_social_post is idempotent — re-running only mirrors what is
missing.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy.orm import sessionmaker  # noqa: E402

from db.models import (  # noqa: E402
    NewsArticle,
    SocialPost,
    SocialPostDelivery,
    SocialPostVariant,
    engine,
)
from db.news_internal import mirror_published_social_post  # noqa: E402

SessionLocal = sessionmaker(bind=engine)


def find_eligible_posts(session) -> list[SocialPost]:
    """Posts visible on the home feed (i.e. not archived) with a published
    funba_internal delivery — same eligibility as mirror_published_social_post."""
    return (
        session.query(SocialPost)
        .join(SocialPostVariant, SocialPostVariant.post_id == SocialPost.id)
        .join(SocialPostDelivery, SocialPostDelivery.variant_id == SocialPostVariant.id)
        .filter(
            SocialPost.status != "archived",
            SocialPostDelivery.platform == "funba",
            SocialPostDelivery.status == "published",
        )
        .distinct()
        .order_by(SocialPost.id.asc())
        .all()
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="List eligible posts but don't write.")
    args = ap.parse_args()

    with SessionLocal() as session:
        posts = find_eligible_posts(session)
        print(f"eligible posts: {len(posts)}")

        already = (
            session.query(NewsArticle.internal_social_post_id)
            .filter(NewsArticle.source == "funba", NewsArticle.internal_social_post_id.isnot(None))
            .all()
        )
        mirrored_ids = {row[0] for row in already}
        pending = [p for p in posts if int(p.id) not in mirrored_ids]
        print(f"already mirrored: {len(mirrored_ids)}, pending: {len(pending)}")

        if args.dry_run:
            for p in pending[:20]:
                print(f"  would mirror: post_id={p.id} status={p.status} topic={(p.topic or '')[:80]}")
            if len(pending) > 20:
                print(f"  ... and {len(pending) - 20} more")
            return 0

        created = 0
        failed = 0
        for p in pending:
            try:
                article = mirror_published_social_post(session, p)
                if article is not None:
                    created += 1
                    if created % 25 == 0:
                        session.commit()
                        print(f"  ... {created} created", flush=True)
            except Exception as exc:
                failed += 1
                print(f"  FAIL post_id={p.id}: {exc}", flush=True)
                session.rollback()
        session.commit()

        print(f"\ndone: created={created} failed={failed}")
        return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

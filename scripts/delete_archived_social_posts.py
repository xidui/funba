"""Hard-delete archived SocialPost rows and their fan-out.

Cascades handled by FKs (post_id -> CASCADE on SocialPost):
  - SocialPostVariant       (which then cascades SocialPostDelivery)
  - SocialPostImage
  - GameContentAnalysisIssuePost

Manual cleanup:
  - NewsArticle mirrors (FK is SET NULL, not CASCADE)
    + their NewsArticlePlayer / NewsArticleTeam tags
    + their singleton NewsCluster (funba mirrors are always solo clusters)
  - On-disk image files referenced by SocialPostImage.file_path

Usage:
    .venv/bin/python scripts/delete_archived_social_posts.py [--dry-run]

Idempotent: a re-run finds nothing to delete and exits cleanly.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy.orm import Session, sessionmaker  # noqa: E402

from db.models import (  # noqa: E402
    GameContentAnalysisIssuePost,
    NewsArticle,
    NewsArticlePlayer,
    NewsArticleTeam,
    NewsCluster,
    SocialPost,
    SocialPostDelivery,
    SocialPostImage,
    SocialPostVariant,
    engine,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as s:
        archived_ids = [
            int(r[0])
            for r in s.query(SocialPost.id).filter(SocialPost.status == "archived").all()
        ]
        if not archived_ids:
            print("no archived SocialPost rows")
            return 0
        print(f"archived SocialPost rows: {len(archived_ids)}")

        # 1. Files to delete after DB ops succeed.
        image_paths = [
            str(r[0])
            for r in s.query(SocialPostImage.file_path)
            .filter(SocialPostImage.post_id.in_(archived_ids))
            .filter(SocialPostImage.file_path.isnot(None))
            .all()
        ]
        print(f"image files to remove: {len(image_paths)}")

        # 2. NewsArticle mirrors and their singleton clusters.
        articles = (
            s.query(NewsArticle.id, NewsArticle.cluster_id)
            .filter(
                NewsArticle.internal_social_post_id.in_(archived_ids),
                NewsArticle.source == "funba",
            )
            .all()
        )
        article_ids = [int(a[0]) for a in articles]
        cluster_ids = [int(a[1]) for a in articles if a[1] is not None]
        print(f"NewsArticle rows to delete: {len(article_ids)}")
        print(f"NewsCluster rows to delete: {len(cluster_ids)}")

        # 3. Counts that will cascade automatically — printed for visibility.
        n_var = s.query(SocialPostVariant).filter(SocialPostVariant.post_id.in_(archived_ids)).count()
        var_ids_q = s.query(SocialPostVariant.id).filter(SocialPostVariant.post_id.in_(archived_ids))
        n_del = s.query(SocialPostDelivery).filter(
            SocialPostDelivery.variant_id.in_(var_ids_q.subquery().select())
        ).count()
        n_img = s.query(SocialPostImage).filter(SocialPostImage.post_id.in_(archived_ids)).count()
        n_issue = s.query(GameContentAnalysisIssuePost).filter(
            GameContentAnalysisIssuePost.post_id.in_(archived_ids)
        ).count()
        print(f"will cascade-delete: variants={n_var} deliveries={n_del} images={n_img} issue_links={n_issue}")

        if args.dry_run:
            print("\n(dry run — no DB writes, no file deletes)")
            return 0

        # ---- Manual deletes (NewsArticle + clusters) ----
        if article_ids:
            s.query(NewsArticlePlayer).filter(NewsArticlePlayer.article_id.in_(article_ids)).delete(synchronize_session=False)
            s.query(NewsArticleTeam).filter(NewsArticleTeam.article_id.in_(article_ids)).delete(synchronize_session=False)
            # Null out cluster.representative_article_id first so the FK from
            # cluster -> article (SET NULL) doesn't re-link a dead row mid-delete.
            for cid in cluster_ids:
                c = s.get(NewsCluster, cid)
                if c is not None:
                    c.representative_article_id = None
            s.flush()
            s.query(NewsArticle).filter(NewsArticle.id.in_(article_ids)).delete(synchronize_session=False)
            if cluster_ids:
                s.query(NewsCluster).filter(NewsCluster.id.in_(cluster_ids)).delete(synchronize_session=False)
            s.flush()

        # ---- SocialPost delete (cascades take care of variants/deliveries/
        # images/issue_links via the ondelete=CASCADE FKs) ----
        s.query(SocialPost).filter(SocialPost.id.in_(archived_ids)).delete(synchronize_session=False)
        s.commit()
        print("DB rows deleted.")

        # ---- File deletion (after DB commit, so we don't orphan files
        # for a subsequent retry on a partial DB failure) ----
        removed = 0
        missing = 0
        for fp in image_paths:
            p = Path(fp)
            try:
                if p.exists():
                    p.unlink()
                    removed += 1
                    # Also nuke the .thumb.webp sibling and color sidecar.
                    for sibling in [p.with_suffix(".thumb.webp"), p.with_suffix(".color.txt")]:
                        if sibling.exists():
                            sibling.unlink()
                else:
                    missing += 1
            except Exception as exc:
                print(f"  failed to remove {p}: {exc}")
        print(f"files removed: {removed}, missing: {missing}")

        # ---- Empty per-post directories ----
        empty_dirs = 0
        for fp in image_paths:
            d = Path(fp).parent
            try:
                if d.exists() and not any(d.iterdir()):
                    d.rmdir()
                    empty_dirs += 1
            except Exception:
                pass
        print(f"empty per-post dirs removed: {empty_dirs}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

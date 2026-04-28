"""One-off cleanup: archive duplicate Hero Highlight SocialPost rows.

Background: under concurrent curator runs the dedup check in
`_create_post_for_card` raced — multiple posts ended up with identical topics.
This script keeps the oldest (lowest id) post per unique
(topic, source_date) pair and archives the rest. Posts already archived are
left alone.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy.orm import Session  # noqa: E402

from db.models import SocialPost, engine  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="report only, no DB writes")
    args = parser.parse_args()

    with Session(engine) as session:
        rows = (
            session.query(SocialPost)
            .filter(SocialPost.topic.like("Hero Highlight %"))
            .filter(SocialPost.status != "archived")
            .order_by(SocialPost.id.asc())
            .all()
        )
        groups: dict[tuple[str, str], list[SocialPost]] = defaultdict(list)
        for p in rows:
            key = (str(p.topic), p.source_date.isoformat() if p.source_date else "")
            groups[key].append(p)

        dupes: list[tuple[tuple[str, str], list[SocialPost]]] = [
            (k, ps) for k, ps in groups.items() if len(ps) > 1
        ]
        print(f"hero highlight posts: {len(rows)} non-archived")
        print(f"unique topics:        {len(groups)}")
        print(f"topics with dupes:    {len(dupes)}")

        archived = 0
        for (topic, source_date), ps in dupes:
            keep = ps[0]
            kill = ps[1:]
            print(f"  KEEP {keep.id}  ARCHIVE {[p.id for p in kill]}  topic={topic[:80]}")
            if not args.dry_run:
                for p in kill:
                    p.status = "archived"
                    p.updated_at = datetime.utcnow()
                    archived += 1
        if not args.dry_run:
            session.commit()
            print(f"archived {archived} duplicate posts")
        else:
            print("(dry run — no changes)")


if __name__ == "__main__":
    main()

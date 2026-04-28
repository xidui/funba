"""Backfill .thumb.webp siblings for every existing image under media/.

New images get thumbnails inline at save time (social_media/images.py +
social_media/hero_poster.py). This script catches up the ~2k images that
already exist on disk.

Usage:
    .venv/bin/python -m scripts.backfill_thumbnails [--root PATH] [--force]

Defaults to walking both the repo-root `media/` and the deploy worktree's
`.paperclip/deploy-main/media/`. Idempotent: skips images that already have
a non-empty thumb newer than the source. Reports created/skipped/failed.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from social_media.thumbnail import (  # noqa: E402
    SOURCE_EXTS,
    is_thumbnail,
    make_thumbnail,
    thumbnail_path_for,
)


DEFAULT_ROOTS = [
    REPO_ROOT / "media",
    REPO_ROOT / ".paperclip" / "deploy-main" / "media",
]


def iter_source_images(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if is_thumbnail(path):
            continue
        if path.suffix.lower() not in SOURCE_EXTS:
            continue
        yield path


def backfill(root: Path, *, force: bool, verbose: bool) -> dict[str, int]:
    stats = {"seen": 0, "created": 0, "skipped": 0, "failed": 0, "bytes_in": 0, "bytes_out": 0}
    for src in iter_source_images(root):
        stats["seen"] += 1
        thumb = thumbnail_path_for(src)
        already_good = (
            thumb.exists()
            and thumb.stat().st_size > 0
            and thumb.stat().st_mtime >= src.stat().st_mtime
        )
        if already_good and not force:
            stats["skipped"] += 1
            continue
        result = make_thumbnail(src, force=force)
        if result is None:
            stats["failed"] += 1
            if verbose:
                print(f"  FAIL  {src}", flush=True)
            continue
        stats["created"] += 1
        stats["bytes_in"] += src.stat().st_size
        stats["bytes_out"] += result.stat().st_size
        if verbose:
            print(f"  ok    {src.name} -> {result.name} ({src.stat().st_size//1024}KB -> {result.stat().st_size//1024}KB)", flush=True)
        if stats["created"] % 50 == 0:
            print(f"  ... {stats['created']} created, {stats['skipped']} skipped", flush=True)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, action="append", default=None,
                    help="Directory to walk (repeatable). Defaults to media/ + deploy worktree.")
    ap.add_argument("--force", action="store_true", help="Regenerate even if thumb exists.")
    ap.add_argument("--verbose", action="store_true", help="Print every file processed.")
    args = ap.parse_args()

    roots = args.root or DEFAULT_ROOTS
    roots = [r for r in roots if r.exists()]
    if not roots:
        print("no media roots found", file=sys.stderr)
        return 1

    overall = {"seen": 0, "created": 0, "skipped": 0, "failed": 0, "bytes_in": 0, "bytes_out": 0}
    t0 = time.time()
    for root in roots:
        print(f"\n== {root} ==", flush=True)
        stats = backfill(root, force=args.force, verbose=args.verbose)
        for k in overall:
            overall[k] += stats[k]
        print(f"  -> seen={stats['seen']} created={stats['created']} skipped={stats['skipped']} failed={stats['failed']}", flush=True)

    elapsed = time.time() - t0
    print("\n== summary ==")
    print(f"  seen    : {overall['seen']}")
    print(f"  created : {overall['created']}")
    print(f"  skipped : {overall['skipped']}")
    print(f"  failed  : {overall['failed']}")
    if overall["bytes_in"]:
        ratio = overall["bytes_in"] / max(overall["bytes_out"], 1)
        print(f"  size    : {overall['bytes_in']/1e6:.1f} MB -> {overall['bytes_out']/1e6:.1f} MB ({ratio:.1f}x)")
    print(f"  elapsed : {elapsed:.1f}s")
    return 0 if overall["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

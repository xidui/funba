"""Mirror published SocialPost rows into NewsArticle as source='funba'.

Funba's own posts always live in their own singleton cluster — they don't
participate in the cosine-similarity merging that ESPN/NBA.com articles do,
because the user's editorial stance is "my site is its own voice, not just
another article about the same story". Skipping the merge also means we
skip the (paid) embedding API call entirely.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

from db.models import (
    NewsArticle,
    NewsCluster,
    SocialPost,
    SocialPostImage,
    SocialPostVariant,
)
from db.news_ingest import build_alias_index, tag_article
from db.news_ranking import recompute_cluster_score

_IMAGE_PLACEHOLDER_RE = re.compile(r"\[\[IMAGE:[^\]]*\]\]")
FUNBA_PLATFORM = "funba"

logger = logging.getLogger(__name__)


def _primary_variant_fields(session, social_post: SocialPost) -> tuple[str, str]:
    """Return (title, summary) from the funba_internal variant if present,
    else the first variant, else fall back to SocialPost.topic.

    The funba variant is preferred because it's the one rendered for funba's
    own audience — twitter/hupu variants are tuned for those platforms and
    may include hashtags or platform-specific framing.
    """
    funba_variant = (
        session.query(SocialPostVariant)
        .filter(SocialPostVariant.post_id == social_post.id)
        .filter(SocialPostVariant.audience_hint.like(f"%{FUNBA_PLATFORM}%"))
        .order_by(SocialPostVariant.id.asc())
        .first()
    )
    chosen = funba_variant or (
        session.query(SocialPostVariant)
        .filter(SocialPostVariant.post_id == social_post.id)
        .order_by(SocialPostVariant.id.asc())
        .first()
    )
    title = social_post.topic or ""
    summary = ""
    if chosen is not None:
        title = chosen.title or title
        summary = (chosen.content_raw or "")
    summary = _IMAGE_PLACEHOLDER_RE.sub("", summary).strip()
    return title[:512], summary[:2000]


def _poster_thumbnail_url(session, post_id: int) -> str | None:
    """Build the public URL for the post's poster thumbnail, if one exists."""
    poster = (
        session.query(SocialPostImage)
        .filter(
            SocialPostImage.post_id == post_id,
            SocialPostImage.slot == "poster",
            SocialPostImage.is_enabled.is_(True),
        )
        .first()
    )
    if poster is None or not poster.file_path:
        return None
    src = Path(str(poster.file_path))
    thumb = src.with_suffix(".thumb.webp")
    fname = thumb.name if thumb.exists() else src.name
    return f"/media/social_posts/{post_id}/{fname}"


def mirror_published_social_post(session, social_post: SocialPost) -> NewsArticle | None:
    """Idempotent. Returns the new or existing NewsArticle row, or None if the
    SocialPost is not yet published.

    Funba posts always create a fresh singleton cluster — no embedding,
    no similarity matching, no merging with external coverage.
    """
    if social_post is None:
        return None
    if (social_post.status or "").lower() not in {"approved", "published"}:
        return None

    source_guid = f"funba:{social_post.id}"
    existing = (
        session.query(NewsArticle)
        .filter(NewsArticle.source == "funba", NewsArticle.source_guid == source_guid)
        .one_or_none()
    )
    if existing is not None:
        return existing

    title, summary = _primary_variant_fields(session, social_post)
    if not title:
        return None

    now = datetime.utcnow()
    published_at = social_post.updated_at or now
    thumbnail_url = _poster_thumbnail_url(session, int(social_post.id))

    cluster = NewsCluster(
        representative_article_id=None,
        first_seen_at=published_at,
        last_seen_at=published_at,
        article_count=1,
        unique_view_count=0,
        score=0.0,
    )
    session.add(cluster)
    session.flush()

    article = NewsArticle(
        cluster_id=cluster.id,
        source="funba",
        internal_social_post_id=social_post.id,
        source_guid=source_guid,
        url=f"/posts/{social_post.id}",
        title=title,
        summary=summary or None,
        thumbnail_url=thumbnail_url,
        published_at=published_at,
        fetched_at=now,
        embedding=None,
        embedding_model=None,
        embedding_text_hash=None,
    )
    session.add(article)
    session.flush()

    cluster.representative_article_id = article.id
    recompute_cluster_score(cluster, now=now)

    alias_index = build_alias_index(session)
    player_ids, team_ids = tag_article(title, summary, alias_index)
    from db.models import NewsArticlePlayer, NewsArticleTeam  # local to avoid cycle
    for pid in player_ids:
        session.add(NewsArticlePlayer(article_id=article.id, player_id=pid))
    for tid in team_ids:
        session.add(NewsArticleTeam(article_id=article.id, team_id=tid))

    return article

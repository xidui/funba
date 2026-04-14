"""Mirror published SocialPost rows into NewsArticle as source='funba'.

Wiring this helper into the existing publish flow is deferred to a
follow-up change. For now this module only exposes the helper so the
schema hook is in place.
"""
from __future__ import annotations

import logging
from datetime import datetime

import numpy as np

from db.embeddings import EMBEDDING_MODEL, embed_texts, hash_embedding_text, vector_to_blob
from db.models import NewsArticle, SocialPost
from db.news_ingest import (
    _attach_or_create_cluster,
    _embedding_text,
    build_alias_index,
    tag_article,
)
from db.news_ranking import recompute_cluster_score  # noqa: F401  (re-export friendly)

logger = logging.getLogger(__name__)


def _primary_variant_fields(social_post: SocialPost) -> tuple[str, str, str | None]:
    """Return (title, summary, thumbnail_url) from the primary variant if any.

    Falls back to the SocialPost topic when no variant exists. Thumbnails are
    intentionally not populated yet: SocialPostImage.file_path is a local
    filesystem path, not an http(s) URL, so rendering it via <img src> would
    both break and risk XSS. Once funba exposes a public /assets/<id> route
    for post images, this helper can map the file_path through that route.
    """
    variants = getattr(social_post, "variants", None)
    title = social_post.topic or ""
    summary = ""
    if variants:
        first = variants[0]
        title = first.title or title
        summary = (first.content_raw or "")[:2000]
    return title[:512], summary, None


def mirror_published_social_post(session, social_post: SocialPost) -> NewsArticle | None:
    """Idempotent. Returns the new or existing NewsArticle row, or None if the
    SocialPost is not yet published.
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

    title, summary, thumbnail = _primary_variant_fields(social_post)
    if not title:
        return None

    now = datetime.utcnow()
    published_at = social_post.updated_at or now

    text = _embedding_text(title, summary)
    try:
        vectors = embed_texts([text])
        vector = np.asarray(vectors[0], dtype=np.float32)
    except Exception as exc:
        logger.warning("mirror_published_social_post embedding failed: %s", exc)
        return None

    article = NewsArticle(
        cluster_id=None,
        source="funba",
        internal_social_post_id=social_post.id,
        source_guid=source_guid,
        url=f"/posts/{social_post.id}",
        title=title,
        summary=summary or None,
        thumbnail_url=thumbnail,
        published_at=published_at,
        fetched_at=now,
        embedding=vector_to_blob(vector),
        embedding_model=EMBEDDING_MODEL,
        embedding_text_hash=hash_embedding_text(text),
    )
    session.add(article)
    session.flush()

    alias_index = build_alias_index(session)
    player_ids, team_ids = tag_article(title, summary, alias_index)
    from db.models import NewsArticlePlayer, NewsArticleTeam  # local to avoid cycle
    for pid in player_ids:
        session.add(NewsArticlePlayer(article_id=article.id, player_id=pid))
    for tid in team_ids:
        session.add(NewsArticleTeam(article_id=article.id, team_id=tid))

    _attach_or_create_cluster(session, article, vector, now=now)
    return article

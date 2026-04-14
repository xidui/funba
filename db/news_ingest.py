"""NBA news scraping + clustering + tagging.

Run via Celery task `tasks.ingest.scrape_nba_news` (hourly) or directly:
    python -c "from db.news_ingest import scrape_all; print(scrape_all())"

Safeguards:
- Per-source hard cap (MAX_ARTICLES_PER_SOURCE_PER_RUN)
- Age cutoff (7d on first run, 24h steady-state)
- Early-exit after N consecutive already-seen GUIDs (RSS is newest-first)
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import numpy as np
import requests
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import undefer

from db.embeddings import (
    EMBEDDING_MODEL,
    blob_to_vector,
    cosine,
    embed_texts,
    hash_embedding_text,
    vector_to_blob,
)
from db.models import (
    NewsArticle,
    NewsArticlePlayer,
    NewsArticleTeam,
    NewsCluster,
    Player,
    Team,
)
from db.news_ranking import recompute_cluster_score

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

MAX_ARTICLES_PER_SOURCE_PER_RUN = 100
AGE_CUTOFF_FIRST_RUN_DAYS = 7
AGE_CUTOFF_STEADY_STATE_HOURS = 24
EARLY_EXIT_CONSECUTIVE_SEEN = 3

CLUSTER_SIMILARITY_THRESHOLD = 0.82
CLUSTER_WINDOW_HOURS = 48

HTTP_TIMEOUT_SECONDS = 10
USER_AGENT = "funba-news-bot/1.0 (+https://funba.app)"

_ALLOWED_URL_SCHEMES = {"http", "https"}


def _safe_external_url(url: str | None) -> str | None:
    """Return url iff it is a normal http(s) link. Prevents javascript:/data: XSS."""
    if not url:
        return None
    url = url.strip()
    # No embedded control chars / whitespace in scheme portion.
    if any(ch.isspace() for ch in url[:16]):
        return None
    head, sep, _ = url.partition(":")
    if not sep:
        return None
    if head.lower() not in _ALLOWED_URL_SCHEMES:
        return None
    return url

ESPN_RSS_URL = "https://www.espn.com/espn/rss/nba/news"
NBA_CONTENT_API_URL = "https://content-api-prod.nba.com/public/1/content/layout/nba/news/en"


# ---------------------------------------------------------------------------
# Feed fetchers
# ---------------------------------------------------------------------------

@dataclass
class FetchedItem:
    source: str
    source_guid: str
    url: str
    title: str
    summary: str
    thumbnail_url: str | None
    published_at: datetime
    raw: dict = field(default_factory=dict)


def _parse_rfc822(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _strip_html(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"<[^>]+>", "", text).strip()


def fetch_espn_rss() -> list[FetchedItem]:
    """Fetch and parse the ESPN NBA RSS feed."""
    try:
        resp = requests.get(
            ESPN_RSS_URL,
            headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml,text/xml,*/*"},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ESPN RSS fetch failed: %s", exc)
        return []

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as exc:
        logger.warning("ESPN RSS parse failed: %s", exc)
        return []

    items: list[FetchedItem] = []
    channel = root.find("channel")
    if channel is None:
        return items
    for item in channel.findall("item"):
        guid_el = item.find("guid")
        link_el = item.find("link")
        guid = (guid_el.text or "").strip() if guid_el is not None else ""
        link = (link_el.text or "").strip() if link_el is not None else ""
        if not guid:
            guid = link
        safe_link = _safe_external_url(link)
        if not guid or not safe_link:
            continue
        link = safe_link
        title = _strip_html((item.findtext("title") or "").strip())
        description = _strip_html((item.findtext("description") or "").strip())
        pub = _parse_rfc822(item.findtext("pubDate"))
        if not pub or not title:
            continue
        thumb = None
        # <media:thumbnail url="..."> namespace
        for child in item:
            if child.tag.endswith("}thumbnail") and child.get("url"):
                thumb = child.get("url")
                break
        if not thumb:
            enc = item.find("enclosure")
            if enc is not None and enc.get("url") and (enc.get("type") or "").startswith("image"):
                thumb = enc.get("url")
        thumb = _safe_external_url(thumb) if thumb else None
        items.append(
            FetchedItem(
                source="espn",
                source_guid=guid[:255],
                url=link[:1024],
                title=title[:512],
                summary=description,
                thumbnail_url=(thumb[:1024] if thumb else None),
                published_at=pub,
            )
        )
    return items


def fetch_nba_official() -> list[FetchedItem]:
    """Best-effort NBA.com content API fetch. Silently returns [] on any failure
    so the ESPN source can still proceed."""
    try:
        resp = requests.get(
            NBA_CONTENT_API_URL,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        payload = resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.info("NBA.com content API unavailable: %s", exc)
        return []

    items: list[FetchedItem] = []
    # Layout is a nested tree of "items" lists under "results". Walk defensively.
    def _walk(node):
        if isinstance(node, dict):
            if node.get("contentType") == "article" or node.get("type") == "article":
                yield node
            for v in node.values():
                yield from _walk(v)
        elif isinstance(node, list):
            for v in node:
                yield from _walk(v)

    seen_guids: set[str] = set()
    for art in _walk(payload):
        art_id = str(art.get("id") or art.get("uuid") or art.get("articleId") or "")
        if not art_id or art_id in seen_guids:
            continue
        seen_guids.add(art_id)
        title = _strip_html(art.get("title") or art.get("headline") or "")
        summary = _strip_html(art.get("description") or art.get("subhead") or art.get("summary") or "")
        url = art.get("permalink") or art.get("url") or ""
        if url and not url.startswith("http"):
            url = "https://www.nba.com" + url
        safe_url = _safe_external_url(url)
        if not safe_url:
            continue
        url = safe_url
        thumb = None
        img = art.get("image") or art.get("thumbnail")
        if isinstance(img, dict):
            thumb = img.get("url") or img.get("href")
        elif isinstance(img, str):
            thumb = img
        thumb = _safe_external_url(thumb) if thumb else None
        published_raw = art.get("published") or art.get("publishedDate") or art.get("date")
        published = None
        if isinstance(published_raw, str):
            try:
                published = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
                if published.tzinfo is not None:
                    published = published.astimezone(timezone.utc).replace(tzinfo=None)
            except ValueError:
                published = _parse_rfc822(published_raw)
        if not (title and url and published):
            continue
        items.append(
            FetchedItem(
                source="nba",
                source_guid=art_id[:255],
                url=url[:1024],
                title=title[:512],
                summary=summary,
                thumbnail_url=(thumb[:1024] if thumb else None),
                published_at=published,
            )
        )
    return items


# ---------------------------------------------------------------------------
# Tagging (player/team alias matching)
# ---------------------------------------------------------------------------

@dataclass
class AliasIndex:
    player_patterns: list[tuple[re.Pattern, str]]  # (regex, player_id)
    team_patterns: list[tuple[re.Pattern, str]]    # (regex, team_id)


def _build_alias_pattern(alias: str) -> re.Pattern:
    return re.compile(r"\b" + re.escape(alias) + r"\b", re.IGNORECASE)


def build_alias_index(session) -> AliasIndex:
    # Teams: full_name, nick_name (e.g. "Lakers"), abbr.
    team_patterns: list[tuple[re.Pattern, str]] = []
    for team in session.query(Team).filter(Team.is_legacy.is_(False)).all():
        aliases: set[str] = set()
        if team.full_name:
            aliases.add(team.full_name.strip())
        if team.nick_name:
            aliases.add(team.nick_name.strip())
        if team.abbr:
            aliases.add(team.abbr.strip())
        # Last word of full_name ("Lakers" from "Los Angeles Lakers")
        if team.full_name and " " in team.full_name:
            last = team.full_name.rsplit(" ", 1)[-1].strip()
            if last:
                aliases.add(last)
        for alias in aliases:
            if not alias:
                continue
            if len(alias) < 3 and alias != (team.abbr or ""):
                continue
            team_patterns.append((_build_alias_pattern(alias), team.team_id))

    # Players: active only (by default). Full name + last name (len>3).
    player_patterns: list[tuple[re.Pattern, str]] = []
    for player in session.query(Player).filter(Player.is_active.is_(True)).all():
        if player.is_team:
            continue
        aliases: set[str] = set()
        if player.full_name:
            aliases.add(player.full_name.strip())
        if player.last_name and len(player.last_name.strip()) > 3:
            aliases.add(player.last_name.strip())
        for alias in aliases:
            if not alias:
                continue
            player_patterns.append((_build_alias_pattern(alias), player.player_id))

    return AliasIndex(player_patterns=player_patterns, team_patterns=team_patterns)


def tag_article(title: str, summary: str, index: AliasIndex) -> tuple[set[str], set[str]]:
    haystack = f"{title} {summary or ''}"
    player_ids: set[str] = set()
    team_ids: set[str] = set()
    for pat, pid in index.player_patterns:
        if pat.search(haystack):
            player_ids.add(pid)
    for pat, tid in index.team_patterns:
        if pat.search(haystack):
            team_ids.add(tid)
    return player_ids, team_ids


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------

def _embedding_text(title: str, summary: str) -> str:
    parts = [title.strip()]
    if summary:
        parts.append(summary.strip())
    return " | ".join(parts)


def _attach_or_create_cluster(session, article: NewsArticle, vector: np.ndarray, now: datetime) -> NewsCluster:
    """Find best-matching recent cluster by cosine sim; attach or create."""
    window_start = now - timedelta(hours=CLUSTER_WINDOW_HOURS)
    candidates = (
        session.query(NewsArticle)
        .options(undefer(NewsArticle.embedding))
        .filter(
            NewsArticle.published_at >= window_start,
            NewsArticle.cluster_id.isnot(None),
            NewsArticle.id != article.id,
            NewsArticle.embedding_model == EMBEDDING_MODEL,
        )
        .all()
    )
    best_score = 0.0
    best_cluster_id: int | None = None
    best_candidate: NewsArticle | None = None
    for cand in candidates:
        if not cand.embedding:
            continue
        cand_vec = blob_to_vector(cand.embedding)
        score = cosine(vector, cand_vec)
        if score > best_score:
            best_score = score
            best_cluster_id = cand.cluster_id
            best_candidate = cand

    if best_cluster_id is not None and best_score >= CLUSTER_SIMILARITY_THRESHOLD:
        cluster = session.get(NewsCluster, best_cluster_id)
        if cluster is None:
            # Fallthrough to create
            pass
        else:
            article.cluster_id = cluster.id
            cluster.article_count = (cluster.article_count or 0) + 1
            if article.published_at and (not cluster.last_seen_at or article.published_at > cluster.last_seen_at):
                cluster.last_seen_at = article.published_at
            # Upgrade representative if the new article has a longer summary
            # (cheap proxy for "richer" content). Funba source wins ties.
            rep = session.get(NewsArticle, cluster.representative_article_id) if cluster.representative_article_id else None
            if _should_replace_representative(rep, article):
                cluster.representative_article_id = article.id
            recompute_cluster_score(cluster, now=now)
            return cluster

    # Create a new cluster
    cluster = NewsCluster(
        representative_article_id=None,
        first_seen_at=article.published_at,
        last_seen_at=article.published_at,
        article_count=1,
        unique_view_count=0,
        score=0.0,
    )
    session.add(cluster)
    session.flush()
    article.cluster_id = cluster.id
    cluster.representative_article_id = article.id
    recompute_cluster_score(cluster, now=now)
    return cluster


def _should_replace_representative(existing: NewsArticle | None, new: NewsArticle) -> bool:
    if existing is None:
        return True
    if new.source == "funba" and existing.source != "funba":
        return True
    if existing.source == "funba" and new.source != "funba":
        return False
    new_len = len(new.summary or "")
    old_len = len(existing.summary or "")
    if new_len != old_len:
        return new_len > old_len
    # Tie: prefer earliest published_at.
    if new.published_at and existing.published_at:
        return new.published_at < existing.published_at
    return False


# ---------------------------------------------------------------------------
# Upsert loop
# ---------------------------------------------------------------------------

@dataclass
class RunStats:
    espn: int = 0
    nba: int = 0
    new_clusters: int = 0
    attached: int = 0
    skipped_seen: int = 0
    errors: int = 0


def _source_has_rows(session, source: str) -> bool:
    return session.query(NewsArticle.id).filter(NewsArticle.source == source).limit(1).first() is not None


def _insert_article(
    session,
    item: FetchedItem,
    alias_index: AliasIndex,
    embedding_vector: np.ndarray,
    now: datetime,
) -> tuple[NewsArticle, bool]:
    """Returns (article, created_new_cluster)."""
    article = NewsArticle(
        cluster_id=None,
        source=item.source,
        source_guid=item.source_guid,
        url=item.url,
        title=item.title,
        summary=item.summary or None,
        thumbnail_url=item.thumbnail_url,
        published_at=item.published_at,
        fetched_at=now,
        embedding=vector_to_blob(embedding_vector),
        embedding_model=EMBEDDING_MODEL,
        embedding_text_hash=hash_embedding_text(_embedding_text(item.title, item.summary or "")),
    )
    session.add(article)
    session.flush()

    player_ids, team_ids = tag_article(item.title, item.summary or "", alias_index)
    for pid in player_ids:
        session.add(NewsArticlePlayer(article_id=article.id, player_id=pid))
    for tid in team_ids:
        session.add(NewsArticleTeam(article_id=article.id, team_id=tid))

    pre_cluster_count = session.query(NewsCluster.id).count()
    cluster = _attach_or_create_cluster(session, article, embedding_vector, now=now)
    created_new = cluster.article_count == 1
    return article, created_new


def _process_source_items(
    session,
    items: list[FetchedItem],
    alias_index: AliasIndex,
    stats: RunStats,
    source_label: str,
) -> None:
    if not items:
        return
    now = datetime.utcnow()
    is_first_run = not _source_has_rows(session, source_label)
    if is_first_run:
        cutoff = now - timedelta(days=AGE_CUTOFF_FIRST_RUN_DAYS)
    else:
        cutoff = now - timedelta(hours=AGE_CUTOFF_STEADY_STATE_HOURS)

    items_sorted = sorted(items, key=lambda it: it.published_at, reverse=True)

    inserted = 0
    consecutive_seen = 0
    pending_embeds: list[tuple[FetchedItem, str]] = []

    # Pre-filter: drop items older than cutoff, drop items already in DB.
    filtered: list[FetchedItem] = []
    for item in items_sorted:
        if item.published_at < cutoff:
            break  # rest are older
        exists = (
            session.query(NewsArticle.id)
            .filter(NewsArticle.source == item.source, NewsArticle.source_guid == item.source_guid)
            .first()
        )
        if exists:
            stats.skipped_seen += 1
            consecutive_seen += 1
            if consecutive_seen >= EARLY_EXIT_CONSECUTIVE_SEEN:
                break
            continue
        consecutive_seen = 0
        filtered.append(item)
        if len(filtered) >= MAX_ARTICLES_PER_SOURCE_PER_RUN:
            break

    if not filtered:
        return

    # Batch-embed to cut OpenAI round trips.
    texts = [_embedding_text(it.title, it.summary or "") for it in filtered]
    try:
        vectors = embed_texts(texts)
    except Exception as exc:
        logger.warning("Embedding batch failed for %s: %s", source_label, exc)
        stats.errors += len(filtered)
        return

    for item, vec in zip(filtered, vectors):
        try:
            np_vec = np.asarray(vec, dtype=np.float32)
            article, created_new = _insert_article(session, item, alias_index, np_vec, now)
            if created_new:
                stats.new_clusters += 1
            else:
                stats.attached += 1
            inserted += 1
            if source_label == "espn":
                stats.espn += 1
            elif source_label == "nba":
                stats.nba += 1
            session.commit()
        except IntegrityError:
            # Concurrent scraper run raced us to (source, source_guid). Safe to skip.
            session.rollback()
            stats.skipped_seen += 1
        except Exception as exc:
            session.rollback()
            logger.exception("Failed to ingest %s article %s: %s", source_label, item.source_guid, exc)
            stats.errors += 1


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def scrape_all() -> dict:
    from db.models import engine
    from sqlalchemy.orm import Session

    stats = RunStats()
    with Session(engine) as session:
        alias_index = build_alias_index(session)

        _process_source_items(session, fetch_espn_rss(), alias_index, stats, "espn")
        _process_source_items(session, fetch_nba_official(), alias_index, stats, "nba")

        # Final score refresh for any cluster touched in the last 48h so
        # brand-new articles outrank stale ones immediately.
        refresh_all_recent_scores(session)
        session.commit()

    result = {
        "espn": stats.espn,
        "nba": stats.nba,
        "new_clusters": stats.new_clusters,
        "attached": stats.attached,
        "skipped_seen": stats.skipped_seen,
        "errors": stats.errors,
    }
    logger.info("scrape_all result: %s", result)
    return result


def refresh_all_recent_scores(session, now: datetime | None = None) -> int:
    """Recompute score for every cluster touched in the last 48h.

    Also recomputes unique_view_count from the PageView table, excluding
    crawler hits. Admin visitors are excluded upstream by the pageview
    middleware (which skips inserts when the session user is_admin)."""
    from db.models import PageView

    now = now or datetime.utcnow()
    window = now - timedelta(hours=CLUSTER_WINDOW_HOURS)
    clusters = (
        session.query(NewsCluster)
        .filter(NewsCluster.last_seen_at >= window)
        .all()
    )
    for cluster in clusters:
        count = (
            session.query(PageView.visitor_id)
            .filter(
                PageView.path.in_([f"/news/{cluster.id}", f"/cn/news/{cluster.id}"]),
                PageView.is_crawler.is_(False),
            )
            .distinct()
            .count()
        )
        cluster.unique_view_count = count or 0
        cluster.view_count_refreshed_at = now
        recompute_cluster_score(cluster, now=now)
    return len(clusters)

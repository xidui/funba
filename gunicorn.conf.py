"""Gunicorn config for funba web.

Master preloads `web.app` and warms the catalog + embedding caches once,
then forks workers that inherit those caches via copy-on-write. Each
worker disposes the master's SQLAlchemy pool right after fork so it
opens fresh MySQL connections instead of sharing master's sockets.
"""
from __future__ import annotations

import logging

preload_app = True

_log = logging.getLogger("gunicorn.error")


def when_ready(server):
    """Run once in the master after the app is loaded but before fork."""
    try:
        from web.app import SessionLocal, _catalog_metrics
        from metrics.framework.search import _ensure_candidate_embeddings

        with SessionLocal() as session:
            catalog = _catalog_metrics(
                session,
                scope_filter="",
                status_filter="",
                include_result_counts=False,
            )
        _ensure_candidate_embeddings(catalog)
        _log.info("funba warmup: %d catalog entries primed", len(catalog))
    except Exception as exc:
        _log.warning("funba warmup skipped: %s", exc)


def post_fork(server, worker):
    """Drop the inherited SQLAlchemy pool so this worker uses fresh sockets."""
    try:
        from web.app import engine

        engine.dispose()
    except Exception as exc:
        _log.warning("post_fork engine.dispose failed: %s", exc)

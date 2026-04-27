"""Gunicorn config for funba web.

Master preloads `web.app` and warms the catalog + embedding caches once,
then forks workers that inherit those caches via copy-on-write. Each
worker disposes the master's SQLAlchemy pool right after fork so it
opens fresh MySQL connections instead of sharing master's sockets.
"""
from __future__ import annotations

from contextlib import contextmanager
import logging
import os
import signal

preload_app = True

_log = logging.getLogger("gunicorn.error")
_WARMUP_TIMEOUT_SECONDS = float(os.getenv("FUNBA_GUNICORN_WARMUP_TIMEOUT_SECONDS", "8"))


class _WarmupTimeout(TimeoutError):
    pass


@contextmanager
def _warmup_deadline(seconds: float):
    """Bound master warmup so gunicorn can still fork workers if caches hang."""
    if seconds <= 0:
        yield
        return

    previous_handler = signal.getsignal(signal.SIGALRM)

    def _raise_timeout(signum, frame):
        raise _WarmupTimeout(f"warmup exceeded {seconds:.1f}s")

    signal.signal(signal.SIGALRM, _raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def when_ready(server):
    """Run once in the master after the app is loaded but before fork."""
    try:
        with _warmup_deadline(_WARMUP_TIMEOUT_SECONDS):
            from web.app import app as flask_app, SessionLocal, _catalog_metrics
            from metrics.framework.search import warm_embedding_cache

            with flask_app.test_request_context("/"):
                with SessionLocal() as session:
                    catalog = _catalog_metrics(
                        session,
                        scope_filter="",
                        status_filter="",
                        include_result_counts=False,
                    )
                    embeddings = warm_embedding_cache(session)
        _log.info(
            "funba warmup: %d catalog entries / %d embeddings primed",
            len(catalog), embeddings,
        )
    except _WarmupTimeout as exc:
        _log.warning("funba warmup timed out: %s; continuing without preloaded caches", exc)
    except Exception as exc:
        _log.warning("funba warmup skipped: %s", exc)


def post_fork(server, worker):
    """Drop the inherited SQLAlchemy pool so this worker uses fresh sockets."""
    try:
        from web.app import engine

        engine.dispose()
    except Exception as exc:
        _log.warning("post_fork engine.dispose failed: %s", exc)

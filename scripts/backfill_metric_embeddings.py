"""One-shot: populate MetricDefinition.embedding for every metric.

Reuses any vectors already in ~/.cache/funba/metric_embeddings.json so we
don't re-pay the OpenAI embedding bill, then computes whatever's left.
Safe to re-run — skips rows whose stored hash matches the current text.

Usage:
    NBA_DB_URL=mysql+pymysql://... OPENAI_API_KEY=... \\
        .venv/bin/python -m scripts.backfill_metric_embeddings
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from sqlalchemy.orm import undefer

from db.models import MetricDefinition, engine
from sqlalchemy.orm import Session

from metrics.framework.search import (
    EMBEDDING_MODEL,
    _candidate_embedding_text,
    _embed_texts,
    _hash_embedding_text,
    _vector_to_blob,
)


_LEGACY_JSON = Path(
    os.environ.get("FUNBA_CACHE_DIR", str(Path.home() / ".cache" / "funba"))
) / "metric_embeddings.json"


def _load_legacy_vectors() -> dict[str, tuple[str, list[float]]]:
    """Returns {key: (hash, vector)} from the old JSON sidecar, if present."""
    if not _LEGACY_JSON.exists():
        return {}
    try:
        data = json.loads(_LEGACY_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict) or data.get("model") != EMBEDDING_MODEL:
        return {}
    entries = data.get("entries") or {}
    out: dict[str, tuple[str, list[float]]] = {}
    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        h = entry.get("hash")
        vec = entry.get("vector")
        if isinstance(h, str) and isinstance(vec, list):
            out[key] = (h, vec)
    return out


def main() -> int:
    legacy = _load_legacy_vectors()
    print(f"legacy JSON: {len(legacy)} vectors loaded from {_LEGACY_JSON}")

    with Session(engine) as session:
        rows = (
            session.query(MetricDefinition)
            .options(
                undefer(MetricDefinition.embedding),
                undefer(MetricDefinition.embedding_model),
                undefer(MetricDefinition.embedding_text_hash),
            )
            .filter(MetricDefinition.status != "archived")
            .all()
        )
        print(f"db rows: {len(rows)}")

        from_legacy = 0
        already_ok = 0
        to_compute: list[tuple[MetricDefinition, str, str]] = []

        for row in rows:
            text = _candidate_embedding_text(row)
            if not text:
                continue
            h = _hash_embedding_text(text)
            if (
                row.embedding_model == EMBEDDING_MODEL
                and row.embedding_text_hash == h
                and row.embedding
            ):
                already_ok += 1
                continue
            cached = legacy.get(row.key)
            if cached and cached[0] == h:
                row.embedding = _vector_to_blob(cached[1])
                row.embedding_model = EMBEDDING_MODEL
                row.embedding_text_hash = h
                from_legacy += 1
                continue
            to_compute.append((row, text, h))

        print(
            f"already_ok={already_ok}  from_legacy={from_legacy}  to_compute={len(to_compute)}"
        )

        if from_legacy and not to_compute:
            session.commit()
            print("commit (legacy only)")

        if to_compute:
            t0 = time.perf_counter()
            texts = [t for _, t, _ in to_compute]
            vectors = _embed_texts(texts)
            print(f"embedded {len(vectors)} via OpenAI in {time.perf_counter()-t0:.2f}s")
            for (row, _text, h), vec in zip(to_compute, vectors):
                row.embedding = _vector_to_blob(vec)
                row.embedding_model = EMBEDDING_MODEL
                row.embedding_text_hash = h
            session.commit()
            print("commit (legacy + computed)")

    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())

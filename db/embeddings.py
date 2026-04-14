"""Shared embedding helpers.

Extracted from metrics/framework/search.py so non-metrics modules
(e.g. news ingestion) can embed text without importing the metrics package.

Float32 little-endian bytes layout matches MetricDefinition.embedding and
NewsArticle.embedding so both tables can share helpers without conversion.
"""
from __future__ import annotations

import hashlib

import numpy as np

EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIM = 3072
_EMBEDDING_BATCH = 256


def embed_texts(texts: list[str]) -> list[list[float]]:
    import openai

    client = openai.OpenAI()
    out: list[list[float]] = []
    for i in range(0, len(texts), _EMBEDDING_BATCH):
        chunk = texts[i : i + _EMBEDDING_BATCH]
        resp = client.embeddings.create(model=EMBEDDING_MODEL, input=chunk)
        for item in resp.data:
            out.append(list(item.embedding))
    return out


def embed_query(query: str) -> np.ndarray:
    return np.asarray(embed_texts([query])[0], dtype=np.float32)


def vector_to_blob(vector) -> bytes:
    return np.asarray(vector, dtype=np.float32).tobytes()


def blob_to_vector(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32)


def hash_embedding_text(text: str) -> str:
    return hashlib.sha256(f"{EMBEDDING_MODEL}::{text}".encode("utf-8")).hexdigest()[:32]


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    denom = float(np.linalg.norm(a)) * float(np.linalg.norm(b))
    if not denom:
        return 0.0
    return float(np.dot(a, b)) / denom

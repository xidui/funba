"""Rank-based noteworthiness helpers.

Rank is now derived at query time via SQL window functions; this module only
provides the is_notable predicate used by templates and logging.
"""
from __future__ import annotations


def is_notable(rank: int, total: int) -> bool:
    """True when the entity is in the top 25% for this metric/season."""
    return total > 0 and rank / total <= 0.25

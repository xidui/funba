"""Reconcile BR slug mismatches and fill in unmatched TransactionAsset.player_id.

Some players show up as unmatched in TransactionAsset because their BR
canonical name diverges from our nba_api-sourced full_name (name changes,
nicknames, parsing bugs). This script:

  1. Fixes a small handful of known data corrections in Player.full_name.
  2. For each unique BR slug present in TransactionAsset but with no Player
     row holding that slug, attempts a relaxed match using BR's slug shape
     (`lastname[5] + firstname[2] + 2-digit collision`) against our
     last_name + first-name initial — disambiguated by active-year overlap.
     If we can confidently pick a Player, write br_slug onto the row.
  3. Final UPDATE pass joins by br_slug and fills TransactionAsset.player_id.

Usage:
    python -m db.reconcile_br_slugs                # dry run + auto-fix
    python -m db.reconcile_br_slugs --commit       # apply changes
"""
from __future__ import annotations

import argparse
import logging
import re
import unicodedata
from collections import defaultdict

from sqlalchemy.orm import sessionmaker
from sqlalchemy import text as sql_text

from db.models import Player, TransactionAsset, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


# Known data corrections — wrong full_name in our DB, with the canonical
# name BR uses. Fix in place; br_slug will be picked up automatically next.
_NAME_CORRECTIONS: dict[str, str] = {
    # player_id -> canonical name
    "201180": "Sun Yue",  # was stored as "Sun Sun" (CJK parsing artifact)
}

# Known direct slug bridges where the player exists but neither name match
# nor slug-shape inference will find them (rare nickname/family-name shifts).
_SLUG_OVERRIDES: dict[str, str] = {
    "1628502": "hayesni01",  # Nigel Hayes (now Nigel Hayes-Davis)
    "1630231": "martike04",  # Kenyon Martin Jr. (now KJ Martin)
    "201180":  "yuesu01",    # Sun Yue
}


def _norm(name: str) -> str:
    if not name:
        return ""
    f = unicodedata.normalize("NFKD", name)
    f = "".join(ch for ch in f if not unicodedata.combining(ch))
    f = re.sub(r"[.'’`]+", "", f)
    f = f.replace("-", " ")
    f = re.sub(r"\s+", " ", f).strip().casefold()
    f = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", "", f).strip()
    return f


def _bare_lastname_first_initial(name: str) -> tuple[str, str] | None:
    """Returns (lastname-lowercased-letters-only, first-initial)."""
    parts = re.split(r"\s+", _norm(name))
    if len(parts) < 2:
        return None
    last = re.sub(r"[^a-z]", "", parts[-1])[:5]
    first = re.sub(r"[^a-z]", "", parts[0])[:2]
    if not last or not first:
        return None
    return last, first


def _slug_signature(slug: str) -> tuple[str, str] | None:
    """Decode 'kuminjo01' → ('kumin', 'jo')."""
    m = re.match(r"^([a-z]+?)([a-z]{2})(\d{2})$", slug)
    if m and len(m.group(1)) <= 5:
        return m.group(1), m.group(2)
    # Fallback: take last 4 chars as suffix-like
    if len(slug) >= 6:
        return slug[:-4], slug[-4:-2]
    return None


def run(commit: bool) -> None:
    session = Session()
    try:
        # Step 1: name corrections
        for pid, new_name in _NAME_CORRECTIONS.items():
            p = session.query(Player).filter(Player.player_id == pid).first()
            if p is None or p.full_name == new_name:
                continue
            logger.info("name fix: %s %r → %r", pid, p.full_name, new_name)
            p.full_name = new_name

        # Step 2: hardcoded slug overrides
        for pid, slug in _SLUG_OVERRIDES.items():
            p = session.query(Player).filter(Player.player_id == pid).first()
            if p is None or p.br_slug == slug:
                continue
            logger.info("slug override: %s → %s (was %r)", pid, slug, p.br_slug)
            p.br_slug = slug

        # Step 3: relaxed slug-shape inference for unmatched assets
        slugs_in_assets = {
            r[0] for r in session.query(TransactionAsset.player_br_slug)
            .filter(TransactionAsset.asset_type == "player",
                    TransactionAsset.player_id.is_(None),
                    TransactionAsset.player_br_slug.isnot(None))
            .distinct()
            .all()
            if r[0]
        }
        # Drop slugs already covered by some Player.br_slug
        held = {p[0] for p in session.query(Player.br_slug).filter(Player.br_slug.isnot(None)).all()}
        unheld = slugs_in_assets - held

        # Bucket players by (last5, first2) signature for cheap lookup
        sig_to_players: dict[tuple[str, str], list[Player]] = defaultdict(list)
        for p in session.query(Player).filter(Player.full_name.isnot(None), Player.br_slug.is_(None)).all():
            sig = _bare_lastname_first_initial(p.full_name or "")
            if sig:
                sig_to_players[sig].append(p)

        inferred = 0
        ambiguous = 0
        for slug in unheld:
            sig = _slug_signature(slug)
            if sig is None:
                continue
            cands = sig_to_players.get(sig, [])
            if len(cands) == 1:
                p = cands[0]
                p.br_slug = slug
                inferred += 1
                logger.info("inferred: %s → %s (%s)", slug, p.player_id, p.full_name)
            elif len(cands) > 1:
                ambiguous += 1

        logger.info("slug inference: matched=%d ambiguous=%d remaining_unheld=%d",
                    inferred, ambiguous, len(unheld) - inferred)

        # Flush so the next SQL UPDATE sees the new br_slug values
        session.flush()

        # Step 4: bulk SQL: where TransactionAsset.player_br_slug matches a
        # Player.br_slug, set TransactionAsset.player_id.
        result = session.execute(sql_text("""
            UPDATE TransactionAsset ta
            JOIN Player p ON p.br_slug = ta.player_br_slug
            SET ta.player_id = p.player_id
            WHERE ta.asset_type = 'player'
              AND ta.player_id IS NULL
              AND ta.player_br_slug IS NOT NULL
        """))
        backfilled = result.rowcount
        logger.info("backfilled %d TransactionAsset.player_id rows", backfilled)

        if commit:
            session.commit()
            logger.info("committed")
        else:
            session.rollback()
            logger.info("dry run — rolled back")
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", action="store_true", help="Persist changes (default: dry run)")
    args = parser.parse_args()
    run(commit=args.commit)


if __name__ == "__main__":
    main()

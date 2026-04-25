"""Create the `career_earnings_total` metric definition.

One row per player, value = sum of per-year cap hits across the
player's NBA career, prefer Spotrac (PlayerContractYear.cap_hit_usd),
fall back to BR (PlayerSalary.salary_usd) — same precedence as the
player-page Career Earnings chip.

Idempotent: running twice updates the existing row in place.

Usage:
    python -m scripts.create_career_earnings_metric                 # create / update def
    python -m scripts.create_career_earnings_metric --run           # also recompute results
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from sqlalchemy.orm import sessionmaker

from db.models import MetricDefinition, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
Session = sessionmaker(bind=engine)


METRIC_KEY = "career_earnings_total"


CODE_PYTHON = '''\
"""Career earnings: sum of per-year cap hits across a player's NBA career.

Per-year value precedence: Spotrac PlayerContractYear.cap_hit_usd >
BR PlayerSalary.salary_usd. Spotrac historical contracts are paywalled
on the public page, so most past years come from BR — which has the
actual paid salary back to the mid-1980s for most players.

This is a career-scope metric: it produces one row per player, with
season="all_regular" so it slots into the existing player-page
Regular-Season Career bucket. The number is the same regardless of
season type — earnings have no regular/playoffs split.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from metrics.framework.base import MetricDefinition, MetricResult
from db.models import PlayerContractYear, PlayerSalary


class CareerEarningsTotal(MetricDefinition):
    key = "career_earnings_total"
    name = "Career Earnings"
    name_zh = "生涯总薪资"
    description = (
        "Total NBA cap hits across a player's career. Prefers Spotrac per-year "
        "cap hit, falls back to Basketball-Reference actual salary. Future "
        "contract years are excluded — only seasons up through the current one."
    )
    description_zh = "球员 NBA 生涯所有赛季工资帽占用总和（Spotrac 优先，BR 兜底，未来合同年份不计入）。"
    scope = "player"
    category = "salary"
    min_sample = 1
    trigger = "season"
    incremental = False
    supports_career = False
    career = True
    rank_order = "desc"
    context_label_template = "{seasons_paid} paid seasons"

    def compute_season(self, session, season):
        # Career-scope only — produce a single row per player under the
        # "all_regular" career bucket and early-return for everything else.
        if season != "all_regular":
            return []

        # NBA seasons start in October. The "current season" is the one
        # whose start year equals this calendar year (Oct–Dec) or last
        # year (Jan–Sept). Anything beyond is a future scheduled year
        # the player hasn't earned yet.
        now = datetime.now()
        current_season = now.year if now.month >= 10 else now.year - 1

        spotrac_rows = (
            session.query(PlayerContractYear.player_id, PlayerContractYear.season,
                          PlayerContractYear.cap_hit_usd)
            .filter(PlayerContractYear.cap_hit_usd.isnot(None))
            .filter(PlayerContractYear.season <= current_season)
            .all()
        )
        spotrac = {(r.player_id, r.season): int(r.cap_hit_usd) for r in spotrac_rows}

        br_rows = (
            session.query(PlayerSalary.player_id, PlayerSalary.season,
                          PlayerSalary.salary_usd)
            .filter(PlayerSalary.season <= current_season)
            .all()
        )
        br = {(r.player_id, r.season): int(r.salary_usd) for r in br_rows}

        # Spotrac wins where both exist.
        merged = {**br, **spotrac}

        per_player = defaultdict(lambda: {"sum": 0, "n": 0})
        for (pid, _season), v in merged.items():
            if v and v > 0:
                per_player[pid]["sum"] += v
                per_player[pid]["n"] += 1

        results = []
        for pid, t in per_player.items():
            usd = t["sum"]
            if usd < 1:
                continue
            n = t["n"]
            # Pretty value_str: $584M / $13.4M / $940K
            if usd >= 1_000_000_000:
                vs = f"${usd/1_000_000_000:.2f}B"
            elif usd >= 10_000_000:
                vs = f"${usd/1_000_000:.0f}M"
            elif usd >= 1_000_000:
                vs = f"${usd/1_000_000:.1f}M"
            elif usd >= 1_000:
                vs = f"${usd/1_000:.0f}K"
            else:
                vs = f"${usd}"
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=str(pid),
                season=season,
                game_id=None,
                value_num=float(usd),
                value_str=vs,
                context={"seasons_paid": n, "rows_counted": n, "usd": usd},
            ))
        return results
'''


def upsert_definition() -> int:
    session = Session()
    try:
        now = datetime.now()
        existing = (
            session.query(MetricDefinition)
            .filter(MetricDefinition.key == METRIC_KEY)
            .first()
        )
        if existing:
            existing.name = "Career Earnings"
            existing.name_zh = "生涯总薪资"
            existing.description = (
                "Total NBA cap hits across a player's career. "
                "Prefers Spotrac per-year cap hit, falls back to "
                "Basketball-Reference actual salary."
            )
            existing.description_zh = "球员 NBA 生涯所有赛季工资帽占用总和。"
            existing.scope = "player"
            existing.category = "salary"
            existing.group_key = "career_earnings"
            existing.source_type = "code"
            existing.status = "published"
            existing.min_sample = 1
            existing.code_python = CODE_PYTHON
            existing.updated_at = now
            mid = existing.id
            logger.info("updated existing metric def id=%d", mid)
        else:
            row = MetricDefinition(
                key=METRIC_KEY,
                family_key=METRIC_KEY,
                variant="career",
                base_metric_key=None,
                managed_family=False,
                name="Career Earnings",
                name_zh="生涯总薪资",
                description=(
                    "Total NBA cap hits across a player's career. "
                    "Prefers Spotrac per-year cap hit, falls back to "
                    "Basketball-Reference actual salary."
                ),
                description_zh="球员 NBA 生涯所有赛季工资帽占用总和。",
                scope="player",
                category="salary",
                group_key="career_earnings",
                source_type="code",
                status="published",
                min_sample=1,
                code_python=CODE_PYTHON,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
            session.flush()
            mid = row.id
            logger.info("created metric def id=%d", mid)
        session.commit()
        return mid
    finally:
        session.close()


def run_metric() -> int:
    """Compute the metric and persist results via the framework's runner."""
    from metrics.framework.runner import run_season_metric
    session = Session()
    try:
        n = run_season_metric(session, METRIC_KEY, season="all_regular")
        logger.info("wrote %d MetricResult rows", n)
        return n
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true",
                        help="Also compute results after upserting the def.")
    args = parser.parse_args()
    upsert_definition()
    if args.run:
        run_metric()


if __name__ == "__main__":
    main()

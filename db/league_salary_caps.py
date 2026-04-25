"""NBA league-wide salary cap thresholds per season.

Public data from NBA Communications and Spotrac. The first apron only became
distinct from the basic tax line under the 2017 CBA; the second apron is new
to the 2023 CBA. Earlier seasons leave those fields as None.

Season is the start year (e.g. 2025 == 2025-26).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CapThresholds:
    season: int
    cap: int
    tax: int
    apron1: int | None = None  # First apron (hard cap trigger pre-2023, restrictions post-2023)
    apron2: int | None = None  # Second apron (only post-2023 CBA)
    minimum_floor: int | None = None  # 90% of cap


_RAW: dict[int, dict] = {
    # 2010s — pre-CBA-2017 era, single tax line, no formal aprons
    2010: {"cap": 58_044_000, "tax": 70_307_000},
    2011: {"cap": 58_044_000, "tax": 70_307_000},
    2012: {"cap": 58_044_000, "tax": 70_307_000},
    2013: {"cap": 58_679_000, "tax": 71_748_000},
    2014: {"cap": 63_065_000, "tax": 76_829_000},
    2015: {"cap": 70_000_000, "tax": 84_740_000},
    # 2017 CBA introduces the apron concept (a "soft cap" 6M above tax line)
    2016: {"cap": 94_143_000, "tax": 113_287_000, "apron1": 119_287_000},
    2017: {"cap": 99_093_000, "tax": 119_266_000, "apron1": 125_266_000},
    2018: {"cap": 101_869_000, "tax": 123_733_000, "apron1": 129_733_000},
    2019: {"cap": 109_140_000, "tax": 132_627_000, "apron1": 138_927_000},
    2020: {"cap": 109_140_000, "tax": 132_627_000, "apron1": 138_927_000},
    2021: {"cap": 112_414_000, "tax": 136_606_000, "apron1": 143_002_000},
    2022: {"cap": 123_655_000, "tax": 150_267_000, "apron1": 156_983_000},
    # 2023 CBA splits into two aprons; first apron now ~ tax+5.7M, second ~ tax+17.5M
    2023: {"cap": 136_021_000, "tax": 165_294_000, "apron1": 172_346_000, "apron2": 182_794_000},
    2024: {"cap": 140_588_000, "tax": 170_814_000, "apron1": 178_132_000, "apron2": 188_931_000},
    2025: {"cap": 154_647_000, "tax": 187_895_000, "apron1": 195_945_000, "apron2": 207_824_000},
    # 2026+ are projections published in summer 2026; refine as official numbers land
    2026: {"cap": 165_300_000, "tax": 200_945_000, "apron1": 209_577_000, "apron2": 222_273_000},
    2027: {"cap": 176_900_000, "tax": 215_010_000, "apron1": 224_239_000, "apron2": 237_832_000},
}


def get_thresholds(season: int | None) -> CapThresholds | None:
    if season is None:
        return None
    raw = _RAW.get(int(season))
    if raw is None:
        return None
    cap = raw["cap"]
    return CapThresholds(
        season=int(season),
        cap=cap,
        tax=raw["tax"],
        apron1=raw.get("apron1"),
        apron2=raw.get("apron2"),
        minimum_floor=int(cap * 0.9),
    )

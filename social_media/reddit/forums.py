from __future__ import annotations

# Canonical subreddit name for each NBA team.
# Key = lowercase team common name, value = exact Reddit subreddit name.
NBA_TEAM_SUBREDDITS: dict[str, str] = {
    "hawks": "AtlantaHawks",
    "celtics": "bostonceltics",
    "nets": "GoNets",
    "hornets": "CharlotteHornets",
    "bulls": "chicagobulls",
    "cavaliers": "clevelandcavs",
    "mavericks": "Mavericks",
    "nuggets": "denvernuggets",
    "pistons": "DetroitPistons",
    "warriors": "warriors",
    "rockets": "rockets",
    "pacers": "pacers",
    "clippers": "LAClippers",
    "lakers": "lakers",
    "grizzlies": "memphisgrizzlies",
    "heat": "heat",
    "bucks": "MkeBucks",
    "timberwolves": "timberwolves",
    "pelicans": "NOLAPelicans",
    "knicks": "NYKnicks",
    "thunder": "Thunder",
    "magic": "OrlandoMagic",
    "76ers": "sixers",
    "suns": "suns",
    "blazers": "ripcity",
    "kings": "kings",
    "spurs": "NBASpurs",
    "raptors": "torontoraptors",
    "jazz": "UtahJazz",
    "wizards": "washingtonwizards",
}

# Extended alias map: team abbreviations, city names, alternate names → canonical subreddit.
_ALIAS_TO_SUBREDDIT: dict[str, str] = {
    # Hawks
    "hawks": "AtlantaHawks",
    "atl": "AtlantaHawks",
    "atlanta": "AtlantaHawks",
    "atlantahawks": "AtlantaHawks",
    # Celtics
    "celtics": "bostonceltics",
    "bos": "bostonceltics",
    "boston": "bostonceltics",
    "bostonceltics": "bostonceltics",
    # Nets
    "nets": "GoNets",
    "bkn": "GoNets",
    "brooklyn": "GoNets",
    "gonets": "GoNets",
    # Hornets
    "hornets": "CharlotteHornets",
    "cha": "CharlotteHornets",
    "charlotte": "CharlotteHornets",
    "charlottehornets": "CharlotteHornets",
    # Bulls
    "bulls": "chicagobulls",
    "chi": "chicagobulls",
    "chicago": "chicagobulls",
    "chicagobulls": "chicagobulls",
    # Cavaliers
    "cavaliers": "clevelandcavs",
    "cavs": "clevelandcavs",
    "cle": "clevelandcavs",
    "cleveland": "clevelandcavs",
    "clevelandcavs": "clevelandcavs",
    # Mavericks
    "mavericks": "Mavericks",
    "mavs": "Mavericks",
    "dal": "Mavericks",
    "dallas": "Mavericks",
    # Nuggets
    "nuggets": "denvernuggets",
    "den": "denvernuggets",
    "denver": "denvernuggets",
    "denvernuggets": "denvernuggets",
    # Pistons
    "pistons": "DetroitPistons",
    "det": "DetroitPistons",
    "detroit": "DetroitPistons",
    "detroitpistons": "DetroitPistons",
    # Warriors
    "warriors": "warriors",
    "gsw": "warriors",
    "golden state": "warriors",
    # Rockets
    "rockets": "rockets",
    "hou": "rockets",
    "houston": "rockets",
    # Pacers
    "pacers": "pacers",
    "ind": "pacers",
    "indiana": "pacers",
    # Clippers
    "clippers": "LAClippers",
    "clips": "LAClippers",
    "lac": "LAClippers",
    "laclippers": "LAClippers",
    # Lakers
    "lakers": "lakers",
    "lal": "lakers",
    # Grizzlies
    "grizzlies": "memphisgrizzlies",
    "mem": "memphisgrizzlies",
    "memphis": "memphisgrizzlies",
    "memphisgrizzlies": "memphisgrizzlies",
    # Heat
    "heat": "heat",
    "mia": "heat",
    "miami": "heat",
    # Bucks
    "bucks": "MkeBucks",
    "mil": "MkeBucks",
    "milwaukee": "MkeBucks",
    "mkebucks": "MkeBucks",
    # Timberwolves
    "timberwolves": "timberwolves",
    "wolves": "timberwolves",
    "min": "timberwolves",
    "minnesota": "timberwolves",
    # Pelicans
    "pelicans": "NOLAPelicans",
    "pels": "NOLAPelicans",
    "nop": "NOLAPelicans",
    "new orleans": "NOLAPelicans",
    "nolapelicans": "NOLAPelicans",
    # Knicks
    "knicks": "NYKnicks",
    "nyk": "NYKnicks",
    "new york": "NYKnicks",
    "nyknicks": "NYKnicks",
    # Thunder
    "thunder": "Thunder",
    "okc": "Thunder",
    "oklahoma city": "Thunder",
    # Magic
    "magic": "OrlandoMagic",
    "orl": "OrlandoMagic",
    "orlando": "OrlandoMagic",
    "orlandomagic": "OrlandoMagic",
    # 76ers
    "76ers": "sixers",
    "sixers": "sixers",
    "phi": "sixers",
    "philly": "sixers",
    "philadelphia": "sixers",
    # Suns
    "suns": "suns",
    "phx": "suns",
    "phoenix": "suns",
    # Trail Blazers
    "blazers": "ripcity",
    "trail blazers": "ripcity",
    "por": "ripcity",
    "portland": "ripcity",
    "ripcity": "ripcity",
    # Kings
    "kings": "kings",
    "sac": "kings",
    "sacramento": "kings",
    # Spurs
    "spurs": "NBASpurs",
    "sas": "NBASpurs",
    "san antonio": "NBASpurs",
    "nbaspurs": "NBASpurs",
    # Raptors
    "raptors": "torontoraptors",
    "tor": "torontoraptors",
    "toronto": "torontoraptors",
    "torontoraptors": "torontoraptors",
    # Jazz
    "jazz": "UtahJazz",
    "uta": "UtahJazz",
    "utah": "UtahJazz",
    "utahjazz": "UtahJazz",
    # Wizards
    "wizards": "washingtonwizards",
    "was": "washingtonwizards",
    "washington": "washingtonwizards",
    "washingtonwizards": "washingtonwizards",
    # General
    "nba": "nba",
    "r/nba": "nba",
}


def normalize_reddit_subreddit(forum: str | None) -> str | None:
    """Normalize Reddit forum inputs into canonical subreddit names.

    Accepts team common names, abbreviations, city names, or raw subreddit
    names (with or without r/ prefix). Returns the canonical subreddit name
    or the original input stripped of r/ prefix if not found in the alias map.
    """
    if forum is None:
        return None
    raw = forum.strip()
    if not raw:
        return None
    # Strip r/ prefix
    import re
    cleaned = re.sub(r"^/+", "", raw)
    cleaned = re.sub(r"^(?i:r/)", "", cleaned).strip("/")
    if not cleaned:
        return None
    # Look up in alias map (case-insensitive)
    canonical = _ALIAS_TO_SUBREDDIT.get(cleaned.lower())
    if canonical:
        return canonical
    # Not in map — return as-is (Reddit will validate at post time)
    return cleaned

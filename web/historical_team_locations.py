"""Complete city/name history for every NBA franchise.

Each entry represents one "era" for a franchise — a continuous span where
the team was in the same city under the same name. Teams that never moved
and never rebranded have a single entry. Teams with relocations or rebrands
have multiple consecutive entries.

Year semantics:
- `year_start` / `year_end` are the season START years. `1948` means the
  1948-49 season. `year_end` is inclusive — the last season in that era.
  `year_end = None` means "current era, still active".
- Era boundaries are adjacent: the season after one era's `year_end` is the
  next era's `year_start`.

Coordinates are the historical city center, suitable for map rendering.
Same-metro moves (Warriors Oakland↔SF, Nets intra-NJ, Cavs Richfield↔Cleveland)
are merged into a single entry. One-season transitional names (Capital
Bullets, Texas Chaparrals) and pre-NBL / obscure earliest seasons (Detroit
Gems) are omitted.

Pre-NBA history:
- Franchises that came from the NBL (1937-1949) or ABA (1967-1976) are
  captured back to their founding year, with a note. NBA was founded in
  1946 as BAA, merged with NBL to form NBA in 1949, absorbed 4 ABA teams
  in 1976.

Schema:
    team_id:    current NBA franchise ID
    franchise:  current short name (e.g. "Hawks")
    era_name:   team name during this era (e.g. "Tri-Cities Blackhawks")
    city:       city during this era
    state:      state / province / country
    year_start: season start year, inclusive
    year_end:   last season start year (inclusive); None = current
    lat, lon:   historical city center
    note:       optional free-form caveat
"""

FRANCHISE_HISTORY = [
    # ────────────────────────────────────────────────────────────────────
    # Atlanta Hawks (1610612737)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612737",
        "franchise": "Hawks",
        "era_name": "Tri-Cities Blackhawks",
        "abbr":     "TRI",
        "city": "Moline",
        "state": "IL",
        "year_start": 1946,
        "year_end": 1950,
        "lat": 41.5067,
        "lon": -90.5151,
        "note": "Played in the NBL (1946-49) then BAA/NBA (1949-51). 'Tri-Cities' refers to Moline, Rock Island, and Davenport.",
    },
    {
        "team_id": "1610612737",
        "franchise": "Hawks",
        "era_name": "Milwaukee Hawks",
        "abbr":     "MLH",
        "city": "Milwaukee",
        "state": "WI",
        "year_start": 1951,
        "year_end": 1954,
        "lat": 43.0389,
        "lon": -87.9065,
    },
    {
        "team_id": "1610612737",
        "franchise": "Hawks",
        "era_name": "St. Louis Hawks",
        "abbr":     "STL",
        "city": "St. Louis",
        "state": "MO",
        "year_start": 1955,
        "year_end": 1967,
        "lat": 38.6270,
        "lon": -90.1994,
    },
    {
        "team_id": "1610612737",
        "franchise": "Hawks",
        "era_name": "Atlanta Hawks",
        "abbr":     "ATL",
        "city": "Atlanta",
        "state": "GA",
        "year_start": 1968,
        "year_end": None,
        "lat": 33.7490,
        "lon": -84.3880,
    },

    # ────────────────────────────────────────────────────────────────────
    # Boston Celtics (1610612738)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612738",
        "franchise": "Celtics",
        "era_name": "Boston Celtics",
        "abbr":     "BOS",
        "city": "Boston",
        "state": "MA",
        "year_start": 1946,
        "year_end": None,
        "lat": 42.3601,
        "lon": -71.0589,
    },

    # ────────────────────────────────────────────────────────────────────
    # Brooklyn Nets (1610612751)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612751",
        "franchise": "Nets",
        "era_name": "New Jersey Americans",
        "abbr":     "NJA",
        "city": "Teaneck",
        "state": "NJ",
        "year_start": 1967,
        "year_end": 1967,
        "lat": 40.8976,
        "lon": -74.0121,
        "note": "Single season in the ABA before moving to Long Island.",
    },
    {
        "team_id": "1610612751",
        "franchise": "Nets",
        "era_name": "New York Nets",
        "abbr":     "NYN",
        "city": "Long Island (Uniondale)",
        "state": "NY",
        "year_start": 1968,
        "year_end": 1976,
        "lat": 40.7229,
        "lon": -73.5907,
        "note": "ABA era; joined the NBA via the 1976 merger. Played in Commack / Island Garden / Nassau Coliseum over this span.",
    },
    {
        "team_id": "1610612751",
        "franchise": "Nets",
        "era_name": "New Jersey Nets",
        "abbr":     "NJN",
        "city": "East Rutherford",
        "state": "NJ",
        "year_start": 1977,
        "year_end": 2011,
        "lat": 40.8135,
        "lon": -74.0744,
        "note": "Played in Piscataway (1977-81), East Rutherford / Meadowlands (1981-2010), and Newark (2010-12).",
    },
    {
        "team_id": "1610612751",
        "franchise": "Nets",
        "era_name": "Brooklyn Nets",
        "abbr":     "BKN",
        "city": "Brooklyn",
        "state": "NY",
        "year_start": 2012,
        "year_end": None,
        "lat": 40.6782,
        "lon": -73.9442,
    },

    # ────────────────────────────────────────────────────────────────────
    # Charlotte Hornets (1610612766)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612766",
        "franchise": "Hornets",
        "era_name": "Charlotte Bobcats",
        "abbr":     "CHA",
        "city": "Charlotte",
        "state": "NC",
        "year_start": 2004,
        "year_end": 2013,
        "lat": 35.2271,
        "lon": -80.8431,
        "note": "Expansion franchise added in 2004. Reclaimed the 'Hornets' name and the 1988-2002 Charlotte Hornets records in 2014.",
    },
    {
        "team_id": "1610612766",
        "franchise": "Hornets",
        "era_name": "Charlotte Hornets",
        "abbr":     "CHA",
        "city": "Charlotte",
        "state": "NC",
        "year_start": 2014,
        "year_end": None,
        "lat": 35.2271,
        "lon": -80.8431,
    },

    # ────────────────────────────────────────────────────────────────────
    # Chicago Bulls (1610612741)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612741",
        "franchise": "Bulls",
        "era_name": "Chicago Bulls",
        "abbr":     "CHI",
        "city": "Chicago",
        "state": "IL",
        "year_start": 1966,
        "year_end": None,
        "lat": 41.8781,
        "lon": -87.6298,
    },

    # ────────────────────────────────────────────────────────────────────
    # Cleveland Cavaliers (1610612739)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612739",
        "franchise": "Cavaliers",
        "era_name": "Cleveland Cavaliers",
        "abbr":     "CLE",
        "city": "Cleveland",
        "state": "OH",
        "year_start": 1970,
        "year_end": None,
        "lat": 41.4993,
        "lon": -81.6944,
        "note": "Played home games at the Coliseum in Richfield Township (1974-94) — same metro.",
    },

    # ────────────────────────────────────────────────────────────────────
    # Dallas Mavericks (1610612742)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612742",
        "franchise": "Mavericks",
        "era_name": "Dallas Mavericks",
        "abbr":     "DAL",
        "city": "Dallas",
        "state": "TX",
        "year_start": 1980,
        "year_end": None,
        "lat": 32.7767,
        "lon": -96.7970,
    },

    # ────────────────────────────────────────────────────────────────────
    # Denver Nuggets (1610612743)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612743",
        "franchise": "Nuggets",
        "era_name": "Denver Rockets",
        "abbr":     "DNR",
        "city": "Denver",
        "state": "CO",
        "year_start": 1967,
        "year_end": 1973,
        "lat": 39.7392,
        "lon": -104.9903,
        "note": "ABA era. Renamed 'Nuggets' in 1974 to avoid conflict with the Houston Rockets ahead of the NBA merger.",
    },
    {
        "team_id": "1610612743",
        "franchise": "Nuggets",
        "era_name": "Denver Nuggets",
        "abbr":     "DEN",
        "city": "Denver",
        "state": "CO",
        "year_start": 1974,
        "year_end": None,
        "lat": 39.7392,
        "lon": -104.9903,
    },

    # ────────────────────────────────────────────────────────────────────
    # Detroit Pistons (1610612765)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612765",
        "franchise": "Pistons",
        "era_name": "Fort Wayne Pistons",
        "abbr":     "FTW",
        "city": "Fort Wayne",
        "state": "IN",
        "year_start": 1948,
        "year_end": 1956,
        "lat": 41.0793,
        "lon": -85.1394,
        "note": "Pre-NBA NBL era 1941-48 omitted. 'Zollner Pistons' was the full name early on (owned by Fred Zollner).",
    },
    {
        "team_id": "1610612765",
        "franchise": "Pistons",
        "era_name": "Detroit Pistons",
        "abbr":     "DET",
        "city": "Detroit",
        "state": "MI",
        "year_start": 1957,
        "year_end": None,
        "lat": 42.3314,
        "lon": -83.0458,
    },

    # ────────────────────────────────────────────────────────────────────
    # Golden State Warriors (1610612744)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612744",
        "franchise": "Warriors",
        "era_name": "Philadelphia Warriors",
        "abbr":     "PHW",
        "city": "Philadelphia",
        "state": "PA",
        "year_start": 1946,
        "year_end": 1961,
        "lat": 39.9526,
        "lon": -75.1652,
    },
    {
        "team_id": "1610612744",
        "franchise": "Warriors",
        "era_name": "San Francisco Warriors",
        "abbr":     "SFW",
        "city": "San Francisco",
        "state": "CA",
        "year_start": 1962,
        "year_end": 1970,
        "lat": 37.7749,
        "lon": -122.4194,
        "note": "Played in multiple Bay Area venues including Cow Palace (Daly City) and SF Civic Auditorium.",
    },
    {
        "team_id": "1610612744",
        "franchise": "Warriors",
        "era_name": "Golden State Warriors",
        "abbr":     "GSW",
        "city": "Oakland / San Francisco",
        "state": "CA",
        "year_start": 1971,
        "year_end": None,
        "lat": 37.7749,
        "lon": -122.4194,
        "note": "Home arena at Oakland Arena / Oracle Arena 1971-2019, Chase Center (SF) 2019-present. Name 'Golden State' adopted to appeal to all of California.",
    },

    # ────────────────────────────────────────────────────────────────────
    # Houston Rockets (1610612745)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612745",
        "franchise": "Rockets",
        "era_name": "San Diego Rockets",
        "abbr":     "SDR",
        "city": "San Diego",
        "state": "CA",
        "year_start": 1967,
        "year_end": 1970,
        "lat": 32.7157,
        "lon": -117.1611,
    },
    {
        "team_id": "1610612745",
        "franchise": "Rockets",
        "era_name": "Houston Rockets",
        "abbr":     "HOU",
        "city": "Houston",
        "state": "TX",
        "year_start": 1971,
        "year_end": None,
        "lat": 29.7604,
        "lon": -95.3698,
    },

    # ────────────────────────────────────────────────────────────────────
    # Indiana Pacers (1610612754)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612754",
        "franchise": "Pacers",
        "era_name": "Indiana Pacers",
        "abbr":     "IND",
        "city": "Indianapolis",
        "state": "IN",
        "year_start": 1967,
        "year_end": None,
        "lat": 39.7684,
        "lon": -86.1581,
        "note": "ABA charter franchise 1967-76, joined the NBA via the 1976 merger.",
    },

    # ────────────────────────────────────────────────────────────────────
    # Los Angeles Clippers (1610612746)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612746",
        "franchise": "Clippers",
        "era_name": "Buffalo Braves",
        "abbr":     "BUF",
        "city": "Buffalo",
        "state": "NY",
        "year_start": 1970,
        "year_end": 1977,
        "lat": 42.8864,
        "lon": -78.8784,
    },
    {
        "team_id": "1610612746",
        "franchise": "Clippers",
        "era_name": "San Diego Clippers",
        "abbr":     "SDC",
        "city": "San Diego",
        "state": "CA",
        "year_start": 1978,
        "year_end": 1983,
        "lat": 32.7157,
        "lon": -117.1611,
    },
    {
        "team_id": "1610612746",
        "franchise": "Clippers",
        "era_name": "Los Angeles Clippers",
        "abbr":     "LAC",
        "city": "Los Angeles",
        "state": "CA",
        "year_start": 1984,
        "year_end": None,
        "lat": 34.0522,
        "lon": -118.2437,
    },

    # ────────────────────────────────────────────────────────────────────
    # Los Angeles Lakers (1610612747)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612747",
        "franchise": "Lakers",
        "era_name": "Minneapolis Lakers",
        "abbr":     "MPL",
        "city": "Minneapolis",
        "state": "MN",
        "year_start": 1947,
        "year_end": 1959,
        "lat": 44.9778,
        "lon": -93.2650,
        "note": "Founded as Detroit Gems in NBL 1946; moved to Minneapolis for 1947-48. Joined BAA/NBA in 1948.",
    },
    {
        "team_id": "1610612747",
        "franchise": "Lakers",
        "era_name": "Los Angeles Lakers",
        "abbr":     "LAL",
        "city": "Los Angeles",
        "state": "CA",
        "year_start": 1960,
        "year_end": None,
        "lat": 34.0522,
        "lon": -118.2437,
    },

    # ────────────────────────────────────────────────────────────────────
    # Memphis Grizzlies (1610612763)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612763",
        "franchise": "Grizzlies",
        "era_name": "Vancouver Grizzlies",
        "abbr":     "VAN",
        "city": "Vancouver",
        "state": "BC",
        "year_start": 1995,
        "year_end": 2000,
        "lat": 49.2827,
        "lon": -123.1207,
    },
    {
        "team_id": "1610612763",
        "franchise": "Grizzlies",
        "era_name": "Memphis Grizzlies",
        "abbr":     "MEM",
        "city": "Memphis",
        "state": "TN",
        "year_start": 2001,
        "year_end": None,
        "lat": 35.1495,
        "lon": -90.0490,
    },

    # ────────────────────────────────────────────────────────────────────
    # Miami Heat (1610612748)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612748",
        "franchise": "Heat",
        "era_name": "Miami Heat",
        "abbr":     "MIA",
        "city": "Miami",
        "state": "FL",
        "year_start": 1988,
        "year_end": None,
        "lat": 25.7617,
        "lon": -80.1918,
    },

    # ────────────────────────────────────────────────────────────────────
    # Milwaukee Bucks (1610612749)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612749",
        "franchise": "Bucks",
        "era_name": "Milwaukee Bucks",
        "abbr":     "MIL",
        "city": "Milwaukee",
        "state": "WI",
        "year_start": 1968,
        "year_end": None,
        "lat": 43.0389,
        "lon": -87.9065,
    },

    # ────────────────────────────────────────────────────────────────────
    # Minnesota Timberwolves (1610612750)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612750",
        "franchise": "Timberwolves",
        "era_name": "Minnesota Timberwolves",
        "abbr":     "MIN",
        "city": "Minneapolis",
        "state": "MN",
        "year_start": 1989,
        "year_end": None,
        "lat": 44.9778,
        "lon": -93.2650,
    },

    # ────────────────────────────────────────────────────────────────────
    # New Orleans Pelicans (1610612740)
    # ────────────────────────────────────────────────────────────────────
    # Franchise physical continuity: the 1988-2002 Charlotte Hornets moved
    # to New Orleans in 2002. The 'Hornets' name and 1988-2002 records were
    # later transferred to the current Charlotte franchise in 2014. From a
    # franchise-movement perspective these eras belong to the Pelicans.
    {
        "team_id": "1610612740",
        "franchise": "Pelicans",
        "era_name": "Charlotte Hornets",
        "abbr":     "CHH",
        "city": "Charlotte",
        "state": "NC",
        "year_start": 1988,
        "year_end": 2001,
        "lat": 35.2271,
        "lon": -80.8431,
        "note": "Original 1988-2002 Charlotte Hornets. Franchise continuity is with the current New Orleans Pelicans; NBA officially transferred the name and records to the new Charlotte franchise in 2014.",
    },
    {
        "team_id": "1610612740",
        "franchise": "Pelicans",
        "era_name": "New Orleans Hornets",
        "abbr":     "NOH",
        "city": "New Orleans",
        "state": "LA",
        "year_start": 2002,
        "year_end": 2012,
        "lat": 29.9511,
        "lon": -90.0715,
        "note": "Played most home games in Oklahoma City 2005-07 as 'New Orleans/Oklahoma City Hornets' due to Hurricane Katrina.",
    },
    {
        "team_id": "1610612740",
        "franchise": "Pelicans",
        "era_name": "New Orleans Pelicans",
        "abbr":     "NOP",
        "city": "New Orleans",
        "state": "LA",
        "year_start": 2013,
        "year_end": None,
        "lat": 29.9511,
        "lon": -90.0715,
    },

    # ────────────────────────────────────────────────────────────────────
    # New York Knicks (1610612752)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612752",
        "franchise": "Knicks",
        "era_name": "New York Knicks",
        "abbr":     "NYK",
        "city": "New York",
        "state": "NY",
        "year_start": 1946,
        "year_end": None,
        "lat": 40.7128,
        "lon": -74.0060,
        "note": "Full name 'New York Knickerbockers', colloquially shortened to Knicks.",
    },

    # ────────────────────────────────────────────────────────────────────
    # Oklahoma City Thunder (1610612760)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612760",
        "franchise": "Thunder",
        "era_name": "Seattle SuperSonics",
        "abbr":     "SEA",
        "city": "Seattle",
        "state": "WA",
        "year_start": 1967,
        "year_end": 2007,
        "lat": 47.6062,
        "lon": -122.3321,
    },
    {
        "team_id": "1610612760",
        "franchise": "Thunder",
        "era_name": "Oklahoma City Thunder",
        "abbr":     "OKC",
        "city": "Oklahoma City",
        "state": "OK",
        "year_start": 2008,
        "year_end": None,
        "lat": 35.4676,
        "lon": -97.5164,
    },

    # ────────────────────────────────────────────────────────────────────
    # Orlando Magic (1610612753)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612753",
        "franchise": "Magic",
        "era_name": "Orlando Magic",
        "abbr":     "ORL",
        "city": "Orlando",
        "state": "FL",
        "year_start": 1989,
        "year_end": None,
        "lat": 28.5383,
        "lon": -81.3792,
    },

    # ────────────────────────────────────────────────────────────────────
    # Philadelphia 76ers (1610612755)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612755",
        "franchise": "76ers",
        "era_name": "Syracuse Nationals",
        "abbr":     "SYR",
        "city": "Syracuse",
        "state": "NY",
        "year_start": 1946,
        "year_end": 1962,
        "lat": 43.0481,
        "lon": -76.1474,
        "note": "NBL 1946-49, NBA 1949-63. Nicknamed 'Nats'.",
    },
    {
        "team_id": "1610612755",
        "franchise": "76ers",
        "era_name": "Philadelphia 76ers",
        "abbr":     "PHI",
        "city": "Philadelphia",
        "state": "PA",
        "year_start": 1963,
        "year_end": None,
        "lat": 39.9526,
        "lon": -75.1652,
    },

    # ────────────────────────────────────────────────────────────────────
    # Phoenix Suns (1610612756)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612756",
        "franchise": "Suns",
        "era_name": "Phoenix Suns",
        "abbr":     "PHX",
        "city": "Phoenix",
        "state": "AZ",
        "year_start": 1968,
        "year_end": None,
        "lat": 33.4484,
        "lon": -112.0740,
    },

    # ────────────────────────────────────────────────────────────────────
    # Portland Trail Blazers (1610612757)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612757",
        "franchise": "Trail Blazers",
        "era_name": "Portland Trail Blazers",
        "abbr":     "POR",
        "city": "Portland",
        "state": "OR",
        "year_start": 1970,
        "year_end": None,
        "lat": 45.5152,
        "lon": -122.6784,
    },

    # ────────────────────────────────────────────────────────────────────
    # Sacramento Kings (1610612758)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612758",
        "franchise": "Kings",
        "era_name": "Rochester Royals",
        "abbr":     "ROC",
        "city": "Rochester",
        "state": "NY",
        "year_start": 1945,
        "year_end": 1956,
        "lat": 43.1566,
        "lon": -77.6088,
        "note": "NBL 1945-48, BAA/NBA 1948-57.",
    },
    {
        "team_id": "1610612758",
        "franchise": "Kings",
        "era_name": "Cincinnati Royals",
        "abbr":     "CIN",
        "city": "Cincinnati",
        "state": "OH",
        "year_start": 1957,
        "year_end": 1971,
        "lat": 39.1031,
        "lon": -84.5120,
    },
    {
        "team_id": "1610612758",
        "franchise": "Kings",
        "era_name": "Kansas City-Omaha Kings",
        "abbr":     "KCO",
        "city": "Kansas City",
        "state": "MO",
        "year_start": 1972,
        "year_end": 1974,
        "lat": 39.0997,
        "lon": -94.5786,
        "note": "Split home games between Kansas City, MO and Omaha, NE during these three seasons.",
    },
    {
        "team_id": "1610612758",
        "franchise": "Kings",
        "era_name": "Kansas City Kings",
        "abbr":     "KCK",
        "city": "Kansas City",
        "state": "MO",
        "year_start": 1975,
        "year_end": 1984,
        "lat": 39.0997,
        "lon": -94.5786,
    },
    {
        "team_id": "1610612758",
        "franchise": "Kings",
        "era_name": "Sacramento Kings",
        "abbr":     "SAC",
        "city": "Sacramento",
        "state": "CA",
        "year_start": 1985,
        "year_end": None,
        "lat": 38.5816,
        "lon": -121.4944,
    },

    # ────────────────────────────────────────────────────────────────────
    # San Antonio Spurs (1610612759)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612759",
        "franchise": "Spurs",
        "era_name": "Dallas Chaparrals",
        "abbr":     "DLC",
        "city": "Dallas",
        "state": "TX",
        "year_start": 1967,
        "year_end": 1972,
        "lat": 32.7767,
        "lon": -96.7970,
        "note": "ABA era. One 1970-71 season played under the 'Texas Chaparrals' name with home games split across Dallas / Fort Worth / Lubbock (omitted here for simplicity).",
    },
    {
        "team_id": "1610612759",
        "franchise": "Spurs",
        "era_name": "San Antonio Spurs",
        "abbr":     "SAS",
        "city": "San Antonio",
        "state": "TX",
        "year_start": 1973,
        "year_end": None,
        "lat": 29.4241,
        "lon": -98.4936,
        "note": "Joined NBA via the 1976 ABA merger.",
    },

    # ────────────────────────────────────────────────────────────────────
    # Toronto Raptors (1610612761)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612761",
        "franchise": "Raptors",
        "era_name": "Toronto Raptors",
        "abbr":     "TOR",
        "city": "Toronto",
        "state": "ON",
        "year_start": 1995,
        "year_end": None,
        "lat": 43.6532,
        "lon": -79.3832,
    },

    # ────────────────────────────────────────────────────────────────────
    # Utah Jazz (1610612762)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612762",
        "franchise": "Jazz",
        "era_name": "New Orleans Jazz",
        "abbr":     "NOJ",
        "city": "New Orleans",
        "state": "LA",
        "year_start": 1974,
        "year_end": 1978,
        "lat": 29.9511,
        "lon": -90.0715,
    },
    {
        "team_id": "1610612762",
        "franchise": "Jazz",
        "era_name": "Utah Jazz",
        "abbr":     "UTA",
        "city": "Salt Lake City",
        "state": "UT",
        "year_start": 1979,
        "year_end": None,
        "lat": 40.7608,
        "lon": -111.8910,
    },

    # ────────────────────────────────────────────────────────────────────
    # Washington Wizards (1610612764)
    # ────────────────────────────────────────────────────────────────────
    {
        "team_id": "1610612764",
        "franchise": "Wizards",
        "era_name": "Chicago Packers",
        "abbr":     "CHP",
        "city": "Chicago",
        "state": "IL",
        "year_start": 1961,
        "year_end": 1961,
        "lat": 41.8781,
        "lon": -87.6298,
        "note": "Expansion franchise, single season under this name.",
    },
    {
        "team_id": "1610612764",
        "franchise": "Wizards",
        "era_name": "Chicago Zephyrs",
        "abbr":     "CHZ",
        "city": "Chicago",
        "state": "IL",
        "year_start": 1962,
        "year_end": 1962,
        "lat": 41.8781,
        "lon": -87.6298,
        "note": "Single season name between Packers and the move to Baltimore.",
    },
    {
        "team_id": "1610612764",
        "franchise": "Wizards",
        "era_name": "Baltimore Bullets",
        "abbr":     "BAL",
        "city": "Baltimore",
        "state": "MD",
        "year_start": 1963,
        "year_end": 1972,
        "lat": 39.2904,
        "lon": -76.6122,
        "note": "Not to be confused with the unrelated original 1947-55 Baltimore Bullets franchise.",
    },
    {
        "team_id": "1610612764",
        "franchise": "Wizards",
        "era_name": "Washington Bullets",
        "abbr":     "WSB",
        "city": "Washington",
        "state": "DC",
        "year_start": 1973,
        "year_end": 1996,
        "lat": 38.9072,
        "lon": -77.0369,
        "note": "Played as 'Capital Bullets' for 1973-74 only (team was technically based in Landover, MD).",
    },
    {
        "team_id": "1610612764",
        "franchise": "Wizards",
        "era_name": "Washington Wizards",
        "abbr":     "WAS",
        "city": "Washington",
        "state": "DC",
        "year_start": 1997,
        "year_end": None,
        "lat": 38.9072,
        "lon": -77.0369,
    },
]



# ─────────────────────────────────────────────────────────────────────────
# FRANCHISE_LOGOS loaded from web/data/team_logos.json
# ─────────────────────────────────────────────────────────────────────────
# The logo registry is kept in JSON rather than Python source so that the
# monthly `refresh_current_team_logos` Celery task can mutate it safely
# (atomic file rename) without rewriting this module. FRANCHISE_HISTORY
# above stays in Python because it changes only on manual franchise
# relocations, not on rebrand-detection.
#
# Schema:
#   [{"team_id": str, "year_start": int, "year_end": int | None, "path": str}, ...]
# `year_end = null` is the sentinel for the still-current era.

import json as _json
import os as _os

_LOGO_JSON_PATH = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data", "team_logos.json")

with open(_LOGO_JSON_PATH, encoding="utf-8") as _fh:
    FRANCHISE_LOGOS = _json.load(_fh)



# ─────────────────────────────────────────────────────────────────────────
# Query helpers
# ─────────────────────────────────────────────────────────────────────────

from functools import lru_cache
from typing import Optional

_CURRENT_LOGO_CDN = "https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"


@lru_cache(maxsize=1)
def _history_by_team() -> dict:
    out: dict[str, list] = {}
    for era in FRANCHISE_HISTORY:
        out.setdefault(era["team_id"], []).append(era)
    for eras in out.values():
        eras.sort(key=lambda e: e["year_start"])
    return out


_Y_INF = 10**6  # sentinel for "None year_end" when sorting


@lru_cache(maxsize=1)
def _logos_by_team() -> dict:
    out: dict[str, list] = {}
    for entry in FRANCHISE_LOGOS:
        out.setdefault(entry["team_id"], []).append(entry)
    for entries in out.values():
        # Order by year_start ascending. Secondary sort by year_end descending
        # with None treated as the largest value so the open-ended current-era
        # entry comes last for the same year_start.
        entries.sort(key=lambda e: (e["year_start"], -(e["year_end"] or _Y_INF)))
    return out


def get_era_for_year(team_id: str, year: int) -> Optional[dict]:
    """Return the FRANCHISE_HISTORY entry covering `year` for `team_id`, or None.

    `year` uses the same season-start convention as the data file (2024 = 2024-25).
    """
    for era in _history_by_team().get(team_id, []):
        if era["year_start"] > year:
            break
        if era["year_end"] is None or era["year_end"] >= year:
            return era
    return None


def _logo_covers_year(entry: dict, year: int) -> bool:
    if entry["year_start"] > year:
        return False
    if entry["year_end"] is None:
        return True
    return entry["year_end"] >= year


def get_logo_for_year(team_id: str, year: int) -> Optional[dict]:
    """Return the best FRANCHISE_LOGOS entry covering `year` for `team_id`, or None.

    When multiple entries cover the same year (alternate / primary variants),
    the one with the latest `year_start` wins — this favors the most
    specific / most recent alternate.
    """
    candidates = [
        e for e in _logos_by_team().get(team_id, [])
        if _logo_covers_year(e, year)
    ]
    if not candidates:
        return None
    # Prefer the latest year_start (most specific to `year`), breaking ties by
    # shortest span (most specific). Open-ended entries (year_end=None) are
    # treated as having the widest span so they lose the tie.
    def span(e):
        end = e["year_end"] if e["year_end"] is not None else _Y_INF
        return end - e["year_start"]
    candidates.sort(key=lambda e: (-e["year_start"], span(e)))
    return candidates[0]


def get_current_logo(team_id: str) -> Optional[dict]:
    """Return the team's current-era FRANCHISE_LOGOS entry (year_end=None), or None."""
    for entry in _logos_by_team().get(team_id, []):
        if entry["year_end"] is None:
            return entry
    return None


def get_nearest_logo(team_id: str, year: int) -> Optional[dict]:
    """Return the logo entry closest in year for `team_id`, regardless of overlap.

    Distance is the minimum year gap to the entry's range; the entry with the
    smallest gap wins (ties broken by preferring an earlier-starting entry).
    Open-ended current-era entries are treated as extending to year 9999 for
    the purpose of gap calculation.
    """
    entries = _logos_by_team().get(team_id, [])
    if not entries:
        return None

    def gap(e):
        end = e["year_end"] if e["year_end"] is not None else 9999
        if year < e["year_start"]:
            return (e["year_start"] - year, 0)
        if year > end:
            return (year - end, 1)
        return (0, 0)

    return min(entries, key=gap)


def get_era_name_for_year(team_id: str, year: int | None) -> Optional[str]:
    """Return the era-appropriate full team name (e.g. 'Seattle SuperSonics')
    for `(team_id, year)`, or None if no era covers that year.
    """
    if year is None:
        return None
    era = get_era_for_year(team_id, year)
    if era:
        return era.get("era_name")
    return None


def get_era_abbr_for_year(team_id: str, year: int | None) -> Optional[str]:
    """Return the era-appropriate abbreviation (e.g. 'SEA' for
    Seattle SuperSonics) for `(team_id, year)`, or None.
    """
    if year is None:
        return None
    era = get_era_for_year(team_id, year)
    if era:
        return era.get("abbr")
    return None


def get_logo_url_for_year(team_id: str, year: int, *, static_prefix: str = "/") -> str:
    """Return a URL for the historical logo at `(team_id, year)`.

    Every current NBA team has a stable local current-era entry
    (`year_end = None`), so the lookup always resolves to a local file. When
    a strict year-range lookup fails (pre-founding year or a rare gap era),
    fall back to the team's current-era local file. Only as a last-resort
    defensive branch do we return the remote CDN URL.
    """
    logo = get_logo_for_year(team_id, year) or get_current_logo(team_id)
    if logo is not None:
        return static_prefix.rstrip("/") + "/" + logo["path"]
    return _CURRENT_LOGO_CDN.format(team_id=team_id)

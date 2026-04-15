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
        "city": "Washington",
        "state": "DC",
        "year_start": 1997,
        "year_end": None,
        "lat": 38.9072,
        "lon": -77.0369,
    },
]


FRANCHISE_LOGOS = [
    # team_id 1610612737
    {
        "team_id":    "1610612737",
        "year_start": 1949,
        "year_end":   1950,
        "path":       "static/team_logos/historical/1610612737/1949_1950.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 1955,
        "year_end":   1956,
        "path":       "static/team_logos/historical/1610612737/1955_1956.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 1957,
        "year_end":   1967,
        "path":       "static/team_logos/historical/1610612737/1957_1967.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 1968,
        "year_end":   1968,
        "path":       "static/team_logos/historical/1610612737/1968_1968.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 1969,
        "year_end":   1969,
        "path":       "static/team_logos/historical/1610612737/1969_1969.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 1970,
        "year_end":   1971,
        "path":       "static/team_logos/historical/1610612737/1970_1971.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 1972,
        "year_end":   1994,
        "path":       "static/team_logos/historical/1610612737/1972_1994.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 2007,
        "year_end":   2014,
        "path":       "static/team_logos/historical/1610612737/2007_2014.png",
    },
    {
        "team_id":    "1610612737",
        "year_start": 2015,
        "year_end":   2019,
        "path":       "static/team_logos/historical/1610612737/2015_2019.png",
    },

    # team_id 1610612738
    {
        "team_id":    "1610612738",
        "year_start": 1946,
        "year_end":   1949,
        "path":       "static/team_logos/historical/1610612738/1946_1949.png",
    },
    {
        "team_id":    "1610612738",
        "year_start": 1950,
        "year_end":   1963,
        "path":       "static/team_logos/historical/1610612738/1950_1963.png",
    },
    {
        "team_id":    "1610612738",
        "year_start": 1964,
        "year_end":   1965,
        "path":       "static/team_logos/historical/1610612738/1964_1965.png",
    },
    {
        "team_id":    "1610612738",
        "year_start": 1966,
        "year_end":   1968,
        "path":       "static/team_logos/historical/1610612738/1966_1968.png",
    },
    {
        "team_id":    "1610612738",
        "year_start": 1969,
        "year_end":   1995,
        "path":       "static/team_logos/historical/1610612738/1969_1995.png",
    },

    # team_id 1610612739
    {
        "team_id":    "1610612739",
        "year_start": 1970,
        "year_end":   1982,
        "path":       "static/team_logos/historical/1610612739/1970_1982.png",
    },
    {
        "team_id":    "1610612739",
        "year_start": 1983,
        "year_end":   1993,
        "path":       "static/team_logos/historical/1610612739/1983_1993.png",
    },
    {
        "team_id":    "1610612739",
        "year_start": 1994,
        "year_end":   2002,
        "path":       "static/team_logos/historical/1610612739/1994_2002.png",
    },
    {
        "team_id":    "1610612739",
        "year_start": 2003,
        "year_end":   2009,
        "path":       "static/team_logos/historical/1610612739/2003_2009.png",
    },
    {
        "team_id":    "1610612739",
        "year_start": 2010,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612739/2010_2016.png",
    },
    {
        "team_id":    "1610612739",
        "year_start": 2017,
        "year_end":   2021,
        "path":       "static/team_logos/historical/1610612739/2017_2021.png",
    },

    # team_id 1610612740
    {
        "team_id":    "1610612740",
        "year_start": 1988,
        "year_end":   2001,
        "path":       "static/team_logos/historical/1610612740/1988_2001.png",
    },
    {
        "team_id":    "1610612740",
        "year_start": 2013,
        "year_end":   2022,
        "path":       "static/team_logos/historical/1610612740/2013_2022.png",
    },

    # team_id 1610612742
    {
        "team_id":    "1610612742",
        "year_start": 1980,
        "year_end":   1992,
        "path":       "static/team_logos/historical/1610612742/1980_1992.png",
    },
    {
        "team_id":    "1610612742",
        "year_start": 1993,
        "year_end":   2000,
        "path":       "static/team_logos/historical/1610612742/1993_2000.png",
    },
    {
        "team_id":    "1610612742",
        "year_start": 2001,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612742/2001_2016.png",
    },

    # team_id 1610612743
    {
        "team_id":    "1610612743",
        "year_start": 1974,
        "year_end":   1980,
        "path":       "static/team_logos/historical/1610612743/1974_1980.png",
    },
    {
        "team_id":    "1610612743",
        "year_start": 1981,
        "year_end":   1992,
        "path":       "static/team_logos/historical/1610612743/1981_1992.png",
    },
    {
        "team_id":    "1610612743",
        "year_start": 1993,
        "year_end":   2002,
        "path":       "static/team_logos/historical/1610612743/1993_2002.png",
    },
    {
        "team_id":    "1610612743",
        "year_start": 2003,
        "year_end":   2007,
        "path":       "static/team_logos/historical/1610612743/2003_2007.png",
    },
    {
        "team_id":    "1610612743",
        "year_start": 2008,
        "year_end":   2017,
        "path":       "static/team_logos/historical/1610612743/2008_2017.png",
    },

    # team_id 1610612744
    {
        "team_id":    "1610612744",
        "year_start": 1946,
        "year_end":   1950,
        "path":       "static/team_logos/historical/1610612744/1946_1950.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 1951,
        "year_end":   1961,
        "path":       "static/team_logos/historical/1610612744/1951_1961.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 1962,
        "year_end":   1968,
        "path":       "static/team_logos/historical/1610612744/1962_1968.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 1969,
        "year_end":   1970,
        "path":       "static/team_logos/historical/1610612744/1969_1970.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 1971,
        "year_end":   1974,
        "path":       "static/team_logos/historical/1610612744/1971_1974.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 1975,
        "year_end":   1987,
        "path":       "static/team_logos/historical/1610612744/1975_1987.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 1988,
        "year_end":   1996,
        "path":       "static/team_logos/historical/1610612744/1988_1996.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 1997,
        "year_end":   2009,
        "path":       "static/team_logos/historical/1610612744/1997_2009.png",
    },
    {
        "team_id":    "1610612744",
        "year_start": 2010,
        "year_end":   2018,
        "path":       "static/team_logos/historical/1610612744/2010_2018.png",
    },

    # team_id 1610612745
    {
        "team_id":    "1610612745",
        "year_start": 1967,
        "year_end":   1970,
        "path":       "static/team_logos/historical/1610612745/1967_1970.png",
    },
    {
        "team_id":    "1610612745",
        "year_start": 1971,
        "year_end":   1971,
        "path":       "static/team_logos/historical/1610612745/1971_1971.png",
    },
    {
        "team_id":    "1610612745",
        "year_start": 1972,
        "year_end":   1994,
        "path":       "static/team_logos/historical/1610612745/1972_1994.png",
    },
    {
        "team_id":    "1610612745",
        "year_start": 1995,
        "year_end":   2002,
        "path":       "static/team_logos/historical/1610612745/1995_2002.png",
    },
    {
        "team_id":    "1610612745",
        "year_start": 2003,
        "year_end":   2018,
        "path":       "static/team_logos/historical/1610612745/2003_2018.png",
    },

    # team_id 1610612746
    {
        "team_id":    "1610612746",
        "year_start": 1970,
        "year_end":   1970,
        "path":       "static/team_logos/historical/1610612746/1970_1970.png",
    },
    {
        "team_id":    "1610612746",
        "year_start": 1971,
        "year_end":   1977,
        "path":       "static/team_logos/historical/1610612746/1971_1977.png",
    },
    {
        "team_id":    "1610612746",
        "year_start": 1978,
        "year_end":   1981,
        "path":       "static/team_logos/historical/1610612746/1978_1981.png",
    },
    {
        "team_id":    "1610612746",
        "year_start": 1982,
        "year_end":   1983,
        "path":       "static/team_logos/historical/1610612746/1982_1983.png",
    },
    {
        "team_id":    "1610612746",
        "year_start": 1984,
        "year_end":   2009,
        "path":       "static/team_logos/historical/1610612746/1984_2009.png",
    },
    {
        "team_id":    "1610612746",
        "year_start": 2010,
        "year_end":   2014,
        "path":       "static/team_logos/historical/1610612746/2010_2014.png",
    },
    {
        "team_id":    "1610612746",
        "year_start": 2015,
        "year_end":   2017,
        "path":       "static/team_logos/historical/1610612746/2015_2017.png",
    },
    {
        "team_id":    "1610612746",
        "year_start": 2018,
        "year_end":   2023,
        "path":       "static/team_logos/historical/1610612746/2018_2023.png",
    },

    # team_id 1610612747
    {
        "team_id":    "1610612747",
        "year_start": 1947,
        "year_end":   1959,
        "path":       "static/team_logos/historical/1610612747/1947_1959.png",
    },
    {
        "team_id":    "1610612747",
        "year_start": 1960,
        "year_end":   1964,
        "path":       "static/team_logos/historical/1610612747/1960_1964.png",
    },
    {
        "team_id":    "1610612747",
        "year_start": 1965,
        "year_end":   1970,
        "path":       "static/team_logos/historical/1610612747/1965_1970.png",
    },
    {
        "team_id":    "1610612747",
        "year_start": 1971,
        "year_end":   1974,
        "path":       "static/team_logos/historical/1610612747/1971_1974.png",
    },
    {
        "team_id":    "1610612747",
        "year_start": 1975,
        "year_end":   1998,
        "path":       "static/team_logos/historical/1610612747/1975_1998.png",
    },
    {
        "team_id":    "1610612747",
        "year_start": 1999,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612747/1999_2016.png",
    },
    {
        "team_id":    "1610612747",
        "year_start": 2017,
        "year_end":   2022,
        "path":       "static/team_logos/historical/1610612747/2017_2022.png",
    },

    # team_id 1610612748
    {
        "team_id":    "1610612748",
        "year_start": 1988,
        "year_end":   1998,
        "path":       "static/team_logos/historical/1610612748/1988_1998.png",
    },

    # team_id 1610612749
    {
        "team_id":    "1610612749",
        "year_start": 1968,
        "year_end":   1992,
        "path":       "static/team_logos/historical/1610612749/1968_1992.png",
    },
    {
        "team_id":    "1610612749",
        "year_start": 1993,
        "year_end":   2005,
        "path":       "static/team_logos/historical/1610612749/1993_2005.png",
    },
    {
        "team_id":    "1610612749",
        "year_start": 2006,
        "year_end":   2014,
        "path":       "static/team_logos/historical/1610612749/2006_2014.png",
    },

    # team_id 1610612750
    {
        "team_id":    "1610612750",
        "year_start": 1989,
        "year_end":   1995,
        "path":       "static/team_logos/historical/1610612750/1989_1995.png",
    },
    {
        "team_id":    "1610612750",
        "year_start": 1996,
        "year_end":   2007,
        "path":       "static/team_logos/historical/1610612750/1996_2007.png",
    },
    {
        "team_id":    "1610612750",
        "year_start": 2008,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612750/2008_2016.png",
    },

    # team_id 1610612751
    {
        "team_id":    "1610612751",
        "year_start": 1968,
        "year_end":   1971,
        "path":       "static/team_logos/historical/1610612751/1968_1971.png",
    },
    {
        "team_id":    "1610612751",
        "year_start": 1972,
        "year_end":   1975,
        "path":       "static/team_logos/historical/1610612751/1972_1975.png",
    },
    {
        "team_id":    "1610612751",
        "year_start": 1976,
        "year_end":   1976,
        "path":       "static/team_logos/historical/1610612751/1976_1976.png",
    },
    {
        "team_id":    "1610612751",
        "year_start": 1977,
        "year_end":   1977,
        "path":       "static/team_logos/historical/1610612751/1977_1977.png",
    },
    {
        "team_id":    "1610612751",
        "year_start": 1978,
        "year_end":   1989,
        "path":       "static/team_logos/historical/1610612751/1978_1989.png",
    },
    {
        "team_id":    "1610612751",
        "year_start": 1990,
        "year_end":   1996,
        "path":       "static/team_logos/historical/1610612751/1990_1996.png",
    },
    {
        "team_id":    "1610612751",
        "year_start": 1997,
        "year_end":   2011,
        "path":       "static/team_logos/historical/1610612751/1997_2011.png",
    },
    {
        "team_id":    "1610612751",
        "year_start": 2012,
        "year_end":   2023,
        "path":       "static/team_logos/historical/1610612751/2012_2023.png",
    },

    # team_id 1610612752
    {
        "team_id":    "1610612752",
        "year_start": 1946,
        "year_end":   1963,
        "path":       "static/team_logos/historical/1610612752/1946_1963.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 1964,
        "year_end":   1978,
        "path":       "static/team_logos/historical/1610612752/1964_1978.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 1979,
        "year_end":   1982,
        "path":       "static/team_logos/historical/1610612752/1979_1982.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 1983,
        "year_end":   1988,
        "path":       "static/team_logos/historical/1610612752/1983_1988.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 1989,
        "year_end":   1991,
        "path":       "static/team_logos/historical/1610612752/1989_1991.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 1992,
        "year_end":   1994,
        "path":       "static/team_logos/historical/1610612752/1992_1994.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 1995,
        "year_end":   2010,
        "path":       "static/team_logos/historical/1610612752/1995_2010.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 2011,
        "year_end":   2021,
        "path":       "static/team_logos/historical/1610612752/2011_2021.png",
    },
    {
        "team_id":    "1610612752",
        "year_start": 2022,
        "year_end":   2022,
        "path":       "static/team_logos/historical/1610612752/2022_2022.png",
    },

    # team_id 1610612753
    {
        "team_id":    "1610612753",
        "year_start": 1989,
        "year_end":   1997,
        "path":       "static/team_logos/historical/1610612753/1989_1997.png",
    },
    {
        "team_id":    "1610612753",
        "year_start": 1998,
        "year_end":   1999,
        "path":       "static/team_logos/historical/1610612753/1998_1999.png",
    },
    {
        "team_id":    "1610612753",
        "year_start": 2000,
        "year_end":   2009,
        "path":       "static/team_logos/historical/1610612753/2000_2009.png",
    },
    {
        "team_id":    "1610612753",
        "year_start": 2010,
        "year_end":   2024,
        "path":       "static/team_logos/historical/1610612753/2010_2024.png",
    },

    # team_id 1610612754
    {
        "team_id":    "1610612754",
        "year_start": 1967,
        "year_end":   1975,
        "path":       "static/team_logos/historical/1610612754/1967_1975.png",
    },
    {
        "team_id":    "1610612754",
        "year_start": 1976,
        "year_end":   1989,
        "path":       "static/team_logos/historical/1610612754/1976_1989.png",
    },
    {
        "team_id":    "1610612754",
        "year_start": 1990,
        "year_end":   2004,
        "path":       "static/team_logos/historical/1610612754/1990_2004.png",
    },
    {
        "team_id":    "1610612754",
        "year_start": 2005,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612754/2005_2016.png",
    },
    {
        "team_id":    "1610612754",
        "year_start": 2017,
        "year_end":   2024,
        "path":       "static/team_logos/historical/1610612754/2017_2024.png",
    },

    # team_id 1610612755
    {
        "team_id":    "1610612755",
        "year_start": 1946,
        "year_end":   1948,
        "path":       "static/team_logos/historical/1610612755/1946_1948.png",
    },
    {
        "team_id":    "1610612755",
        "year_start": 1949,
        "year_end":   1962,
        "path":       "static/team_logos/historical/1610612755/1949_1962.png",
    },
    {
        "team_id":    "1610612755",
        "year_start": 1963,
        "year_end":   1976,
        "path":       "static/team_logos/historical/1610612755/1963_1976.png",
    },
    {
        "team_id":    "1610612755",
        "year_start": 1977,
        "year_end":   1996,
        "path":       "static/team_logos/historical/1610612755/1977_1996.png",
    },
    {
        "team_id":    "1610612755",
        "year_start": 1997,
        "year_end":   2008,
        "path":       "static/team_logos/historical/1610612755/1997_2008.png",
    },
    {
        "team_id":    "1610612755",
        "year_start": 2009,
        "year_end":   2014,
        "path":       "static/team_logos/historical/1610612755/2009_2014.png",
    },

    # team_id 1610612756
    {
        "team_id":    "1610612756",
        "year_start": 1968,
        "year_end":   1991,
        "path":       "static/team_logos/historical/1610612756/1968_1991.png",
    },
    {
        "team_id":    "1610612756",
        "year_start": 1992,
        "year_end":   1999,
        "path":       "static/team_logos/historical/1610612756/1992_1999.png",
    },
    {
        "team_id":    "1610612756",
        "year_start": 2000,
        "year_end":   2012,
        "path":       "static/team_logos/historical/1610612756/2000_2012.png",
    },

    # team_id 1610612757
    {
        "team_id":    "1610612757",
        "year_start": 1970,
        "year_end":   1989,
        "path":       "static/team_logos/historical/1610612757/1970_1989.png",
    },
    {
        "team_id":    "1610612757",
        "year_start": 1990,
        "year_end":   2001,
        "path":       "static/team_logos/historical/1610612757/1990_2001.png",
    },
    {
        "team_id":    "1610612757",
        "year_start": 2002,
        "year_end":   2002,
        "path":       "static/team_logos/historical/1610612757/2002_2002.png",
    },
    {
        "team_id":    "1610612757",
        "year_start": 2003,
        "year_end":   2003,
        "path":       "static/team_logos/historical/1610612757/2003_2003.png",
    },
    {
        "team_id":    "1610612757",
        "year_start": 2004,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612757/2004_2016.png",
    },

    # team_id 1610612758
    {
        "team_id":    "1610612758",
        "year_start": 1945,
        "year_end":   1956,
        "path":       "static/team_logos/historical/1610612758/1945_1956.png",
    },
    {
        "team_id":    "1610612758",
        "year_start": 1957,
        "year_end":   1970,
        "path":       "static/team_logos/historical/1610612758/1957_1970.png",
    },
    {
        "team_id":    "1610612758",
        "year_start": 1971,
        "year_end":   1971,
        "path":       "static/team_logos/historical/1610612758/1971_1971.png",
    },
    {
        "team_id":    "1610612758",
        "year_start": 1972,
        "year_end":   1974,
        "path":       "static/team_logos/historical/1610612758/1972_1974.png",
    },
    {
        "team_id":    "1610612758",
        "year_start": 1975,
        "year_end":   1984,
        "path":       "static/team_logos/historical/1610612758/1975_1984.png",
    },
    {
        "team_id":    "1610612758",
        "year_start": 1985,
        "year_end":   1993,
        "path":       "static/team_logos/historical/1610612758/1985_1993.png",
    },
    {
        "team_id":    "1610612758",
        "year_start": 1994,
        "year_end":   2015,
        "path":       "static/team_logos/historical/1610612758/1994_2015.png",
    },

    # team_id 1610612759
    {
        "team_id":    "1610612759",
        "year_start": 1976,
        "year_end":   1988,
        "path":       "static/team_logos/historical/1610612759/1976_1988.png",
    },
    {
        "team_id":    "1610612759",
        "year_start": 1989,
        "year_end":   2001,
        "path":       "static/team_logos/historical/1610612759/1989_2001.png",
    },
    {
        "team_id":    "1610612759",
        "year_start": 2002,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612759/2002_2016.png",
    },

    # team_id 1610612760
    {
        "team_id":    "1610612760",
        "year_start": 1967,
        "year_end":   1969,
        "path":       "static/team_logos/historical/1610612760/1967_1969.png",
    },
    {
        "team_id":    "1610612760",
        "year_start": 1970,
        "year_end":   1970,
        "path":       "static/team_logos/historical/1610612760/1970_1970.png",
    },
    {
        "team_id":    "1610612760",
        "year_start": 1971,
        "year_end":   1974,
        "path":       "static/team_logos/historical/1610612760/1971_1974.png",
    },
    {
        "team_id":    "1610612760",
        "year_start": 1975,
        "year_end":   1994,
        "path":       "static/team_logos/historical/1610612760/1975_1994.png",
    },
    {
        "team_id":    "1610612760",
        "year_start": 1995,
        "year_end":   2000,
        "path":       "static/team_logos/historical/1610612760/1995_2000.png",
    },
    {
        "team_id":    "1610612760",
        "year_start": 2001,
        "year_end":   2007,
        "path":       "static/team_logos/historical/1610612760/2001_2007.png",
    },

    # team_id 1610612761
    {
        "team_id":    "1610612761",
        "year_start": 1995,
        "year_end":   2007,
        "path":       "static/team_logos/historical/1610612761/1995_2007.png",
    },
    {
        "team_id":    "1610612761",
        "year_start": 2008,
        "year_end":   2014,
        "path":       "static/team_logos/historical/1610612761/2008_2014.png",
    },
    {
        "team_id":    "1610612761",
        "year_start": 2015,
        "year_end":   2019,
        "path":       "static/team_logos/historical/1610612761/2015_2019.png",
    },

    # team_id 1610612762
    {
        "team_id":    "1610612762",
        "year_start": 1974,
        "year_end":   1978,
        "path":       "static/team_logos/historical/1610612762/1974_1978.png",
    },
    {
        "team_id":    "1610612762",
        "year_start": 1979,
        "year_end":   1995,
        "path":       "static/team_logos/historical/1610612762/1979_1995.png",
    },
    {
        "team_id":    "1610612762",
        "year_start": 1996,
        "year_end":   2003,
        "path":       "static/team_logos/historical/1610612762/1996_2003.png",
    },
    {
        "team_id":    "1610612762",
        "year_start": 2004,
        "year_end":   2009,
        "path":       "static/team_logos/historical/1610612762/2004_2009.png",
    },
    {
        "team_id":    "1610612762",
        "year_start": 2010,
        "year_end":   2015,
        "path":       "static/team_logos/historical/1610612762/2010_2015.png",
    },
    {
        "team_id":    "1610612762",
        "year_start": 2016,
        "year_end":   2021,
        "path":       "static/team_logos/historical/1610612762/2016_2021.png",
    },
    {
        "team_id":    "1610612762",
        "year_start": 2022,
        "year_end":   2024,
        "path":       "static/team_logos/historical/1610612762/2022_2024.png",
    },

    # team_id 1610612763
    {
        "team_id":    "1610612763",
        "year_start": 1995,
        "year_end":   2000,
        "path":       "static/team_logos/historical/1610612763/1995_2000.png",
    },
    {
        "team_id":    "1610612763",
        "year_start": 2001,
        "year_end":   2003,
        "path":       "static/team_logos/historical/1610612763/2001_2003.png",
    },
    {
        "team_id":    "1610612763",
        "year_start": 2004,
        "year_end":   2017,
        "path":       "static/team_logos/historical/1610612763/2004_2017.png",
    },

    # team_id 1610612764
    {
        "team_id":    "1610612764",
        "year_start": 1961,
        "year_end":   1961,
        "path":       "static/team_logos/historical/1610612764/1961_1961.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1962,
        "year_end":   1962,
        "path":       "static/team_logos/historical/1610612764/1962_1962.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1963,
        "year_end":   1968,
        "path":       "static/team_logos/historical/1610612764/1963_1968.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1968,
        "year_end":   1968,
        "path":       "static/team_logos/historical/1610612764/1968_1968.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1969,
        "year_end":   1970,
        "path":       "static/team_logos/historical/1610612764/1969_1970.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1971,
        "year_end":   1971,
        "path":       "static/team_logos/historical/1610612764/1971_1971.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1972,
        "year_end":   1972,
        "path":       "static/team_logos/historical/1610612764/1972_1972.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1973,
        "year_end":   1973,
        "path":       "static/team_logos/historical/1610612764/1973_1973.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1974,
        "year_end":   1986,
        "path":       "static/team_logos/historical/1610612764/1974_1986.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1987,
        "year_end":   1996,
        "path":       "static/team_logos/historical/1610612764/1987_1996.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 1997,
        "year_end":   2006,
        "path":       "static/team_logos/historical/1610612764/1997_2006.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 2007,
        "year_end":   2010,
        "path":       "static/team_logos/historical/1610612764/2007_2010.png",
    },
    {
        "team_id":    "1610612764",
        "year_start": 2011,
        "year_end":   2014,
        "path":       "static/team_logos/historical/1610612764/2011_2014.png",
    },

    # team_id 1610612765
    {
        "team_id":    "1610612765",
        "year_start": 1957,
        "year_end":   1967,
        "path":       "static/team_logos/historical/1610612765/1957_1967.png",
    },
    {
        "team_id":    "1610612765",
        "year_start": 1968,
        "year_end":   1974,
        "path":       "static/team_logos/historical/1610612765/1968_1974.png",
    },
    {
        "team_id":    "1610612765",
        "year_start": 1975,
        "year_end":   1977,
        "path":       "static/team_logos/historical/1610612765/1975_1977.png",
    },
    {
        "team_id":    "1610612765",
        "year_start": 1978,
        "year_end":   1995,
        "path":       "static/team_logos/historical/1610612765/1978_1995.png",
    },
    {
        "team_id":    "1610612765",
        "year_start": 1996,
        "year_end":   2000,
        "path":       "static/team_logos/historical/1610612765/1996_2000.png",
    },
    {
        "team_id":    "1610612765",
        "year_start": 2001,
        "year_end":   2004,
        "path":       "static/team_logos/historical/1610612765/2001_2004.png",
    },
    {
        "team_id":    "1610612765",
        "year_start": 2005,
        "year_end":   2016,
        "path":       "static/team_logos/historical/1610612765/2005_2016.png",
    },

    # team_id 1610612766
    {
        "team_id":    "1610612766",
        "year_start": 2004,
        "year_end":   2006,
        "path":       "static/team_logos/historical/1610612766/2004_2006.png",
    },
    {
        "team_id":    "1610612766",
        "year_start": 2007,
        "year_end":   2011,
        "path":       "static/team_logos/historical/1610612766/2007_2011.png",
    },
    {
        "team_id":    "1610612766",
        "year_start": 2012,
        "year_end":   2013,
        "path":       "static/team_logos/historical/1610612766/2012_2013.png",
    },
]




# Current-era logos downloaded from cdn.nba.com/logos/nba/.../global/L/logo.svg
# These cover each franchise from the year after their last recorded historical
# logo (or franchise founding for teams with no historical logos on file) to now.
# year_end=9999 is a sentinel meaning "still current".
FRANCHISE_LOGOS += [
    {
        "team_id":    "1610612737",
        "year_start": 2020,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612737/current.svg",
    },
    {
        "team_id":    "1610612738",
        "year_start": 1996,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612738/current.svg",
    },
    {
        "team_id":    "1610612739",
        "year_start": 2022,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612739/current.svg",
    },
    {
        "team_id":    "1610612740",
        "year_start": 2023,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612740/current.svg",
    },
    {
        "team_id":    "1610612741",
        "year_start": 1966,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612741/current.svg",
    },
    {
        "team_id":    "1610612742",
        "year_start": 2017,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612742/current.svg",
    },
    {
        "team_id":    "1610612743",
        "year_start": 2018,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612743/current.svg",
    },
    {
        "team_id":    "1610612744",
        "year_start": 2019,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612744/current.svg",
    },
    {
        "team_id":    "1610612745",
        "year_start": 2019,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612745/current.svg",
    },
    {
        "team_id":    "1610612746",
        "year_start": 2024,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612746/current.svg",
    },
    {
        "team_id":    "1610612747",
        "year_start": 2023,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612747/current.svg",
    },
    {
        "team_id":    "1610612748",
        "year_start": 1999,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612748/current.svg",
    },
    {
        "team_id":    "1610612749",
        "year_start": 2015,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612749/current.svg",
    },
    {
        "team_id":    "1610612750",
        "year_start": 2017,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612750/current.svg",
    },
    {
        "team_id":    "1610612751",
        "year_start": 2024,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612751/current.svg",
    },
    {
        "team_id":    "1610612752",
        "year_start": 2023,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612752/current.svg",
    },
    {
        "team_id":    "1610612753",
        "year_start": 2025,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612753/current.svg",
    },
    {
        "team_id":    "1610612754",
        "year_start": 2025,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612754/current.svg",
    },
    {
        "team_id":    "1610612755",
        "year_start": 2015,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612755/current.svg",
    },
    {
        "team_id":    "1610612756",
        "year_start": 2013,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612756/current.svg",
    },
    {
        "team_id":    "1610612757",
        "year_start": 2017,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612757/current.svg",
    },
    {
        "team_id":    "1610612758",
        "year_start": 2016,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612758/current.svg",
    },
    {
        "team_id":    "1610612759",
        "year_start": 2017,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612759/current.svg",
    },
    {
        "team_id":    "1610612760",
        "year_start": 2008,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612760/current.svg",
    },
    {
        "team_id":    "1610612761",
        "year_start": 2020,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612761/current.svg",
    },
    {
        "team_id":    "1610612762",
        "year_start": 2025,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612762/current.svg",
    },
    {
        "team_id":    "1610612763",
        "year_start": 2018,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612763/current.svg",
    },
    {
        "team_id":    "1610612764",
        "year_start": 2015,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612764/current.svg",
    },
    {
        "team_id":    "1610612765",
        "year_start": 2017,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612765/current.svg",
    },
    {
        "team_id":    "1610612766",
        "year_start": 2014,
        "year_end":   9999,
        "path":       "static/team_logos/historical/1610612766/current.svg",
    },
]


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


@lru_cache(maxsize=1)
def _logos_by_team() -> dict:
    out: dict[str, list] = {}
    for entry in FRANCHISE_LOGOS:
        out.setdefault(entry["team_id"], []).append(entry)
    for entries in out.values():
        # Order by year_start ascending; when multiple entries cover the same
        # year (overlapping ranges), the later year_start wins for a given
        # query, so sort newest-first as secondary key.
        entries.sort(key=lambda e: (e["year_start"], -e["year_end"]))
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


def get_logo_for_year(team_id: str, year: int) -> Optional[dict]:
    """Return the best FRANCHISE_LOGOS entry covering `year` for `team_id`, or None.

    When multiple entries cover the same year (alternate / primary variants),
    the one with the latest `year_start` wins — this favors the most
    specific / most recent alternate.
    """
    candidates = [
        e for e in _logos_by_team().get(team_id, [])
        if e["year_start"] <= year <= e["year_end"]
    ]
    if not candidates:
        return None
    # Pick the latest year_start (most specific to `year`), breaking ties by
    # shortest span (most specific).
    candidates.sort(key=lambda e: (-e["year_start"], e["year_end"] - e["year_start"]))
    return candidates[0]


def get_nearest_logo(team_id: str, year: int) -> Optional[dict]:
    """Return the logo entry closest in year for `team_id`, regardless of overlap.

    Falls back to this when no logo strictly covers `year`. Distance is the
    minimum year gap to the entry's range; the entry with the smallest gap
    wins (ties broken by preferring an earlier-starting entry).
    """
    entries = _logos_by_team().get(team_id, [])
    if not entries:
        return None

    def gap(e):
        if year < e["year_start"]:
            return (e["year_start"] - year, 0)
        if year > e["year_end"]:
            return (year - e["year_end"], 1)
        return (0, 0)

    return min(entries, key=gap)


def get_logo_url_for_year(team_id: str, year: int, *, static_prefix: str = "/") -> str:
    """Return a URL for the historical logo at `(team_id, year)`.

    Every current NBA team has a stable local current-era entry
    (`year_end = 9999`), so the lookup always resolves to a local file. When
    a strict year-range lookup fails (pre-founding year or a rare gap era),
    fall back to the team's current-era local file. Only as a last-resort
    defensive branch do we return the remote CDN URL.
    """
    logo = get_logo_for_year(team_id, year) or get_logo_for_year(team_id, 9999)
    if logo is not None:
        return static_prefix.rstrip("/") + "/" + logo["path"]
    return _CURRENT_LOGO_CDN.format(team_id=team_id)

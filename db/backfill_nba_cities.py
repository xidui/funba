"""Backfill NbaCity table and link Team rows to their city.

Usage:
    python -m db.backfill_nba_cities
"""

from sqlalchemy.orm import Session

from db.models import NbaCity, Team, engine

# ---- NBA arena cities with coordinates ----
# (name, state, country, lat, lon)
CITIES = [
    ("Atlanta", "GA", "US", 33.7490, -84.3880),
    ("Boston", "MA", "US", 42.3601, -71.0589),
    ("Brooklyn", "NY", "US", 40.6828, -73.9754),
    ("Charlotte", "NC", "US", 35.2271, -80.8431),
    ("Chicago", "IL", "US", 41.8781, -87.6298),
    ("Cleveland", "OH", "US", 41.4993, -81.6944),
    ("Dallas", "TX", "US", 32.7767, -96.7970),
    ("Denver", "CO", "US", 39.7392, -104.9903),
    ("Detroit", "MI", "US", 42.3314, -83.0458),
    ("East Rutherford", "NJ", "US", 40.8128, -74.0742),  # NJN Nets
    ("Houston", "TX", "US", 29.7604, -95.3698),
    ("Indianapolis", "IN", "US", 39.7684, -86.1581),      # Indiana Pacers
    ("Kansas City", "MO", "US", 39.0997, -94.5786),       # KC Kings
    ("Los Angeles", "CA", "US", 34.0522, -118.2437),
    ("Memphis", "TN", "US", 35.1495, -90.0490),
    ("Miami", "FL", "US", 25.7617, -80.1918),
    ("Milwaukee", "WI", "US", 43.0389, -87.9065),
    ("Minneapolis", "MN", "US", 44.9778, -93.2650),       # Minnesota T-Wolves
    ("New Orleans", "LA", "US", 29.9511, -90.0715),
    ("New York", "NY", "US", 40.7505, -73.9934),          # MSG
    ("Oklahoma City", "OK", "US", 35.4634, -97.5151),
    ("Orlando", "FL", "US", 28.5383, -81.3792),
    ("Philadelphia", "PA", "US", 39.9012, -75.1720),
    ("Phoenix", "AZ", "US", 33.4458, -112.0712),
    ("Portland", "OR", "US", 45.5316, -122.6668),
    ("Sacramento", "CA", "US", 38.5802, -121.4998),
    ("Salt Lake City", "UT", "US", 40.7683, -111.8881),   # Utah Jazz
    ("San Antonio", "TX", "US", 29.4271, -98.4375),
    ("San Diego", "CA", "US", 32.7157, -117.1611),        # SD Clippers
    ("San Francisco", "CA", "US", 37.7680, -122.3879),    # Golden State Warriors
    ("Seattle", "WA", "US", 47.6062, -122.3321),
    ("Toronto", "ON", "CA", 43.6435, -79.3791),
    ("Vancouver", "BC", "CA", 49.2781, -123.1089),
    ("Washington", "DC", "US", 38.8981, -77.0209),
]

# Map Team.city (as stored in DB) → (NbaCity.name, NbaCity.state)
TEAM_CITY_MAP = {
    "Atlanta": ("Atlanta", "GA"),
    "Boston": ("Boston", "MA"),
    "Brooklyn": ("Brooklyn", "NY"),
    "Charlotte": ("Charlotte", "NC"),
    "Chicago": ("Chicago", "IL"),
    "Cleveland": ("Cleveland", "OH"),
    "Dallas": ("Dallas", "TX"),
    "Denver": ("Denver", "CO"),
    "Detroit": ("Detroit", "MI"),
    "Golden State": ("San Francisco", "CA"),
    "Houston": ("Houston", "TX"),
    "Indiana": ("Indianapolis", "IN"),
    "Kansas City": ("Kansas City", "MO"),
    "Los Angeles": ("Los Angeles", "CA"),
    "Memphis": ("Memphis", "TN"),
    "Miami": ("Miami", "FL"),
    "Milwaukee": ("Milwaukee", "WI"),
    "Minnesota": ("Minneapolis", "MN"),
    "New Jersey": ("East Rutherford", "NJ"),
    "New Orleans": ("New Orleans", "LA"),
    "New Orleans/Oklahoma City": ("Oklahoma City", "OK"),
    "New York": ("New York", "NY"),
    "Oklahoma City": ("Oklahoma City", "OK"),
    "Orlando": ("Orlando", "FL"),
    "Philadelphia": ("Philadelphia", "PA"),
    "Phoenix": ("Phoenix", "AZ"),
    "Portland": ("Portland", "OR"),
    "Sacramento": ("Sacramento", "CA"),
    "San Antonio": ("San Antonio", "TX"),
    "San Diego": ("San Diego", "CA"),
    "Seattle": ("Seattle", "WA"),
    "Toronto": ("Toronto", "ON"),
    "Utah": ("Salt Lake City", "UT"),
    "Vancouver": ("Vancouver", "BC"),
    "Washington": ("Washington", "DC"),
}


def run() -> None:
    with Session(engine) as session:
        # 1. Upsert cities
        city_lookup: dict[tuple[str, str | None], NbaCity] = {}
        for name, state, country, lat, lon in CITIES:
            existing = session.query(NbaCity).filter_by(name=name, state=state).first()
            if existing:
                existing.latitude = lat
                existing.longitude = lon
                existing.country = country
                city_lookup[(name, state)] = existing
                print(f"  updated  {name}, {state}")
            else:
                city = NbaCity(name=name, state=state, country=country, latitude=lat, longitude=lon)
                session.add(city)
                session.flush()
                city_lookup[(name, state)] = city
                print(f"  inserted {name}, {state}")

        # 2. Link teams
        teams = session.query(Team).all()
        linked = 0
        skipped = 0
        for team in teams:
            if team.city and team.city in TEAM_CITY_MAP:
                city_name, city_state = TEAM_CITY_MAP[team.city]
                nba_city = city_lookup.get((city_name, city_state))
                if nba_city:
                    team.city_id = nba_city.id
                    linked += 1
                else:
                    print(f"  WARNING: city not found for {team.abbr} -> {city_name}, {city_state}")
            else:
                skipped += 1

        session.commit()
        print(f"\nDone: {linked} teams linked, {skipped} teams skipped (no city data)")


if __name__ == "__main__":
    run()

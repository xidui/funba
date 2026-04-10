"""Geographic distance utilities for NbaCity."""

import math
from typing import Optional

from sqlalchemy.orm import Session

from db.models import NbaCity, Team

_EARTH_RADIUS_KM = 6371.0
_KM_TO_MILES = 0.621371


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres between two (lat, lon) points."""
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def city_distance_km(city_a: NbaCity, city_b: NbaCity) -> float:
    """Distance in km between two NbaCity objects."""
    return haversine_km(city_a.latitude, city_a.longitude, city_b.latitude, city_b.longitude)


def city_distance_miles(city_a: NbaCity, city_b: NbaCity) -> float:
    """Distance in miles between two NbaCity objects."""
    return city_distance_km(city_a, city_b) * _KM_TO_MILES


def team_distance_miles(session: Session, team_id_a: str, team_id_b: str) -> Optional[float]:
    """Distance in miles between two teams' cities. Returns None if either team has no city."""
    teams = {
        t.team_id: t
        for t in session.query(Team).filter(Team.team_id.in_([team_id_a, team_id_b])).all()
    }
    ta, tb = teams.get(team_id_a), teams.get(team_id_b)
    if not ta or not tb or not ta.city_id or not tb.city_id:
        return None
    cities = {
        c.id: c
        for c in session.query(NbaCity).filter(NbaCity.id.in_([ta.city_id, tb.city_id])).all()
    }
    ca, cb = cities.get(ta.city_id), cities.get(tb.city_id)
    if not ca or not cb:
        return None
    return city_distance_miles(ca, cb)

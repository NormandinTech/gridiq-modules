"""
GridIQ Weather Service — NOAA API Integration
Fetches real-time weather conditions, fire weather alerts, and wind data
for utility service territories. Uses NOAA's free public API (no key required).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("gridiq.weather")

DEFAULT_LAT = 39.7596
DEFAULT_LON = -121.6219

NOAA_BASE = "https://api.weather.gov"
HEADERS = {
    "User-Agent": "GridIQ/1.0 (gridiq.ink; gridiq_support@gridiq.ink)",
    "Accept": "application/geo+json",
}


@dataclass
class WeatherConditions:
    lat: float
    lon: float
    station_id: str
    station_name: str
    timestamp: datetime
    temperature_c: float | None = None
    temperature_f: float | None = None
    humidity_pct: float | None = None
    wind_speed_kmh: float | None = None
    wind_speed_mph: float | None = None
    wind_direction_deg: float | None = None
    wind_direction_str: str | None = None
    wind_gust_kmh: float | None = None
    wind_gust_mph: float | None = None
    fire_weather_index: float | None = None
    red_flag_warning: bool = False
    heat_index_c: float | None = None
    conductor_sag_risk: str = "LOW"
    vegetation_risk_modifier: float = 1.0
    outage_risk: str = "LOW"
    text_description: str = ""
    alerts: list = field(default_factory=list)


def _wind_direction(deg: float | None) -> str:
    if deg is None:
        return "Unknown"
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(deg / 22.5) % 16]


def _fire_weather_index(temp_f: float, humidity: float, wind_mph: float) -> float:
    if humidity <= 0:
        humidity = 1
    return round((temp_f / humidity) * (1 + wind_mph / 10), 1)


def _conductor_sag_risk(temp_f, wind_mph) -> str:
    if temp_f is None:
        return "UNKNOWN"
    score = 0
    if temp_f > 100: score += 3
    elif temp_f > 90: score += 2
    elif temp_f > 80: score += 1
    if wind_mph and wind_mph > 40: score += 3
    elif wind_mph and wind_mph > 25: score += 2
    elif wind_mph and wind_mph > 15: score += 1
    if score >= 5: return "CRITICAL"
    elif score >= 3: return "HIGH"
    elif score >= 1: return "MEDIUM"
    return "LOW"


def _vegetation_risk_modifier(wind_mph, humidity) -> float:
    m = 1.0
    if wind_mph:
        if wind_mph > 40: m += 1.0
        elif wind_mph > 25: m += 0.5
        elif wind_mph > 15: m += 0.2
    if humidity:
        if humidity < 15: m += 0.5
        elif humidity < 25: m += 0.2
    return round(m, 2)


class NOAAWeatherService:
    def __init__(self):
        self._client = None
        self._cache = {}
        self._cache_ttl_seconds = 300

    async def _get_client(self):
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(headers=HEADERS, timeout=httpx.Timeout(10.0), follow_redirects=True)
        return self._client

    async def get_conditions(self, lat=DEFAULT_LAT, lon=DEFAULT_LON) -> WeatherConditions:
        cache_key = f"{lat:.3f},{lon:.3f}"
        now = datetime.now(timezone.utc)
        if cache_key in self._cache:
            cached_at, cached_data = self._cache[cache_key]
            if (now - cached_at).total_seconds() < self._cache_ttl_seconds:
                return cached_data
        logger.info(f"[Weather] Fetching NOAA data for {lat:.4f},{lon:.4f}")
        try:
            client = await self._get_client()
            r = await client.get(f"{NOAA_BASE}/points/{lat:.4f},{lon:.4f}")
            r.raise_for_status()
            point = r.json()
            stations_url = point["properties"]["observationStations"]
            r2 = await client.get(stations_url)
            r2.raise_for_status()
            station = r2.json()["features"][0]
            station_id = station["properties"]["stationIdentifier"]
            station_name = station["properties"]["name"]
            r3 = await client.get(f"{NOAA_BASE}/stations/{station_id}/observations/latest")
            r3.raise_for_status()
            props = r3.json().get("properties", {})
            def val(k):
                v = props.get(k, {})
                return v.get("value") if isinstance(v, dict) else None
            temp_c = val("temperature")
            temp_f = round(temp_c * 9/5 + 32, 1) if temp_c is not None else None
            humidity = val("relativeHumidity")
            wind_kmh = val("windSpeed")
            wind_mph = round(wind_kmh * 0.621371, 1) if wind_kmh is not None else None
            wind_dir = val("windDirection")
            wind_gust_kmh = val("windGust")
            wind_gust_mph = round(wind_gust_kmh * 0.621371, 1) if wind_gust_kmh is not None else None
            fwi = _fire_weather_index(temp_f, humidity, wind_mph) if all(x is not None for x in [temp_f, humidity, wind_mph]) else None
            r4 = await client.get(f"{NOAA_BASE}/alerts/active?point={lat:.4f},{lon:.4f}")
            alerts = []
            if r4.status_code == 200:
                alerts = [{"event": f["properties"].get("event",""), "headline": f["properties"].get("headline",""), "severity": f["properties"].get("severity","")} for f in r4.json().get("features",[])]
            red_flag = any("red flag" in a.get("event","").lower() or "fire weather" in a.get("event","").lower() for a in alerts)
            if not red_flag and temp_f and humidity and wind_mph:
                red_flag = temp_f > 75 and humidity < 25 and wind_mph > 15
            conditions = WeatherConditions(
                lat=lat, lon=lon, station_id=station_id, station_name=station_name, timestamp=now,
                temperature_c=round(temp_c,1) if temp_c is not None else None, temperature_f=temp_f,
                humidity_pct=round(humidity,1) if humidity is not None else None,
                wind_speed_kmh=round(wind_kmh,1) if wind_kmh is not None else None, wind_speed_mph=wind_mph,
                wind_direction_deg=wind_dir, wind_direction_str=_wind_direction(wind_dir),
                wind_gust_kmh=round(wind_gust_kmh,1) if wind_gust_kmh is not None else None, wind_gust_mph=wind_gust_mph,
                fire_weather_index=fwi, red_flag_warning=red_flag,
                conductor_sag_risk=_conductor_sag_risk(temp_f, wind_mph),
                vegetation_risk_modifier=_vegetation_risk_modifier(wind_mph, humidity),
                outage_risk="HIGH" if red_flag else ("MEDIUM" if fwi and fwi > 50 else "LOW"),
                text_description=props.get("textDescription",""), alerts=alerts,
            )
            self._cache[cache_key] = (now, conditions)
            return conditions
        except Exception as e:
            logger.error(f"[Weather] NOAA fetch failed: {e}")
            return WeatherConditions(lat=lat, lon=lon, station_id="ERROR", station_name="Data Unavailable", timestamp=now, text_description=str(e))

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


_weather_service = None

def get_weather_service() -> NOAAWeatherService:
    global _weather_service
    if _weather_service is None:
        _weather_service = NOAAWeatherService()
    return _weather_service

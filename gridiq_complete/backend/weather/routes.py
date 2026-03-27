from fastapi import APIRouter, Query
from backend.weather.service import get_weather_service
weather_router = APIRouter(prefix="/weather", tags=["Weather"])

@weather_router.get("/current")
async def get_current_weather(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    return (await get_weather_service().get_conditions(lat=lat, lon=lon)).__dict__

@weather_router.get("/grid-impact")
async def get_grid_impact(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    c = await get_weather_service().get_conditions(lat=lat, lon=lon)
    return {"red_flag_warning": c.red_flag_warning, "conductor_sag_risk": c.conductor_sag_risk, "outage_risk": c.outage_risk, "fire_weather_index": c.fire_weather_index, "wind_speed_mph": c.wind_speed_mph, "humidity_pct": c.humidity_pct, "temperature_f": c.temperature_f, "station_name": c.station_name, "timestamp": c.timestamp.isoformat()}

@weather_router.get("/alerts")
async def get_weather_alerts(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    c = await get_weather_service().get_conditions(lat=lat, lon=lon)
    return {"count": len(c.alerts), "red_flag_warning": c.red_flag_warning, "alerts": c.alerts}

from fastapi import APIRouter, Query
from backend.satellite.service import get_satellite_service
from backend.vegetation.transmission_lines import TRANSMISSION_LINES

satellite_router = APIRouter(prefix='/satellite', tags=['Satellite Imagery'])

def _corridors():
    return [
        {
            'corridor_id': l['line_id'],
            'corridor_name': l['line_name'],
            'lat': (l['start'].lat + l['end'].lat) / 2,
            'lon': (l['start'].lon + l['end'].lon) / 2,
        }
        for l in TRANSMISSION_LINES
    ]

@satellite_router.get('/ndvi/fleet', summary='NDVI analysis for all transmission corridors')
async def get_fleet_ndvi():
    svc = get_satellite_service()
    summary = await svc.analyze_fleet(_corridors())
    return {
        'total_corridors': summary.total_corridors,
        'high_risk': summary.high_risk,
        'medium_risk': summary.medium_risk,
        'low_risk': summary.low_risk,
        'avg_ndvi': summary.avg_ndvi,
        'generated_at': summary.generated_at,
        'corridors': [c.__dict__ for c in summary.corridors],
    }

@satellite_router.get('/ndvi/corridor', summary='NDVI analysis for a specific corridor')
async def get_corridor_ndvi(
    lat: float = Query(default=39.7621),
    lon: float = Query(default=-121.6219),
    name: str = Query(default='Sierra 230kV'),
):
    svc = get_satellite_service()
    result = await svc.analyze_corridor(
        corridor_id='CUSTOM', corridor_name=name, lat=lat, lon=lon
    )
    return result.__dict__

@satellite_router.get('/summary', summary='Satellite imagery fleet summary')
async def get_satellite_summary():
    svc = get_satellite_service()
    summary = await svc.analyze_fleet(_corridors())
    return {
        'total_corridors': summary.total_corridors,
        'high_risk': summary.high_risk,
        'medium_risk': summary.medium_risk,
        'low_risk': summary.low_risk,
        'avg_ndvi': summary.avg_ndvi,
        'highest_risk': sorted([c.__dict__ for c in summary.corridors], key=lambda x: x['risk_score'], reverse=True)[:3],
        'generated_at': summary.generated_at,
    }

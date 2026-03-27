from fastapi import APIRouter, Query
from backend.lidarservice.service import get_lidar_engine

lidar_router = APIRouter(prefix='/lidar', tags=['LiDAR-as-a-Service'])

@lidar_router.get('/coverage', summary='Check USGS LiDAR coverage for a utility territory')
async def check_coverage(
    utility_name: str = Query(default='Paradise Demo Utility'),
    north: float = Query(default=39.85),
    south: float = Query(default=39.70),
    east: float = Query(default=-121.55),
    west: float = Query(default=-121.70),
):
    bbox = {'north':north,'south':south,'east':east,'west':west}
    engine = get_lidar_engine()
    r = await engine.check_coverage(utility_name, bbox)
    return {
        'report_id': r.report_id,
        'utility_name': r.utility_name,
        'generated_at': r.generated_at,
        'onboarding_status': r.onboarding_status,
        'recommended_action': r.recommended_action,
        'summary': r.summary,
        'best_dataset': r.best_dataset.__dict__ if r.best_dataset else None,
        'all_datasets': [d.__dict__ for d in r.all_datasets],
    }

@lidar_router.get('/status', summary='Quick LiDAR onboarding status check')
async def get_status(
    north: float = Query(default=39.85),
    south: float = Query(default=39.70),
    east: float = Query(default=-121.55),
    west: float = Query(default=-121.70),
    utility_name: str = Query(default='Paradise Demo Utility'),
):
    bbox = {'north':north,'south':south,'east':east,'west':west}
    engine = get_lidar_engine()
    r = await engine.check_coverage(utility_name, bbox)
    return r.summary

from fastapi import APIRouter, Query
from backend.mutualaid.service import get_mutualaid_engine
from backend.assets.fault_detector import fault_detector

mutualaid_router = APIRouter(prefix='/mutualaid', tags=['Mutual Aid Network'])

@mutualaid_router.get('/report', summary='Full mutual aid network report')
async def get_report(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    engine = get_mutualaid_engine()
    faults = fault_detector.get_active_faults()
    r = engine.generate_report(faults, base_lat=lat, base_lon=lon)
    return {
        'report_id': r.report_id,
        'generated_at': r.generated_at,
        'summary': r.summary,
        'active_requests': [x.__dict__ for x in r.active_requests],
        'utility_statuses': [s.__dict__ for s in r.utility_statuses],
    }

@mutualaid_router.get('/summary', summary='Mutual aid network summary')
async def get_summary(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    engine = get_mutualaid_engine()
    faults = fault_detector.get_active_faults()
    r = engine.generate_report(faults, base_lat=lat, base_lon=lon)
    return r.summary

@mutualaid_router.get('/available', summary='Utilities available to send aid')
async def get_available(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    engine = get_mutualaid_engine()
    faults = fault_detector.get_active_faults()
    r = engine.generate_report(faults, base_lat=lat, base_lon=lon)
    available = [s for s in r.utility_statuses if s.can_send_aid]
    return {'count': len(available), 'utilities': [s.__dict__ for s in available]}

@mutualaid_router.get('/requests', summary='Active mutual aid requests')
async def get_requests(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    engine = get_mutualaid_engine()
    faults = fault_detector.get_active_faults()
    r = engine.generate_report(faults, base_lat=lat, base_lon=lon)
    return {'count': len(r.active_requests), 'requests': [x.__dict__ for x in r.active_requests]}

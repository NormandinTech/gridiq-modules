from fastapi import APIRouter, Query
from backend.crew.service import get_crew_engine
from backend.assets.fault_detector import fault_detector
from backend.predictive.service import get_scoring_engine
from backend.weather.service import get_weather_service

crew_router = APIRouter(prefix='/crew', tags=['Crew Scheduling'])

def _preds():
    e = get_scoring_engine()
    assets = []
    seen = set()
    for aid in fault_detector._history.keys():
        h = list(fault_detector._history[aid])
        last = h[-1].get('telemetry',{}) if h else {}
        assets.append({'asset_id':aid,'asset_name':last.get('asset_name',aid),'asset_type':last.get('asset_type','default'),'asset_meta':{}})
        seen.add(aid)
    for f in fault_detector.get_active_faults():
        if f.asset_id not in seen:
            assets.append({'asset_id':f.asset_id,'asset_name':f.asset_name,'asset_type':f.asset_type,'asset_meta':{}})
    return e.score_fleet(assets, fault_detector)

@crew_router.get('/schedule', summary='Generate optimized crew schedule for today')
async def get_schedule(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    weather = await get_weather_service().get_conditions(lat=lat, lon=lon)
    preds = _preds()
    faults = fault_detector.get_active_faults()
    engine = get_crew_engine()
    work_orders = engine.generate_work_orders(preds, faults, weather)
    report = engine.optimize_schedule(work_orders)
    return {
        'report_id': report.report_id,
        'schedule_date': report.schedule_date,
        'generated_at': report.generated_at,
        'summary': report.summary,
        'optimization_notes': report.optimization_notes,
        'crew_schedules': [
            {
                'crew_id': cs.crew_id,
                'crew_name': cs.crew_name,
                'crew_size': cs.crew_size,
                'specialty': cs.specialty,
                'total_jobs': cs.total_jobs,
                'total_hours': cs.total_hours,
                'total_drive_time_minutes': cs.total_drive_time_minutes,
                'start_location': cs.start_location,
                'work_orders': [wo.__dict__ for wo in cs.work_orders],
            }
            for cs in report.crew_schedules
        ],
        'unscheduled_orders': [wo.__dict__ for wo in report.unscheduled_orders],
    }

@crew_router.get('/summary', summary='Crew scheduling summary')
async def get_crew_summary(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    weather = await get_weather_service().get_conditions(lat=lat, lon=lon)
    preds = _preds()
    faults = fault_detector.get_active_faults()
    engine = get_crew_engine()
    work_orders = engine.generate_work_orders(preds, faults, weather)
    report = engine.optimize_schedule(work_orders)
    return report.summary

@crew_router.get('/work-orders', summary='All work orders ranked by priority')
async def get_work_orders(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    weather = await get_weather_service().get_conditions(lat=lat, lon=lon)
    preds = _preds()
    faults = fault_detector.get_active_faults()
    engine = get_crew_engine()
    work_orders = engine.generate_work_orders(preds, faults, weather)
    return {'total': len(work_orders), 'work_orders': [wo.__dict__ for wo in work_orders]}

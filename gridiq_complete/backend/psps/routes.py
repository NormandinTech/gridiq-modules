from fastapi import APIRouter, Query
from backend.psps.service import get_psps_engine
from backend.weather.service import get_weather_service
from backend.assets.fault_detector import fault_detector
from backend.predictive.service import get_scoring_engine

psps_router = APIRouter(prefix='/psps', tags=['PSPS Decision Support'])

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

@psps_router.get('/report')
async def get_report(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219), utility_name: str = Query(default='NormandinTECH Demo Utility')):
    w = await get_weather_service().get_conditions(lat=lat, lon=lon)
    r = get_psps_engine().report(weather=w, faults=fault_detector.get_active_faults(), preds=_preds(), utility=utility_name)
    return {'report_id':r.report_id,'generated_at':r.generated_at,'utility_name':r.utility_name,'weather_station':r.weather_station,'red_flag_active':r.red_flag_active,'summary':r.summary,'recommended_actions':r.recommended_actions,'regulatory_notes':r.regulatory_notes,'circuits':[c.__dict__ for c in r.circuits]}

@psps_router.get('/summary')
async def get_summary(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    w = await get_weather_service().get_conditions(lat=lat, lon=lon)
    r = get_psps_engine().report(weather=w, faults=fault_detector.get_active_faults(), preds=_preds())
    return {'summary':r.summary,'recommended_actions':r.recommended_actions}

@psps_router.get('/circuits')
async def get_circuits(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    w = await get_weather_service().get_conditions(lat=lat, lon=lon)
    r = get_psps_engine().report(weather=w, faults=fault_detector.get_active_faults(), preds=_preds())
    return {'circuits':[{'circuit_id':c.circuit_id,'circuit_name':c.circuit_name,'composite_score':c.composite_score,'recommendation':c.recommendation,'recommendation_color':c.recommendation_color,'customers_affected':c.customers_affected,'justification':c.justification,'wind_speed_mph':c.wind_speed_mph,'humidity_pct':c.humidity_pct,'red_flag_warning':c.red_flag_warning} for c in r.circuits]}

from fastapi import APIRouter, Query
from backend.outage.service import get_outage_engine
from backend.assets.fault_detector import fault_detector
from backend.weather.service import get_weather_service
from backend.predictive.service import get_scoring_engine

outage_router = APIRouter(prefix='/outage', tags=['Outage Prediction'])

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

@outage_router.get('/predict', summary='Outage predictions for all circuits')
async def get_predictions(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    weather = await get_weather_service().get_conditions(lat=lat, lon=lon)
    preds = _preds()
    r = get_outage_engine().predict_fleet(fault_detector, weather, preds)
    return {
        'report_id': r.report_id,
        'generated_at': r.generated_at,
        'summary': r.summary,
        'predictions': [p.__dict__ for p in r.predictions],
    }

@outage_router.get('/summary', summary='Outage prediction summary')
async def get_summary(lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    weather = await get_weather_service().get_conditions(lat=lat, lon=lon)
    preds = _preds()
    r = get_outage_engine().predict_fleet(fault_detector, weather, preds)
    return r.summary

@outage_router.get('/circuit/{circuit_id}', summary='Outage prediction for specific circuit')
async def get_circuit_prediction(circuit_id: str, lat: float = Query(default=39.7596), lon: float = Query(default=-121.6219)):
    weather = await get_weather_service().get_conditions(lat=lat, lon=lon)
    preds = _preds()
    p = get_outage_engine().predict_circuit(circuit_id, fault_detector, weather, preds)
    return p.__dict__

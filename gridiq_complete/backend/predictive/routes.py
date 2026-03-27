from fastapi import APIRouter, Query
from backend.predictive.service import get_scoring_engine
from backend.assets.fault_detector import fault_detector

predict_router = APIRouter(prefix="/predict", tags=["Predictive Scoring"])

def _assets():
    assets = []
    seen = set()
    for aid in fault_detector._history.keys():
        h = list(fault_detector._history[aid])
        last = h[-1].get("telemetry",{}) if h else {}
        assets.append({"asset_id":aid,"asset_name":last.get("asset_name",aid),"asset_type":last.get("asset_type","default"),"asset_meta":{}})
        seen.add(aid)
    for f in fault_detector.get_active_faults():
        if f.asset_id not in seen:
            assets.append({"asset_id":f.asset_id,"asset_name":f.asset_name,"asset_type":f.asset_type,"asset_meta":{}})
            seen.add(f.asset_id)
    return assets

@predict_router.get("/scores")
async def get_all_scores():
    e = get_scoring_engine()
    s = e.score_fleet(_assets(), fault_detector)
    return {"summary":e.get_fleet_summary(s),"scores":[x.__dict__ for x in s]}

@predict_router.get("/rankings")
async def get_rankings(limit: int = Query(default=10, le=50)):
    e = get_scoring_engine()
    s = e.score_fleet(_assets(), fault_detector)
    return {"rankings":[{"rank":i+1,"asset_id":x.asset_id,"asset_name":x.asset_name,"score_30d":x.score_30d,"score_60d":x.score_60d,"score_90d":x.score_90d,"risk_level":x.risk_level,"primary_driver":x.primary_driver,"recommended_action":x.recommended_action,"maintenance_window":x.maintenance_window,"estimated_failure_date":x.estimated_failure_date} for i,x in enumerate(s[:limit])]}

@predict_router.get("/summary")
async def get_summary():
    e = get_scoring_engine()
    s = e.score_fleet(_assets(), fault_detector)
    return e.get_fleet_summary(s)

@predict_router.get("/scores/{asset_id}")
async def get_asset_score(asset_id: str):
    e = get_scoring_engine()
    from collections import deque
    h = list(fault_detector._history.get(asset_id, deque()))
    af = fault_detector.get_active_faults(asset_id=asset_id)
    if not h and not af:
        return {"error": f"No data for {asset_id}"}
    name = h[-1].get("telemetry",{}).get("asset_name",asset_id) if h else (af[0].asset_name if af else asset_id)
    atype = h[-1].get("telemetry",{}).get("asset_type","default") if h else (af[0].asset_type if af else "default")
    return e.score_asset(asset_id=asset_id,asset_name=name,asset_type=atype,history=h,active_faults=af).__dict__

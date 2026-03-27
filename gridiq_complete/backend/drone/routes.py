from fastapi import APIRouter, Query
from backend.drone.service import get_drone_engine

drone_router = APIRouter(prefix='/drone', tags=['Drone Data Ingestion'])

@drone_router.get('/report', summary='Full drone inspection report')
async def get_report():
    r = get_drone_engine().get_report()
    return {
        'report_id': r.report_id,
        'generated_at': r.generated_at,
        'summary': r.summary,
        'flights': [
            {
                'flight_id': f.flight_id,
                'drone_id': f.drone_id,
                'pilot': f.pilot,
                'date': f.date,
                'duration_minutes': f.duration_minutes,
                'distance_km': f.distance_km,
                'assets_inspected': f.assets_inspected,
                'total_observations': f.total_observations,
                'critical_findings': f.critical_findings,
                'high_findings': f.high_findings,
                'generated_work_orders': f.generated_work_orders,
                'status': f.status,
                'observations': [o.__dict__ for o in f.observations],
            }
            for f in r.flights
        ],
    }

@drone_router.get('/summary', summary='Drone inspection summary')
async def get_summary():
    r = get_drone_engine().get_report()
    return r.summary

@drone_router.get('/observations', summary='All drone observations ranked by severity')
async def get_observations():
    r = get_drone_engine().get_report()
    all_obs = [o.__dict__ for f in r.flights for o in f.observations]
    order = {'CRITICAL':0,'HIGH':1,'MEDIUM':2,'LOW':3}
    all_obs.sort(key=lambda o: order.get(o['severity'],4))
    return {'total': len(all_obs), 'observations': all_obs}

@drone_router.get('/flights', summary='List all drone flights')
async def get_flights():
    r = get_drone_engine().get_report()
    return {'total': r.total_flights, 'flights': [{'flight_id':f.flight_id,'date':f.date,'pilot':f.pilot,'assets':f.assets_inspected,'critical':f.critical_findings,'work_orders':f.generated_work_orders} for f in r.flights]}

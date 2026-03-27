from __future__ import annotations
import logging
import hashlib
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger('gridiq.drone')

@dataclass
class DroneObservation:
    obs_id: str
    flight_id: str
    timestamp: str
    lat: float
    lon: float
    altitude_m: float
    asset_id: Optional[str]
    observation_type: str
    severity: str
    description: str
    confidence: float
    image_url: Optional[str]
    thermal_anomaly: bool
    vegetation_clearance_m: Optional[float]
    recommended_action: str

@dataclass
class DroneFlight:
    flight_id: str
    drone_id: str
    pilot: str
    date: str
    duration_minutes: float
    distance_km: float
    waypoints: int
    assets_inspected: List[str]
    observations: List[DroneObservation]
    total_observations: int
    critical_findings: int
    high_findings: int
    status: str
    generated_work_orders: int
    ingested_at: str

@dataclass
class DroneReport:
    report_id: str
    generated_at: str
    total_flights: int
    total_observations: int
    critical_findings: int
    work_orders_generated: int
    flights: List[DroneFlight]
    summary: Dict[str, Any]

SIMULATED_FLIGHTS = [
    {
        'flight_id':'FLT-2026-0342',
        'drone_id':'DJI-M300-RTK-001',
        'pilot':'J. Martinez',
        'date':'2026-03-26',
        'duration_minutes':47.3,
        'distance_km':18.2,
        'waypoints':24,
        'assets':['txn-001','txn-002','txn-003'],
        'observations':[
            {'type':'vegetation_encroachment','severity':'CRITICAL','desc':'Oak tree crown within 0.6m of conductor at span 14 — immediate trimming required','lat':39.7621,'lon':-121.6219,'asset':'txn-001','clearance':0.6,'thermal':False,'confidence':0.97,'action':'Emergency vegetation trim — dispatch crew within 24 hours'},
            {'type':'insulator_damage','severity':'HIGH','desc':'Cracked insulator cap visible on tower 22 crossarm — partial discharge risk','lat':39.7589,'lon':-121.6180,'asset':'txn-002','clearance':None,'thermal':True,'confidence':0.88,'action':'Schedule insulator replacement within 7 days — monitor PD levels'},
            {'type':'conductor_sag','severity':'HIGH','desc':'Conductor sag 15% above design spec between towers 28-31 — thermal expansion factor','lat':39.7612,'lon':-121.6150,'asset':'txn-003','clearance':None,'thermal':True,'confidence':0.91,'action':'Thermal sag analysis required — check loading schedule'},
            {'type':'corrosion','severity':'MEDIUM','desc':'Surface corrosion on tower 14 cross-arm bolts — early stage','lat':39.7621,'lon':-121.6219,'asset':'txn-001','clearance':None,'thermal':False,'confidence':0.79,'action':'Apply corrosion inhibitor at next scheduled maintenance'},
        ],
    },
    {
        'flight_id':'FLT-2026-0341',
        'drone_id':'DJI-M300-RTK-002',
        'pilot':'K. Thompson',
        'date':'2026-03-25',
        'duration_minutes':31.8,
        'distance_km':11.4,
        'waypoints':16,
        'assets':['bess-002','solar-001'],
        'observations':[
            {'type':'thermal_hotspot','severity':'CRITICAL','desc':'BESS rack 3 cell cluster showing 42C above ambient — thermal runaway precursor confirmed visually','lat':39.7543,'lon':-121.6090,'asset':'bess-002','clearance':None,'thermal':True,'confidence':0.96,'action':'Immediate BESS rack 3 isolation — dispatch substation crew now'},
            {'type':'soiling','severity':'LOW','desc':'Solar panel soiling loss estimated 8% from drone visual — consistent with AMI data','lat':39.7621,'lon':-121.6350,'asset':'solar-001','clearance':None,'thermal':False,'confidence':0.82,'action':'Schedule panel cleaning — next available maintenance window'},
        ],
    },
]

class DroneIngestionEngine:
    def __init__(self):
        self._flights: List[DroneFlight] = []
        self._ingested = False

    def _make_obs(self, data: Dict, flight_id: str, idx: int) -> DroneObservation:
        now = datetime.now(timezone.utc)
        return DroneObservation(
            obs_id='OBS-{}-{:03d}'.format(flight_id, idx),
            flight_id=flight_id,
            timestamp=(now - timedelta(hours=random.randint(1,24))).isoformat(),
            lat=data['lat'], lon=data['lon'],
            altitude_m=round(random.uniform(30,80),1),
            asset_id=data.get('asset'),
            observation_type=data['type'],
            severity=data['severity'],
            description=data['desc'],
            confidence=data['confidence'],
            image_url='https://drone-storage.gridiq.ink/flights/{}/obs-{:03d}.jpg'.format(flight_id, idx),
            thermal_anomaly=data['thermal'],
            vegetation_clearance_m=data.get('clearance'),
            recommended_action=data['action'],
        )

    def _make_flight(self, data: Dict) -> DroneFlight:
        now = datetime.now(timezone.utc)
        obs = [self._make_obs(o, data['flight_id'], i+1) for i,o in enumerate(data['observations'])]
        crit = sum(1 for o in obs if o.severity=='CRITICAL')
        high = sum(1 for o in obs if o.severity=='HIGH')
        wo_count = crit * 2 + high
        return DroneFlight(
            flight_id=data['flight_id'],
            drone_id=data['drone_id'],
            pilot=data['pilot'],
            date=data['date'],
            duration_minutes=data['duration_minutes'],
            distance_km=data['distance_km'],
            waypoints=data['waypoints'],
            assets_inspected=data['assets'],
            observations=obs,
            total_observations=len(obs),
            critical_findings=crit,
            high_findings=high,
            status='processed',
            generated_work_orders=wo_count,
            ingested_at=now.isoformat(),
        )

    def _ensure_loaded(self):
        if not self._ingested:
            self._flights = [self._make_flight(f) for f in SIMULATED_FLIGHTS]
            self._ingested = True

    def get_report(self) -> DroneReport:
        self._ensure_loaded()
        now = datetime.now(timezone.utc)
        total_obs = sum(f.total_observations for f in self._flights)
        total_crit = sum(f.critical_findings for f in self._flights)
        total_wo = sum(f.generated_work_orders for f in self._flights)
        summary = {
            'total_flights': len(self._flights),
            'total_observations': total_obs,
            'critical_findings': total_crit,
            'high_findings': sum(f.high_findings for f in self._flights),
            'work_orders_generated': total_wo,
            'assets_inspected': list(set(a for f in self._flights for a in f.assets_inspected)),
            'thermal_anomalies': sum(1 for f in self._flights for o in f.observations if o.thermal_anomaly),
            'vegetation_violations': sum(1 for f in self._flights for o in f.observations if o.vegetation_clearance_m and o.vegetation_clearance_m < 1.5),
            'drones_active': len(set(f.drone_id for f in self._flights)),
            'last_flight_date': max(f.date for f in self._flights),
        }
        return DroneReport(
            report_id='DR-{}'.format(now.strftime('%Y%m%d-%H%M%S')),
            generated_at=now.isoformat(),
            total_flights=len(self._flights),
            total_observations=total_obs,
            critical_findings=total_crit,
            work_orders_generated=total_wo,
            flights=self._flights,
            summary=summary,
        )

    def ingest_flight(self, flight_data: Dict) -> DroneFlight:
        flight = self._make_flight(flight_data)
        self._flights.append(flight)
        logger.info('Ingested drone flight {} — {} observations'.format(flight.flight_id, flight.total_observations))
        return flight

_engine = None
def get_drone_engine():
    global _engine
    if _engine is None: _engine = DroneIngestionEngine()
    return _engine

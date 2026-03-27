from __future__ import annotations
import logging
import random
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger('gridiq.mutualaid')

UTILITY_NETWORK = [
    {'utility_id':'PLUMAS-SIERRA','name':'Plumas-Sierra REC','state':'CA','lat':39.9300,'lon':-120.9500,'members':5000,'crews':4,'specialty':['transmission','vegetation'],'contact':'bmarshall@psrec.coop'},
    {'utility_id':'LASSEN-MUD','name':'Lassen MUD','state':'CA','lat':40.3785,'lon':-120.6505,'members':8200,'crews':6,'specialty':['distribution','substation'],'contact':'n.dominguez@lmud.org'},
    {'utility_id':'REDDING-ELEC','name':'City of Redding Electric','state':'CA','lat':40.5865,'lon':-122.3917,'members':42000,'crews':18,'specialty':['transmission','substation','vegetation'],'contact':'nick.zettel@cityofredding.gov'},
    {'utility_id':'TRINITY-PUD','name':'Trinity PUD','state':'CA','lat':40.7282,'lon':-122.9347,'members':3200,'crews':3,'specialty':['distribution','vegetation'],'contact':'info@trinitypud.com'},
    {'utility_id':'MODOC-PUD','name':'Modoc PUD','state':'CA','lat':41.5200,'lon':-120.3700,'members':2100,'crews':2,'specialty':['distribution'],'contact':'info@modocpud.org'},
    {'utility_id':'FALL-RIVER','name':'Fall River Rural Electric','state':'CA','lat':41.0000,'lon':-121.4300,'members':4800,'crews':4,'specialty':['transmission','vegetation'],'contact':'bryan.case@fallriverelectric.com'},
    {'utility_id':'SURPRISE-VALLEY','name':'Surprise Valley Electrification','state':'CA','lat':41.5300,'lon':-120.0600,'members':1800,'crews':2,'specialty':['distribution'],'contact':'info@surprisevalley.coop'},
]

@dataclass
class UtilityStatus:
    utility_id: str
    utility_name: str
    state: str
    lat: float
    lon: float
    members_served: int
    total_crews: int
    available_crews: int
    active_incidents: int
    capacity_pct: float
    can_send_aid: bool
    needs_aid: bool
    specialties: List[str]
    contact: str
    distance_km: float
    estimated_response_hours: float
    status_color: str
    last_updated: str

@dataclass
class MutualAidRequest:
    request_id: str
    requesting_utility: str
    event_type: str
    event_severity: str
    customers_affected: int
    resources_needed: List[Dict]
    location: str
    issued_at: str
    expires_at: str
    status: str
    responding_utilities: List[str]

@dataclass
class MutualAidReport:
    report_id: str
    generated_at: str
    network_utilities: int
    utilities_available: int
    utilities_at_capacity: int
    total_available_crews: int
    active_requests: List[MutualAidRequest]
    utility_statuses: List[UtilityStatus]
    summary: Dict[str, Any]

import math
def _haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2-lat1); dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return 2*R*math.asin(math.sqrt(a))

def _response_hours(km: float) -> float:
    return round(1.0 + km/80, 1)

def _sim_status(utility: Dict, base_lat: float, base_lon: float) -> UtilityStatus:
    seed = int(hashlib.md5(utility['utility_id'].encode()).hexdigest(),16) % 10000
    random.seed(seed)
    active = random.randint(0, 3)
    available = max(0, utility['crews'] - active - random.randint(0,1))
    capacity = round((utility['crews'] - available) / max(utility['crews'],1) * 100, 1)
    can_send = available >= 1 and capacity < 80
    needs = capacity > 85 or active > utility['crews'] * 0.7
    color = 'red' if needs else ('yellow' if capacity > 60 else 'green')
    dist = _haversine(base_lat, base_lon, utility['lat'], utility['lon'])
    return UtilityStatus(
        utility_id=utility['utility_id'],
        utility_name=utility['name'],
        state=utility['state'],
        lat=utility['lat'], lon=utility['lon'],
        members_served=utility['members'],
        total_crews=utility['crews'],
        available_crews=available,
        active_incidents=active,
        capacity_pct=capacity,
        can_send_aid=can_send,
        needs_aid=needs,
        specialties=utility['specialty'],
        contact=utility['contact'],
        distance_km=round(dist,1),
        estimated_response_hours=_response_hours(dist),
        status_color=color,
        last_updated=datetime.now(timezone.utc).isoformat(),
    )

class MutualAidEngine:
    def generate_report(self, active_faults: list, base_lat=39.7596, base_lon=-121.6219) -> MutualAidReport:
        now = datetime.now(timezone.utc)
        statuses = [_sim_status(u, base_lat, base_lon) for u in UTILITY_NETWORK]
        statuses.sort(key=lambda s: s.distance_km)

        available = [s for s in statuses if s.can_send_aid]
        at_capacity = [s for s in statuses if s.needs_aid]
        total_avail_crews = sum(s.available_crews for s in available)

        active_requests = []
        crit_faults = [f for f in active_faults if f.severity.value=='critical']
        if len(crit_faults) >= 2:
            req = MutualAidRequest(
                request_id='MAR-{}'.format(now.strftime('%Y%m%d-%H%M')),
                requesting_utility='GridIQ Demo Utility — Paradise CA',
                event_type='Multiple Critical Equipment Failures',
                event_severity='HIGH',
                customers_affected=sum(u['members'] for u in UTILITY_NETWORK[:1]),
                resources_needed=[
                    {'type':'Transmission crew','count':2,'specialty':'transmission','duration_days':3},
                    {'type':'Substation technician','count':1,'specialty':'substation','duration_days':2},
                ],
                location='Paradise CA — Sierra 230kV corridor',
                issued_at=now.isoformat(),
                expires_at=(now+timedelta(hours=48)).isoformat(),
                status='active',
                responding_utilities=[s.utility_id for s in available[:2]],
            )
            active_requests.append(req)

        summary = {
            'network_utilities': len(statuses),
            'utilities_available': len(available),
            'utilities_at_capacity': len(at_capacity),
            'total_available_crews': total_avail_crews,
            'active_aid_requests': len(active_requests),
            'nearest_available_utility': available[0].utility_name if available else None,
            'nearest_response_hours': available[0].estimated_response_hours if available else None,
            'network_coverage_states': list(set(u['state'] for u in UTILITY_NETWORK)),
            'total_network_members': sum(u['members'] for u in UTILITY_NETWORK),
        }

        return MutualAidReport(
            report_id='MAR-{}'.format(now.strftime('%Y%m%d-%H%M%S')),
            generated_at=now.isoformat(),
            network_utilities=len(statuses),
            utilities_available=len(available),
            utilities_at_capacity=len(at_capacity),
            total_available_crews=total_avail_crews,
            active_requests=active_requests,
            utility_statuses=statuses,
            summary=summary,
        )

_engine = None
def get_mutualaid_engine():
    global _engine
    if _engine is None: _engine = MutualAidEngine()
    return _engine

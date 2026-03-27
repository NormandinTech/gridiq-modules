from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
logger = logging.getLogger('gridiq.psps')

CIRCUITS = [
    {'circuit_id':'CKT-001','circuit_name':'Sierra 230kV Feeder A','voltage_kv':230,'customers_affected':4200,'lat':39.7621,'lon':-121.6219},
    {'circuit_id':'CKT-002','circuit_name':'North Feeder Magalia','voltage_kv':115,'customers_affected':1850,'lat':39.8100,'lon':-121.5780},
    {'circuit_id':'CKT-003','circuit_name':'West Feeder Paradise Ridge','voltage_kv':115,'customers_affected':3100,'lat':39.7580,'lon':-121.6500},
    {'circuit_id':'CKT-004','circuit_name':'East Transmission Chico Tie','voltage_kv':345,'customers_affected':8900,'lat':39.7200,'lon':-121.5500},
    {'circuit_id':'CKT-005','circuit_name':'Jarbo Gap 115kV','voltage_kv':115,'customers_affected':1100,'lat':39.7900,'lon':-121.5200},
    {'circuit_id':'CKT-006','circuit_name':'Pulga Line Camp Creek','voltage_kv':230,'customers_affected':280,'lat':39.8200,'lon':-121.4700},
]

@dataclass
class CircuitRisk:
    circuit_id: str
    circuit_name: str
    voltage_kv: float
    customers_affected: int
    composite_score: float = 0.0
    recommendation: str = 'MONITOR'
    recommendation_color: str = 'green'
    justification: List[str] = field(default_factory=list)
    wind_speed_mph: Optional[float] = None
    humidity_pct: Optional[float] = None
    red_flag_warning: bool = False
    weather_score: float = 0.0
    vegetation_score: float = 0.0
    fault_score: float = 0.0
    predictive_score: float = 0.0
    estimated_outage_hours: Optional[float] = None
    regulatory_justification: str = ''
    last_updated: str = ''

@dataclass
class PSPSReport:
    report_id: str
    generated_at: str
    utility_name: str
    weather_station: str
    red_flag_active: bool
    circuits: List[CircuitRisk]
    summary: Dict[str, Any]
    recommended_actions: List[str]
    regulatory_notes: str

class PSPSDecisionEngine:
    def _wscore(self, w):
        s = 0; f = []
        if w is None: return 30.0, ['Weather unavailable']
        wind = w.wind_speed_mph or 0; hum = w.humidity_pct or 50
        if wind > 60: s += 45; f.append('Extreme winds: {}mph'.format(wind))
        elif wind > 40: s += 35; f.append('High winds: {}mph'.format(wind))
        elif wind > 25: s += 20; f.append('Elevated winds: {}mph'.format(wind))
        elif wind > 15: s += 10; f.append('Moderate winds: {}mph'.format(wind))
        if hum < 10: s += 25; f.append('Critical humidity: {}%'.format(hum))
        elif hum < 15: s += 18; f.append('Very low humidity: {}%'.format(hum))
        elif hum < 25: s += 10; f.append('Low humidity: {}%'.format(hum))
        if w.red_flag_warning: s += 20; f.append('Red Flag Warning active')
        fwi = w.fire_weather_index or 0
        if fwi > 100: s += 15; f.append('Extreme FWI: {}'.format(fwi))
        elif fwi > 75: s += 8; f.append('High FWI: {}'.format(fwi))
        return round(min(s,100),1), f

    def _fscore(self, faults):
        s = 0; f = []
        txn = [x for x in faults if 'txn' in x.asset_id.lower()]
        crit = [x for x in txn if x.severity.value=='critical']
        high = [x for x in txn if x.severity.value=='high']
        if crit: s += len(crit)*30; f.append('{} critical fault(s) on transmission'.format(len(crit)))
        if high: s += len(high)*15; f.append('{} high fault(s) on transmission'.format(len(high)))
        return round(min(s,100),1), f

    def _pscore(self, preds):
        if not preds: return 15.0, []
        top = sorted(preds, key=lambda x: x.score_30d, reverse=True)[0]
        return round(min(top.score_30d,100),1), ['Highest asset risk: {}% ({})'.format(top.score_30d, top.asset_name)]

    def _rec(self, score, red_flag):
        if score >= 75 or (score >= 60 and red_flag): return 'DE-ENERGIZE', 'red'
        elif score >= 55: return 'WARNING', 'orange'
        elif score >= 35: return 'WATCH', 'yellow'
        return 'MONITOR', 'green'

    def analyze(self, circuit, weather, faults, preds):
        ws, wf = self._wscore(weather)
        fs, ff = self._fscore(faults)
        ps, pf = self._pscore(preds)
        vs = 35.0; vf = ['Default vegetation risk']
        comp = round(ws*0.40 + vs*0.30 + fs*0.20 + ps*0.10, 1)
        red = getattr(weather,'red_flag_warning',False) if weather else False
        rec, color = self._rec(comp, red)
        outage = 4.0+(comp-75)/5 if rec=='DE-ENERGIZE' else None
        return CircuitRisk(
            circuit_id=circuit['circuit_id'], circuit_name=circuit['circuit_name'],
            voltage_kv=circuit['voltage_kv'], customers_affected=circuit['customers_affected'],
            composite_score=comp, recommendation=rec, recommendation_color=color,
            justification=(wf+vf+ff+pf)[:5],
            wind_speed_mph=getattr(weather,'wind_speed_mph',None),
            humidity_pct=getattr(weather,'humidity_pct',None),
            red_flag_warning=red, weather_score=ws, vegetation_score=vs,
            fault_score=fs, predictive_score=ps, estimated_outage_hours=outage,
            regulatory_justification='Per CPUC GO 95 and NERC FAC-003.',
            last_updated=datetime.now(timezone.utc).isoformat()
        )

    def report(self, weather, faults, preds, circuits=None, utility='NormandinTECH Demo Utility'):
        circuits = circuits or CIRCUITS
        now = datetime.now(timezone.utc)
        analyzed = sorted([self.analyze(c,weather,faults,preds) for c in circuits], key=lambda x: x.composite_score, reverse=True)
        de = [c for c in analyzed if c.recommendation=='DE-ENERGIZE']
        warn = [c for c in analyzed if c.recommendation=='WARNING']
        watch = [c for c in analyzed if c.recommendation=='WATCH']
        mon = [c for c in analyzed if c.recommendation=='MONITOR']
        affected = sum(c.customers_affected for c in de)
        red = getattr(weather,'red_flag_warning',False) if weather else False
        station = getattr(weather,'station_name','Unknown') if weather else 'Unavailable'
        actions = []
        if de: actions.append('IMMEDIATE: De-energize {} circuit(s) - {:,} customers affected'.format(len(de), affected))
        if warn: actions.append('PREPARE: {} circuit(s) at WARNING'.format(len(warn)))
        if watch: actions.append('MONITOR: {} circuit(s) at WATCH'.format(len(watch)))
        if red: actions.append('Red Flag active - emergency ops protocol')
        if not actions: actions.append('All circuits normal')
        summary = {'total_circuits':len(analyzed),'de_energize':len(de),'warning':len(warn),'watch':len(watch),'monitor':len(mon),'customers_at_risk':affected,'red_flag_warning':red,'highest_risk_circuit':analyzed[0].circuit_name,'highest_composite_score':analyzed[0].composite_score,'weather_station':station}
        ts = now.strftime('%Y%m%d-%H%M%S')
        return PSPSReport(report_id='PSPS-'+ts, generated_at=now.isoformat(), utility_name=utility, weather_station=station, red_flag_active=red, circuits=analyzed, summary=summary, recommended_actions=actions, regulatory_notes='Per CPUC GO 95 and NERC FAC-003. All decisions must be reviewed by a qualified engineer.')

_engine = None
def get_psps_engine():
    global _engine
    if _engine is None: _engine = PSPSDecisionEngine()
    return _engine

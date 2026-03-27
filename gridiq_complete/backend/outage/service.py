from __future__ import annotations
import logging
import math
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger('gridiq.outage')

CIRCUIT_ASSETS = {
    'CKT-001': ['txn-001','txn-002','txn-003'],
    'CKT-002': ['wind-001','wind-002'],
    'CKT-003': ['solar-001','solar-002'],
    'CKT-004': ['bess-001','bess-002'],
    'CKT-005': ['gas-001'],
    'CKT-006': ['hydro-001','dam-001'],
    'CKT-007': ['ami-001','ami-002'],
}

CIRCUIT_CUSTOMERS = {
    'CKT-001': 4200, 'CKT-002': 1850, 'CKT-003': 3100,
    'CKT-004': 8900, 'CKT-005': 620, 'CKT-006': 1100, 'CKT-007': 450,
}

CIRCUIT_NAMES = {
    'CKT-001': 'Sierra 230kV Feeder A',
    'CKT-002': 'North Feeder Magalia',
    'CKT-003': 'West Feeder Paradise Ridge',
    'CKT-004': 'East Transmission Chico Tie',
    'CKT-005': 'South Feeder Butte Creek Canyon',
    'CKT-006': 'Jarbo Gap 115kV',
    'CKT-007': 'Feather River Canyon Tie',
}

HISTORICAL_BASE_RATES = {
    'CKT-001': 0.08, 'CKT-002': 0.05, 'CKT-003': 0.04,
    'CKT-004': 0.06, 'CKT-005': 0.07, 'CKT-006': 0.09, 'CKT-007': 0.06,
}

@dataclass
class OutagePrediction:
    circuit_id: str
    circuit_name: str
    customers_at_risk: int
    prob_24h: float
    prob_72h: float
    prob_168h: float
    risk_level: str
    primary_failure_mode: str
    most_likely_asset: str
    contributing_factors: List[str]
    estimated_duration_hours: float
    estimated_customer_hours: float
    recommended_preemptive_action: str
    confidence: float
    predicted_at: str

@dataclass
class OutageReport:
    report_id: str
    generated_at: str
    total_circuits: int
    high_risk_24h: int
    total_customers_at_risk: int
    predictions: List[OutagePrediction]
    summary: Dict[str, Any]

def _fault_contribution(asset_ids: List[str], fault_detector) -> tuple:
    score = 0.0
    factors = []
    worst_asset = None
    worst_score = 0
    for aid in asset_ids:
        faults = fault_detector.get_active_faults(asset_id=aid)
        crit = [f for f in faults if f.severity.value == 'critical']
        high = [f for f in faults if f.severity.value == 'high']
        asset_score = len(crit) * 0.25 + len(high) * 0.10 + len(faults) * 0.02
        if asset_score > worst_score:
            worst_score = asset_score
            worst_asset = faults[0].asset_name if faults else aid
        score += asset_score
        if crit:
            factors.append('{} critical fault(s) on {}'.format(len(crit), aid))
        elif high:
            factors.append('{} high fault(s) on {}'.format(len(high), aid))
    return min(score, 0.50), factors, worst_asset

def _weather_contribution(weather) -> tuple:
    if weather is None:
        return 0.05, []
    score = 0.0
    factors = []
    wind = weather.wind_speed_mph or 0
    hum = weather.humidity_pct or 50
    if wind > 40: score += 0.20; factors.append('High winds {}mph'.format(wind))
    elif wind > 25: score += 0.10; factors.append('Elevated winds {}mph'.format(wind))
    if hum < 15: score += 0.10; factors.append('Critical low humidity {}%'.format(hum))
    elif hum < 25: score += 0.05; factors.append('Low humidity {}%'.format(hum))
    if weather.red_flag_warning: score += 0.15; factors.append('Red Flag Warning active')
    return min(score, 0.35), factors

def _pred_contribution(asset_ids: List[str], pred_scores: list) -> tuple:
    if not pred_scores:
        return 0.05, []
    relevant = [s for s in pred_scores if s.asset_id in asset_ids]
    if not relevant:
        return 0.05, []
    worst = max(relevant, key=lambda s: s.score_30d)
    score = worst.score_30d / 100 * 0.20
    factors = []
    if worst.score_30d > 70:
        factors.append('Asset {} at {}% failure probability'.format(worst.asset_name, worst.score_30d))
    return score, factors

def _failure_mode(asset_ids: List[str], fault_detector) -> str:
    for aid in asset_ids:
        faults = fault_detector.get_active_faults(asset_id=aid)
        for f in faults:
            if f.severity.value == 'critical':
                return f.title
    return 'Cumulative degradation'

def _duration_estimate(prob: float, customers: int) -> float:
    base = 2.0 + prob * 8.0
    if customers > 5000: base += 2.0
    elif customers > 1000: base += 1.0
    return round(base, 1)

def _risk_level(prob_24h: float) -> str:
    if prob_24h >= 0.35: return 'CRITICAL'
    elif prob_24h >= 0.20: return 'HIGH'
    elif prob_24h >= 0.10: return 'MEDIUM'
    return 'LOW'

def _preemptive_action(risk: str, failure_mode: str) -> str:
    if risk == 'CRITICAL':
        return 'Dispatch crew immediately — inspect and remediate before next high-wind event'
    elif risk == 'HIGH':
        return 'Schedule priority inspection within 24 hours — pre-position restoration crew'
    elif risk == 'MEDIUM':
        return 'Include in next scheduled maintenance cycle — monitor telemetry closely'
    return 'Continue routine monitoring — no immediate action required'

class OutagePredictionEngine:
    def predict_circuit(self, circuit_id: str, fault_detector, weather, pred_scores: list) -> OutagePrediction:
        now = datetime.now(timezone.utc)
        asset_ids = CIRCUIT_ASSETS.get(circuit_id, [])
        base_rate = HISTORICAL_BASE_RATES.get(circuit_id, 0.05)

        fault_score, fault_factors, worst_asset = _fault_contribution(asset_ids, fault_detector)
        weather_score, weather_factors = _weather_contribution(weather)
        pred_score, pred_factors = _pred_contribution(asset_ids, pred_scores)

        total_24h = min(base_rate + fault_score + weather_score + pred_score, 0.95)
        total_72h = min(total_24h * 1.4, 0.97)
        total_168h = min(total_24h * 1.8, 0.99)

        customers = CIRCUIT_CUSTOMERS.get(circuit_id, 500)
        risk = _risk_level(total_24h)
        failure_mode = _failure_mode(asset_ids, fault_detector)
        duration = _duration_estimate(total_24h, customers)
        customer_hours = round(customers * duration, 0)

        all_factors = fault_factors + weather_factors + pred_factors
        if not all_factors:
            all_factors = ['Historical base rate for circuit type']

        confidence = round(min(0.90, 0.55 + len(fault_factors) * 0.08 + len(weather_factors) * 0.06), 2)

        return OutagePrediction(
            circuit_id=circuit_id,
            circuit_name=CIRCUIT_NAMES.get(circuit_id, circuit_id),
            customers_at_risk=customers,
            prob_24h=round(total_24h * 100, 1),
            prob_72h=round(total_72h * 100, 1),
            prob_168h=round(total_168h * 100, 1),
            risk_level=risk,
            primary_failure_mode=failure_mode,
            most_likely_asset=worst_asset or (asset_ids[0] if asset_ids else circuit_id),
            contributing_factors=all_factors[:4],
            estimated_duration_hours=duration,
            estimated_customer_hours=customer_hours,
            recommended_preemptive_action=_preemptive_action(risk, failure_mode),
            confidence=confidence,
            predicted_at=now.isoformat(),
        )

    def predict_fleet(self, fault_detector, weather, pred_scores: list) -> OutageReport:
        now = datetime.now(timezone.utc)
        predictions = []
        for cid in CIRCUIT_ASSETS.keys():
            p = self.predict_circuit(cid, fault_detector, weather, pred_scores)
            predictions.append(p)
        predictions.sort(key=lambda p: p.prob_24h, reverse=True)

        high_risk = [p for p in predictions if p.risk_level in ('CRITICAL','HIGH')]
        total_at_risk = sum(p.customers_at_risk for p in high_risk)

        summary = {
            'total_circuits': len(predictions),
            'critical_risk': sum(1 for p in predictions if p.risk_level=='CRITICAL'),
            'high_risk': sum(1 for p in predictions if p.risk_level=='HIGH'),
            'medium_risk': sum(1 for p in predictions if p.risk_level=='MEDIUM'),
            'low_risk': sum(1 for p in predictions if p.risk_level=='LOW'),
            'customers_at_risk_24h': total_at_risk,
            'highest_risk_circuit': predictions[0].circuit_name if predictions else None,
            'highest_prob_24h': predictions[0].prob_24h if predictions else None,
            'avg_prob_24h': round(sum(p.prob_24h for p in predictions)/len(predictions),1) if predictions else 0,
            'total_predicted_customer_hours': sum(p.estimated_customer_hours for p in high_risk),
        }

        return OutageReport(
            report_id='OPR-{}'.format(now.strftime('%Y%m%d-%H%M%S')),
            generated_at=now.isoformat(),
            total_circuits=len(predictions),
            high_risk_24h=len(high_risk),
            total_customers_at_risk=total_at_risk,
            predictions=predictions,
            summary=summary,
        )

_engine = None
def get_outage_engine():
    global _engine
    if _engine is None: _engine = OutagePredictionEngine()
    return _engine

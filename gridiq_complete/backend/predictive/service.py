"""GridIQ Predictive Fault Scoring Engine"""
from __future__ import annotations
import logging, math, random, statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
logger = logging.getLogger("gridiq.predictive")

@dataclass
class AssetRiskScore:
    asset_id: str
    asset_name: str
    asset_type: str
    score_30d: float
    score_60d: float
    score_90d: float
    risk_level: str
    primary_driver: str
    contributing_factors: List[str]
    active_fault_count: int
    trend_direction: str
    recommended_action: str
    maintenance_window: str
    estimated_failure_date: Optional[str]
    confidence: float
    last_updated: str

ASSET_BASE_RATES = {"transformer":15,"circuit_breaker":8,"transmission_line":12,"bess":18,"solar_farm":6,"wind_farm":14,"hydro_plant":5,"gas_peaker":20,"default":10}

class PredictiveScoringEngine:
    def __init__(self):
        self._scores = {}

    def score_asset(self, asset_id, asset_name, asset_type, history, active_faults, asset_meta=None):
        meta = asset_meta or {}
        now = datetime.now(timezone.utc)
        factors = []
        base = ASSET_BASE_RATES.get(asset_type, ASSET_BASE_RATES["default"])
        crit = [f for f in active_faults if hasattr(f,"severity") and f.severity.value=="critical"]
        high = [f for f in active_faults if hasattr(f,"severity") and f.severity.value=="high"]
        fault_score = min(len(crit)*25 + len(high)*12 + len(active_faults)*4, 40)
        if crit: factors.append(f"{len(crit)} critical fault(s) active")
        if high: factors.append(f"{len(high)} high severity fault(s) active")
        trend_score = 0
        trend_dir = "STABLE"
        if len(history) >= 10:
            trend_score, trend_dir, tf = self._trends(history)
            factors.extend(tf)
        age = meta.get("age_years", 0)
        age_score = 15 if age > 30 else 8 if age > 20 else 3 if age > 10 else 0
        if age > 20: factors.append(f"Asset age {age}yr")
        last_maint = meta.get("last_maintenance_days_ago", 180)
        maint_score = 12 if last_maint > 365 else 6 if last_maint > 180 else 0
        if last_maint > 180: factors.append(f"Maintenance overdue ({last_maint}d)")
        raw = base + fault_score + trend_score + age_score + maint_score
        s30 = round(min(99, max(1, raw)), 1)
        s60 = round(min(99, s30 * 1.15), 1)
        s90 = round(min(99, s30 * 1.28), 1)
        risk = "CRITICAL" if s30 >= 75 else "HIGH" if s30 >= 50 else "MEDIUM" if s30 >= 25 else "LOW"
        action = {"CRITICAL":"Schedule immediate inspection","HIGH":"Schedule inspection within 30 days","MEDIUM":"Include in next maintenance cycle","LOW":"Continue routine monitoring"}[risk]
        window = {"CRITICAL":"Within 7 days","HIGH":"Within 30 days","MEDIUM":"Within 60 days","LOW":"Next scheduled maintenance"}[risk]
        failure_date = None
        if s30 > 50:
            days = int((100 - s30) / s30 * 30)
            failure_date = (now + timedelta(days=max(7, days))).strftime("%Y-%m-%d")
        conf = round(min(0.95, 0.60 + len(history)/720*0.35), 2)
        score = AssetRiskScore(asset_id=asset_id, asset_name=asset_name, asset_type=asset_type, score_30d=s30, score_60d=s60, score_90d=s90, risk_level=risk, primary_driver=factors[0] if factors else "Normal degradation", contributing_factors=factors[:5], active_fault_count=len(active_faults), trend_direction=trend_dir, recommended_action=action, maintenance_window=window, estimated_failure_date=failure_date, confidence=conf, last_updated=now.isoformat())
        self._scores[asset_id] = score
        return score

    def _trends(self, history):
        score = 0; direction = "STABLE"; factors = []
        try:
            recent = list(history)[-50:]
            for param in ["efficiency_pct","state_of_health_pct","performance_ratio","roundtrip_efficiency_pct"]:
                vals = [float(h.get("telemetry",{}).get(param)) for h in recent if h.get("telemetry",{}).get(param) is not None]
                if len(vals) >= 5:
                    h1 = statistics.mean(vals[:len(vals)//2]); h2 = statistics.mean(vals[len(vals)//2:])
                    if h1 > 0:
                        chg = (h2-h1)/h1*100
                        if chg < -10: score += 20; direction = "RAPID_DEGRADATION"; factors.append(f"Rapid {param} decline: {abs(chg):.1f}%")
                        elif chg < -3: score += 12; direction = "DEGRADING"; factors.append(f"{param} declining: {abs(chg):.1f}%")
        except: pass
        return min(score, 30), direction, factors

    def score_fleet(self, assets, fault_detector):
        scores = []
        for a in assets:
            aid = a.get("asset_id","")
            if not aid: continue
            history = list(fault_detector._history.get(aid, []))
            active = fault_detector.get_active_faults(asset_id=aid)
            scores.append(self.score_asset(asset_id=aid, asset_name=a.get("asset_name",aid), asset_type=a.get("asset_type","default"), history=history, active_faults=active, asset_meta=a.get("asset_meta",{})))
        scores.sort(key=lambda s: s.score_30d, reverse=True)
        return scores

    def get_fleet_summary(self, scores):
        if not scores: return {}
        return {"total_assets":len(scores),"critical":sum(1 for s in scores if s.risk_level=="CRITICAL"),"high":sum(1 for s in scores if s.risk_level=="HIGH"),"medium":sum(1 for s in scores if s.risk_level=="MEDIUM"),"low":sum(1 for s in scores if s.risk_level=="LOW"),"avg_score_30d":round(statistics.mean(s.score_30d for s in scores),1),"highest_risk_asset":scores[0].asset_name if scores else None,"highest_risk_score":scores[0].score_30d if scores else None,"assets_needing_immediate_action":[{"asset_id":s.asset_id,"asset_name":s.asset_name,"score":s.score_30d,"action":s.recommended_action} for s in scores if s.risk_level in ("CRITICAL","HIGH")][:10]}

_engine = None
def get_scoring_engine():
    global _engine
    if _engine is None: _engine = PredictiveScoringEngine()
    return _engine

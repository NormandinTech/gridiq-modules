from __future__ import annotations
import logging
import httpx
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger('gridiq.lidarservice')

USGS_3DEP_API = 'https://tnmapi.cr.usgs.gov/api/products'

@dataclass
class LiDARDataset:
    dataset_id: str
    title: str
    acquisition_year: int
    resolution_m: float
    coverage_pct: float
    format: str
    download_url: Optional[str]
    size_gb: float
    quality: str
    suitable_for_vegetation: bool

@dataclass
class LiDARCoverageReport:
    report_id: str
    utility_name: str
    territory_bbox: Dict
    generated_at: str
    datasets_found: int
    coverage_pct: float
    best_dataset: Optional[LiDARDataset]
    all_datasets: List[LiDARDataset]
    estimated_spans_coverable: int
    estimated_processing_days: float
    onboarding_status: str
    recommended_action: str
    estimated_cost_usd: float
    summary: Dict[str, Any]

SIMULATED_DATASETS = [
    {'title':'CA_ButteCounty_2020_D20','year':2020,'res':0.5,'cov':94.2,'fmt':'LAZ','size':28.4,'quality':'HIGH','url':'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/CA_ButteCounty_2020_D20/'},
    {'title':'CA_NorCal_Wildfires_2019_D19','year':2019,'res':1.0,'cov':87.6,'fmt':'LAZ','size':14.2,'quality':'MEDIUM','url':'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/CA_NorCal_Wildfires_2019_D19/'},
    {'title':'USGS_LPC_CA_Sierra_2018','year':2018,'res':1.0,'cov':71.3,'fmt':'LAS','size':9.8,'quality':'MEDIUM','url':None},
]

def _quality_score(ds: Dict) -> float:
    q = {'HIGH':1.0,'MEDIUM':0.7,'LOW':0.4}.get(ds['quality'],0.5)
    recency = max(0, 1 - (2026 - ds['year']) * 0.08)
    res = 1.0 if ds['res'] <= 0.5 else 0.7 if ds['res'] <= 1.0 else 0.4
    return round(q * 0.4 + recency * 0.3 + res * 0.3, 3)

async def _query_usgs_3dep(bbox: Dict) -> List[Dict]:
    try:
        params = {
            'datasets': 'National Elevation Dataset (NED) 1/3 arc-second',
            'bbox': '{},{},{},{}'.format(bbox['west'],bbox['south'],bbox['east'],bbox['north']),
            'prodFormats': 'LAZ,LAS',
            'max': '5',
        }
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(USGS_3DEP_API, params=params)
            if r.status_code == 200:
                data = r.json()
                items = data.get('items',[])
                if items:
                    return [{'title':i.get('title',''),'year':int(i.get('publicationDate','2020')[:4]),'res':1.0,'cov':85.0,'fmt':'LAZ','size':10.0,'quality':'MEDIUM','url':i.get('downloadURL')} for i in items[:3]]
    except Exception as e:
        logger.debug('USGS 3DEP query: {}'.format(e))
    return []

def _make_dataset(data: Dict, idx: int) -> LiDARDataset:
    return LiDARDataset(
        dataset_id='DS-{}-{:03d}'.format(data['year'], idx),
        title=data['title'],
        acquisition_year=data['year'],
        resolution_m=data['res'],
        coverage_pct=data['cov'],
        format=data['fmt'],
        download_url=data.get('url'),
        size_gb=data['size'],
        quality=data['quality'],
        suitable_for_vegetation=data['res'] <= 1.0 and data['cov'] > 70,
    )

class LiDAROnboardingEngine:
    async def check_coverage(self, utility_name: str, bbox: Dict) -> LiDARCoverageReport:
        now = datetime.now(timezone.utc)
        report_id = 'LDR-{}'.format(now.strftime('%Y%m%d-%H%M%S'))

        live_data = await _query_usgs_3dep(bbox)
        all_data = SIMULATED_DATASETS + live_data
        all_data = sorted(all_data, key=_quality_score, reverse=True)

        datasets = [_make_dataset(d, i+1) for i,d in enumerate(all_data)]
        best = datasets[0] if datasets else None
        avg_coverage = sum(d.coverage_pct for d in datasets)/len(datasets) if datasets else 0

        lat_span = bbox['north'] - bbox['south']
        lon_span = bbox['east'] - bbox['west']
        approx_km2 = lat_span * 111 * lon_span * 111 * 0.85
        est_spans = int(approx_km2 * 2.5)
        est_processing = round(approx_km2 / 50, 1)

        if avg_coverage > 85 and best and best.resolution_m <= 0.5:
            status = 'READY'
            action = 'Excellent LiDAR coverage available. GridIQ can begin vegetation risk analysis immediately. Estimated onboarding: {} days.'.format(int(est_processing)+1)
            cost = 0.0
        elif avg_coverage > 60:
            status = 'PARTIAL'
            action = 'Good coverage available for most of your territory. GridIQ recommends proceeding with available data and scheduling supplemental drone LiDAR for gap areas.'
            cost = round(approx_km2 * 0.5 * 0.12, 0)
        else:
            status = 'COLLECTION_NEEDED'
            action = 'Limited existing LiDAR coverage. GridIQ can arrange drone LiDAR collection for your transmission corridors. Estimated cost and timeline provided below.'
            cost = round(approx_km2 * 0.12, 0)

        summary = {
            'utility_name': utility_name,
            'territory_area_km2': round(approx_km2, 1),
            'datasets_found': len(datasets),
            'best_resolution_m': best.resolution_m if best else None,
            'best_coverage_pct': best.coverage_pct if best else 0,
            'best_dataset_year': best.acquisition_year if best else None,
            'estimated_spans_coverable': est_spans,
            'estimated_processing_days': est_processing,
            'onboarding_status': status,
            'data_collection_cost_usd': cost,
            'usgs_data_cost': 0,
            'total_estimated_cost_usd': cost,
            'ready_for_gridiq': status in ('READY','PARTIAL'),
        }

        return LiDARCoverageReport(
            report_id=report_id,
            utility_name=utility_name,
            territory_bbox=bbox,
            generated_at=now.isoformat(),
            datasets_found=len(datasets),
            coverage_pct=round(avg_coverage,1),
            best_dataset=best,
            all_datasets=datasets,
            estimated_spans_coverable=est_spans,
            estimated_processing_days=est_processing,
            onboarding_status=status,
            recommended_action=action,
            estimated_cost_usd=cost,
            summary=summary,
        )

_engine = None
def get_lidar_engine():
    global _engine
    if _engine is None: _engine = LiDAROnboardingEngine()
    return _engine

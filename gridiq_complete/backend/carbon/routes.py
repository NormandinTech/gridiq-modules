from fastapi import APIRouter, Query
from backend.carbon.service import get_carbon_engine
from backend.assets.fault_detector import fault_detector

carbon_router = APIRouter(prefix='/carbon', tags=['Carbon Credit Reporting'])

@carbon_router.get('/report', summary='Generate carbon credit report')
async def get_carbon_report(
    utility_name: str = Query(default='NormandinTECH Demo Utility'),
    monitoring_days: int = Query(default=30, ge=1, le=365),
):
    engine = get_carbon_engine()
    faults = fault_detector.get_active_faults()
    report = engine.generate_report(faults, utility_name=utility_name, monitoring_days=monitoring_days)
    return {
        'report_id': report.report_id,
        'utility_name': report.utility_name,
        'reporting_period': report.reporting_period,
        'generated_at': report.generated_at,
        'total_co2e_avoided_tonnes': report.total_co2e_avoided,
        'total_value_usd': report.total_value_usd,
        'total_credits': report.total_credits,
        'methodology_notes': report.methodology_notes,
        'verification_body': report.verification_body,
        'summary': report.summary,
        'credits': [c.__dict__ for c in report.credits],
    }

@carbon_router.get('/summary', summary='Carbon credit summary')
async def get_carbon_summary(monitoring_days: int = Query(default=30, ge=1, le=365)):
    engine = get_carbon_engine()
    faults = fault_detector.get_active_faults()
    report = engine.generate_report(faults, monitoring_days=monitoring_days)
    return report.summary

@carbon_router.get('/credits', summary='List all carbon credits')
async def get_credits(monitoring_days: int = Query(default=30, ge=1, le=365)):
    engine = get_carbon_engine()
    faults = fault_detector.get_active_faults()
    report = engine.generate_report(faults, monitoring_days=monitoring_days)
    return {'count': report.total_credits, 'credits': [c.__dict__ for c in report.credits]}

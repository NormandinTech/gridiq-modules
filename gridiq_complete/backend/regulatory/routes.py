from fastapi import APIRouter
from backend.regulatory.service import get_regulatory_monitor

regulatory_router = APIRouter(prefix='/regulatory', tags=['Regulatory Monitoring'])

@regulatory_router.get('/report', summary='Full regulatory change report')
async def get_report():
    m = get_regulatory_monitor()
    r = await m.get_report()
    return {
        'report_id': r.report_id,
        'generated_at': r.generated_at,
        'summary': r.summary,
        'next_check': r.next_check,
        'updates': [u.__dict__ for u in r.updates],
        'compliance_gaps': [g.__dict__ for g in r.compliance_gaps],
    }

@regulatory_router.get('/summary', summary='Regulatory monitoring summary')
async def get_summary():
    m = get_regulatory_monitor()
    r = await m.get_report()
    return r.summary

@regulatory_router.get('/updates', summary='Recent regulatory updates')
async def get_updates():
    m = get_regulatory_monitor()
    r = await m.get_report()
    return {'count': len(r.updates), 'updates': [u.__dict__ for u in r.updates]}

@regulatory_router.get('/gaps', summary='Compliance gap analysis')
async def get_gaps():
    m = get_regulatory_monitor()
    r = await m.get_report()
    gaps_with_issues = [g for g in r.compliance_gaps if g.severity not in ('NONE',)]
    return {
        'total_gaps': len(gaps_with_issues),
        'compliant': len([g for g in r.compliance_gaps if g.severity=='NONE']),
        'gaps': [g.__dict__ for g in r.compliance_gaps],
    }

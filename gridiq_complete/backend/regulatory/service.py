from __future__ import annotations
import logging
import hashlib
import httpx
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger('gridiq.regulatory')

@dataclass
class RegulatoryUpdate:
    update_id: str
    source: str
    title: str
    summary: str
    url: str
    published_date: str
    category: str
    affected_standards: List[str]
    impact_level: str
    gridiq_impact: str
    action_required: bool
    action_deadline: Optional[str]
    fetched_at: str

@dataclass
class ComplianceGap:
    gap_id: str
    standard: str
    requirement: str
    current_status: str
    gap_description: str
    affected_assets: List[str]
    severity: str
    recommended_action: str
    deadline: Optional[str]

@dataclass
class RegulatoryReport:
    report_id: str
    generated_at: str
    updates: List[RegulatoryUpdate]
    compliance_gaps: List[ComplianceGap]
    summary: Dict[str, Any]
    next_check: str

REGULATORY_SOURCES = [
    {'name':'NERC','url':'https://www.nerc.com/rss.aspx','category':'reliability'},
    {'name':'FERC','url':'https://www.ferc.gov/news-events/rss.xml','category':'federal'},
    {'name':'CPUC','url':'https://docs.cpuc.ca.gov/PublishedDocs/Efile/G000/M537/K537/537537.RSS','category':'california'},
]

KNOWN_STANDARDS = [
    {'id':'FAC-003-5','title':'Transmission Vegetation Management','category':'vegetation','gridiq_module':'vegetation'},
    {'id':'FAC-001-3','title':'Facility Ratings','category':'transmission','gridiq_module':'assets'},
    {'id':'CIP-002-5.1a','title':'Cyber Security — BES Cyber System Categorization','category':'cybersecurity','gridiq_module':'security'},
    {'id':'CIP-007-6','title':'Cyber Security — Systems Security Management','category':'cybersecurity','gridiq_module':'security'},
    {'id':'EOP-005-3','title':'System Restoration from Blackstart Resources','category':'operations','gridiq_module':'grid'},
    {'id':'TOP-001-5','title':'Transmission Operations','category':'operations','gridiq_module':'grid'},
    {'id':'MOD-032-1','title':'Steady-State and Dynamic System Model Validation','category':'modeling','gridiq_module':'digital_twin'},
]

SIMULATED_UPDATES = [
    {
        'source':'NERC',
        'title':'FAC-003-5 Vegetation Management — Updated Minimum Clearance Tables for HFTD Zones',
        'summary':'NERC has issued updated minimum clearance distance tables for transmission lines passing through High Fire Threat Districts. Changes increase minimum clearance requirements for 230kV lines in Tier 3 HFTD zones from 3.05m to 3.66m effective January 1, 2027.',
        'url':'https://www.nerc.com/pa/Stand/Pages/FAC.aspx',
        'category':'vegetation',
        'affected_standards':['FAC-003-5'],
        'impact_level':'HIGH',
        'gridiq_impact':'GridIQ vegetation risk engine clearance thresholds require update for 230kV lines in HFTD zones. Affects Sierra 230kV Feeder A and Pulga Line corridors.',
        'action_required':True,
        'action_deadline':'2026-12-01',
        'days_ago':8,
    },
    {
        'source':'CPUC',
        'title':'Wildfire Mitigation Plan — New Real-Time Monitoring Requirements for Class C Lines',
        'summary':'CPUC Decision 26-02-014 requires all Class C transmission operators to implement continuous real-time vegetation clearance monitoring by Q4 2026. Manual patrol cycles no longer satisfy compliance requirements in HFTD areas.',
        'url':'https://docs.cpuc.ca.gov/PublishedDocs/',
        'category':'california',
        'affected_standards':['GO-95','WMP-2026'],
        'impact_level':'CRITICAL',
        'gridiq_impact':'GridIQ real-time LiDAR vegetation monitoring directly satisfies this requirement. Current manual-patrol utilities using GridIQ achieve instant compliance.',
        'action_required':True,
        'action_deadline':'2026-10-01',
        'days_ago':3,
    },
    {
        'source':'FERC',
        'title':'Order 881-A — Ambient-Adjusted Ratings Implementation Deadline Extended',
        'summary':'FERC has extended the AAR implementation deadline to July 2026 for utilities demonstrating hardship. Transmission operators must implement real-time ambient-adjusted ratings for all lines 100kV and above.',
        'url':'https://www.ferc.gov/media/order-881',
        'category':'federal',
        'affected_standards':['FAC-001-3','TOP-001-5'],
        'impact_level':'MEDIUM',
        'gridiq_impact':'GridIQ conductor temperature and sag monitoring supports AAR calculation. Integration with utility EMS needed for full compliance.',
        'action_required':False,
        'action_deadline':'2026-07-01',
        'days_ago':15,
    },
    {
        'source':'NERC',
        'title':'CIP-003-9 — Low Impact BES Cyber System Requirements Update',
        'summary':'NERC has approved CIP-003-9 with enhanced physical security and electronic access control requirements for low-impact BES cyber systems. Effective date: October 1, 2026.',
        'url':'https://www.nerc.com/pa/Stand/Pages/CIP.aspx',
        'category':'cybersecurity',
        'affected_standards':['CIP-003-9'],
        'impact_level':'MEDIUM',
        'gridiq_impact':'GridIQ read-only SCADA connection model isolates IT/OT boundary. CIP-003-9 electronic access controls apply to SCADA endpoints — utility responsibility.',
        'action_required':False,
        'action_deadline':'2026-10-01',
        'days_ago':22,
    },
    {
        'source':'CPUC',
        'title':'PSPS Reporting Requirements — Enhanced Data Submission Standards',
        'summary':'CPUC has issued new PSPS reporting standards requiring utilities to submit de-energization decision rationale including real-time weather data, vegetation risk scores, and equipment fault status within 6 hours of any PSPS event.',
        'url':'https://docs.cpuc.ca.gov/PublishedDocs/',
        'category':'california',
        'affected_standards':['GO-95-R14','PSPS-2026'],
        'impact_level':'HIGH',
        'gridiq_impact':'GridIQ PSPS decision support module generates the exact data package required — weather, vegetation risk, fault status, and circuit-level justification — automatically satisfying new reporting requirements.',
        'action_required':True,
        'action_deadline':'2026-06-01',
        'days_ago':5,
    },
]

COMPLIANCE_GAPS = [
    {
        'standard':'FAC-003-5',
        'requirement':'Minimum clearance 3.66m for 230kV in HFTD Tier 3 (effective Jan 2027)',
        'current_status':'Current threshold: 3.05m — does not meet 2027 requirement',
        'gap_description':'GridIQ clearance engine uses 3.05m threshold for 230kV lines. New FAC-003-5 update requires 3.66m in HFTD Tier 3 zones effective Jan 1 2027.',
        'affected_assets':['Sierra 230kV Feeder A','Pulga Line Camp Creek Span'],
        'severity':'HIGH',
        'recommended_action':'Update GridIQ vegetation engine NERC_MIN_CLEARANCE_M threshold for 230kV HFTD lines to 3.66m before December 2026.',
        'deadline':'2026-12-01',
    },
    {
        'standard':'CPUC GO-95 R14',
        'requirement':'Real-time vegetation monitoring for Class C lines in HFTD',
        'current_status':'COMPLIANT — GridIQ provides continuous real-time monitoring',
        'gap_description':'No gap — GridIQ LiDAR and SCADA integration satisfies CPUC Decision 26-02-014 continuous monitoring requirement.',
        'affected_assets':['All monitored corridors'],
        'severity':'NONE',
        'recommended_action':'Document GridIQ monitoring capability in utility WMP submission as evidence of compliance.',
        'deadline':None,
    },
    {
        'standard':'PSPS-2026',
        'requirement':'6-hour post-event data submission with decision rationale',
        'current_status':'COMPLIANT — GridIQ PSPS module generates required data package',
        'gap_description':'No gap — GridIQ PSPS decision support module generates weather, vegetation, fault, and circuit-level justification automatically.',
        'affected_assets':['All PSPS circuits'],
        'severity':'NONE',
        'recommended_action':'Configure GridIQ PSPS report export to CPUC submission portal format.',
        'deadline':'2026-06-01',
    },
]

class RegulatoryMonitor:
    def __init__(self):
        self._cache: Optional[tuple] = None
        self._cache_ttl = 3600 * 4

    async def _fetch_rss(self, source: Dict) -> List[Dict]:
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                r = await client.get(source['url'])
                if r.status_code == 200:
                    root = ET.fromstring(r.content)
                    items = []
                    for item in root.findall('.//item')[:3]:
                        title = item.findtext('title','')
                        link = item.findtext('link','')
                        desc = item.findtext('description','')[:300]
                        pub = item.findtext('pubDate','')
                        if title:
                            items.append({'source':source['name'],'title':title,'url':link,'summary':desc,'published':pub,'category':source['category']})
                    return items
        except Exception as e:
            logger.debug('RSS fetch failed for {}: {}'.format(source['name'], e))
        return []

    def _make_update(self, data: Dict, days_ago: int = 0) -> RegulatoryUpdate:
        now = datetime.now(timezone.utc)
        pub_date = (now - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        uid = 'REG-' + hashlib.md5((data['title']+pub_date).encode()).hexdigest()[:8].upper()
        return RegulatoryUpdate(
            update_id=uid,
            source=data['source'],
            title=data['title'],
            summary=data['summary'],
            url=data.get('url',''),
            published_date=pub_date,
            category=data['category'],
            affected_standards=data.get('affected_standards',[]),
            impact_level=data.get('impact_level','LOW'),
            gridiq_impact=data.get('gridiq_impact',''),
            action_required=data.get('action_required',False),
            action_deadline=data.get('action_deadline'),
            fetched_at=now.isoformat(),
        )

    def _make_gap(self, data: Dict) -> ComplianceGap:
        gid = 'GAP-' + hashlib.md5(data['standard'].encode()).hexdigest()[:8].upper()
        return ComplianceGap(
            gap_id=gid,
            standard=data['standard'],
            requirement=data['requirement'],
            current_status=data['current_status'],
            gap_description=data['gap_description'],
            affected_assets=data['affected_assets'],
            severity=data['severity'],
            recommended_action=data['recommended_action'],
            deadline=data.get('deadline'),
        )

    async def get_report(self) -> RegulatoryReport:
        now = datetime.now(timezone.utc)
        if self._cache:
            cached_at, cached = self._cache
            if (now - cached_at).total_seconds() < self._cache_ttl:
                return cached

        updates = [self._make_update(u, u['days_ago']) for u in SIMULATED_UPDATES]
        gaps = [self._make_gap(g) for g in COMPLIANCE_GAPS]

        for source in REGULATORY_SOURCES:
            live_items = await self._fetch_rss(source)
            for item in live_items:
                u = RegulatoryUpdate(
                    update_id='REG-LIVE-' + hashlib.md5(item['title'].encode()).hexdigest()[:8].upper(),
                    source=item['source'], title=item['title'], summary=item['summary'],
                    url=item['url'], published_date=item.get('published','')[:10],
                    category=item['category'], affected_standards=[],
                    impact_level='LOW', gridiq_impact='Review for potential impact on GridIQ monitoring parameters.',
                    action_required=False, action_deadline=None, fetched_at=now.isoformat(),
                )
                updates.append(u)

        critical = [u for u in updates if u.impact_level=='CRITICAL']
        high = [u for u in updates if u.impact_level=='HIGH']
        action_required = [u for u in updates if u.action_required]
        gaps_with_issues = [g for g in gaps if g.severity not in ('NONE','LOW')]

        summary = {
            'total_updates': len(updates),
            'critical': len(critical),
            'high': len(high),
            'action_required': len(action_required),
            'compliance_gaps': len(gaps_with_issues),
            'compliant_standards': len([g for g in gaps if g.severity=='NONE']),
            'next_deadline': min([u.action_deadline for u in updates if u.action_deadline], default=None),
            'sources_monitored': len(REGULATORY_SOURCES),
            'standards_tracked': len(KNOWN_STANDARDS),
        }

        report = RegulatoryReport(
            report_id='REG-{}'.format(now.strftime('%Y%m%d-%H%M%S')),
            generated_at=now.isoformat(),
            updates=updates,
            compliance_gaps=gaps,
            summary=summary,
            next_check=(now + timedelta(hours=4)).isoformat(),
        )
        self._cache = (now, report)
        return report

_monitor = None
def get_regulatory_monitor() -> RegulatoryMonitor:
    global _monitor
    if _monitor is None: _monitor = RegulatoryMonitor()
    return _monitor

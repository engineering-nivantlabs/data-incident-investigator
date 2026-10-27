"""Deterministic diagnosis; dependency exposure is never asserted as corruption."""
from datetime import datetime, timezone

FAILURES = {'FAILED', 'TIMEDOUT', 'TIMED_OUT', 'ERROR', 'INTERNAL_ERROR'}


def investigate(asset, events, dependencies, now=None):
    now = now or datetime.now(timezone.utc)
    relevant = sorted([e for e in events if e['asset_id'] == asset['id']], key=lambda e: e['time'])
    evidence, hypotheses = [], []
    # The most recent run supersedes historical failures for readiness evaluation.
    runs = [e for e in relevant if e['kind'] == 'run']
    latest = runs[-1] if runs else None
    if latest and latest['state'] in FAILURES:
        evidence.append(latest)
        hypotheses.append({'cause': 'Pipeline execution failed', 'assessment': 'confirmed', 'evidence_ids': [latest['id']], 'detail': 'The latest observed run ended unsuccessfully. The underlying cause requires error review.'})
    checks = [e for e in relevant if e['kind'] == 'quality']
    if checks and checks[-1]['state'] == 'FAILED':
        evidence.append(checks[-1])
        hypotheses.append({'cause': 'Data quality rule failed', 'assessment': 'confirmed', 'evidence_ids': [checks[-1]['id']], 'detail': checks[-1]['message']})
    freshness = [e for e in relevant if e['kind'] == 'freshness']
    current = freshness[-1] if freshness else None
    freshness_ok = False
    if asset.get('table'):
        recent = current and (now - datetime.fromisoformat(current['time'])).total_seconds() <= asset.get('check_ttl_minutes', 15)*60
        if recent and current['state'] == 'FRESH' and current.get('observed_at'):
            age = (now-datetime.fromisoformat(current['observed_at'])).total_seconds()/60
            freshness_ok = -5 <= age <= asset['max_age_minutes']
        if current:
            evidence.append(current)
        if not freshness_ok:
            hypotheses.append({'cause': 'Table freshness not verified' if not recent or not current or current['state'] == 'UNKNOWN' else 'Table freshness deadline missed', 'assessment': 'insufficient evidence' if not recent or not current or current['state'] == 'UNKNOWN' else 'confirmed', 'evidence_ids': [current['id']] if current else [], 'detail': 'A recent successful table observation within the freshness threshold is required. Job success alone cannot verify recovery.'})
    else:
        successes = [e for e in runs if e['state'] == 'SUCCEEDED']
        age = (now-datetime.fromisoformat(successes[-1]['time'])).total_seconds()/60 if successes else None
        freshness_ok = age is not None and age <= asset.get('max_age_minutes', 60)
        if not freshness_ok:
            if successes:
                evidence.append(successes[-1])
            hypotheses.append({'cause': 'Readiness not verified' if age is None else 'Successful pipeline run is overdue', 'assessment': 'insufficient evidence' if age is None else 'supported', 'evidence_ids': [successes[-1]['id']] if successes else [], 'detail': 'Run completion is a readiness proxy only.'})
    job_coverage = [e for e in relevant if e['kind'] == 'coverage' and e.get('source') == 'Databricks Jobs API']
    if asset.get('job_id') and job_coverage and (not latest or job_coverage[-1]['time'] >= latest.get('collected_at', latest['time'])):
        evidence.append(job_coverage[-1])
        hypotheses.append({'cause': 'Job telemetry unavailable', 'assessment': 'insufficient evidence', 'evidence_ids': [job_coverage[-1]['id']], 'detail': 'Refresh job telemetry before verifying recovery.'})
    seen, frontier = {asset['id']}, [asset['id']]
    while frontier:
        source = frontier.pop()
        for edge in dependencies:
            if edge['source'] == source and edge['target'] not in seen:
                seen.add(edge['target'])
                frontier.append(edge['target'])
    evidence = list({e['id']: e for e in evidence}.values())
    healthy = bool(freshness_ok and (not asset.get('job_id') or (latest and latest['state'] == 'SUCCEEDED')) and not hypotheses)
    verified = {}
    if healthy:
        for event in relevant:
            if (event['kind'] == 'downstream_refresh' and event.get('simulated') is True
                    and event.get('target_asset_id') in seen - {asset['id']}
                    and event['state'] == 'PASSED'
                    and current and event['time'] >= current['time']
                    and 0 <= (now-datetime.fromisoformat(event['time'])).total_seconds() <= asset.get('check_ttl_minutes', 15)*60):
                verified[event['target_asset_id']] = {'at':event['time'], 'evidence_id':event['id'], 'simulated':True}
    downstream_complete = bool(seen - {asset['id']}) and len(verified) == len(seen - {asset['id']})
    return {'downstream_verified': verified, 'downstream_complete': downstream_complete, 'hypotheses': hypotheses, 'evidence': evidence, 'timeline': relevant, 'potentially_affected': sorted(seen - {asset['id']}), 'recovery_verified': healthy, 'dependencies': [e for e in dependencies if e['source'] in seen and e['target'] in seen], 'coverage': 'Declared and available Unity Catalog dependencies within the configured asset allowlist. Lineage can be incomplete or delayed. Freshness uses a table timestamp when configured; commit freshness does not prove business completeness.', 'runbook': asset.get('runbook', 'Review source evidence with the asset owner.')}

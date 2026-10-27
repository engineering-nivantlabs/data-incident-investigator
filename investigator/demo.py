"""Synthetic, deterministic scenario families; never connects to a workspace."""
from datetime import datetime, timezone, timedelta

SCENARIOS = [
    ('failed_pipeline', 'Failed settlement ingestion', 'Pipeline failure', 'A schema mismatch stops ingestion; settlement and treasury outputs may be exposed.', 'Payments engineering'),
    ('stale_success', 'Successful job, stale table', 'Silent stale data', 'The job succeeds, but the table has not received a commit for three hours.', 'Finance platform'),
    ('late_events', 'Delayed payment events', 'Event-time lag', 'Recent pipeline execution does not compensate for old ingested event timestamps.', 'Streaming operations'),
    ('permission_gap', 'Freshness access denied', 'Missing permissions', 'The app cannot observe the table. It must abstain and block recovery.', 'Data governance'),
    ('quality_failure', 'Duplicate transaction IDs', 'Quality failure', 'Fresh data fails an illustrative uniqueness rule; freshness alone cannot clear the case.', 'Payments quality'),
    ('expired_check', 'Expired freshness observation', 'Expired evidence', 'A previously healthy check is 35 minutes old, past its 15-minute evidence TTL.', 'Platform operations'),
    ('lineage_gap', 'Incomplete lineage coverage', 'Coverage gap', 'The table is stale and observed lineage is unavailable; only declared dependencies remain.', 'Analytics engineering'),
    ('recovered', 'Recovered cash-position pipeline', 'Recovery verified', 'A failure is followed by a successful rerun and a fresh table observation.', 'Treasury analytics'),
]


def demo_config():
    assets, dependencies = [], []
    for index, (key, name, label, description, owner) in enumerate(SCENARIOS):
        identity = 'demo.'+key
        asset = {'id':identity,'name':name,'scenario':key,'scenario_label':label,'description':description,'owner':owner,'table':'demo.payments.'+key,'job_id':100+index,'max_age_minutes':60,'check_ttl_minutes':15,'runbook':'Review the evidence with the asset owner. Fix the underlying issue, rerun the pipeline, and verify freshness and quality before closing.'}
        if key == 'late_events':asset['freshness_column']='ingested_at'
        assets.append(asset)
        curated=identity+'.curated'
        assets.append({'id':curated,'name':'Curated payment metrics','type':'TABLE','owner':'Analytics engineering'})
        assets.append({'id':identity+'.report','name':'Treasury dashboard','type':'REPORT','owner':'Treasury operations'})
        assets.append({'id':identity+'.ops_report','name':'Operations report','type':'REPORT','owner':'Payments operations'})
        dependencies.extend([{'source':identity,'target':curated}, {'source':curated,'target':identity+'.report'}, {'source':curated,'target':identity+'.ops_report'}])
    return {'assets':assets,'dependencies':dependencies}


def scenario_events(config, now=None):
    now = now or datetime.now(timezone.utc)
    events=[]
    for asset in config['assets']:
        key=asset.get('scenario')
        if not key:continue
        def add(suffix,kind,state,minutes,message,**extra):
            events.append(dict(id=asset['id']+'-'+suffix,asset_id=asset['id'],kind=kind,state=state,time=(now-timedelta(minutes=minutes)).isoformat(),message=message,source='Synthetic demo',**extra))
        add('previous','run','FAILED' if key=='recovered' else 'SUCCEEDED',180,'Earlier ingestion failed.' if key=='recovered' else 'Previous scheduled run completed.')
        add('latest','run','FAILED' if key=='failed_pipeline' else 'SUCCEEDED',8,'SCHEMA_MISMATCH: source amount changed from DECIMAL to STRING.' if key=='failed_pipeline' else 'Latest pipeline execution completed successfully.')
        age=180 if key in {'failed_pipeline','stale_success','late_events','lineage_gap'} else 5
        observed=(now-timedelta(minutes=age)).isoformat()
        state='UNKNOWN' if key=='permission_gap' else ('STALE' if age>60 else 'FRESH')
        add('freshness','freshness',state,35 if key=='expired_check' else 0,
            'Simulated SELECT permission denied; table health is unknown.' if key=='permission_gap' else f'Simulated table timestamp is {age} minutes old; freshness threshold is 60 minutes.',
            observed_at=None if key=='permission_gap' else observed,age_minutes=age,basis='max_event_timestamp' if key=='late_events' else 'latest_delta_commit')
        if key=='quality_failure':
            add('quality','quality','FAILED',0,'Simulated uniqueness check: 243 duplicate transaction IDs out of 50,000 records.')
        if key=='lineage_gap':
            add('coverage','coverage','UNKNOWN',0,'Simulated lineage permission gap. The declared report dependency remains; additional downstream exposure is unknown.')
    return events


def recovery_events(asset, now=None):
    now=now or datetime.now(timezone.utc)
    prefix=asset['id']+'-recovery-'+now.isoformat()
    common={'asset_id':asset['id'],'time':now.isoformat(),'source':'Synthetic demo'}
    return [dict(common,id=prefix+'-run',kind='run',state='SUCCEEDED',message='Simulated recovery rerun completed.'),
            dict(common,id=prefix+'-fresh',kind='freshness',state='FRESH',observed_at=now.isoformat(),basis='latest_delta_commit',message='Simulated recovery: table timestamp is current.'),
            dict(common,id=prefix+'-quality',kind='quality',state='PASSED',message='Simulated recovery: quality checks passed.')]


def downstream_events(asset, investigation, now=None):
    now = now or datetime.now(timezone.utc)
    return [{'id': asset['id']+'-downstream-'+target+'-'+now.isoformat(), 'asset_id':asset['id'], 'target_asset_id':target, 'kind':'downstream_refresh', 'state':'PASSED', 'time':now.isoformat(), 'source':'Synthetic demo', 'simulated':True, 'message':'Simulated downstream refresh and validation passed: '+target} for target in investigation['potentially_affected']]

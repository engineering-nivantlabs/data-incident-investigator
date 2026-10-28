"""Pure evaluator. Never publishes messages, changes delivery state, or executes recovery."""
from .contracts import fingerprint


def evaluate(run):
    manifest=run['manifest'];clock=run['clock'];config=run['config']
    required=[(row,ch,v) for row in manifest for ch,v in row['channels'].items() if v['required']]
    pending=[(row,ch,v) for row,ch,v in required if v['state']!='SUCCEEDED']
    unknown=any(v['state']=='UNKNOWN' for _,_,v in pending)
    exhausted=any(v['attempts']>=config['max_attempts'] and v['state']=='FAILED' for _,_,v in pending)
    complete=bool(manifest) and not pending
    action=None;reasons=[]
    if run['quarantine']:
        status='BLOCKED';reasons=['Invalid or conflicting records require review; delivered valid records do not prove full-file completeness.']
    elif any(v['state']=='CONFLICT' for _,_,v in pending):
        status='BLOCKED';reasons=['An accepted broker key has conflicting payload evidence. Escalate; do not blindly replay.']
    elif exhausted:
        status='BLOCKED';reasons=['A required channel exhausted its retry budget. Escalate instead of automatically replaying.']
    elif not config['kafka_enabled']:
        status='BLOCKED';reasons=['Required Kafka delivery is disabled/deferred in this configuration.']
    elif run['stage'] in {'CREATED','VALIDATED'}:
        status='BREACHED' if clock>config['deadline_seconds'] else 'AT_RISK' if clock-run['last_progress']>=config['stall_seconds'] else 'ON_TRACK'
        reasons=['File awaits ingestion or distribution. No delivery completion is inferred.']
    elif run['stage']=='STALLED':
        status='BREACHED' if clock>config['deadline_seconds'] else 'AT_RISK'
        reasons=['Worker made no progress before channel publication. Resume the worker without replaying successful distributions.'];action='resume_processing'
    elif complete:
        status='ON_TRACK';reasons=['All required channels acknowledged the valid records. Optional channel failures do not create a delivery breach.']
    else:
        status='BREACHED' if clock>config['deadline_seconds'] else 'AT_RISK'
        reasons=['Required channel delivery remains incomplete.']
        if unknown:
            reasons.append('Broker acknowledgment is ambiguous. Reconcile accepted keys before considering a retry.')
            action='reconcile_kafka'
        elif all(row['channels']['kafka']['state']=='SUCCEEDED' for row in manifest):
            action='retry_mfts';reasons.append('Kafka is complete. Recover MFTS only; do not republish Kafka.')
        else: action='retry_failed'
    evidence={'version':1,'request_id':run['id'],'clock':clock,'config':config,'manifest':manifest,'quarantine':run['quarantine'],'stage':run['stage'],'broker':run['broker'],'receiver':run['receiver']}
    return {'status':status,'reasons':reasons,'recommended_action':action,'required_complete':complete,'pending_channels':sorted({ch for _,ch,_ in pending}),'fingerprint':fingerprint(evidence),'deadline_breached':clock>config['deadline_seconds'],'remaining_seconds':config['deadline_seconds']-clock}

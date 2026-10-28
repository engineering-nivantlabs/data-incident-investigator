"""Local execution service: canonical state + manifests + Signal projection commit atomically."""
import copy
import json
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from .contracts import canonicalize, fingerprint
from .adapters import LocalBroker, LocalTransfer
from .advisor import evaluate
from .scenarios import SCENARIOS

BASE_CONFIG={'dataset':'synthetic_treasury_v1','routes':{'TREASURY':'synthetic.treasury','OPERATIONS':'synthetic.operations'},'kafka_enabled':True,'mfts_required':True,'max_attempts':3,'deadline_seconds':600,'stall_seconds':300}


class DeliveryLab:
    def __init__(self,store):
        self.store=store
        with store.connect() as db:
            db.executescript('CREATE TABLE IF NOT EXISTS delivery_runs (id TEXT PRIMARY KEY, body TEXT NOT NULL); CREATE TABLE IF NOT EXISTS delivery_config_history (revision INTEGER PRIMARY KEY AUTOINCREMENT, digest TEXT UNIQUE, body TEXT NOT NULL);')

    def runs(self):
        with self.store.connect() as db:
            return [json.loads(r[0]) for r in db.execute('SELECT body FROM delivery_runs ORDER BY rowid DESC')]

    def get(self,identity):
        with self.store.connect() as db:
            row=db.execute('SELECT body FROM delivery_runs WHERE id=?',(identity,)).fetchone()
        if not row:raise ValueError('Unknown delivery run')
        return json.loads(row[0])

    def audit(self,run,kind,message,actor='simulator',**details):
        run['audit'].append({'sequence':len(run['audit'])+1,'kind':kind,'message':message,'actor':actor,'virtual_second':run['clock'],'recorded_at':datetime.now(timezone.utc).isoformat(),'details':details})

    def create(self,scenario,workflow_plan=None):
        if scenario not in SCENARIOS:raise ValueError('Unknown scenario')
        spec=SCENARIOS[scenario];config=copy.deepcopy(BASE_CONFIG)
        config['mfts_required']=not spec.get('optional_mfts',False)
        config['kafka_enabled']=spec.get('kafka_enabled',True)
        source=Path(__file__).parent/'fixtures/treasury_batch.json'
        records=json.loads(source.read_text())
        if spec.get('invalid'):records += [{'source_record_id':'SYN-BAD','route_key':'TREASURY','amount':'not-a-number','currency':'USD'}]
        if spec.get('duplicate'):records += [copy.deepcopy(records[0])]
        with self.store.connect() as db:
            digest=fingerprint(config)
            db.execute('INSERT OR IGNORE INTO delivery_config_history (digest,body) VALUES (?,?)',(digest,json.dumps(config)))
            revision=db.execute('SELECT revision FROM delivery_config_history WHERE digest=?',(digest,)).fetchone()[0]
        config['revision']=revision
        run={'id':str(uuid.uuid4()),'scenario':scenario,'name':spec['name'],'story':spec['story'],'clock':0,'last_progress':0,'created_at':datetime.now(timezone.utc).isoformat(),'stage':'CREATED','config':config,'records':records,'source_file':source.name,'file_fingerprint':fingerprint(records),'request_key':fingerprint({'dataset':config['dataset'],'file':fingerprint(records),'revision':revision}),'manifest':[],'quarantine':[],'duplicates':0,'broker':[],'receiver':{},'consumers':{},'audit':[],'proposal':None,'fault_consumed':False,'snapshots':[]}
        if workflow_plan is not None:
            if workflow_plan['status']=='BLOCKED':raise ValueError('Blocked workflow cannot run')
            plan_hash=fingerprint(workflow_plan)
            run['workflow']={'plan_fingerprint':plan_hash,'plan':copy.deepcopy(workflow_plan), 'definition':copy.deepcopy(workflow_plan['local_workflow']), 'status':'QUEUED','tasks':[]}
            self.audit(run,'WORKFLOW_CREATED','Generated plan bound to local execution profile.',plan_fingerprint=plan_hash)
        self.audit(run,'FILE_ARRIVAL','Synthetic file arrival registered; content hash and configuration revision captured.',records=len(records),file_fingerprint=run['file_fingerprint'])
        with self.store.connect() as db:self._save(db,run)
        return run

    def ingest(self,run):
        if run['stage']!='CREATED':raise ValueError('File has already been ingested')
        by_key={}
        for record in run['records']:
            try:
                item=canonicalize(record,run['config']['dataset'],run['config']['routes'])
                key=item['distribution_id']
                if key in by_key:
                    if by_key[key]['payload_fingerprint']!=item['payload_fingerprint']:raise ValueError('Conflicting payload for an existing source identity')
                    run['duplicates']+=1;continue
                by_key[key]=item
            except ValueError as exc:
                run['quarantine'].append({'record':record,'reason':str(exc)})
        for item in by_key.values():
            run['manifest'].append({'distribution_id':item['distribution_id'],'record':item,'topic':run['config']['routes'][item['route_key']], 'channels':{'kafka':{'required':True,'state':'PENDING','attempts':0},'mfts':{'required':run['config']['mfts_required'],'state':'PENDING','attempts':0}}})
        run['stage']='VALIDATED';run['clock']+=30;run['last_progress']=run['clock']
        self.audit(run,'VALIDATION','Canonical records, quarantine and duplicate suppression persisted.',valid=len(run['manifest']),quarantined=len(run['quarantine']),duplicates=run['duplicates'])

    def send(self,run,channels,inject=False,replay_only=False):
        spec=SCENARIOS[run['scenario']];fault=spec['fault'] if inject and not run['fault_consumed'] else 'none'
        if fault=='stall':
            run['stage']='STALLED';run['clock']=max(run['clock'],spec['clock']);run['fault_consumed']=True
            self.audit(run,'WORKER_STALLED','Fault injection stopped processing before publication.');return
        broker=LocalBroker(run['broker']);transfer=LocalTransfer(run['receiver'])
        for row in run['manifest']:
            for name in channels:
                channel=row['channels'][name]
                if replay_only and channel['state']!='SUCCEEDED':
                    self.audit(run,'REPLAY_NOT_REQUESTED','Incomplete optional channel left unchanged; replay does not authorize recovery.',channel=name,key=row['distribution_id']);continue
                if channel['state']=='SUCCEEDED':
                    self.audit(run,'REPLAY_SKIPPED','Acknowledged distribution skipped.',channel=name,key=row['distribution_id']);continue
                if name=='kafka' and not run['config']['kafka_enabled']:
                    channel['state']='DEFERRED';continue
                if channel['state']=='UNKNOWN':raise ValueError('Ambiguous acknowledgment requires reconciliation before retry')
                if channel['attempts']>=run['config']['max_attempts']:
                    continue
                tries=run['config']['max_attempts'] if fault=='kafka_reject' and name=='kafka' else 1
                for _ in range(tries):
                    channel['attempts']+=1
                    if name=='kafka':
                        mode='timeout_after_accept' if fault=='kafka_timeout' else 'reject' if fault=='kafka_reject' else 'ok'
                        channel['state']=broker.publish(row['distribution_id'],row['record'],mode,topic=row['topic'])
                    else:channel['state']=transfer.deliver(row['distribution_id'],row['record'],'timeout' if fault=='mfts_timeout' else 'ok')
                    self.audit(run,'CHANNEL_RESULT','Local adapter returned delivery evidence.',channel=name,key=row['distribution_id'],attempt=channel['attempts'],state=channel['state'])
        run['clock']=max(run['clock']+15,spec['clock'] if inject else run['clock']+15)
        run['stage']='DISTRIBUTED';run['last_progress']=run['clock'];run['fault_consumed']=True

    def _snapshot(self,run,label):
        run['snapshots'].append({'label':label,'at':run['clock'],'lineage':self.projection(run)})

    def verify_consumers(self,run):
        risk=evaluate(run)
        if not risk['required_complete'] or run['quarantine']:raise ValueError('Required deliveries and file quality must pass before consumer refresh')
        for name in ['treasury','operations']:
            expected=[i for i in run['manifest'] if i['record']['route_key']==name.upper()]
            keys={i['distribution_id'] for i in expected}
            broker_payloads={row['key']:row['payload'] for row in run['broker']}
            kafka_ok=all(broker_payloads.get(i['distribution_id'])==i['record'] for i in expected)
            mfts_ok=all(run['receiver'].get(i['distribution_id'])==i['record'] for i in expected) if run['config']['mfts_required'] else True
            if not kafka_ok or not mfts_ok:raise ValueError('Receiver contents do not match the expected canonical records')
            run['consumers'][name]={'state':'CURRENT','records':len(keys),'verified_at':run['clock'],'content_fingerprint':fingerprint([i['record'] for i in expected])}
            self.audit(run,'CONSUMER_VERIFIED','Consumer contents matched canonical records in local receivers.',consumer=name,records=len(keys))
        self._snapshot(run,'3 · Consumers verified')

    def command(self,identity,command,data=None):
        data=data or {}
        with self.store.connect() as db:
            # Cross-process claim and state/evidence/projection changes share one SQLite transaction.
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT body FROM delivery_runs WHERE id=?',(identity,)).fetchone()
            if not row:raise ValueError('Unknown delivery run')
            run=json.loads(row[0])
            guide_label=None
            if command=='guided_start':
                if not run.get('workflow') or run['stage']!='CREATED' or run.get('guided_demo'):
                    raise ValueError('Guided demo requires a fresh generated workflow')
                run['guided_demo']=[]
                guide_label='Workflow ready'
                command='guided_observe'
            elif command=='guided_next':
                if not run.get('guided_demo'):raise ValueError('Start the guided demo first')
                step=len(run['guided_demo'])
                if data.get('expected_step')!=step:raise ValueError('Demo step changed; reload before continuing')
                steps=[('run_workflow','Execute workflow'),('guided_observe','Detect failure'),('guided_observe','Investigate impact'),('request_approval','Request recovery'),('approve','Approve recovery'),('execute','Recover MFTS only'),('refresh_consumers','Verify consumers')]
                if step>len(steps):raise ValueError('Guided demo is complete')
                command,guide_label=steps[step-1]
            if command=='guided_observe':
                if not guide_label:raise ValueError('Use the guided demo controls for evidence review')
                self.audit(run,'GUIDED_REVIEW','Guided evidence review: '+guide_label)
            elif command=='run_workflow':
                if not run.get('workflow'):raise ValueError('Run is not linked to a generated workflow')
                if run['stage']!='CREATED':raise ValueError('Workflow has already started; use approved recovery')
                completed=set()
                for task in run['workflow']['definition']['tasks']:
                    if not set(task['depends_on']).issubset(completed):break
                    if task['operation']=='ingest':
                        self.ingest(run)
                        if run['quarantine']:break
                    elif task['operation']=='dispatch':
                        self.send(run,['kafka','mfts'],True)
                        self._snapshot(run,'1 · Delivery observed')
                        if not evaluate(run)['required_complete']:break
                    elif task['operation']=='refresh_consumers':self.verify_consumers(run)
                    else:raise ValueError('Unsupported local task operation')
                    completed.add(task['key'])
                self.audit(run,'WORKFLOW_EXECUTED','Local ingest and distribution executed; verification is gated by channel evidence.')
            elif command=='ingest':self.ingest(run)
            elif command in {'dispatch','run_pipeline'}:
                if command=='run_pipeline' and run['stage']=='CREATED':self.ingest(run)
                if run['stage']!='VALIDATED':raise ValueError('Ingest the file before distribution; use replay to test completed keys')
                self.send(run,['kafka','mfts'],True)
                self._snapshot(run,'1 · Delivery observed')
            elif command=='tick':
                seconds=data.get('seconds',300)
                if not isinstance(seconds,int) or not 1<=seconds<=3600:raise ValueError('Clock advance must be 1–3600 seconds')
                run['clock']+=seconds;self.audit(run,'CLOCK_ADVANCED','Virtual deadline clock advanced.',seconds=seconds)
            elif command=='request_approval':
                risk=evaluate(run);action=data.get('action') or risk['recommended_action']
                if not action or action!=risk['recommended_action']:raise ValueError('Only the current advisor recommendation can be proposed')
                requester=str(data.get('actor','operator-demo')).strip()
                if not requester or len(requester)>80:raise ValueError('Requester is required (max 80 characters)')
                run['proposal']={'action':action,'fingerprint':risk['fingerprint'],'requester':requester,'status':'REQUESTED'}
                self.audit(run,'RECOVERY_REQUESTED','Recovery proposal bound to current evidence.',requester,action=action,fingerprint=risk['fingerprint'])
            elif command=='approve':
                proposal=run['proposal'];risk=evaluate(run)
                if not proposal or proposal['status']!='REQUESTED':raise ValueError('Request recovery approval first')
                actor=str(data.get('actor','architect-demo')).strip()
                if not actor or len(actor)>80 or actor==proposal['requester']:raise ValueError('A different simulated approver is required')
                if proposal['fingerprint']!=risk['fingerprint']:raise ValueError('Evidence changed; request a fresh approval')
                proposal.update(status='APPROVED',approver=actor)
                self.audit(run,'APPROVAL_RECORDED','Simulated human approval recorded. This is not enterprise identity enforcement.',actor,action=proposal['action'])
            elif command=='execute':
                risk=evaluate(run);proposal=run['proposal']
                if not proposal or proposal['status']!='APPROVED' or proposal['fingerprint']!=risk['fingerprint']:raise ValueError('Fresh approved evidence is required before execution')
                run['consumers']={}
                action=proposal['action'];before=len(run['broker'])
                if action=='reconcile_kafka':
                    broker=LocalBroker(run['broker'])
                    for item in run['manifest']:
                        channel=item['channels']['kafka']
                        if channel['state']=='UNKNOWN':
                            result=broker.reconcile(item['distribution_id'],item['record'])
                            channel['state']='SUCCEEDED' if result=='MATCH' else 'CONFLICT' if result=='CONFLICT' else 'FAILED'
                            self.audit(run,'ACK_RECONCILED','Checked accepted local broker keys without publishing.',key=item['distribution_id'],state=channel['state'])
                elif action=='retry_mfts':
                    if not all(i['channels']['kafka']['state']=='SUCCEEDED' for i in run['manifest']):raise ValueError('Kafka completion is required for MFTS-only recovery')
                    self.send(run,['mfts'])
                elif action in {'retry_failed','resume_processing'}:self.send(run,['kafka','mfts'])
                else:raise ValueError('Unsupported recovery action')
                proposal['status']='EXECUTED'
                self.audit(run,'RECOVERY_EXECUTED','Approved local recovery executed.',proposal['approver'],action=action,kafka_writes_before=before,kafka_writes_after=len(run['broker']))
                self._snapshot(run,'2 · Channels recovered')
            elif command=='refresh_consumers':
                self.verify_consumers(run)
            elif command=='replay':
                if not evaluate(run)['required_complete']:raise ValueError('Replay demonstration requires completed required channels')
                self.send(run,['kafka','mfts'],replay_only=True)
                self.audit(run,'REPLAY_COMPLETE','Replay completed; acknowledged channel writes were skipped.')
            else:raise ValueError('Unknown command')
            self._save(db,run,guide_label)
        return run

    def graph_assets(self,run):
        root='delivery:'+run['id']
        return [{'id':root,'name':'Canonical treasury data','type':'DATASET','table':'synthetic.delivery.canonical','kind':'delivery','owner':'Synthetic treasury team'}, {'id':root+':kafka','name':'Local Kafka adapter','type':'CHANNEL'}, {'id':root+':mfts','name':'Local MFTS adapter','type':'CHANNEL'}, {'id':root+':treasury','name':'Treasury dashboard','type':'REPORT'}, {'id':root+':operations','name':'Operations report','type':'REPORT'}]

    def projection(self,run):
        risk=evaluate(run);root='delivery:'+run['id'];states={}
        complete=risk['required_complete'] and not run['quarantine']
        source_ok=bool(run['manifest']) and not run['quarantine']
        states[root]={'status':'healthy' if source_ok else 'failed' if run['quarantine'] else 'pending','caption':'Validated file' if source_ok else 'Quarantined records' if run['quarantine'] else 'Awaiting ingestion'}
        for name in ['kafka','mfts']:
            values=[r['channels'][name] for r in run['manifest']]
            done=bool(values) and all(v['state']=='SUCCEEDED' for v in values)
            optional=name=='mfts' and not run['config']['mfts_required']
            states[root+':'+name]={'status':'healthy' if done else 'unknown' if optional else 'failed' if any(v['state'] in {'FAILED','UNKNOWN','DEFERRED','CONFLICT'} for v in values) else 'pending','caption':'Acknowledged' if done else 'Optional · not gating' if optional else 'Delivery incomplete'}
        for name in ['treasury','operations']:
            done=complete and run['consumers'].get(name,{}).get('state')=='CURRENT'
            states[root+':'+name]={'status':'healthy' if done else 'pending' if complete else 'affected','caption':'Contents verified locally' if done else 'Refresh pending' if complete else 'Consumer at risk'}
        deps=[{'source':root,'target':root+':'+ch,'origin':'synthetic executed flow'} for ch in ['kafka','mfts']]
        for ch in ['kafka','mfts']:
            if ch=='mfts' and not run['config']['mfts_required']:continue
            deps.extend({'source':root+':'+ch,'target':root+':'+name,'origin':'synthetic consumer contract'} for name in ['treasury','operations'])
        healthy=complete and all(run['consumers'].get(n,{}).get('state')=='CURRENT' for n in ['treasury','operations'])
        timeline=[{'id':run['id']+':'+str(e['sequence']),'asset_id':root,'kind':e['kind'],'state':e['details'].get('state','RECORDED'),'time':e['recorded_at'],'message':e['message']+' '+json.dumps(e['details'],sort_keys=True),'source':'Executed local delivery simulator'} for e in run['audit']]
        return {'downstream_complete':healthy,'hypotheses':[] if healthy else [{'cause':'Delivery '+risk['status'],'assessment':'supported','evidence_ids':[risk['fingerprint']],'detail':' '.join(risk['reasons'])+(' Consumer refresh is pending.' if complete else '')}], 'evidence':timeline[-8:],'timeline':timeline,'potentially_affected':[a['id'] for a in self.graph_assets(run)[1:]],'recovery_verified':healthy,'dependencies':deps,'node_states':states,'coverage':'Executed synthetic flow. Local adapter evidence only; no Kafka, MFTS, Spark or Databricks connection is used by Delivery Lab.','runbook':'Request approval for the recommended channel-specific action, execute it, then refresh and verify consumers.'}

    def _save(self,db,run,guide_label=None):
        run['risk']=evaluate(run)
        projection=self.projection(run)
        if run.get('workflow'):
            started=run['stage']!='CREATED'
            validated=started and not run['quarantine']
            delivered=run['risk']['required_complete'] and validated
            verified=projection['recovery_verified']
            states=['SUCCEEDED' if validated else 'FAILED' if run['quarantine'] else 'PENDING',
                    'SUCCEEDED' if delivered else 'BLOCKED' if run['quarantine'] else 'FAILED' if started else 'PENDING',
                    'SUCCEEDED' if verified else 'READY' if delivered else 'BLOCKED']
            run['workflow']['tasks']=[dict(task,status=status) for task,status in zip(run['workflow']['definition']['tasks'],states)]
            run['workflow']['status']='SUCCEEDED' if verified else 'VERIFY_PENDING' if delivered else 'RECOVERY_REQUIRED' if started else 'QUEUED'
        run['projection']=projection
        if guide_label:
            view=copy.deepcopy({k:v for k,v in run.items() if k!='guided_demo'})
            run['guided_demo'].append({'label':guide_label,'view':view})
        identity='delivery-incident:'+run['id']
        existing=db.execute('SELECT body FROM incidents WHERE id=?',(identity,)).fetchone()
        case=json.loads(existing[0]) if existing else {'id':identity,'asset_id':'delivery:'+run['id'],'title':run['name'],'status':'open','assignee':'Synthetic treasury team','history':[]}
        # A later replay/fault that invalidates recovery reopens a previously resolved projection.
        if case['status']=='resolved' and not projection['recovery_verified']:
            case['status']='open'
        case.update(investigation=projection,updated_at=datetime.now(timezone.utc).isoformat())
        db.execute('INSERT OR REPLACE INTO delivery_runs VALUES (?,?)',(run['id'],json.dumps(run)))
        db.execute('INSERT OR REPLACE INTO incidents VALUES (?,?)',(identity,json.dumps(case)))
        db.executemany('INSERT OR REPLACE INTO events VALUES (?,?)',[(e['id'],json.dumps(e)) for e in projection['timeline']])

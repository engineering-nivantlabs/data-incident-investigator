"""Synthetic-only Fraud Guard. SQLite owns decisions, simulated authorization and Signal evidence."""
import copy
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone

POLICY={'version':'demo-rules-v1','velocity_window_seconds':60,'velocity_limit':4,'large_amount_minor':100000,'approval_ttl_seconds':120,'challenge_ttl_seconds':120,'fallback':'HOLD'}
SCENARIOS={
 'review_timeout':{'name':'Analyst review and uncertain authorization','story':'A large purchase needs review. Release it, then reconcile an issuer acknowledgment timeout without another authorization.'},
 'normal':{'name':'Normal purchase','story':'Fraud clearance permits authorization. The issuer simulator approves.'},
 'issuer_decline':{'name':'Issuer declines after fraud clearance','story':'Low fraud risk does not override the issuer decision.'},
 'velocity':{'name':'Rapid repeated purchases','story':'Four attempts within one minute trigger the velocity block rule.'},
 'challenge':{'name':'New device and unusual amount','story':'The transaction must pass simulated customer authentication before authorization.'},
 'challenge_fail':{'name':'Customer authentication fails','story':'Failed authentication stops authorization and cannot be overridden by an analyst release.'},
 'risk_unavailable':{'name':'Risk service unavailable','story':'An injected service outage invokes the explicit HOLD fallback. No issuer call occurs.'},
 'authorization_timeout':{'name':'Issuer accepted but acknowledgment lost','story':'The local issuer ledger records an approval. The gateway sees UNKNOWN and must reconcile before any retry.'}
}

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def validate_transaction(value):
    fields={'transaction_id','idempotency_key','card_token','account_token','amount_minor','currency','merchant','device','country','event_second'}
    if not isinstance(value,dict) or set(value)!=fields:raise ValueError('Use only the synthetic transaction contract; extra or missing fields are rejected')
    for key in fields-{'amount_minor','event_second','currency','country'}:
        if not isinstance(value[key],str) or not re.fullmatch(r'SYN-[A-Za-z0-9_-]{1,64}',value[key]):raise ValueError(key+' must be a synthetic SYN- identifier')
    for key in ['amount_minor','event_second']:
        if type(value[key]) is not int or value[key]<0:raise ValueError(key+' must be a nonnegative integer')
    if not 0<value['amount_minor']<=100000000:raise ValueError('Amount must be 1–100000000 minor units')
    if value['currency']!='USD':raise ValueError('Synthetic MVP supports USD minor units only')
    if value['country'] not in {'US','GB','IN','CA'}:raise ValueError('Unsupported synthetic country')
    return copy.deepcopy(value)

def features(tx,history,clock,policy):
    # Online receipt clock drives velocity. Future/out-of-order event times cannot evade it.
    recent=[h for h in history if h['card_token']==tx['card_token'] and clock-policy['velocity_window_seconds']<=h['received_second']<=clock]
    known=any(h['account_token']==tx['account_token'] and h['device']==tx['device'] for h in history)
    return {'attempts_last_minute_including_current':len(recent)+1,'known_device':known,'amount_minor':tx['amount_minor'],'feature_clock':clock}

def evaluate(tx,observed,policy,available=True):
    if not available:return {'decision':'UNAVAILABLE','reasons':['Risk service unavailable. Policy holds the transaction.'],'rule_ids':['service-fallback']}
    if observed['attempts_last_minute_including_current']>=policy['velocity_limit']:
        return {'decision':'BLOCK','reasons':['Attempt velocity reached the configured one-minute limit.'],'rule_ids':['velocity-limit']}
    large=tx['amount_minor']>=policy['large_amount_minor']
    if large and not observed['known_device']:
        return {'decision':'CHALLENGE','reasons':['Large amount on an unfamiliar device requires customer authentication.'],'rule_ids':['new-device-large-amount']}
    if large:return {'decision':'REVIEW','reasons':['Large amount on a known device requires an analyst review under demo policy.'],'rule_ids':['large-amount-review']}
    return {'decision':'ALLOW','reasons':['No configured fraud rule triggered. Continue normal issuer authorization.'],'rule_ids':['default-allow']}

class IssuerSimulator:
    def authorize(self,run,tx):
        key=tx['input']['idempotency_key'];payload=digest(tx['input'])
        old=next((v for v in run['issuer_ledger'] if v['key']==key),None)
        if old:
            if old['payload_fingerprint']!=payload:raise ValueError('Conflicting issuer idempotency payload')
            return old
        receipt={'key':key,'payload_fingerprint':payload,'authorization_id':'SYN-AUTH-'+uuid.uuid4().hex[:12],'outcome':'DECLINED' if run['scenario']=='issuer_decline' else 'APPROVED'}
        run['issuer_ledger'].append(receipt)
        return receipt

class GatewaySimulator:
    def authorize(self,run,tx):
        receipt=IssuerSimulator().authorize(run,tx)
        timeout=run['scenario'] in {'review_timeout','authorization_timeout'} and tx['input']['transaction_id']=='SYN-FLAGGED'
        tx['gateway']='UNKNOWN' if timeout else receipt['outcome']
        tx['authorization_id']=None if timeout else receipt['authorization_id']

class FraudLab:
    def __init__(self,store):
        self.store=store
        with store.connect() as db:db.execute('CREATE TABLE IF NOT EXISTS fraud_runs (id TEXT PRIMARY KEY, body TEXT NOT NULL)')
    def runs(self):
        with self.store.connect() as db:return [json.loads(r[0]) for r in db.execute('SELECT body FROM fraud_runs ORDER BY rowid DESC')]
    def get(self,identity):
        with self.store.connect() as db:r=db.execute('SELECT body FROM fraud_runs WHERE id=?',(identity,)).fetchone()
        if not r:raise ValueError('Unknown fraud run')
        return json.loads(r[0])
    def audit(self,run,kind,message,**details):
        run['audit'].append({'sequence':len(run['audit'])+1,'kind':kind,'message':message,'virtual_second':run['clock'],'time':datetime.now(timezone.utc).isoformat(),'details':details})
    def create(self,scenario='review_timeout',guided=False):
        if scenario not in SCENARIOS:raise ValueError('Unknown fraud scenario')
        tx={'transaction_id':'SYN-FLAGGED','idempotency_key':'SYN-KEY-FLAGGED','card_token':'SYN-CARD-1','account_token':'SYN-ACCOUNT-1','amount_minor':150000 if scenario in {'review_timeout','challenge','challenge_fail'} else 2500,'currency':'USD','merchant':'SYN-MERCHANT-1','device':'SYN-DEVICE-1','country':'US','event_second':1000}
        baseline=dict(tx,transaction_id='SYN-NORMAL',idempotency_key='SYN-KEY-NORMAL',card_token='SYN-CARD-2',account_token='SYN-ACCOUNT-2',amount_minor=1500)
        history=[dict(tx,received_second=900,amount_minor=2000)]
        if scenario in {'challenge','challenge_fail'}:tx['device']='SYN-DEVICE-NEW'
        if scenario=='velocity':history=[dict(tx,transaction_id='SYN-PRIOR-'+str(i),received_second=t) for i,t in enumerate([950,970,990])]
        run={'id':str(uuid.uuid4()),'scenario':scenario,'name':SCENARIOS[scenario]['name'],'story':SCENARIOS[scenario]['story'],'clock':1000,'policy':copy.deepcopy(POLICY),'history':history,'transactions':[],'issuer_ledger':[],'audit':[],'guided':bool(guided),'steps':[]}
        for item in ([baseline,tx] if guided else [tx]):self._submit(run,item)
        self.audit(run,'SESSION_CREATED','Synthetic merchant-side payment session created. No external service is connected.',policy_version=POLICY['version'])
        with self.store.connect() as db:self._save(db,run,'Synthetic workflow ready' if guided else None)
        return run
    def _submit(self,run,data):
        value=validate_transaction(data)
        if value['event_second']>run['clock']:raise ValueError('Future synthetic event time is not accepted')
        for item in run['transactions']:
            existing=item['input']
            if existing['idempotency_key']==value['idempotency_key'] or existing['transaction_id']==value['transaction_id']:
                if existing!=value:raise ValueError('Conflicting transaction identity or idempotency payload')
                return item
        item={'input':value,'features':None,'risk':None,'challenge':'NOT_REQUESTED','challenge_expires':None,'proposal':None,'released':False,'gateway':'NOT_CALLED','authorization_id':None}
        run['transactions'].append(item)
        self.audit(run,'TRANSACTION_RECEIVED','Synthetic payment request validated.',transaction_id=value['transaction_id'],payload_fingerprint=digest(value))
        return item
    def fingerprint(self,run,tx):
        return digest({'input':tx['input'],'features':tx['features'],'risk':tx['risk'],'challenge':tx['challenge'],'gateway':tx['gateway'],'clock':run['clock'],'policy':run['policy']})
    def command(self,identity,command,data=None):
        data=data or {}
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT body FROM fraud_runs WHERE id=?',(identity,)).fetchone()
            if not row:raise ValueError('Unknown fraud run')
            run=json.loads(row[0]);label=None
            if command=='guided_next':
                if not run['guided'] or data.get('expected_step')!=len(run['steps']):raise ValueError('Guided step changed; reload before continuing')
                flow=[('evaluate','Evaluate fraud rules'),('authorize','Authorize normal purchase'),('observe','Investigate flagged purchase'),('request_release','Request analyst release'),('approve_release','Approve current evidence'),('release','Release reviewed transaction'),('authorize','Gateway acknowledgment timeout'),('reconcile','Reconcile issuer receipt')]
                step=len(run['steps'])-1
                if not 0<=step<len(flow):raise ValueError('Guided demo is complete')
                command,label=flow[step]
                data=dict(data,transaction_id='SYN-FLAGGED')
            tx=next((t for t in run['transactions'] if t['input']['transaction_id']==data.get('transaction_id','SYN-FLAGGED')),None)
            if command=='submit':self._submit(run,data.get('transaction'))
            elif command=='evaluate':
                for item in run['transactions']:
                    if item['risk']:continue
                    item['features']=features(item['input'],run['history'],run['clock'],run['policy'])
                    item['risk']=evaluate(item['input'],item['features'],run['policy'],run['scenario']!='risk_unavailable')
                    run['history'].append(dict(item['input'],received_second=run['clock']))
                    if item['risk']['decision']=='CHALLENGE':
                        item['challenge']='PENDING';item['challenge_expires']=run['clock']+run['policy']['challenge_ttl_seconds']
                    self.audit(run,'RISK_EVALUATED','Versioned fraud rules evaluated observed features.',transaction_id=item['input']['transaction_id'],features=item['features'],**item['risk'])
            elif command=='authorize':
                for item in run['transactions']:
                    if item['gateway'] in {'APPROVED','DECLINED'}:
                        self.audit(run,'AUTHORIZATION_REUSED','Existing terminal result reused; no new issuer call.',transaction_id=item['input']['transaction_id']);continue
                    if item['gateway']=='UNKNOWN':raise ValueError('Authorization outcome is UNKNOWN; reconcile before retry')
                    if not self.eligible(run,item):continue
                    GatewaySimulator().authorize(run,item)
                    self.audit(run,'GATEWAY_RESULT','Gateway simulator returned an authorization observation.',transaction_id=item['input']['transaction_id'],state=item['gateway'])
            elif command in {'request_release','approve_release','release','challenge_pass','challenge_fail','reconcile'}:
                if tx is None:raise ValueError('Unknown transaction')
                if command in {'challenge_pass','challenge_fail'}:
                    if not tx['risk'] or tx['risk']['decision']!='CHALLENGE' or tx['challenge']!='PENDING':raise ValueError('A pending authentication challenge is required')
                    if run['clock']>=tx['challenge_expires']:raise ValueError('Authentication challenge expired')
                    tx['challenge']='PASSED' if command=='challenge_pass' else 'FAILED'
                    self.audit(run,'AUTHENTICATION_RESULT','Simulated authentication completed; this is not EMV 3DS integration.',transaction_id=tx['input']['transaction_id'],state=tx['challenge'])
                elif command=='reconcile':
                    if tx['gateway']!='UNKNOWN':raise ValueError('Only an UNKNOWN authorization can be reconciled')
                    receipt=next((v for v in run['issuer_ledger'] if v['key']==tx['input']['idempotency_key'] and v['payload_fingerprint']==digest(tx['input'])),None)
                    if not receipt:raise ValueError('No matching issuer evidence; outcome remains UNKNOWN')
                    tx['gateway']=receipt['outcome'];tx['authorization_id']=receipt['authorization_id']
                    self.audit(run,'AUTHORIZATION_RECONCILED','Trusted local issuer receipt resolved ambiguity without a new authorization.',transaction_id=tx['input']['transaction_id'],state=tx['gateway'])
                else:self._approval(run,tx,command,data)
            elif command=='tick':
                seconds=data.get('seconds',121)
                if type(seconds) is not int or not 1<=seconds<=3600:raise ValueError('Clock advance must be 1–3600 seconds')
                run['clock']+=seconds;self.audit(run,'CLOCK_ADVANCED','Virtual clock advanced.',seconds=seconds)
            elif command=='observe':self.audit(run,'INVESTIGATION_REVIEWED','Signal investigation exposes fraud reasons, rule version and authorization evidence.')
            else:raise ValueError('Unknown fraud command')
            self._save(db,run,label)
        return run
    def eligible(self,run,tx):
        if not tx['risk']:return False
        if tx['risk']['decision']=='ALLOW':return True
        if tx['risk']['decision']=='CHALLENGE':return tx['challenge']=='PASSED' and run['clock']<tx['challenge_expires']
        proposal=tx['proposal']
        return tx['risk']['decision']=='REVIEW' and tx['released'] and proposal and proposal['status']=='EXECUTED' and run['clock']<proposal['expires'] and proposal['release_fingerprint']==self.fingerprint(run,tx)
    def _approval(self,run,tx,command,data):
        if not tx['risk'] or tx['risk']['decision']!='REVIEW' or tx['gateway']!='NOT_CALLED':raise ValueError('Only REVIEW transactions before authorization permit analyst release')
        actor=data.get('actor','analyst-demo' if command=='request_release' else 'supervisor-demo')
        if actor not in {'analyst-demo','supervisor-demo'}:raise ValueError('Use a named simulated demo role')
        if command=='request_release':
            if actor!='analyst-demo':raise ValueError('Only analyst-demo can request a release')
            if tx['released']:raise ValueError('Transaction already released')
            tx['proposal']={'status':'REQUESTED','requester':actor,'fingerprint':self.fingerprint(run,tx),'expires':run['clock']+run['policy']['approval_ttl_seconds']}
        else:
            proposal=tx['proposal']
            if not proposal or run['clock']>=proposal['expires'] or proposal['fingerprint']!=self.fingerprint(run,tx):raise ValueError('Fresh, unexpired evidence-bound approval is required')
            if actor!='supervisor-demo' or actor==proposal['requester']:raise ValueError('A distinct supervisor-demo is required')
            if command=='approve_release':
                if proposal['status']!='REQUESTED':raise ValueError('Release request required')
                proposal.update(status='APPROVED',approver=actor)
            else:
                if proposal['status']!='APPROVED':raise ValueError('Supervisor approval required')
                tx['released']=True;proposal.update(status='EXECUTED',release_fingerprint=self.fingerprint(run,tx))
        self.audit(run,'ANALYST_'+command.upper(),'Simulated analyst release guard executed. No issuer decision is overridden.',actor=actor,transaction_id=tx['input']['transaction_id'],proposal=copy.deepcopy(tx['proposal']))
    def assets(self,run):
        root='fraud:'+run['id']
        return [{'id':root,'name':'Synthetic payment requests','kind':'fraud','type':'TRANSACTIONS','owner':'Fraud demo team'}]+[{'id':root+':'+key,'name':label,'type':kind} for key,label,kind in [('risk','Fraud rules','CHECK'),('review','Authentication / analyst review','CONTROL'),('gateway','Gateway simulator','GATEWAY'),('issuer','Issuer simulator','ISSUER')]]
    def projection(self,run):
        root='fraud:'+run['id'];txs=run['transactions']
        terminal=lambda t:t['gateway'] in {'APPROVED','DECLINED'} or bool(t['risk'] and (t['risk']['decision']=='BLOCK' or t['challenge']=='FAILED'))
        complete=bool(txs) and all(terminal(t) for t in txs)
        decisions=[t['risk']['decision'] if t['risk'] else 'PENDING' for t in txs]
        reasons=[r for t in txs if t['risk'] for r in t['risk']['reasons']]
        states={root:{'status':'healthy','caption':str(len(txs))+' validated synthetic requests'},root+':risk':{'status':'healthy' if complete else 'failed' if any(d!='ALLOW' and d!='PENDING' for d in decisions) else 'pending','caption':', '.join(decisions)},root+':review':{'status':'healthy' if complete else 'pending','caption':'Handled under demo policy' if complete else 'Review or authentication may be required'},root+':gateway':{'status':'unknown' if any(t['gateway']=='UNKNOWN' for t in txs) else 'healthy' if complete else 'pending','caption':', '.join(t['gateway'] for t in txs)},root+':issuer':{'status':'healthy' if complete else 'pending','caption':str(len(run['issuer_ledger']))+' local issuer receipts'}}
        timeline=[{'id':'fraud-event:'+run['id']+':'+str(e['sequence']),'asset_id':root,'kind':e['kind'],'state':e['details'].get('state','RECORDED'),'time':e['time'],'message':e['message']+' '+json.dumps(e['details'],sort_keys=True),'source':'Executed synthetic Fraud Guard'} for e in run['audit']]
        return {'payment_flow':True,'recovery_verified':complete,'downstream_complete':complete,'hypotheses':[] if complete else [{'cause':'Fraud or authorization review','assessment':'supported','evidence_ids':[e['id'] for e in timeline[-3:]],'detail':' '.join(reasons) or 'Awaiting risk evaluation.'}], 'evidence':timeline[-8:],'timeline':timeline,'dependencies':[{'source':root+(':'+a if a else ''),'target':root+':'+b,'origin':'synthetic payment flow'} for a,b in [('', 'risk'),('risk','review'),('review','gateway'),('gateway','issuer')]],'node_states':states,'potentially_affected':[root+':gateway',root+':issuer'],'coverage':'Local synthetic engine and declared payment-flow lineage. No real gateway, issuer, EMV 3DS, Databricks or model connection. Healthy means safely handled under demo policy, not confirmed legitimate.','runbook':'Inspect reasons and simulated receipts. Use authentication for CHALLENGE, evidence-bound approval for REVIEW, and reconciliation for UNKNOWN. Never override issuer declines.'}
    def _save(self,db,run,label=None):
        run['projection']=self.projection(run)
        run['summary']={'transactions':len(run['transactions']),'issuer_calls':len(run['issuer_ledger']),'complete':run['projection']['recovery_verified']}
        if label:run['steps'].append({'label':label,'view':copy.deepcopy({k:v for k,v in run.items() if k!='steps'})})
        db.execute('INSERT OR REPLACE INTO fraud_runs VALUES (?,?)',(run['id'],json.dumps(run)))
        identity='fraud-incident:'+run['id']
        flagged=any(t['risk'] and t['risk']['decision']!='ALLOW' or t['gateway']=='UNKNOWN' for t in run['transactions'])
        old=db.execute('SELECT body FROM incidents WHERE id=?',(identity,)).fetchone()
        if flagged or old:
            case=json.loads(old[0]) if old else {'id':identity,'asset_id':'fraud:'+run['id'],'title':run['name'],'status':'open','assignee':'Fraud demo team','history':[]}
            case.update(investigation=run['projection'],updated_at=datetime.now(timezone.utc).isoformat())
            if case['status']=='resolved' and not run['projection']['recovery_verified']:case['status']='open'
            db.execute('INSERT OR REPLACE INTO incidents VALUES (?,?)',(identity,json.dumps(case)))
        db.executemany('INSERT OR REPLACE INTO events VALUES (?,?)',[(e['id'],json.dumps(e)) for e in run['projection']['timeline']])

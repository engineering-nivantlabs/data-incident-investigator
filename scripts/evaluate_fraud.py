"""Exercise all synthetic Fraud Guard paths and write a reviewable local scorecard."""
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from investigator.fraud import FraudLab, SCENARIOS
from investigator.store import Store

results=[]
with tempfile.TemporaryDirectory() as folder:
    lab=FraudLab(Store(folder+'/evaluation.sqlite3'))
    for scenario in SCENARIOS:
        run=lab.create(scenario);run=lab.command(run['id'],'evaluate')
        initial=run['transactions'][0]['risk']['decision']
        if initial=='REVIEW':
            for command in ['request_release','approve_release','release']:run=lab.command(run['id'],command)
        if initial=='CHALLENGE':run=lab.command(run['id'],'challenge_fail' if scenario=='challenge_fail' else 'challenge_pass')
        run=lab.command(run['id'],'authorize')
        ambiguous=run['transactions'][0]['gateway']=='UNKNOWN'
        before=len(run['issuer_ledger'])
        if ambiguous:run=lab.command(run['id'],'reconcile')
        after=len(run['issuer_ledger'])
        expected='NOT_CALLED' if scenario in {'velocity','challenge_fail','risk_unavailable'} else 'DECLINED' if scenario=='issuer_decline' else 'APPROVED'
        assert run['transactions'][0]['gateway']==expected
        assert after==before
        assert after==(0 if expected=='NOT_CALLED' else 1)
        if expected in {'APPROVED','DECLINED'}:
            run=lab.command(run['id'],'authorize');assert len(run['issuer_ledger'])==after
        results.append({'scenario':scenario,'initial_fraud_decision':initial,'final_gateway_outcome':run['transactions'][0]['gateway'],'ambiguous_ack_reconciled':ambiguous,'issuer_calls':after,'safely_handled':run['summary']['complete'],'assertions_passed':True})
report={'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'Synthetic USD transactions and local Python gateway/issuer adapters. No external payment or Databricks calls.','fraud_accuracy_measured':False,'production_safety_claim':False,'scenarios_evaluated':len(results),'results':results}
target=Path(__file__).resolve().parent.parent/'docs/hackathon/fraud-evaluation.json'
target.write_text(json.dumps(report,indent=2)+'\n')
print('Verified',len(results),'synthetic scenarios:',target)

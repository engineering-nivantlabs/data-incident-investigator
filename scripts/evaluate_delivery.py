"""Run the synthetic scenarios and write an inspectable architecture-review scorecard."""
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from delivery.lab import DeliveryLab
from delivery.scenarios import SCENARIOS
from investigator.store import Store


def main():
    started=perf_counter();results=[]
    expected={'mfts_late':'BREACHED','success':'ON_TRACK','timeout_retry':'AT_RISK','exhausted_retries':'BLOCKED','stalled':'AT_RISK','optional_mfts':'ON_TRACK','quarantine':'BLOCKED','duplicate':'ON_TRACK','kafka_deferred':'BLOCKED'}
    with tempfile.TemporaryDirectory() as folder:
        lab=DeliveryLab(Store(folder+'/evaluation.sqlite3'))
        for key in SCENARIOS:
            run=lab.command(lab.create(key)['id'],'run_pipeline');initial=run['risk']['status'];assert initial==expected[key]
            before=len(run['broker']);action=run['risk']['recommended_action'];history=[]
            if action:
                lab.command(run['id'],'request_approval');lab.command(run['id'],'approve');run=lab.command(run['id'],'execute');history.append(action)
                if action in {'retry_mfts','reconcile_kafka'}:assert len(run['broker'])==before
            if run['risk']['required_complete'] and not run['quarantine']:
                run=lab.command(run['id'],'refresh_consumers')
                count=len(run['broker']);run=lab.command(run['id'],'replay');assert len(run['broker'])==count
            results.append({'scenario':key,'observed_initial_status':initial,'expected_initial_status':expected[key],'recovery_actions':history,'kafka_writes_before_recovery':before,'kafka_writes_after':len(run['broker']),'required_channels_complete':run['risk']['required_complete'],'consumers_verified':run['projection']['recovery_verified'],'quarantined_records':len(run['quarantine']),'evidence_fingerprint':run['risk']['fingerprint'],'assertions_passed':True})
    report={'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'Local Python engine with synthetic source data and local adapters. No external network or Databricks execution.','scenarios_evaluated':len(results),'local_execution_seconds':round(perf_counter()-started,3),'triage_improvement_measured':False,'production_safety_claim':False,'results':results}
    out=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parent.parent/'docs/hackathon/evaluation.json'
    out.write_text(json.dumps(report,indent=2)+'\n');print(f'{len(results)} scenario evaluations passed. Report: {out}')

if __name__=='__main__':main()

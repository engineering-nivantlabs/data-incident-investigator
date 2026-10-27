import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from investigator.engine import investigate
from investigator.store import Store
from investigator.connectors import collect_databricks

NOW = datetime.now(timezone.utc)
ASSET = {'id': 'a', 'max_age_minutes': 60}
def event(identity, minutes, state):
    return {'id':identity,'asset_id':'a','kind':'run','time':(NOW-timedelta(minutes=minutes)).isoformat(),'state':state,'message':'test'}

class InvestigationTests(unittest.TestCase):
    def test_failure_has_evidence_and_cycle_safe_exposure(self):
        r=investigate(ASSET,[event('old',120,'SUCCEEDED'),event('bad',10,'FAILED')],[{'source':'a','target':'b'},{'source':'b','target':'a'}],NOW)
        self.assertEqual(r['potentially_affected'],['b'])
        self.assertEqual(r['hypotheses'][0]['evidence_ids'],['bad'])
        self.assertFalse(r['recovery_verified'])
    def test_later_success_supersedes_failure(self):
        self.assertTrue(investigate(ASSET,[event('bad',30,'FAILED'),event('ok',1,'SUCCEEDED')],[],NOW)['recovery_verified'])
    def test_missing_telemetry_abstains(self):
        r=investigate(ASSET,[],[],NOW)
        self.assertEqual(r['hypotheses'][0]['assessment'],'insufficient evidence')
        self.assertFalse(r['recovery_verified'])
    def test_quality_failure_blocks_recovery(self):
        q=event('q',0,'FAILED');q['kind']='quality'
        self.assertFalse(investigate(ASSET,[event('ok',1,'SUCCEEDED'),q],[],NOW)['recovery_verified'])
    def test_ingestion_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            s=Store(d+'/test.sqlite3');s.ingest([event('x',1,'FAILED')]);s.ingest([event('x',1,'FAILED')]);self.assertEqual(len(s.events()),1)
    def test_sdk_normalization(self):
        run=SimpleNamespace(run_id=7,state=SimpleNamespace(result_state=SimpleNamespace(value='FAILED'),state_message='failed'),end_time=int(NOW.timestamp()*1000),start_time=0,run_page_url='https://example.com/run')
        client=SimpleNamespace(config=SimpleNamespace(host='https://example.com'),jobs=SimpleNamespace(list_runs=lambda **kwargs:iter([run])))
        e=collect_databricks({'assets':[{'id':'a','job_id':1}]},client)
        self.assertEqual(e[0]['state'],'FAILED');self.assertEqual(e[0]['asset_id'],'a')

if __name__=='__main__':unittest.main()

import copy
import json
import socket
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from delivery.lab import DeliveryLab
from delivery.contracts import canonicalize
from delivery.advisor import evaluate
from delivery.scenarios import SCENARIOS
from investigator.store import Store
from investigator.server import create_app
from investigator.demo import demo_config


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.store=Store(self.folder.name+'/lab.sqlite3');self.lab=DeliveryLab(self.store)
    def tearDown(self):self.folder.cleanup()
    def pipeline(self,scenario):
        return self.lab.command(self.lab.create(scenario)['id'],'run_pipeline')
    def recover(self,run):
        self.lab.command(run['id'],'request_approval')
        self.lab.command(run['id'],'approve')
        return self.lab.command(run['id'],'execute')
    def test_golden_flow_and_mfts_only_recovery(self):
        run=self.pipeline('mfts_late');self.assertEqual(run['risk']['status'],'BREACHED')
        self.assertEqual(len(run['broker']),5);self.assertEqual(len(run['receiver']),0)
        self.assertEqual(run['risk']['recommended_action'],'retry_mfts')
        run=self.recover(run)
        self.assertEqual(len(run['broker']),5);self.assertEqual(len(run['receiver']),5)
        self.assertTrue(run['risk']['required_complete'])
        self.assertFalse(run['projection']['recovery_verified'])
        run=self.lab.command(run['id'],'refresh_consumers')
        self.assertTrue(run['projection']['recovery_verified'])
        self.assertTrue(all(n['status']=='healthy' for n in run['projection']['node_states'].values()))
        self.assertEqual(len(run['snapshots']),3)
        self.assertEqual(run['consumers']['treasury']['records'],3)
        case=next(c for c in self.store.incidents() if c['asset_id']=='delivery:'+run['id'])
        self.assertTrue(case['investigation']['recovery_verified'])
        self.assertEqual(len(self.store.events()),len(run['audit']))
    def test_ambiguous_ack_reconciles_without_republish(self):
        run=self.pipeline('timeout_retry')
        self.assertEqual(run['risk']['recommended_action'],'reconcile_kafka')
        self.assertEqual(len(run['broker']),5)
        run=self.recover(run)
        self.assertEqual(len(run['broker']),5);self.assertTrue(run['risk']['required_complete'])
        self.assertEqual(len([e for e in run['audit'] if e['kind']=='ACK_RECONCILED']),5)
    def test_exhaustion_quarantine_optional_and_deferred(self):
        for scenario in ['exhausted_retries','quarantine','kafka_deferred']:
            run=self.pipeline(scenario);self.assertEqual(run['risk']['status'],'BLOCKED',scenario)
            self.assertIsNone(run['risk']['recommended_action'])
            with self.assertRaises(ValueError):self.lab.command(run['id'],'refresh_consumers')
        run=self.pipeline('optional_mfts')
        self.assertEqual(run['risk']['status'],'ON_TRACK');self.assertTrue(run['risk']['required_complete'])
        run=self.lab.command(run['id'],'refresh_consumers');self.assertTrue(run['projection']['recovery_verified'])
        self.assertNotEqual(run['projection']['node_states']['delivery:'+run['id']+':mfts']['status'],'healthy')
        run=self.lab.command(run['id'],'replay')
        self.assertEqual(len(run['receiver']),0)
        self.assertEqual(len(run['broker']),5)
    def test_stalled_resume_and_virtual_deadline(self):
        run=self.pipeline('stalled');self.assertEqual(len(run['broker']),0)
        self.assertEqual(run['risk']['status'],'AT_RISK')
        run=self.lab.command(run['id'],'tick',{'seconds':300})
        self.assertEqual(run['risk']['status'],'BREACHED')
        run=self.recover(run);self.assertTrue(run['risk']['required_complete']);self.assertEqual(len(run['broker']),5)
    def test_duplicates_and_replay_preserve_keys_and_counts(self):
        run=self.pipeline('duplicate');self.assertEqual(run['duplicates'],1)
        keys=[r['key'] for r in run['broker']]
        run=self.lab.command(run['id'],'replay')
        self.assertEqual([r['key'] for r in run['broker']],keys)
        self.assertEqual(len(run['receiver']),5)
        self.assertEqual(len([e for e in run['audit'] if e['kind']=='REPLAY_SKIPPED']),10)
        another=self.pipeline('success')
        self.assertEqual([r['key'] for r in another['broker']],keys)
    def test_approval_gates_and_stale_evidence(self):
        run=self.pipeline('mfts_late')
        with self.assertRaises(ValueError):self.lab.command(run['id'],'execute')
        self.lab.command(run['id'],'request_approval')
        with self.assertRaises(ValueError):self.lab.command(run['id'],'approve',{'actor':'operator-demo'})
        self.lab.command(run['id'],'approve')
        self.lab.command(run['id'],'tick',{'seconds':1})
        before=self.lab.get(run['id'])
        with self.assertRaises(ValueError):self.lab.command(run['id'],'execute')
        self.assertEqual(before,self.lab.get(run['id']))
        with self.assertRaises(ValueError):self.lab.command(run['id'],'request_approval',{'action':'replay_all'})
    def test_advisor_pure_and_fingerprint_stable(self):
        run=self.pipeline('mfts_late');before=copy.deepcopy(run)
        a=evaluate(run);b=evaluate(run);self.assertEqual(a,b);self.assertEqual(before,run)
        run['clock']+=1;self.assertNotEqual(evaluate(run)['fingerprint'],a['fingerprint'])
    def test_transaction_claim_and_atomic_evidence(self):
        run=self.lab.create('success')
        def execute():
            try:self.lab.command(run['id'],'run_pipeline');return True
            except ValueError:return False
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:execute(),range(2)))
        self.assertEqual(sum(results),1);self.assertEqual(len(self.lab.get(run['id'])['broker']),5)
        restarted=DeliveryLab(Store(self.store.path));self.assertEqual(restarted.get(run['id']),self.lab.get(run['id']))
        with self.assertRaises(ValueError):self.lab.command(run['id'],'unknown')
        self.assertEqual(restarted.get(run['id']),self.lab.get(run['id']))
    def test_local_endpoints_no_network_and_signal_link(self):
        api=create_app(self.store,demo_config(),'demo').test_client()
        with patch.object(socket.socket,'connect',side_effect=AssertionError('Network forbidden')):
            with api.get('/delivery') as response:self.assertEqual(response.status_code,200)
            with api.get('/architecture') as response:self.assertEqual(response.status_code,200)
            run=api.post('/api/delivery',json={'command':'create','scenario':'mfts_late'}).get_json()
            for cmd in ['run_pipeline','request_approval','approve','execute','refresh_consumers']:
                result=api.post('/api/delivery',json={'id':run['id'],'command':cmd})
                self.assertEqual(result.status_code,200,result.get_json())
            state=api.get('/api/state').get_json()
            asset='delivery:'+run['id']
            case=next(i for i in state['incidents'] if i['asset_id']==asset)
            self.assertTrue(case['investigation']['recovery_verified'])
            result=api.post('/api/update',json={'id':case['id'],'status':'resolved'})
            self.assertEqual(result.status_code,200,result.get_json())
            before_case=next(c for c in self.store.incidents() if c['asset_id']==asset)
            self.assertEqual(api.post('/api/sync',json={}).status_code,200)
            self.assertEqual(next(c for c in self.store.incidents() if c['asset_id']==asset),before_case)
            evidence=api.get('/api/delivery/evidence/'+run['id'])
            self.assertEqual(len(evidence.get_json()['broker']),5)
            self.assertIn('attachment',evidence.headers['Content-Disposition'])
    def test_live_mode_simulation_disabled(self):
        api=create_app(self.store,demo_config(),'databricks',sql=object()).test_client()
        self.assertFalse(api.get('/api/delivery/state').get_json()['available'])
        self.assertEqual(api.post('/api/delivery',json={'command':'create','scenario':'success'}).status_code,403)
    def test_conflicting_broker_payload_requires_escalation(self):
        run=self.pipeline('timeout_retry');run['broker'][0]['payload']=dict(run['broker'][0]['payload'],amount='999.00')
        with self.store.connect() as db:self.lab._save(db,run)
        run=self.recover(run)
        self.assertEqual(run['risk']['status'],'BLOCKED')
        self.assertIsNone(run['risk']['recommended_action'])
        self.assertEqual(len(run['broker']),5)
    def test_atomic_rollback_on_projection_failure(self):
        run=self.lab.create('success');before=self.lab.get(run['id'])
        with patch.object(self.lab,'projection',side_effect=RuntimeError('Injected projection failure')):
            with self.assertRaises(RuntimeError):self.lab.command(run['id'],'run_pipeline')
        self.assertEqual(before,self.lab.get(run['id']))
        self.assertEqual(len(self.store.events()),1)
    def test_conflicting_source_payload_quarantined(self):
        item={'source_record_id':'SYN-1','route_key':'TREASURY','amount':'1','currency':'USD'}
        first=canonicalize(item,'dataset',{'TREASURY':'topic'})
        second=canonicalize(dict(item,amount='2'),'dataset',{'TREASURY':'topic'})
        self.assertEqual(first['distribution_id'],second['distribution_id'])
        self.assertNotEqual(first['payload_fingerprint'],second['payload_fingerprint'])
        with self.assertRaises(ValueError):canonicalize(dict(item,amount='NaN'),'dataset',{'TREASURY':'topic'})
        run=self.lab.create('success');run['records'] += [dict(run['records'][0],amount='999.00')]
        self.lab.ingest(run)
        self.assertEqual(len(run['quarantine']),1)
        self.assertIn('Conflicting payload',run['quarantine'][0]['reason'])

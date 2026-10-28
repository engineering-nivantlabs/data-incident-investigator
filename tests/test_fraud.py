import copy
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from investigator.fraud import FraudLab, SCENARIOS, validate_transaction
from investigator.store import Store
from investigator.server import create_app
from investigator.demo import demo_config

class FraudTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.store=Store(self.folder.name+'/fraud.sqlite3');self.lab=FraudLab(self.store)
    def tearDown(self):self.folder.cleanup()
    def new(self,scenario='review_timeout'):
        run=self.lab.create(scenario);return self.lab.command(run['id'],'evaluate')
    def cmd(self,run,command,**data):return self.lab.command(run['id'],command,data)
    def release(self,run):
        for cmd in ['request_release','approve_release','release']:run=self.cmd(run,cmd)
        return run
    def test_fraud_clearance_does_not_override_issuer_decline(self):
        run=self.new('issuer_decline');self.assertEqual(run['transactions'][0]['risk']['decision'],'ALLOW')
        run=self.cmd(run,'authorize');self.assertEqual(run['transactions'][0]['gateway'],'DECLINED')
        with self.assertRaises(ValueError):self.cmd(run,'request_release')
        run=self.cmd(run,'authorize');self.assertEqual(len(run['issuer_ledger']),1)
    def test_challenge_gate_pass_and_fail(self):
        for scenario,command,calls in [('challenge','challenge_pass',1),('challenge_fail','challenge_fail',0)]:
            run=self.new(scenario);run=self.cmd(run,'authorize');self.assertEqual(len(run['issuer_ledger']),0)
            with self.assertRaises(ValueError):self.cmd(run,'request_release')
            run=self.cmd(run,command);run=self.cmd(run,'authorize');self.assertEqual(len(run['issuer_ledger']),calls)
            if calls:self.assertEqual(run['transactions'][0]['gateway'],'APPROVED')
    def test_expired_authentication_never_authorizes(self):
        run=self.new('challenge');run=self.cmd(run,'tick')
        with self.assertRaises(ValueError):self.cmd(run,'challenge_pass')
        run=self.cmd(run,'authorize');self.assertEqual(run['issuer_ledger'],[])
    def test_review_guard_and_receipt_reconciliation(self):
        run=self.new();self.assertEqual(run['transactions'][0]['risk']['decision'],'REVIEW')
        self.assertEqual(self.cmd(run,'authorize')['issuer_ledger'],[])
        with self.assertRaises(ValueError):self.cmd(run,'release')
        run=self.release(run);run=self.cmd(run,'authorize')
        self.assertEqual(run['transactions'][0]['gateway'],'UNKNOWN');self.assertEqual(len(run['issuer_ledger']),1)
        with self.assertRaises(ValueError):self.cmd(run,'authorize')
        run=self.cmd(run,'reconcile');self.assertEqual(run['transactions'][0]['gateway'],'APPROVED')
        run=self.cmd(run,'authorize');self.assertEqual(len(run['issuer_ledger']),1)
    def test_approval_roles_and_stale_evidence(self):
        run=self.new();run=self.cmd(run,'request_release')
        with self.assertRaises(ValueError):self.cmd(run,'approve_release',actor='analyst-demo')
        with self.assertRaises(ValueError):self.cmd(run,'approve_release',actor='unknown')
        run=self.cmd(run,'approve_release');run=self.cmd(run,'tick',seconds=1)
        with self.assertRaises(ValueError):self.cmd(run,'release')
        run=self.cmd(run,'request_release');run=self.cmd(run,'tick',seconds=121)
        with self.assertRaises(ValueError):self.cmd(run,'approve_release')
    def test_released_transaction_expires_before_authorization(self):
        run=self.release(self.new());run=self.cmd(run,'tick',seconds=121);run=self.cmd(run,'authorize')
        self.assertEqual(run['issuer_ledger'],[])
    def test_velocity_and_service_outage_block_issuer(self):
        for scenario,decision in [('velocity','BLOCK'),('risk_unavailable','UNAVAILABLE')]:
            run=self.new(scenario);self.assertEqual(run['transactions'][0]['risk']['decision'],decision)
            run=self.cmd(run,'authorize');self.assertEqual(run['issuer_ledger'],[])
            with self.assertRaises(ValueError):self.cmd(run,'request_release')
        self.assertEqual(self.new('velocity')['transactions'][0]['features']['attempts_last_minute_including_current'],4)
    def test_idempotency_conflicts_and_duplicate_requests(self):
        run=self.new('normal');tx=copy.deepcopy(run['transactions'][0]['input'])
        run=self.cmd(run,'submit',transaction=tx);self.assertEqual(len(run['transactions']),1)
        run=self.cmd(run,'evaluate');self.assertEqual(len(run['history']),2)
        tx['amount_minor']+=1
        with self.assertRaises(ValueError):self.cmd(run,'submit',transaction=tx)
        run=self.cmd(run,'authorize');run=self.cmd(run,'authorize');self.assertEqual(len(run['issuer_ledger']),1)
    def test_contract_rejects_sensitive_and_invalid_fields(self):
        tx=self.lab.create('normal')['transactions'][0]['input']
        for change in [{'pan':'4111111111111111'},{'amount_minor':True},{'currency':'EUR'},{'card_token':'4111111111111111'},{'amount_minor':0}]:
            with self.assertRaises(ValueError):validate_transaction(dict(tx,**change))
    def test_concurrent_authorization_and_restart(self):
        run=self.new('normal')
        with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(lambda _:self.cmd(run,'authorize'),range(2)))
        self.assertEqual(len(FraudLab(self.store).get(run['id'])['issuer_ledger']),1)
    def test_projection_failure_rolls_back_issuer_and_state(self):
        run=self.new('normal')
        with patch.object(self.lab,'projection',side_effect=RuntimeError('injected')):
            with self.assertRaises(RuntimeError):self.cmd(run,'authorize')
        self.assertEqual(self.lab.get(run['id'])['issuer_ledger'],[])
    def test_guided_flow_snapshots_and_signal(self):
        import socket
        with patch.object(socket.socket,'connect',side_effect=AssertionError('Network forbidden')):
            run=self.lab.create('review_timeout',guided=True)
            for step in range(1,9):run=self.cmd(run,'guided_next',expected_step=step)
        self.assertTrue(run['summary']['complete']);self.assertEqual(len(run['issuer_ledger']),2)
        self.assertEqual(run['steps'][2]['view']['transactions'][0]['gateway'],'APPROVED')
        self.assertEqual(run['steps'][2]['view']['transactions'][1]['gateway'],'NOT_CALLED')
        self.assertEqual(run['steps'][7]['view']['transactions'][1]['gateway'],'UNKNOWN')
        self.assertEqual(run['steps'][8]['view']['transactions'][1]['gateway'],'APPROVED')
        self.assertEqual(len(run['steps'][7]['view']['issuer_ledger']),2)
        case=next(c for c in self.store.incidents() if c['id']=='fraud-incident:'+run['id'])
        self.assertTrue(case['investigation']['recovery_verified'])
        with self.assertRaises(ValueError):self.cmd(run,'guided_next',expected_step=9)
    def test_expected_step_guard(self):
        run=self.lab.create(guided=True);run=self.cmd(run,'guided_next',expected_step=1)
        with self.assertRaises(ValueError):self.cmd(run,'guided_next',expected_step=1)
        self.assertEqual(len(self.lab.get(run['id'])['steps']),2)
    def test_signal_api_sync_preserves_fraud_projection(self):
        client=create_app(store=self.store,config=demo_config(),mode='demo').test_client()
        response=client.post('/api/fraud',json={'command':'create','scenario':'velocity'})
        self.assertEqual(response.status_code,200)
        run=client.post('/api/fraud',json={'id':response.json['id'],'command':'evaluate'}).json
        before=run['projection']
        self.assertEqual(client.post('/api/sync',json={}).status_code,200)
        state=client.get('/api/state').json
        case=next(c for c in state['incidents'] if c['id']=='fraud-incident:'+run['id'])
        self.assertEqual(case['investigation'],before)
        self.assertTrue(any(a['id']=='fraud:'+run['id'] for a in state['assets']))
        self.assertEqual(client.post('/api/investigate',json={'asset_id':'fraud:'+run['id']}).status_code,200)
        self.assertEqual(client.get('/api/fraud/evidence/'+run['id']).status_code,200)
    def test_live_mode_is_disabled(self):
        client=create_app(store=self.store,config=demo_config(),mode='databricks',sql=object(),client=object()).test_client()
        self.assertEqual(client.post('/api/fraud',json={'command':'create'}).status_code,403)
        self.assertFalse(client.get('/api/fraud/state').json['available'])

import tempfile
import unittest
from investigator.server import create_app
from investigator.store import Store
from investigator.demo import demo_config

class GuidedTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory()
        self.store=Store(self.folder.name+'/demo.sqlite3')
        self.client=create_app(store=self.store,config=demo_config(),mode='demo').test_client()
    def tearDown(self):self.folder.cleanup()
    def test_guided_story_persists_historical_evidence(self):
        import socket
        from unittest.mock import patch
        with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
            response=self.client.post('/api/guided-demo',json={'command':'start'})
            self.assertEqual(response.status_code,200,response.json)
            run=response.json
            identity=run['id']
            for step in range(1,8):
                response=self.client.post('/api/guided-demo',json={'command':'next','id':identity,'expected_step':step})
                self.assertEqual(response.status_code,200,response.json)
                run=response.json
            self.assertEqual(run['workflow']['status'],'SUCCEEDED')
            self.assertEqual(len(run['guided_demo']),8)
            failure=run['guided_demo'][2]['view']
            self.assertEqual(len(failure['receiver']),0)
            self.assertEqual(failure['risk']['status'],'BREACHED')
            self.assertFalse(failure['projection']['recovery_verified'])
            self.assertEqual(len(run['broker']),5)
            self.assertEqual(len(run['receiver']),5)
            self.assertEqual(run['guided_demo'][4]['view']['proposal']['status'],'REQUESTED')
            self.assertEqual(run['guided_demo'][5]['view']['proposal']['status'],'APPROVED')
            self.assertTrue(run['guided_demo'][7]['view']['projection']['recovery_verified'])
            from delivery.lab import DeliveryLab
            self.assertEqual(len(DeliveryLab(self.store).get(identity)['guided_demo']),8)
            response=self.client.post('/api/guided-demo',json={'command':'next','id':identity,'expected_step':8})
            self.assertEqual(response.status_code,400)
    def test_stale_step_does_not_repeat_processing(self):
        run=self.client.post('/api/guided-demo',json={'command':'start'}).json
        request={'command':'next','id':run['id'],'expected_step':1}
        self.assertEqual(self.client.post('/api/guided-demo',json=request).status_code,200)
        self.assertEqual(self.client.post('/api/guided-demo',json=request).status_code,400)
        from delivery.lab import DeliveryLab
        self.assertEqual(len(DeliveryLab(self.store).get(run['id'])['broker']),5)

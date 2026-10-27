import tempfile
import unittest
from datetime import datetime, timezone
from investigator.demo import demo_config, scenario_events, recovery_events
from investigator.engine import investigate
from investigator.server import create_app
from investigator.store import Store

class DemoTests(unittest.TestCase):
    def test_scenarios_and_recovery(self):
        config=demo_config();now=datetime.now(timezone.utc);events=scenario_events(config,now)
        assets=[a for a in config['assets'] if a.get('scenario')]
        self.assertEqual(len(assets),8)
        for asset in assets:
            result=investigate(asset,events,config['dependencies'],now)
            self.assertEqual(result['recovery_verified'],asset['scenario']=='recovered',asset['scenario'])
            self.assertEqual(len(result['potentially_affected']),3)
            recovered=investigate(asset,events+recovery_events(asset,now),config['dependencies'],now)
            self.assertTrue(recovered['recovery_verified'],asset['scenario'])
    def test_demo_seed_and_recover_endpoint(self):
        with tempfile.TemporaryDirectory() as path:
            config=demo_config();store=Store(path+'/demo.sqlite3')
            api=create_app(store,config,'demo').test_client()
            cases=api.get('/api/state').get_json()['incidents'];self.assertEqual(len(cases),8)
            case=next(i for i in cases if i['asset_id']=='demo.quality_failure')
            self.assertEqual(api.post('/api/update',json={'id':case['id'],'status':'resolved'}).status_code,400)
            self.assertEqual(api.post('/api/demo-recover',json={'asset_id':case['asset_id']}).status_code,200)
            recovered=next(i for i in store.incidents() if i['id']==case['id'])
            self.assertFalse(recovered['before_recovery']['recovery_verified'])
            self.assertTrue(recovered['investigation']['recovery_verified'])
            self.assertEqual(api.post('/api/sync',json={}).status_code,200)
            replayed=next(i for i in store.incidents() if i['id']==case['id'])
            self.assertFalse(replayed['investigation']['recovery_verified'])
            api.post('/api/demo-recover',json={'asset_id':case['asset_id']})
            self.assertEqual(api.post('/api/update',json={'id':case['id'],'status':'resolved'}).status_code,200)
            api=create_app(store,config,'demo').test_client()
            self.assertEqual(len(api.get('/api/state').get_json()['incidents']),8)
            self.assertEqual(next(i for i in store.incidents() if i['id']==case['id'])['status'],'resolved')
    def test_full_downstream_progression_and_replay(self):
        with tempfile.TemporaryDirectory() as path:
            config=demo_config();store=Store(path+'/demo.sqlite3');api=create_app(store,config,'demo').test_client()
            asset='demo.failed_pipeline'
            def current():return next(i for i in store.incidents() if i['asset_id']==asset)
            self.assertEqual(api.post('/api/demo-refresh-downstream',json={'asset_id':asset}).status_code,400)
            api.post('/api/demo-recover',json={'asset_id':asset})
            self.assertFalse(current()['investigation']['downstream_complete'])
            self.assertEqual(api.post('/api/demo-refresh-downstream',json={'asset_id':asset}).status_code,200)
            case=current()
            self.assertTrue(case['investigation']['downstream_complete'])
            self.assertEqual(len(case['investigation']['downstream_verified']),3)
            self.assertFalse(case['before_recovery']['recovery_verified'])
            self.assertTrue(case['source_recovered']['recovery_verified'])
            self.assertFalse(case['source_recovered']['downstream_complete'])
            restarted=create_app(store,config,'demo').test_client()
            persisted=next(i for i in restarted.get('/api/state').get_json()['incidents'] if i['asset_id']==asset)
            self.assertTrue(persisted['investigation']['downstream_complete'])
            api.post('/api/demo-recover',json={'asset_id':asset})
            self.assertFalse(current()['investigation']['downstream_complete'])
            api.post('/api/sync',json={})
            self.assertNotIn('source_recovered',current())
            self.assertFalse(current()['investigation']['downstream_complete'])
            self.assertFalse(any(e['kind']=='downstream_refresh' for e in store.events()))
    def test_simulation_blocked_in_live_mode(self):
        with tempfile.TemporaryDirectory() as path:
            api=create_app(Store(path+'/live.sqlite3'),demo_config(),'databricks',sql=object()).test_client()
            self.assertEqual(api.post('/api/demo-recover',json={'asset_id':'demo.failed_pipeline'}).status_code,403)
            self.assertEqual(api.post('/api/demo-refresh-downstream',json={'asset_id':'demo.failed_pipeline'}).status_code,403)

if __name__=='__main__':unittest.main()

import io
import json
import unittest
from zipfile import ZipFile
from investigator.workflow_builder import build, archive

class WorkflowBuilderTests(unittest.TestCase):
    def test_templates_export_executable_sources(self):
        for kind in ['job','pipeline','workflow']:
            plan=build({'prompt':'Create an hourly synthetic delivery flow','kind':kind})
            with ZipFile(archive(plan)) as z:
                config=json.loads(z.read('databricks.yml'))
                self.assertEqual(config['targets']['dev']['mode'],'development')
                for path in z.namelist():
                    if path.endswith('.py'): compile(z.read(path),path,'exec')
                jobs=config['resources'].get('jobs',{})
                for job in jobs.values():
                    self.assertEqual(job['schedule']['pause_status'],'PAUSED')
                    keys={t['task_key'] for t in job['tasks']}
                    for task in job['tasks']:
                        for dep in task.get('depends_on',[]): self.assertIn(dep['task_key'],keys)
    def test_untrusted_prompt_never_becomes_code(self):
        plan=build({'prompt':'Synthetic input $(touch /tmp/unsafe); import os'})
        self.assertNotIn('touch', '\n'.join(plan['files'].values()))
        with self.assertRaises(ValueError): build({'prompt':'Synthetic batch execution','name':'../escape'})
        with self.assertRaises(ValueError): build({'prompt':'Synthetic batch execution','kind':'shell'})
    def test_sensitive_intent_blocks_export(self):
        plan=build({'prompt':'Delete production tables'})
        self.assertEqual(plan['files'],{})
        with self.assertRaises(ValueError):archive(plan)
    def test_no_schedule_without_hourly(self):
        plan=build({'prompt':'Synthetic manual batch processing'})
        config=json.loads(plan['files']['databricks.yml'])
        self.assertNotIn('schedule',config['resources']['jobs']['synthetic_workflow'])

class LocalWorkflowIntegrationTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        from investigator.store import Store
        from investigator.server import create_app
        from investigator.demo import demo_config
        self.folder=tempfile.TemporaryDirectory()
        self.store=Store(self.folder.name+'/workflow.sqlite3')
        self.client=create_app(store=self.store,config=demo_config(),mode='demo').test_client()
        self.spec={'prompt':'Create hourly synthetic delivery workflow','kind':'workflow','scenario':'mfts_late'}
    def tearDown(self):self.folder.cleanup()
    def command(self,run,command):
        response=self.client.post('/api/delivery',json={'id':run['id'],'command':command})
        self.assertEqual(response.status_code,200,response.json)
        return response.json
    def test_failure_investigation_and_recovery_use_same_run(self):
        import socket
        from unittest.mock import patch
        with patch.object(socket.socket,'connect',side_effect=AssertionError('Network forbidden')):
            response=self.client.post('/api/workflow-run',json=self.spec)
            self.assertEqual(response.status_code,200,response.json)
            run=response.json
            self.assertEqual(run['workflow']['status'],'RECOVERY_REQUIRED')
            self.assertEqual([t['status'] for t in run['workflow']['tasks']],['SUCCEEDED','FAILED','BLOCKED'])
            digest=run['workflow']['plan_fingerprint']
            self.assertEqual(len(run['broker']),5)
            self.assertEqual(self.client.post('/api/delivery',json={'id':run['id'],'command':'execute'}).status_code,400)
            for action in ['request_approval','approve','execute','refresh_consumers']:
                run=self.command(run,action)
            self.assertEqual(run['workflow']['status'],'SUCCEEDED')
            self.assertEqual(digest,run['workflow']['plan_fingerprint'])
            self.assertEqual(len(run['broker']),5)
            self.assertEqual(len(run['receiver']),5)
            case=next(c for c in self.store.incidents() if c['id']=='delivery-incident:'+run['id'])
            self.assertTrue(case['investigation']['recovery_verified'])
            from delivery.lab import DeliveryLab
            self.assertEqual(DeliveryLab(self.store).get(run['id'])['workflow']['status'],'SUCCEEDED')
    def test_success_runs_all_tasks_and_quarantine_stops_dispatch(self):
        for scenario,expected in [('success','SUCCEEDED'),('quarantine','RECOVERY_REQUIRED')]:
            response=self.client.post('/api/workflow-run',json=dict(self.spec,scenario=scenario))
            self.assertEqual(response.status_code,200,response.json)
            run=response.json
            self.assertEqual(run['workflow']['status'],expected)
            if scenario=='quarantine':self.assertEqual(run['broker'],[])
            else:self.assertTrue(run['projection']['recovery_verified'])
    def test_blocked_plan_does_not_create_run(self):
        response=self.client.post('/api/workflow-run',json=dict(self.spec,prompt='Delete production tables'))
        self.assertEqual(response.status_code,400)
        from delivery.lab import DeliveryLab
        self.assertEqual(DeliveryLab(self.store).runs(),[])

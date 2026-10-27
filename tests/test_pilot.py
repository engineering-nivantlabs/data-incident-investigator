import copy
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace as N
from unittest.mock import patch
from investigator.connectors import collect_catalog
from investigator.engine import investigate
from investigator.server import create_app, validate_config
from investigator.store import Store, DeltaStore
from investigator.sql import SQL, identifier

NOW = datetime.now(timezone.utc)
ASSET = {'id':'a','name':'A','table':'main.test.a','max_age_minutes':60,'check_ttl_minutes':15}
CONFIG = {'assets':[ASSET,dict(ASSET,id='b',name='B',table='main.test.b')],'dependencies':[]}

class FakeSQL:
    def __init__(self, observed=None, fail=False): self.observed=observed or NOW.isoformat();self.fail=fail;self.calls=[]
    def query(self, statement, parameters=None):
        self.calls.append((statement,parameters))
        if self.fail: raise RuntimeError('secret error')
        if statement.startswith('DESCRIBE'): return [{'timestamp':self.observed}]
        if 'observed_epoch' in statement: return [{'observed_epoch':str(NOW.timestamp())}]
        return [{'source_table_full_name':'main.test.a','target_table_full_name':'main.test.b','last_seen':NOW.isoformat()}, {'source_table_full_name':'main.test.a','target_table_full_name':'outside.secret.table','last_seen':NOW.isoformat()}] if parameters['table']=='main.test.a' else []

class PilotTests(unittest.TestCase):
    def test_lineage_is_allowlisted_and_parameterized(self):
        sql=FakeSQL();events,edges=collect_catalog(CONFIG,sql,NOW)
        self.assertEqual([(e['source'],e['target']) for e in edges],[('a','b')])
        self.assertEqual(edges[0]['origin'],'unity_catalog')
        self.assertEqual(events[0]['state'],'FRESH')
        self.assertIn(':table',sql.calls[2][0])
    def test_failed_observations_are_unknown(self):
        events,edges=collect_catalog(CONFIG,FakeSQL(fail=True),NOW)
        self.assertEqual(edges,[])
        self.assertTrue(all(e['state']=='UNKNOWN' for e in events))
        self.assertNotIn('secret',str(events))
    def test_event_timestamp_and_empty_table(self):
        config=copy.deepcopy(CONFIG);config['assets'][0]['freshness_column']='ingested_at'
        events,_=collect_catalog(config,FakeSQL(),NOW)
        self.assertEqual(events[0]['basis'],'max_event_timestamp')
        class EmptySQL(FakeSQL):
            def query(self,*args,**kwargs):return []
        events,_=collect_catalog(config,EmptySQL(),NOW)
        self.assertEqual(events[0]['state'],'UNKNOWN')
    def test_job_success_cannot_override_stale_table(self):
        events,_=collect_catalog(CONFIG,FakeSQL((NOW-timedelta(hours=2)).isoformat()),NOW)
        events.append({'id':'run','asset_id':'a','kind':'run','time':NOW.isoformat(),'state':'SUCCEEDED','message':'ok'})
        result=investigate(dict(ASSET,job_id=1),events,[],NOW)
        self.assertFalse(result['recovery_verified'])
        self.assertIn('Table freshness deadline missed',[h['cause'] for h in result['hypotheses']])
    def test_expired_or_missing_freshness_cannot_close(self):
        events,_=collect_catalog(CONFIG,FakeSQL(),NOW)
        self.assertTrue(investigate(ASSET,events,[],NOW)['recovery_verified'])
        self.assertFalse(investigate(ASSET,events,[],NOW+timedelta(minutes=16))['recovery_verified'])
        self.assertFalse(investigate(ASSET,[],[],NOW)['recovery_verified'])
    def test_future_timestamp_unknown(self):
        events,_=collect_catalog(CONFIG,FakeSQL((NOW+timedelta(hours=1)).isoformat()),NOW)
        self.assertEqual(events[0]['state'],'UNKNOWN')
    def test_identifier_injection_rejected(self):
        for name in ['main.a.t;DROP TABLE x','main.a.`t`','a.b']:
            with self.assertRaises(ValueError):identifier(name)
        with self.assertRaises(ValueError):validate_config({'assets':[dict(ASSET,max_age_minutes=0)]})
    def test_http_flow_persistence_and_resolution_gate(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(d+'/state.sqlite3')
            events,edges=collect_catalog(CONFIG,FakeSQL(),NOW)
            store.ingest(events);store.set_dependencies(edges)
            app=create_app(store,CONFIG,'databricks',sql=FakeSQL())
            api=app.test_client()
            case=api.post('/api/investigate',json={'asset_id':'a'}).get_json()
            self.assertEqual(case['investigation']['potentially_affected'],['b'])
            response=api.post('/api/update',json={'id':case['id'],'status':'resolved'})
            self.assertEqual(response.status_code,200)
            self.assertEqual(Store(d+'/state.sqlite3').incidents()[0]['status'],'resolved')
            self.assertEqual(api.post('/api/investigate',json={'asset_id':'a'},headers={'Sec-Fetch-Site':'cross-site'}).status_code,403)
            self.assertEqual(api.get('/healthz').status_code,200)
            with api.get('/') as response:
                self.assertEqual(response.status_code,200)
            with api.get('/app.js') as response:
                self.assertEqual(response.status_code,200)
    def test_stale_http_resolution_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(d+'/state.sqlite3');api=create_app(store,CONFIG,'databricks',sql=FakeSQL()).test_client()
            case=api.post('/api/investigate',json={'asset_id':'a'}).get_json()
            self.assertEqual(api.post('/api/update',json={'id':case['id'],'status':'resolved'}).status_code,400)
    def test_job_collection_gap_blocks_recovery_until_successful_refresh(self):
        events,_=collect_catalog(CONFIG,FakeSQL(),NOW)
        run={'id':'run','asset_id':'a','kind':'run','state':'SUCCEEDED','time':(NOW-timedelta(minutes=1)).isoformat(),'collected_at':(NOW-timedelta(seconds=30)).isoformat()}
        gap={'id':'gap','asset_id':'a','kind':'coverage','source':'Databricks Jobs API','state':'UNKNOWN','time':NOW.isoformat()}
        events += [run,gap]
        asset=dict(ASSET,job_id=1)
        self.assertFalse(investigate(asset,events,[],NOW)['recovery_verified'])
        run['collected_at']=(NOW+timedelta(seconds=1)).isoformat()
        self.assertTrue(investigate(asset,events,[],NOW)['recovery_verified'])
    def test_delta_storage_binds_json_instead_of_interpolating(self):
        calls=[]
        class DB:
            def query(self,s,p=None):calls.append((s,p));return []
        store=DeltaStore(DB(),'main.test.state')
        store.save({'id':'x', 'title':"O'Brien; DROP TABLE x"})
        self.assertIn(':body',calls[-1][0]);self.assertNotIn("O'Brien",calls[-1][0])
        self.assertIn("O'Brien",calls[-1][1]['body'])
    def test_sql_polling_chunks_and_failure(self):
        chunk=N(data_array=[['one']],next_chunk_index=1)
        response=N(statement_id='s',status=N(state='SUCCEEDED'),manifest=N(truncated=False,schema=N(columns=[N(name='value')])),result=chunk)
        class API:
            def execute_statement(self,**kwargs):return N(statement_id='s',status=N(state='RUNNING'))
            def get_statement(self,*args):return response
            def get_statement_result_chunk_n(self,*args):return N(data_array=[['two']],next_chunk_index=None)
        with patch('investigator.sql.time.sleep'):
            self.assertEqual(SQL(N(statement_execution=API()),'w').query('SELECT 1'),[{'value':'one'},{'value':'two'}])
        response.manifest.truncated=True
        with patch('investigator.sql.time.sleep'),self.assertRaises(RuntimeError):SQL(N(statement_execution=API()),'w').query('SELECT 1')

if __name__=='__main__':unittest.main()

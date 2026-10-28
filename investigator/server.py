"""Single-tenant Databricks App; shared app-identity evidence visibility."""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from flask import Flask, jsonify, request, send_from_directory, send_file
from .store import Store, DeltaStore
from .engine import investigate
from .connectors import demo_events, collect_databricks, collect_catalog

ROOT = Path(__file__).resolve().parent.parent


def validate_config(config):
    from .sql import identifier
    ids = set()
    for asset in config['assets']:
        if asset['id'] in ids:
            raise ValueError('Duplicate asset identity')
        ids.add(asset['id'])
        if asset.get('table'):
            identifier(asset['table'])
            if asset.get('freshness_column'):
                identifier(asset['freshness_column'], 1)
            if not isinstance(asset.get('max_age_minutes'), (int, float)) or asset['max_age_minutes'] <= 0:
                raise ValueError('Table freshness requires a positive max_age_minutes')
        if asset.get('check_ttl_minutes', 15) <= 0:
            raise ValueError('check_ttl_minutes must be positive')
    for edge in config.get('dependencies', []):
        if edge['source'] not in ids or edge['target'] not in ids:
            raise ValueError('Dependency references an unconfigured asset')


def create_app(store=None, config=None, mode=None, client=None, sql=None):
    mode = mode or os.getenv('DII_MODE', 'demo')
    if mode not in {'demo', 'databricks'}:
        raise ValueError('DII_MODE must be demo or databricks')
    config = config or json.loads(Path(os.getenv('DII_CONFIG', str(ROOT/'config/assets.json'))).read_text())
    validate_config(config)
    if mode == 'databricks' and sql is None:
        from databricks.sdk import WorkspaceClient
        from .sql import SQL
        client = client or WorkspaceClient()
        sql = SQL(client, os.getenv('DATABRICKS_WAREHOUSE_ID'))
    if store is None:
        if os.getenv('DATABRICKS_APP_NAME'):
            if mode != 'databricks' or not os.getenv('DII_STATE_TABLE'):
                raise ValueError('Databricks Apps requires live mode and DII_STATE_TABLE for durable state')
            store = DeltaStore(sql, os.environ['DII_STATE_TABLE'])
        else:
            filename = 'demo-scenarios.sqlite3' if mode == 'demo' and any(a.get('scenario') for a in config['assets']) else f'{mode}.sqlite3'
            store = Store(os.getenv('DII_DB', str(ROOT/filename)))
    if mode == 'demo' and not store.events():
        store.ingest(demo_events(config))
    app = Flask(__name__, static_folder=None)
    assets = {a['id']: a for a in config['assets']}
    lock = Lock()
    delivery = None
    fraud = None
    if mode == 'demo' and hasattr(store, 'connect'):
        from delivery.lab import DeliveryLab
        delivery = DeliveryLab(store)
        from .fraud import FraudLab
        fraud = FraudLab(store)

    def dependencies():
        return [dict(e, origin='declared') for e in config.get('dependencies', [])] + store.dependencies()

    def get_asset(identity):
        if fraud and identity.startswith('fraud:'):
            return fraud.assets(fraud.get(identity.split(':',1)[1]))[0]
        if delivery and identity.startswith('delivery:'):
            return delivery.graph_assets(delivery.get(identity.split(':',1)[1]))[0]
        return assets[identity]

    def analyze(asset):
        if fraud and asset.get('kind') == 'fraud':
            return fraud.projection(fraud.get(asset['id'].split(':',1)[1]))
        if delivery and asset.get('kind') == 'delivery':
            return delivery.projection(delivery.get(asset['id'].split(':',1)[1]))
        return investigate(asset, store.events(), dependencies())

    if mode == 'demo' and not store.incidents():
        for asset in config['assets']:
            if asset.get('scenario'):
                store.save({'id': 'scenario-'+asset['scenario'], 'asset_id': asset['id'], 'title': asset['name'], 'status': 'investigating' if asset['scenario']=='recovered' else 'open', 'assignee': asset['owner'], 'history': [], 'investigation': analyze(asset)})

    @app.after_request
    def headers(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Content-Security-Policy'] = "default-src 'self'; style-src 'self'; script-src 'self'; frame-ancestors 'self'"
        return response

    @app.get('/healthz')
    def health():
        return jsonify(status='ok')

    @app.get('/')
    def index():
        return send_from_directory(ROOT/'static', 'index.html')

    @app.get('/fraud')
    def fraud_page():
        return send_from_directory(ROOT/'static', 'fraud.html')

    @app.get('/api/fraud/state')
    def fraud_state():
        from .fraud import SCENARIOS
        return jsonify(available=fraud is not None,scenarios=SCENARIOS if fraud else {},runs=fraud.runs() if fraud else [])

    @app.get('/api/fraud/evidence/<identity>')
    def fraud_evidence(identity):
        if not fraud:return jsonify(error='Fraud Guard is local synthetic mode only'),403
        try:
            response=jsonify(fraud.get(identity))
            response.headers['Content-Disposition']='attachment; filename=fraud-evidence.json'
            return response
        except ValueError as exc:return jsonify(error=str(exc)),404

    @app.get('/architecture')
    def architecture_page():
        return send_from_directory(ROOT/'static', 'architecture.html')

    @app.get('/demo')
    def guided_page():
        return send_from_directory(ROOT/'static', 'guided.html')

    @app.get('/builder')
    def builder_page():
        return send_from_directory(ROOT/'static', 'builder.html')

    @app.get('/delivery')
    def delivery_page():
        return send_from_directory(ROOT/'static', 'delivery.html')

    @app.get('/api/delivery/state')
    def delivery_state():
        from delivery.scenarios import SCENARIOS
        return jsonify(available=delivery is not None, scenarios=SCENARIOS if delivery else {}, runs=delivery.runs() if delivery else [])

    @app.get('/api/delivery/evaluation')
    def delivery_evaluation():
        if not delivery:
            return jsonify(error='Evaluation scorecard is local demo only'),403
        return send_from_directory(ROOT/'docs/hackathon', 'evaluation.json', as_attachment=True)

    @app.get('/api/delivery/evidence/<identity>')
    def delivery_evidence(identity):
        if not delivery:
            return jsonify(error='Delivery Lab is local synthetic mode only'), 403
        try:
            response=jsonify(delivery.get(identity))
            response.headers['Content-Disposition']='attachment; filename=delivery-evidence.json'
            return response
        except ValueError as exc:
            return jsonify(error=str(exc)),404

    @app.get('/<name>')
    def static_file(name):
        if name not in {'app.js', 'style.css', 'lineage.js', 'lineage-view.js', 'delivery.js', 'builder.js', 'guided.js', 'fraud.js'}:
            return jsonify(error='Not found'), 404
        return send_from_directory(ROOT/'static', name)

    @app.get('/api/state')
    def state():
        return jsonify(mode=mode, assets=list(assets.values())+([a for run in fraud.runs() for a in fraud.assets(run)] if fraud else [])+([a for run in delivery.runs() for a in delivery.graph_assets(run)] if delivery else []), incidents=store.incidents(), event_count=len(store.events()))

    @app.post('/api/<operation>')
    def mutate(operation):
        if not request.is_json or request.headers.get('Sec-Fetch-Site') == 'cross-site':
            return jsonify(error='Same-origin JSON requests required'), 403
        if request.content_length and request.content_length > 16384:
            return jsonify(error='Request too large'), 413
        try:
            data = request.get_json()
            if not isinstance(data, dict):
                raise ValueError('Expected an object')
            # One Gunicorn worker and this lock serialize shared-state mutations for this pilot.
            with lock:
                if operation == 'fraud':
                    if not fraud:return jsonify(error='Fraud Guard requires local synthetic mode'),403
                    if data.get('command')=='create':return jsonify(fraud.create(data.get('scenario','review_timeout'),data.get('guided',False)))
                    return jsonify(fraud.command(data['id'],data['command'],data))
                if operation == 'guided-demo':
                    if not delivery:return jsonify(error='Guided demo requires local synthetic mode'),403
                    if data.get('command')=='start':
                        from .workflow_builder import build
                        plan=build({'prompt':'Hourly synthetic delivery workflow with consumer verification','kind':'workflow','name':'manager_demo'})
                        run=delivery.create('mfts_late',workflow_plan=plan)
                        return jsonify(delivery.command(run['id'],'guided_start'))
                    if data.get('command')!='next':raise ValueError('Unknown guided command')
                    return jsonify(delivery.command(data['id'],'guided_next',data))
                if operation == 'workflow-run':
                    if not delivery:
                        return jsonify(error='Local workflow execution is disabled outside synthetic mode'),403
                    from .workflow_builder import build
                    plan=build(data)
                    if plan['status']=='BLOCKED':raise ValueError('Blocked workflow cannot run')
                    run=delivery.create(data.get('scenario','mfts_late'),workflow_plan=plan)
                    return jsonify(delivery.command(run['id'],'run_workflow'))
                if operation in {'workflow-plan', 'workflow-export'}:
                    from .workflow_builder import build, archive
                    plan = build(data)
                    if operation == 'workflow-export':
                        return send_file(archive(plan), mimetype='application/zip', as_attachment=True, download_name='synthetic-databricks-bundle.zip')
                    return jsonify(plan)
                if operation == 'delivery':
                    if not delivery:
                        return jsonify(error='Delivery Lab is disabled outside local synthetic mode'),403
                    if data.get('command')=='create':
                        return jsonify(delivery.create(data['scenario']))
                    return jsonify(delivery.command(data['id'],data['command'],data))
                if operation == 'demo-refresh-downstream':
                    if mode != 'demo':
                        return jsonify(error='Simulation is available only in demo mode'), 403
                    from .demo import downstream_events
                    asset = get_asset(data['asset_id'])
                    if not asset.get('scenario'):
                        raise ValueError('Select a demo scenario')
                    case = next(i for i in store.incidents() if i['asset_id'] == asset['id'] and i['status'] != 'resolved')
                    source = analyze(asset)
                    if not source['recovery_verified']:
                        raise ValueError('Recover the source first; current source checks must pass.')
                    if not case.get('source_recovered'):
                        case['source_recovered'] = source
                    store.ingest(downstream_events(asset, source))
                    case['investigation'] = analyze(asset)
                    store.save(case)
                    return jsonify(message='Simulated downstream refresh and checks passed.')
                if operation == 'demo-recover':
                    if mode != 'demo':
                        return jsonify(error='Simulation is available only in demo mode'), 403
                    from .demo import recovery_events
                    asset = get_asset(data['asset_id'])
                    if not asset.get('scenario'):
                        raise ValueError('Select a demo scenario')
                    store.ingest(recovery_events(asset))
                    for case in store.incidents():
                        if case['asset_id'] == asset['id'] and case['status'] != 'resolved':
                            if not case.get('before_recovery'):
                                case['before_recovery'] = case['investigation']
                            case.pop('source_recovered', None)
                            case['investigation'] = analyze(asset)
                            store.save(case)
                    return jsonify(message='Simulated successful rerun, current table timestamp and passing quality checks. Review and resolve the incident.')
                if operation == 'sync':
                    if mode == 'demo':
                        open_assets = {i['asset_id'] for i in store.incidents() if i['status'] != 'resolved'}
                        store.delete_events([e['id'] for e in store.events() if e['asset_id'] in open_assets and ('-recovery-' in e['id'] or e['kind'] == 'downstream_refresh')])
                    events = demo_events(config) if mode == 'demo' else collect_databricks(config, client)
                    edges = []
                    if mode == 'databricks':
                        catalog_events, edges = collect_catalog(config, sql)
                        events += catalog_events
                    store.ingest(events)
                    store.set_dependencies(edges)
                    if mode == 'demo':
                        for case in store.incidents():
                            if case['status'] != 'resolved' and not case['asset_id'].startswith(('delivery:','fraud:')):
                                case.pop('before_recovery', None)
                                case.pop('source_recovered', None)
                                case['investigation'] = analyze(get_asset(case['asset_id']))
                                store.save(case)
                    return jsonify(collected=len(events), observed_edges=len(edges))
                if operation == 'investigate':
                    asset = get_asset(data['asset_id'])
                    case = next((i for i in store.incidents() if i['asset_id'] == asset['id'] and i['status'] != 'resolved'), None)
                    case = case or {'id': str(uuid.uuid4()), 'asset_id': asset['id'], 'title': asset['name'], 'status': 'open', 'assignee': '', 'history': []}
                    case['investigation'] = analyze(asset)
                    return jsonify(store.save(case))
                if operation == 'update':
                    case = next(i for i in store.incidents() if i['id'] == data['id'])
                    status = data.get('status', case['status'])
                    if status not in {'open', 'investigating', 'resolved'}:
                        raise ValueError('Invalid status')
                    if case['status'] == 'resolved':
                        raise ValueError('Resolved incidents are immutable; create a new investigation')
                    if status == 'resolved':
                        result = analyze(get_asset(case['asset_id']))
                        if not result['recovery_verified']:
                            raise ValueError('Recovery is not verified. Sync current telemetry and investigate again.')
                        case['investigation'] = result
                    assignee = data.get('assignee', case['assignee'])
                    if not isinstance(assignee, str) or len(assignee) > 120:
                        raise ValueError('Assignee must be at most 120 characters')
                    # Header is authoritative only behind the Databricks Apps authentication proxy.
                    actor = request.headers.get('X-Forwarded-Email', 'local-user') if os.getenv('DATABRICKS_APP_NAME') else 'local-user'
                    case['history'].append({'from': case['status'], 'to': status, 'assignee': assignee, 'actor': actor, 'at': datetime.now(timezone.utc).isoformat()})
                    case.update(status=status, assignee=assignee)
                    return jsonify(store.save(case))
                return jsonify(error='Not found'), 404
        except (KeyError, ValueError, StopIteration, TypeError) as exc:
            return jsonify(error=str(exc) or 'Invalid request'), 400
        except Exception:
            # Never log raw SQL error payloads or source data/credentials.
            app.logger.error('Operation failed: %s', operation)
            return jsonify(error='Collection or storage failed. Check workspace permissions, warehouse query history and configuration.'), 502
    return app


def main():
    port = int(os.getenv('DATABRICKS_APP_PORT', os.getenv('DII_PORT', '8080')))
    create_app().run(host='0.0.0.0' if os.getenv('DATABRICKS_APP_NAME') else '127.0.0.1', port=port)


if __name__ == '__main__':
    main()

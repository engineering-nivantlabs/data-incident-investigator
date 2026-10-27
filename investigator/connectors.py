import hashlib
from datetime import datetime, timezone, timedelta


def demo_events(config):
    if any(a.get('scenario') for a in config['assets']):
        from .demo import scenario_events
        return scenario_events(config)
    now = datetime.now(timezone.utc)
    asset = next(a for a in config['assets'] if a.get('job_id'))
    return [
        {'id': 'demo-success', 'asset_id': asset['id'], 'kind': 'run', 'state': 'SUCCEEDED', 'time': (now-timedelta(hours=4)).isoformat(), 'message': 'Previous scheduled run completed.', 'source': 'Demo fixture'},
        {'id': 'demo-failed', 'asset_id': asset['id'], 'kind': 'run', 'state': 'FAILED', 'time': (now-timedelta(minutes=18)).isoformat(), 'message': 'Ingestion task failed: incompatible source schema. Inspect the source change before rerunning.', 'source': 'Demo fixture'}
    ]


def collect_databricks(config, client=None):
    if client is None:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
    events = []
    since = int((datetime.now(timezone.utc)-timedelta(days=7)).timestamp()*1000)
    for asset in config['assets']:
        if not asset.get('job_id'):
            continue
        try:
            for run in client.jobs.list_runs(job_id=int(asset['job_id']), start_time_from=since, completed_only=True, expand_tasks=False):
                state = run.state
                result = getattr(state, 'result_state', None) or getattr(state, 'life_cycle_state', None)
                result = getattr(result, 'value', result) or 'UNKNOWN'
                identity = f"{client.config.host}:{asset['id']}:{run.run_id}"
                events.append({'id': hashlib.sha256(identity.encode()).hexdigest(), 'asset_id': asset['id'], 'kind': 'run', 'state': result, 'time': datetime.fromtimestamp((run.end_time or run.start_time)/1000, timezone.utc).isoformat(), 'message': getattr(state, 'state_message', None) or 'No diagnostic message supplied.', 'source': 'Databricks Jobs API', 'collected_at': datetime.now(timezone.utc).isoformat(), 'run_id': run.run_id, 'url': run.run_page_url})
        except Exception:
            observed = datetime.now(timezone.utc).isoformat()
            events.append({'id': hashlib.sha256((asset['id']+'jobs'+observed).encode()).hexdigest(), 'asset_id': asset['id'], 'kind': 'coverage', 'state': 'UNKNOWN', 'time': observed, 'source': 'Databricks Jobs API', 'message': 'Job collection unavailable. Check job permissions and workspace authentication.'})
    return events


def collect_catalog(config, sql, now=None):
    """Observe Delta commit timestamps or max event timestamps; never infer from job success."""
    from .sql import identifier
    now = now or datetime.now(timezone.utc)
    events, edges = [], []
    tables = {a['table'].lower(): a['id'] for a in config['assets'] if a.get('table')}
    for asset in config['assets']:
        if not asset.get('table'):
            continue
        table = identifier(asset['table'])
        event = {'id': hashlib.sha256((asset['id']+now.isoformat()).encode()).hexdigest(), 'asset_id': asset['id'], 'kind': 'freshness', 'time': now.isoformat(), 'source': 'Databricks SQL', 'state': 'UNKNOWN', 'observed_at': None}
        try:
            if asset.get('freshness_column'):
                column = identifier(asset['freshness_column'], parts=1)
                rows = sql.query(f'SELECT MAX(unix_timestamp({column})) AS observed_epoch FROM {table}')
                epoch = rows[0]['observed_epoch'] if rows else None
                timestamp = datetime.fromtimestamp(float(epoch), timezone.utc).isoformat() if epoch is not None else None
                event['basis'] = 'max_event_timestamp'
            else:
                rows = sql.query(f'DESCRIBE HISTORY {table} LIMIT 1')
                timestamp = rows[0]['timestamp'] if rows else None
                event['basis'] = 'latest_delta_commit'
            if timestamp:
                observed = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=timezone.utc)
                age = (now-observed).total_seconds()/60
                event.update(observed_at=observed.isoformat(), age_minutes=age)
                event['state'] = 'UNKNOWN' if age < -5 else ('FRESH' if age <= asset['max_age_minutes'] else 'STALE')
            event['message'] = f"Table freshness: {event['state']}; basis: {event.get('basis')}; observed timestamp: {event['observed_at']}."
        except Exception:
            event['message'] = 'Table freshness unavailable. Check SELECT/USE grants, table format, timestamp column and SQL query history.'
        events.append(event)
    # Explicit allowlist prevents account-wide lineage being exposed to this pilot.
    for name, source_id in tables.items():
        try:
            rows = sql.query('SELECT source_table_full_name, target_table_full_name, MAX(event_time) AS last_seen FROM system.access.table_lineage WHERE event_date >= current_date() - INTERVAL 30 DAYS AND lower(source_table_full_name) = :table AND target_table_full_name IS NOT NULL GROUP BY source_table_full_name, target_table_full_name', {'table': name})
            for row in rows:
                target = tables.get(row['target_table_full_name'].lower())
                if target:
                    edges.append({'source': source_id, 'target': target, 'origin': 'unity_catalog', 'last_seen': row['last_seen']})
        except Exception:
            events.append({'id': 'lineage-'+hashlib.sha256((name+now.isoformat()).encode()).hexdigest(), 'asset_id': source_id, 'kind': 'coverage', 'state': 'UNKNOWN', 'time': now.isoformat(), 'source': 'Unity Catalog', 'message': 'Observed lineage unavailable. Declared dependencies remain usable; check system table permissions.'})
    return events, edges

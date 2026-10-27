import json
import sqlite3
from datetime import datetime, timezone


class Store:
    def __init__(self, path):
        self.path = path
        with self.connect() as db:
            db.executescript('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, body TEXT NOT NULL); CREATE TABLE IF NOT EXISTS incidents (id TEXT PRIMARY KEY, body TEXT NOT NULL);')

    def connect(self):
        return sqlite3.connect(self.path, timeout=15)

    def ingest(self, events):
        with self.connect() as db:
            db.executemany('INSERT OR REPLACE INTO events VALUES (?, ?)', [(e['id'], json.dumps(e)) for e in events])

    def events(self):
        with self.connect() as db:
            return [json.loads(r[0]) for r in db.execute('SELECT body FROM events')]

    def delete_events(self, identities):
        with self.connect() as db:
            db.executemany('DELETE FROM events WHERE id = ?', [(identity,) for identity in identities])

    def incidents(self):
        with self.connect() as db:
            return [json.loads(r[0]) for r in db.execute('SELECT body FROM incidents')]

    def save(self, incident):
        incident['updated_at'] = datetime.now(timezone.utc).isoformat()
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO incidents VALUES (?, ?)', (incident['id'], json.dumps(incident)))
        return incident

    def dependencies(self):
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS metadata (id TEXT PRIMARY KEY, body TEXT NOT NULL)')
            row = db.execute("SELECT body FROM metadata WHERE id='dependencies'").fetchone()
            return json.loads(row[0]) if row else []

    def set_dependencies(self, edges):
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS metadata (id TEXT PRIMARY KEY, body TEXT NOT NULL)')
            db.execute("INSERT OR REPLACE INTO metadata VALUES ('dependencies', ?)", (json.dumps(edges),))


class DeltaStore:
    """Durable single-worker pilot state. Not a concurrent transactional SaaS backend."""
    def __init__(self, sql, table):
        from .sql import identifier
        self.sql, self.table = sql, identifier(table)
        sql.query(f'SELECT kind, id, body FROM {self.table} LIMIT 0')

    def _read(self, kind):
        return [json.loads(r['body']) for r in self.sql.query(f'SELECT body FROM {self.table} WHERE kind = :kind', {'kind': kind})]

    def _write(self, kind, identity, value):
        self.sql.query(f'MERGE INTO {self.table} t USING (SELECT :kind AS kind, :id AS id, :body AS body) s ON t.kind=s.kind AND t.id=s.id WHEN MATCHED THEN UPDATE SET body=s.body WHEN NOT MATCHED THEN INSERT *', {'kind': kind, 'id': identity, 'body': json.dumps(value)})

    def ingest(self, events):
        # Each normalized event is an immutable observation; repeated IDs are identical.
        existing = {e['id'] for e in self.events()}
        for event in events:
            if event['id'] not in existing or event['kind'] == 'run':
                self._write('event', event['id'], event)
                existing.add(event['id'])

    def events(self):
        return self._read('event')

    def incidents(self):
        return self._read('incident')

    def save(self, incident):
        incident['updated_at'] = datetime.now(timezone.utc).isoformat()
        self._write('incident', incident['id'], incident)
        return incident

    def dependencies(self):
        data = self._read('dependencies')
        return data[0]['edges'] if data else []

    def set_dependencies(self, edges):
        self._write('dependencies', 'latest', {'edges': edges})

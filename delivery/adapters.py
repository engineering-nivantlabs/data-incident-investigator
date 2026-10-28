"""Local adapters execute writes and acknowledgments; no network dependencies."""
from typing import Protocol


class Broker(Protocol):
    def publish(self, key, payload, mode='ok', topic=None): ...


class LocalBroker:
    def __init__(self, ledger): self.ledger=ledger

    def publish(self, key, payload, mode='ok', topic=None):
        # A timed-out acknowledgment can follow successful broker acceptance.
        if mode=='reject':return 'FAILED'
        self.ledger.append({'offset':len(self.ledger),'key':key,'payload':payload,'topic':topic})
        return 'UNKNOWN' if mode=='timeout_after_accept' else 'SUCCEEDED'

    def reconcile(self,key,payload):
        rows=[row for row in self.ledger if row['key']==key]
        if not rows:return 'MISSING'
        return 'MATCH' if all(row['payload']==payload for row in rows) else 'CONFLICT'


class Transfer(Protocol):
    def deliver(self, artifact_id, payload, mode='ok'): ...


class LocalTransfer:
    def __init__(self,receiver): self.receiver=receiver

    def deliver(self,artifact_id,payload,mode='ok'):
        if mode=='timeout':return 'FAILED'
        # Artifact identity allows idempotent receiver replacement.
        self.receiver[artifact_id]=payload
        return 'SUCCEEDED'

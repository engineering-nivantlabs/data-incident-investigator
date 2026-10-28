import hashlib
import json
from decimal import Decimal, InvalidOperation


def fingerprint(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',', ':'),ensure_ascii=True).encode()).hexdigest()


def canonicalize(record, dataset, routes):
    identity=str(record.get('source_record_id','')).strip()
    route=str(record.get('route_key','')).strip().upper()
    if not identity or route not in routes:
        raise ValueError('Missing source_record_id or unknown route_key')
    currency=str(record.get('currency','')).strip().upper()
    if currency not in {'USD','GBP','EUR','INR'}:
        raise ValueError('Unsupported synthetic currency')
    try:
        amount=Decimal(str(record['amount']))
        if not amount.is_finite() or amount <= 0 or amount != amount.quantize(Decimal('0.01')):
            raise ValueError('Amount must be positive with at most two decimal places')
    except (KeyError,InvalidOperation):
        raise ValueError('Invalid amount')
    payload={'source_record_id':identity,'route_key':route,'currency':currency,'amount':format(amount,'.2f')}
    processing_id=fingerprint({'dataset':dataset,'record':identity})
    distribution_id=fingerprint({'processing_id':processing_id,'route':route})
    return dict(payload,processing_id=processing_id,distribution_id=distribution_id,payload_fingerprint=fingerprint(payload))

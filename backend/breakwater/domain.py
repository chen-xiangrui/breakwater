"""Pure input contracts and comparison rules. No infrastructure dependencies."""
import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, StrictBool

INTERNAL = ['trade_id','account','instrument','side','quantity','execution_price','currency','executed_at']
BROKER = ['execution_ref','client_account','symbol','buy_sell','filled_qty','fill_price','ccy','execution_time']
CANONICAL = ['reconciliation_ref','account','instrument','side','quantity','execution_price','currency','executed_at']
INSTRUMENTS = {'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','JPM','V','ABC','XYZ'}
ACCOUNTS = {'ACC01','ACC02'}
CURRENCIES = {'USD','SGD','EUR','GBP','JPY','HKD'}


class Manifest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    schema_version: Literal[1]
    side: Literal['INTERNAL','BROKER']
    broker_id: str = Field(min_length=1, max_length=80)
    business_date: date
    dataset_id: str = Field(min_length=1, max_length=80)
    revision: int = Field(gt=0, strict=True)
    representation: Literal['EXECUTION']
    account_ids: list[str] = Field(min_length=1, max_length=100)
    expected_bytes: int = Field(ge=0, strict=True)
    expected_data_rows: int = Field(ge=0, le=1000000, strict=True)
    sha256: str = Field(pattern=r'^[a-f0-9]{64}$')
    complete: StrictBool


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def fingerprint(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def stable_digest(scope_id, ref):
    return int(fingerprint([scope_id,ref])[:15],16)


def decimal_value(value, positive=False):
    if not isinstance(value,str) or not re.fullmatch(r'[+-]?\d+(\.\d+)?',value.strip()):
        raise ValueError('Expected a plain finite decimal')
    try:
        n = Decimal(value)
    except InvalidOperation:
        raise ValueError('Invalid decimal') from None
    if not n.is_finite() or n < 0 or (positive and n == 0):
        raise ValueError('Quantity must be positive; price must be non-negative')
    if n.as_tuple().exponent < -8 or n >= Decimal('10000000000000000'):
        raise ValueError('Outside NUMERIC(24,8); rounding is not allowed')
    return format(n, 'f')


def normalize(raw, side, accounts):
    headers = INTERNAL if side == 'INTERNAL' else BROKER
    errors = []
    result = {}
    for source, target in zip(headers,CANONICAL):
        v = raw.get(source)
        if not isinstance(v,str) or not v.strip():
            errors.append({'field':source,'code':'REQUIRED','message':'A value is required'})
        else:
            result[target] = v.strip()
    if errors:
        return None, errors
    for f in ['reconciliation_ref','account','instrument']:
        if len(result[f])>128:
            errors.append({'field':f,'code':'TOO_LONG','message':'Maximum 128 characters'})
    if side == 'BROKER':
        result['side'] = {'B':'BUY','S':'SELL'}.get(result['side'], result['side'])
    for field, allowed in [('account',ACCOUNTS),('instrument',INSTRUMENTS),('currency',CURRENCIES),('side',{'BUY','SELL'})]:
        if result[field] not in allowed:
            errors.append({'field':field,'code':'UNKNOWN_MAPPING','message':f'Unknown {field} in equities-v1'})
    if result['account'] not in accounts:
        errors.append({'field':'account','code':'OUTSIDE_COVERAGE','message':'Account is outside declared coverage'})
    for field in ['quantity','execution_price']:
        try:
            result[field] = decimal_value(result[field], positive=field=='quantity')
        except ValueError as e:
            errors.append({'field':field,'code':'INVALID_DECIMAL','message':str(e)})
    try:
        dt = datetime.fromisoformat(result['executed_at'].replace('Z','+00:00'))
        if dt.tzinfo is None:
            raise ValueError()
        result['executed_at'] = dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        errors.append({'field':'executed_at','code':'INVALID_TIMESTAMP','message':'Use an ISO timestamp with timezone'})
    return (None if errors else result), errors


def compare(internal, broker, rules):
    """One group per business key; duplicate keys never receive arbitrary pairings."""
    evidence = {'internal':internal,'broker':broker,'differences':[], 'rules':rules}
    families = []
    if len(internal)>1:
        families.append('DUPLICATE_INTERNAL')
    if len(broker)>1:
        families.append('DUPLICATE_BROKER')
    if families:
        evidence['duplicate_detail'] = {
            side: 'IDENTICAL_CONTENT' if len({fingerprint(r['normalized']) for r in rows})==1 else 'CONFLICTING_CONTENT'
            for side,rows in [('internal',internal),('broker',broker)] if len(rows)>1
        }
        return 'DUPLICATE', families, evidence
    if not internal or not broker:
        return 'MISSING', ['MISSING_INTERNAL' if not internal else 'MISSING_BROKER'], evidence
    a,b = internal[0]['normalized'],broker[0]['normalized']
    for field in ['account','instrument','side','currency','quantity','execution_price']:
        delta, tolerance = None, None
        if field in ['quantity','execution_price']:
            delta = Decimal(b[field])-Decimal(a[field])
            tolerance = Decimal(rules['quantity_tolerance' if field=='quantity' else 'price_tolerance'])
            different = abs(delta)>tolerance
        else:
            different = a[field]!=b[field]
        if different:
            evidence['differences'].append({'field':field,'internal':a[field],'broker':b[field],
                'delta':str(delta) if delta is not None else None,
                'tolerance':str(tolerance) if tolerance is not None else None,
                'rule':f'execution-v1:{field}'})
    if a['currency']==b['currency']:
        with localcontext() as ctx:
            ctx.prec=64  # Exact products and subtraction for two NUMERIC(24,8) values.
            evidence['notional_delta'] = str(Decimal(b['quantity'])*Decimal(b['execution_price'])-Decimal(a['quantity'])*Decimal(a['execution_price']))
        evidence['notional_currency'] = a['currency']
    return ('MISMATCH',['FIELD_MISMATCH'],evidence) if evidence['differences'] else ('MATCHED',[],evidence)


def evidence_fingerprint(family,evidence):
    # Independent of file/row order, IDs, and revision: a waiver follows business evidence.
    return fingerprint({'family':family,'rules':evidence['rules'],
        **{s:sorted([r['normalized'] for r in evidence[s]], key=canonical_json) for s in ['internal','broker']}})


def safe_csv(value):
    value = str(value) if value is not None else ''
    return "'"+value if value.lstrip().startswith(('=','+','-','@','\t','\r')) else value


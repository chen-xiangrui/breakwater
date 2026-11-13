"""Pure input contracts and comparison rules. No infrastructure dependencies."""
import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, StrictBool

# Source columns map by position to the shared field names in CANONICAL.
INTERNAL = ['trade_id','account','instrument','side','quantity','execution_price','currency','executed_at']
BROKER = ['execution_ref','client_account','symbol','buy_sell','filled_qty','fill_price','ccy','execution_time']
CANONICAL = ['reconciliation_ref','account','instrument','side','quantity','execution_price','currency','executed_at']
# Supported reference values for the current equities comparison rules.
INSTRUMENTS = {'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','JPM','V','ABC','XYZ'}
# Fixture account IDs; ingestion checks accounts against the batch's declared coverage.
ACCOUNTS = {'ACC01','ACC02'}
CURRENCIES = {'USD','SGD','EUR','GBP','JPY','HKD'}


# Validate the declaration describing one uploaded file; reject unexpected fields.
class Manifest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    # Identify the file's format, source, reconciliation scope, and revision.
    schema_version: Literal[1]
    side: Literal['INTERNAL','BROKER']
    broker_id: str = Field(min_length=1, max_length=80)
    business_date: date
    dataset_id: str = Field(min_length=1, max_length=80)
    revision: int = Field(gt=0, strict=True)
    representation: Literal['EXECUTION']
    # Declare account coverage and the expected file size, row count, and checksum.
    account_ids: list[str] = Field(min_length=1, max_length=100)
    expected_bytes: int = Field(ge=0, strict=True)
    expected_data_rows: int = Field(ge=0, le=1000000, strict=True)
    sha256: str = Field(pattern=r'^[a-f0-9]{64}$')
    # Completeness is declared by the producer or uploader.
    complete: StrictBool


def canonical_json(value):
    # Stable key ordering and compact separators make equivalent objects hash consistently.
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def fingerprint(value):
    # Hash the serialized business data to identify changes or repeated requests.
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def stable_digest(scope_id, ref):
    # Turn a scope/reference hash into a stable integer used to assign comparison buckets.
    return int(fingerprint([scope_id,ref])[:15],16)


def decimal_value(value, positive=False):
    # Accept ordinary decimal strings; reject scientific notation and special values.
    if not isinstance(value,str) or not re.fullmatch(r'[+-]?\d+(\.\d+)?',value.strip()):
        raise ValueError('Expected a plain finite decimal')
    try:
        n = Decimal(value)
    except InvalidOperation:
        raise ValueError('Invalid decimal') from None
    # Quantities must exceed zero; prices can be zero but cannot be negative.
    if not n.is_finite() or n < 0 or (positive and n == 0):
        raise ValueError('Quantity must be positive; price must be non-negative')
    # Match PostgreSQL NUMERIC(24,8) precision without silently rounding the input.
    if n.as_tuple().exponent < -8 or n >= Decimal('10000000000000000'):
        raise ValueError('Outside NUMERIC(24,8); rounding is not allowed')
    # Keep an exact, plain decimal string for JSON evidence and later Decimal arithmetic.
    return format(n, 'f')


# Convert one source row into common fields and return all detected validation errors.
def normalize(raw, side, accounts):
    # Choose the internal or broker column names used to read this source row.
    headers = INTERNAL if side == 'INTERNAL' else BROKER
    errors = []
    result = {}
    # Map each source column to its canonical name, requiring nonempty string values.
    for source, target in zip(headers,CANONICAL):
        v = raw.get(source)
        if not isinstance(v,str) or not v.strip():
            errors.append({'field':source,'code':'REQUIRED','message':'A value is required'})
        else:
            # Remove surrounding whitespace before storing or comparing the value.
            result[target] = v.strip()
    if errors:
        # Later checks need all required fields, so stop early if any are missing.
        return None, errors
    # Enforce identifier lengths supported by the normalized record contract.
    for f in ['reconciliation_ref','account','instrument']:
        if len(result[f])>128:
            errors.append({'field':f,'code':'TOO_LONG','message':'Maximum 128 characters'})
    if side == 'BROKER':
        # Express the broker's B/S trade direction using the shared BUY/SELL values.
        result['side'] = {'B':'BUY','S':'SELL'}.get(result['side'], result['side'])
    # Check supported instruments, currencies, and trade directions.
    for field, allowed in [('instrument',INSTRUMENTS),('currency',CURRENCIES),('side',{'BUY','SELL'})]:
        if result[field] not in allowed:
            errors.append({'field':field,'code':'UNKNOWN_MAPPING','message':f'Unknown {field} in equities-v1'})
    # Restrict this row to the account coverage declared for its batch.
    if result['account'] not in accounts:
        errors.append({'field':'account','code':'OUTSIDE_COVERAGE','message':'Account is outside declared coverage'})
    # Validate exact decimals and store them in a consistent string representation.
    for field in ['quantity','execution_price']:
        try:
            result[field] = decimal_value(result[field], positive=field=='quantity')
        except ValueError as e:
            errors.append({'field':field,'code':'INVALID_DECIMAL','message':str(e)})
    # Require a timezone and represent equivalent execution times as UTC ISO timestamps.
    try:
        dt = datetime.fromisoformat(result['executed_at'].replace('Z','+00:00'))
        if dt.tzinfo is None:
            raise ValueError()
        result['executed_at'] = dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        errors.append({'field':'executed_at','code':'INVALID_TIMESTAMP','message':'Use an ISO timestamp with timezone'})
    # Invalid rows return no normalized record; their errors are saved as rejections.
    return (None if errors else result), errors


def compare(internal, broker, rules):
    """One group per business key; duplicate keys never receive arbitrary pairings."""
    # Retain all source records and the rules used to explain this comparison.
    evidence = {'internal':internal,'broker':broker,'differences':[], 'rules':rules}
    families = []
    # Detect duplicate references before attempting ordinary one-to-one comparisons.
    if len(internal)>1:
        families.append('DUPLICATE_INTERNAL')
    if len(broker)>1:
        families.append('DUPLICATE_BROKER')
    if families:
        # Distinguish repeated identical rows from rows with conflicting business values.
        evidence['duplicate_detail'] = {
            side: 'IDENTICAL_CONTENT' if len({fingerprint(r['normalized']) for r in rows})==1 else 'CONFLICTING_CONTENT'
            for side,rows in [('internal',internal),('broker',broker)] if len(rows)>1
        }
        return 'DUPLICATE', families, evidence
    # A reference present on only one side becomes a missing-record finding.
    if not internal or not broker:
        return 'MISSING', ['MISSING_INTERNAL' if not internal else 'MISSING_BROKER'], evidence
    # With one row per side, compare business fields using exact or decimal rules.
    a,b = internal[0]['normalized'],broker[0]['normalized']
    for field in ['account','instrument','side','currency','quantity','execution_price']:
        delta, tolerance = None, None
        if field in ['quantity','execution_price']:
            # A difference equal to the configured tolerance is still a match.
            delta = Decimal(b[field])-Decimal(a[field])
            tolerance = Decimal(rules['quantity_tolerance' if field=='quantity' else 'price_tolerance'])
            different = abs(delta)>tolerance
        else:
            different = a[field]!=b[field]
        if different:
            # Preserve both values, the delta, and the rule that explains the mismatch.
            evidence['differences'].append({'field':field,'internal':a[field],'broker':b[field],
                'delta':str(delta) if delta is not None else None,
                'tolerance':str(tolerance) if tolerance is not None else None,
                'rule':f'execution-v1:{field}'})
    # Calculate broker-minus-internal notional only when both sides share a currency.
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
    # Prefix spreadsheet formula-like values so exported evidence is treated as text.
    value = str(value) if value is not None else ''
    return "'"+value if value.lstrip().startswith(('=','+','-','@','\t','\r')) else value


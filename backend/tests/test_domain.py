# Fast, database-free checks for normalization, matching, fingerprints, and CSV safety.
from copy import deepcopy
from decimal import Decimal
import pytest
from breakwater.domain import normalize,decimal_value,compare,evidence_fingerprint,stable_digest,safe_csv

RULES={'price_tolerance':'0.01','quantity_tolerance':'0'}


def row(**updates):
    # Build a normalized record with selected fields overridden for a comparison scenario.
    r={'reconciliation_ref':'T1','account':'ACC01','instrument':'AAPL','side':'BUY','currency':'USD','quantity':'100','execution_price':'10.60','executed_at':'2026-11-06T14:30:00+00:00'}
    r.update(updates)
    return {'id':1,'normalized':r,'record_number':1}


@pytest.mark.parametrize('value',['NaN','Infinity','-1','0','1e2','0.000000001','10000000000000000',''])
def test_invalid_quantities(value):
    # Reject special values, invalid signs/precision, and formats that would require rounding.
    with pytest.raises(ValueError):
        decimal_value(value,positive=True)


@pytest.mark.parametrize('price,expected',[('10.60','MATCHED'),('10.61','MATCHED'),('10.61000001','MISMATCH'),('10.59','MATCHED'),('10.58999999','MISMATCH')])
def test_exact_decimal_tolerance(price,expected):
    # Price differences exactly at the configured tolerance remain matched on either side.
    assert compare([row()],[row(execution_price=price)],RULES)[0]==expected


def test_duplicates_exclude_ordinary_pairing():
    # Preserve duplicate evidence instead of choosing arbitrary row pairs to compare.
    outcome,families,evidence=compare([row(),row()],[row(quantity='300'),row()],RULES)
    assert outcome=='DUPLICATE'
    assert families==['DUPLICATE_INTERNAL','DUPLICATE_BROKER']
    assert evidence['differences']==[]
    assert evidence['duplicate_detail']=={'internal':'IDENTICAL_CONTENT','broker':'CONFLICTING_CONTENT'}


def test_account_and_currency_are_compared_not_keys():
    # Changed account/currency values stay in one reference group and become field findings.
    _,families,evidence=compare([row()],[row(account='ACC02',currency='EUR')],RULES)
    assert families==['FIELD_MISMATCH']
    assert {d['field'] for d in evidence['differences']}=={'account','currency'}
    assert 'notional_delta' not in evidence


def test_missing_and_fingerprint_ignore_lineage():
    # Missing sides produce findings; source row IDs do not change a waiver fingerprint.
    assert compare([row()],[],RULES)[1]==['MISSING_BROKER']
    assert compare([],[row()],RULES)[1]==['MISSING_INTERNAL']
    a=compare([row()],[row(quantity='120')],RULES)[2]
    b=deepcopy(a)
    b['internal'][0]['id']=999
    assert evidence_fingerprint('FIELD_MISMATCH',a)==evidence_fingerprint('FIELD_MISMATCH',b)
    b['broker'][0]['normalized']['quantity']='130'
    assert evidence_fingerprint('FIELD_MISMATCH',a)!=evidence_fingerprint('FIELD_MISMATCH',b)


def test_normalization_retains_explicit_business_contract():
    # Normalize broker names/direction/time while retaining raw evidence and scope coverage.
    raw=dict(zip(['execution_ref','client_account','symbol','buy_sell','filled_qty','fill_price','ccy','execution_time'],
                 [' T1 ','ACC01','AAPL','B','100','10.60','USD','2026-11-07T00:30:00+08:00']))
    normalized,errors=normalize(raw,'BROKER',['ACC01'])
    assert not errors
    assert normalized['side']=='BUY'
    assert normalized['executed_at']=='2026-11-06T16:30:00+00:00'
    assert raw['execution_ref']==' T1 '
    assert 'business_date' not in normalized
    assert normalize(raw,'BROKER',['ACC02'])[1][0]['code']=='OUTSIDE_COVERAGE'


@pytest.mark.parametrize('value',['=SUM(A1)','+cmd','-cmd','@SUM','  =cmd','\t=cmd'])
def test_csv_injection_neutralized(value):
    # Exported source values that resemble spreadsheet formulas must be escaped as text.
    assert safe_csv(value).startswith("'")


def test_partition_digest_stable_and_scoped():
    # The same reference stays in a stable scope-specific hash space across repeated runs.
    assert stable_digest('scope','T1')==stable_digest('scope','T1')
    assert stable_digest('scope','T1')!=stable_digest('other','T1')


def test_large_notional_does_not_round():
    # High-precision products/deltas must retain all digits supported by the input contract.
    from decimal import localcontext
    a=row(quantity='9999999999999999.12345678',execution_price='9999999999999999.12345678')
    b=row(quantity='9999999999999999.12345679',execution_price='9999999999999999.12345679')
    evidence=compare([a],[b],RULES)[2]
    with localcontext() as ctx:
        ctx.prec=64
        expected=Decimal(b['normalized']['quantity'])**2-Decimal(a['normalized']['quantity'])**2
    assert Decimal(evidence['notional_delta'])==expected


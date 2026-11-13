"""Scope setup uses the real API/database and feeds the normal workflow."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from breakwater.db import transaction, query, uid
from breakwater.demo import fixture
from test_integration import client, wait_for


@pytest.fixture
def scope_data():
    # Isolate requests with a unique broker and exercise accounts beyond the seeded demo pair.
    data = {'broker_id': 'scope-test-'+uid(), 'broker_name': 'Scope setup test',
            'business_date': '2026-11-13', 'dataset_id': 'equities',
            'account_ids': ['CLIENT02', 'CLIENT01'],
            'internal_deadline': '2026-11-13T22:00:00+08:00',
            'broker_deadline': '2026-11-13T22:30:00+08:00'}
    yield data
    # Preserve evidence while keeping test scopes out of the user workspace.
    with transaction() as c:
        query(c, 'UPDATE scopes SET hidden=true WHERE broker_id=:broker', broker=data['broker_id'])


def create(client, data, key=None):
    # Match the frontend's JSON scope request, including its retry-protection header.
    return client.post('/scopes', json=data, headers={'Idempotency-Key': key or uid()})


def test_scope_creation_registers_deliveries_and_supports_filters(client, scope_data):
    # New scopes atomically register expected arrivals and are discoverable through list filters.
    response = create(client, scope_data)
    assert response.status_code == 201, response.text
    scope = response.json()
    assert scope['account_ids'] == ['CLIENT01', 'CLIENT02']
    assert scope['business_date'] == scope_data['business_date']
    assert scope['published_run_id'] is None
    deliveries = client.get('/scopes/'+scope['id']+'/deliveries').json()['deliveries']
    assert {d['side'] for d in deliveries} == {'INTERNAL', 'BROKER'}
    for d in deliveries:
        assert d['arrived_at'] is None and d['batches'] == []
        assert d['deadline'].startswith('2026-11-13T14:')
    result = client.get('/scopes', params={'broker_id': scope_data['broker_id'],
        'business_date': scope_data['business_date'], 'dataset_id': 'equities'}).json()
    assert [s['id'] for s in result] == [scope['id']]
    assert client.get('/scopes', params={'broker_id': scope_data['broker_id'], 'business_date': '2026-11-14'}).json() == []
    assert client.get('/scopes', params={'business_date': 'not-a-date'}).status_code == 422


def test_retries_reuse_scope_and_conflicting_details_are_rejected(client, scope_data):
    # Same-key retries and equivalent new-key requests reuse the scope; changed setup conflicts.
    key = uid()
    first = create(client, scope_data, key)
    assert first.status_code == 201, first.text
    repeated = create(client, scope_data, key)
    assert repeated.status_code == 200 and repeated.json()['id'] == first.json()['id']
    same_scope = create(client, scope_data)
    assert same_scope.status_code == 200 and same_scope.json()['id'] == first.json()['id']
    changed = {**scope_data, 'account_ids': ['OTHER']}
    assert create(client, changed, key).status_code == 409
    assert create(client, changed).status_code == 409
    with transaction() as c:
        assert query(c, 'SELECT count(*) FROM deliveries WHERE scope_id=:id', id=first.json()['id']).scalar() == 2


def test_concurrent_scope_creation_and_next_business_date(client, scope_data):
    # Concurrent callers share one scope; the next business date gets a separate identity.
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: create(client, scope_data), range(4)))
    assert sorted(r.status_code for r in results) == [200, 200, 200, 201]
    assert len({r.json()['id'] for r in results}) == 1
    tomorrow = create(client, {**scope_data, 'business_date': '2026-11-14',
        'internal_deadline': '2026-11-14T22:00:00+08:00', 'broker_deadline': '2026-11-14T22:30:00+08:00'})
    assert tomorrow.status_code == 201
    assert tomorrow.json()['id'] != results[0].json()['id']


@pytest.mark.parametrize('change', [
    {'broker_id': '   '}, {'account_ids': []}, {'account_ids': ['ACC01', ' ACC01 ']},
    {'account_ids': [' ']}, {'business_date': 'invalid'},
    {'internal_deadline': '2026-11-13T22:00:00'},
])
def test_invalid_scope_details_do_not_create_scope(client, scope_data, change):
    # Invalid identifiers, account coverage, dates, or timezone-free deadlines leave no scope.
    assert create(client, {**scope_data, **change}).status_code == 422
    assert client.get('/scopes', params={'broker_id': scope_data['broker_id']}).json() == []


def test_new_scope_with_configured_accounts_can_finish_reconciliation(client, scope_data):
    # Verify that newly configured accounts propagate through manifests and row validation.
    created = create(client, scope_data)
    assert created.status_code == 201, created.text
    sid = created.json()['id']
    files = {}
    for side, name in [('INTERNAL', 'internal_file'), ('BROKER', 'broker_file')]:
        content = fixture(side, 'breaks', 60)[0]
        content = content.replace(b'ACC01', b'CLIENT01').replace(b'ACC02', b'CLIENT02')
        files[name] = (side.lower()+'.csv', content, 'text/csv')
    response = client.post('/workflows', files=files, data={'scope_id': sid, 'complete': 'true'},
                           headers={'Idempotency-Key': uid()})
    assert response.status_code == 202, response.text
    # Follow the actual workers through validation, run creation, and result publication.
    workflow = wait_for(lambda: client.get('/workflows/'+response.json()['id']).json(),
                        lambda w: w['status'] in ['SUCCEEDED', 'BLOCKED', 'FAILED'])
    assert workflow['status'] == 'SUCCEEDED', workflow
    run = client.get('/runs/'+workflow['run_id']).json()
    assert run['scope_id'] == sid and run['sequence'] == 1
    assert run['metrics']['matched'] == 45
    for batch in run['inputs']:
        assert batch['manifest']['account_ids'] == ['CLIENT01', 'CLIENT02']
        assert batch['manifest']['broker_id'] == scope_data['broker_id']
        assert batch['manifest']['business_date'] == scope_data['business_date']

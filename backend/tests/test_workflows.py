import time
import pytest

from breakwater.db import transaction,query,one,uid
from breakwater.demo import fixture
from test_integration import client,scope,wait_for


def submit(client,scope,scenario='breaks',key=None,complete=True,count=60):
    internal=fixture('INTERNAL',scenario,count,dataset=scope)[0]
    broker=fixture('BROKER',scenario,count,dataset=scope)[0]
    return client.post('/workflows',files={'internal_file':('internal.csv',internal,'text/csv'),
        'broker_file':('broker.csv',broker,'text/csv')},data={'scope_id':scope,'complete':str(complete).lower()},
        headers={'Idempotency-Key':key or uid()})


@pytest.mark.parametrize('count',[60,10000])
def test_manual_pair_validates_then_starts_one_pinned_run(client,scope,count):
    key=uid()
    response=submit(client,scope,key=key,count=count)
    assert response.status_code==202,response.text
    workflow=response.json()
    repeated=submit(client,scope,key=key,count=count)
    assert repeated.status_code==202 and repeated.json()['id']==workflow['id']
    conflicting=submit(client,scope,scenario='corrected',key=key,count=count)
    assert conflicting.status_code==409
    result=wait_for(lambda:client.get('/workflows/'+workflow['id']).json(),lambda w:w['status'] in ['SUCCEEDED','FAILED','BLOCKED'])
    assert result['status']=='SUCCEEDED',result
    r=client.get('/runs/'+result['run_id']).json()
    assert r['metrics']['matched']==count-15 and r['metrics']['findings']==16
    assert r['internal_batch_id']==workflow['internal_batch_id'] and r['broker_batch_id']==workflow['broker_batch_id']
    for b in r['inputs']:
        assert b['manifest_origin']=='OPERATOR'
        assert b['manifest']['expected_data_rows']==count+1
        assert b['manifest']['complete'] is True
    with transaction() as c:
        assert query(c,'SELECT count(*) FROM runs WHERE scope_id=:id',id=scope).scalar()==1
        assert query(c,'SELECT count(*) FROM batches WHERE scope_id=:id',id=scope).scalar()==2
    latest=client.get('/workflows',params={'scope_id':scope}).json()
    assert latest['id']==workflow['id'] and latest['run_id']==r['id']


def test_rejected_manual_input_blocks_workflow_without_creating_run(client,scope):
    response=submit(client,scope,scenario='rejected')
    assert response.status_code==202,response.text
    workflow=response.json()
    result=wait_for(lambda:client.get('/workflows/'+workflow['id']).json(),lambda w:w['status']=='BLOCKED')
    assert result['run_id'] is None and 'Broker' in result['error'] and 'rejected' in result['error']
    assert client.get('/cases',params={'scope_id':scope}).json()['total']==0
    with transaction() as c:
        assert query(c,'SELECT count(*) FROM runs WHERE scope_id=:id',id=scope).scalar()==0


def test_manual_upload_requires_both_inputs_and_completeness_declaration(client,scope):
    assert submit(client,scope,complete=False).status_code==422
    only_internal=client.post('/workflows',files={'internal_file':('internal.csv',fixture('INTERNAL')[0])},
        data={'scope_id':scope,'complete':'true'},headers={'Idempotency-Key':uid()})
    assert only_internal.status_code==422
    swapped=client.post('/workflows',files={'internal_file':('broker.csv',fixture('BROKER')[0]),
        'broker_file':('internal.csv',fixture('INTERNAL')[0])},data={'scope_id':scope,'complete':'true'},headers={'Idempotency-Key':uid()})
    assert swapped.status_code==422 and 'Internal CSV headers' in swapped.text
    with transaction() as c:
        assert query(c,'SELECT count(*) FROM batches WHERE scope_id=:id',id=scope).scalar()==0


def test_manual_complete_empty_broker_is_valid(client,scope):
    response=submit(client,scope,scenario='empty')
    assert response.status_code==202,response.text
    result=wait_for(lambda:client.get('/workflows/'+response.json()['id']).json(),lambda w:w['status']=='SUCCEEDED')
    run=client.get('/runs/'+result['run_id']).json()
    assert run['metrics']['missing']==60

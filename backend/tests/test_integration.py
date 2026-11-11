"""Run against live Compose PostgreSQL, RabbitMQ, dispatcher and worker processes."""
import csv
import hashlib
import io
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.exc import DBAPIError

from breakwater.db import transaction,query,one,many,uid,dumps
from breakwater.demo import fixture
from breakwater.domain import Manifest,INTERNAL,BROKER

BASE=os.getenv('TEST_API_URL','http://api:8000/api')


@pytest.fixture
def client():
    with httpx.Client(base_url=BASE,timeout=15) as c:
        yield c


@pytest.fixture
def scope():
    sid=uid()
    with transaction() as c:
        query(c,"""INSERT INTO scopes(id,broker_id,broker_name,business_date,dataset_id,account_ids,hidden)
          VALUES(:id,'BRK01','Integration test','2026-11-06',:id,'["ACC01","ACC02"]',true)""",id=sid)
        for side in ['INTERNAL','BROKER']:
            query(c,"INSERT INTO deliveries(scope_id,side,deadline) VALUES(:id,:side,'2026-11-06T22:00:00Z')",id=sid,side=side)
    return sid


def wait_for(fn,predicate,seconds=60):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        value=fn()
        if predicate(value):
            return value
        time.sleep(.3)
    raise AssertionError(f'Timed out; last observed value: {value}')


def upload(client,scope,side,scenario='breaks',revision=1,count=60,transform=None,key=None):
    data,manifest,_=fixture(side,scenario,count,revision,dataset=scope)
    if transform:
        data=transform(data)
        manifest.update(sha256=hashlib.sha256(data).hexdigest(),expected_bytes=len(data),expected_data_rows=len(list(csv.reader(io.StringIO(data.decode()))))-1)
    response=client.post('/uploads',files={'file':('test.csv',data,'text/csv')},data={'manifest':json.dumps(manifest)},headers={'Idempotency-Key':key or uid()})
    assert response.status_code==202,response.text
    return response.json(),data,manifest


def ready(client,b):
    result=wait_for(lambda:client.get('/batches/'+b['id']).json(),lambda x:x['status'] in ['READY','BLOCKED','FAILED'])
    assert result['status']=='READY',result
    return result


def run_pair(client,scope,a,b,buckets=8,**extra):
    response=client.post('/runs',json={'scope_id':scope,'internal_batch_id':a['id'],'broker_batch_id':b['id'],'bucket_count':buckets,**extra},headers={'Idempotency-Key':uid()})
    assert response.status_code==202,response.text
    return response.json()


def finished(client,r):
    value=wait_for(lambda:client.get('/runs/'+r['id']).json(),lambda x:x['status'] in ['SUCCEEDED','SUPERSEDED','FAILED'])
    assert value['status'] in ['SUCCEEDED','SUPERSEDED'],value
    return value


def test_end_to_end_history_concurrency_and_reports(client,scope):
    a,_,_=upload(client,scope,'INTERNAL')
    a=ready(client,a)
    blocked=client.post('/runs',json={'scope_id':scope,'internal_batch_id':a['id'],'broker_batch_id':a['id']},headers={'Idempotency-Key':uid()})
    assert blocked.status_code==409
    assert client.get('/cases',params={'scope_id':scope}).json()['total']==0
    b,data,manifest=upload(client,scope,'BROKER')
    b=ready(client,b)
    duplicate=client.post('/uploads',files={'file':('again.csv',data)},data={'manifest':json.dumps(manifest)},headers={'Idempotency-Key':uid()})
    assert duplicate.json()['id']==b['id']
    changed=client.post('/uploads',files={'file':('changed.csv',data+b'\n')},data={'manifest':json.dumps(manifest)},headers={'Idempotency-Key':uid()})
    assert changed.status_code==409
    r=finished(client,run_pair(client,scope,a,b,fail_bucket=1))
    assert r['metrics']['findings']==16
    assert r['metrics']['groups']==61
    assert int(r['metrics']['records'])==122
    assert r['metrics']['matched']==45
    cases=client.get('/cases',params={'scope_id':scope,'limit':100}).json()
    assert cases['total']==16
    mismatch=next(x for x in cases['items'] if x['family']=='FIELD_MISMATCH')
    payload={'expected_version':mismatch['version'],'actor':'Alice','action':'claim'}
    def claim(actor):
        return client.post(f'/cases/{mismatch["id"]}/actions',json={**payload,'actor':actor})
    with ThreadPoolExecutor(2) as pool:
        responses=list(pool.map(claim,['Alice','Bob']))
    assert sorted(x.status_code for x in responses)==[200,409]
    current=client.get('/cases/'+mismatch['id']).json()
    actor=current['assignee']
    note=client.post(f'/cases/{mismatch["id"]}/actions',json={'expected_version':current['version'],'actor':actor,'action':'note','reason':'Compared the broker execution record.'})
    assert note.status_code==200
    current=note.json()
    invalid_resolve=client.post(f'/cases/{current["id"]}/actions',json={'expected_version':current['version'],'actor':actor,'action':'resolve'})
    assert invalid_resolve.status_code==409
    parts=client.get(f'/runs/{r["id"]}/partitions').json()
    assert any('injected transient' in (a['error'] or '') for a in parts['attempts'])
    wid=parts['items'][0]['id']
    assert client.post('/demo/work/'+wid,json={'action':'redeliver'}).status_code==200
    with transaction() as c:
        assert query(c,'SELECT count(*) FROM comparison_groups WHERE run_id=:id',id=r['id']).scalar()==61
    with pytest.raises(DBAPIError):
        with transaction() as c:
            query(c,"UPDATE records SET quantity=1 WHERE batch_id=:id",id=a['id'])
    with pytest.raises(DBAPIError):
        with transaction() as c:
            query(c,"UPDATE batches SET status='PARSING' WHERE id=:id",id=a['id'])
    a2,_,_=upload(client,scope,'INTERNAL','corrected',2)
    b2,_,_=upload(client,scope,'BROKER','corrected',2)
    r2=finished(client,run_pair(client,scope,ready(client,a2),ready(client,b2)))
    assert r2['metrics']['findings']==0
    case=client.get('/cases/'+current['id']).json()
    assert case['detection']=='CLEARED' and case['workflow']=='RESOLVED'
    assert case['assignee']==actor and len(case['observations'])==2
    assert any(e['reason']=='Compared the broker execution record.' for e in case['events'])
    vanished=next(x for x in cases['items'] if x['reconciliation_ref']=='B9001')
    assert client.get('/cases/'+vanished['id']).json()['detection']=='NOT_PRESENT_IN_INPUTS'
    assert client.get('/runs/'+r['id']).json()['metrics']==r['metrics']
    delta=client.get(f'/runs/{r2["id"]}/diff',params={'previous_run_id':r['id']}).json()
    assert delta['counts']['CLEARED']==15 and delta['counts']['NOT_PRESENT_IN_INPUTS']==1
    report=wait_for(lambda:client.get(f'/runs/{r2["id"]}/report'),lambda x:x.status_code==200)
    assert r2['id'] in report.text and a2['sha256'] in report.text
    assert hashlib.sha256(report.content).hexdigest()==report.headers['x-artifact-sha256']


@pytest.mark.parametrize('scenario,code',[('corrupt','sha256'),('rejected','rejected')])
def test_bad_inputs_are_blocked(client,scope,scenario,code):
    b,_,_=upload(client,scope,'BROKER',scenario)
    result=wait_for(lambda:client.get('/batches/'+b['id']).json(),lambda x:x['status'] in ['BLOCKED','FAILED'])
    assert result['status']=='BLOCKED' and code in result['error']
    if scenario=='rejected':
        rej=client.get(f'/batches/{b["id"]}/rejections').json()['items']
        assert rej[0]['raw']['filled_qty']=='NaN'
        assert result['committed_accepted']==59


def test_empty_complete_inputs_and_idempotency(client,scope):
    a,_,_=upload(client,scope,'INTERNAL','corrected')
    b,_,_=upload(client,scope,'BROKER','empty')
    a,b=ready(client,a),ready(client,b)
    assert b['accepted']==0
    key=uid()
    payload={'scope_id':scope,'internal_batch_id':a['id'],'broker_batch_id':b['id'],'bucket_count':2}
    first=client.post('/runs',json=payload,headers={'Idempotency-Key':key})
    second=client.post('/runs',json=payload,headers={'Idempotency-Key':key})
    assert first.json()['id']==second.json()['id']
    assert client.post('/runs',json={**payload,'bucket_count':3},headers={'Idempotency-Key':key}).status_code==409
    r=finished(client,first.json())
    assert r['metrics']['missing']==60 and r['metrics']['findings']==60


def test_older_run_cannot_publish_over_newer(client,scope):
    a,_,_=upload(client,scope,'INTERNAL','corrected')
    b,_,_=upload(client,scope,'BROKER','corrected')
    a,b=ready(client,a),ready(client,b)
    old=run_pair(client,scope,a,b,buckets=2,pause_bucket=0)
    paused=wait_for(lambda:client.get(f'/runs/{old["id"]}/partitions').json()['items'],lambda ps:any(p['state']=='RUNNING' and p['pause'] for p in ps))
    new=finished(client,run_pair(client,scope,a,b,buckets=3))
    for p in paused:
        if p['pause']:
            client.post('/demo/work/'+p['id'],json={'action':'release'})
    old=finished(client,old)
    assert old['status']=='SUPERSEDED'
    with transaction() as c:
        assert one(c,'SELECT published_run_id FROM scopes WHERE id=:id',id=scope)['published_run_id']==new['id']


def test_record_order_and_bucket_count_do_not_change_results(client,scope):
    def reverse(data):
        rows=list(csv.reader(io.StringIO(data.decode())))
        out=io.StringIO(newline='');writer=csv.writer(out);writer.writerow(rows[0]);writer.writerows(reversed(rows[1:]));return out.getvalue().encode()
    a,_,_=upload(client,scope,'INTERNAL')
    b,_,_=upload(client,scope,'BROKER')
    r1=finished(client,run_pair(client,scope,ready(client,a),ready(client,b),buckets=1))
    a2,_,_=upload(client,scope,'INTERNAL',revision=2,transform=reverse)
    b2,_,_=upload(client,scope,'BROKER',revision=2,transform=reverse)
    r2=finished(client,run_pair(client,scope,ready(client,a2),ready(client,b2),buckets=7))
    with transaction() as c:
        def logical(r):
            return many(c,'SELECT reconciliation_ref,family,fingerprint FROM findings WHERE run_id=:id ORDER BY reconciliation_ref,family',id=r['id'])
        assert logical(r1)==logical(r2)
        assert query(c,'SELECT count(*) FROM assignments WHERE run_id=:id',id=r2['id']).scalar()==122


def test_fenced_old_worker_rolls_back_staged_effects():
    import threading
    from breakwater import jobs
    entered=threading.Event();release=threading.Event()
    wid,stale_effect,winning_effect=uid(),uid(),uid()
    def handler(c,w):
        query(c,"INSERT INTO rule_sets(id,config) VALUES(:id,'{}')",id=stale_effect if w['token']==1 else winning_effect)
        if w['token']==1:
            entered.set()
            assert release.wait(10)
        return {}
    jobs.HANDLERS['fence_probe']=handler
    with transaction() as c:
        query(c,"INSERT INTO work(id,kind,state) VALUES(:id,'fence_probe','QUEUED')",id=wid)
    worker=threading.Thread(target=jobs.execute,args=(wid,))
    worker.start()
    assert entered.wait(10)
    try:
        with transaction() as c:
            query(c,"UPDATE work SET state='QUEUED',token=token+1 WHERE id=:id",id=wid)
        jobs.execute(wid)
    finally:
        release.set();worker.join(10)
        jobs.HANDLERS.pop('fence_probe')
    assert not worker.is_alive()
    with transaction() as c:
        assert one(c,'SELECT * FROM rule_sets WHERE id=:id',id=stale_effect) is None
        assert one(c,'SELECT * FROM rule_sets WHERE id=:id',id=winning_effect) is not None
        assert one(c,'SELECT * FROM work WHERE id=:id',id=wid)['state']=='SUCCEEDED'


def test_failed_partition_preserves_published_cases(client,scope):
    a,_,_=upload(client,scope,'INTERNAL')
    b,_,_=upload(client,scope,'BROKER')
    a,b=ready(client,a),ready(client,b)
    old=finished(client,run_pair(client,scope,a,b))
    before=client.get('/cases',params={'scope_id':scope,'limit':100}).json()['items']
    def oversize(data):
        lines=data.splitlines(keepends=True)
        return data+lines[1]*101
    a2,_,_=upload(client,scope,'INTERNAL',revision=2,transform=oversize)
    failed=run_pair(client,scope,ready(client,a2),b)
    result=wait_for(lambda:client.get('/runs/'+failed['id']).json(),lambda x:x['status']=='FAILED')
    assert '100-row' in result['error']
    assert client.get('/cases',params={'scope_id':scope,'limit':100}).json()['items']==before
    with transaction() as c:
        assert one(c,'SELECT published_run_id FROM scopes WHERE id=:id',id=scope)['published_run_id']==old['id']


def test_sql_results_agree_with_independent_reference(client,scope):
    a,_,_=upload(client,scope,'INTERNAL')
    b,_,_=upload(client,scope,'BROKER')
    a,b=ready(client,a),ready(client,b)
    r=finished(client,run_pair(client,scope,a,b,buckets=5))
    with transaction() as c:
        sides=[]
        for batch in [a,b]:
            groups={}
            for record in many(c,'SELECT normalized FROM records WHERE batch_id=:id',id=batch['id']):
                row=record['normalized'];groups.setdefault(row['reconciliation_ref'],[]).append(row)
            sides.append(groups)
        expected={}
        for ref in set(sides[0])|set(sides[1]):
            left,right=sides[0].get(ref,[]),sides[1].get(ref,[])
            if len(left)>1 or len(right)>1:
                outcome='DUPLICATE'
            elif not left or not right:
                outcome='MISSING'
            else:
                x,y=left[0],right[0]
                categorical=any(x[f]!=y[f] for f in ['account','instrument','side','currency'])
                quantity=Decimal(x['quantity'])!=Decimal(y['quantity'])
                price=abs(Decimal(x['execution_price'])-Decimal(y['execution_price']))>Decimal('.01')
                outcome='MISMATCH' if categorical or quantity or price else 'MATCHED'
            expected[ref]=outcome
        actual={x['reconciliation_ref']:x['outcome'] for x in many(c,'SELECT reconciliation_ref,outcome FROM comparison_groups WHERE run_id=:id',id=r['id'])}
        assert actual==expected


def test_outbox_send_failure_and_confirm_gap_are_safe(monkeypatch):
    from breakwater import dispatcher
    wid,oid=uid(),uid()
    with transaction() as c:
        # Completed durable work lets us exercise repeated real broker delivery safely.
        query(c,"INSERT INTO work(id,kind,state,metrics) VALUES(:id,'probe','SUCCEEDED',CAST(:metrics AS jsonb))",id=wid,metrics=dumps({'groups':7}))
        query(c,'INSERT INTO outbox(id,work_id) VALUES(:id,:work)',id=oid,work=wid)
    sender=dispatcher.celery.send_task
    def unavailable(*args,**kwargs):
        raise ConnectionError('Injected broker connection loss')
    monkeypatch.setattr(dispatcher.celery,'send_task',unavailable)
    with transaction() as c:
        assert dispatcher.publish_event(c,one(c,'SELECT * FROM outbox WHERE id=:id FOR UPDATE',id=oid)) is False
    with transaction() as c:
        o=one(c,'SELECT * FROM outbox WHERE id=:id',id=oid)
        assert o['sent_at'] is None and o['attempts']==1
    monkeypatch.setattr(dispatcher.celery,'send_task',sender)
    # Confirmed RabbitMQ send followed by DB rollback: consumer may see work twice.
    try:
        with transaction() as c:
            assert dispatcher.publish_event(c,one(c,'SELECT * FROM outbox WHERE id=:id FOR UPDATE',id=oid))
            raise RuntimeError('Crash after broker confirmation, before commit')
    except RuntimeError:
        pass
    with transaction() as c:
        o=one(c,'SELECT * FROM outbox WHERE id=:id FOR UPDATE',id=oid)
        assert o['sent_at'] is None
        assert dispatcher.publish_event(c,o)
    time.sleep(.3)
    with transaction() as c:
        work=one(c,'SELECT * FROM work WHERE id=:id',id=wid)
        assert work['metrics']=={'groups':7} and work['attempts']==0
        assert one(c,'SELECT * FROM outbox WHERE id=:id',id=oid)['sent_at'] is not None

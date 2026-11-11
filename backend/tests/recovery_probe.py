"""Helper for scripts/verify-recovery.py. The host terminates the actual worker."""
import json
import sys
import time
from pathlib import Path
import httpx
from breakwater.db import transaction,query,one,many,uid
from test_integration import upload,ready,run_pair,finished,wait_for

state=Path('/tmp/breakwater-recovery.json')
with httpx.Client(base_url='http://api:8000/api',timeout=15) as client:
    if sys.argv[1]=='prepare':
        sid=uid()
        with transaction() as c:
            query(c,"""INSERT INTO scopes(id,broker_id,broker_name,business_date,dataset_id,account_ids,hidden)
              VALUES(:id,'BRK01','Worker recovery test','2026-11-06',:id,'["ACC01","ACC02"]',true)""",id=sid)
        a,_,_=upload(client,sid,'INTERNAL',count=1000)
        b,_,_=upload(client,sid,'BROKER',count=1000)
        r=run_pair(client,sid,ready(client,a),ready(client,b),buckets=8,pause_bucket=0)
        parts=wait_for(lambda:client.get(f'/runs/{r["id"]}/partitions').json()['items'],lambda ps:any(p['pause'] and p['state']=='RUNNING' for p in ps))
        p=next(p for p in parts if p['pause'])
        state.write_text(json.dumps({'scope':sid,'run':r,'work':p['id'],'token':p['token']}))
        print(json.dumps({'run_id':r['id'],'paused_partition':p['id'],'initial_token':p['token']}))
    else:
        s=json.loads(state.read_text())
        parts=wait_for(lambda:client.get(f'/runs/{s["run"]["id"]}/partitions').json()['items'],lambda ps:any(p['id']==s['work'] and p['token']>s['token'] and p['state']=='RUNNING' for p in ps),seconds=90)
        client.post('/demo/work/'+s['work'],json={'action':'release'}).raise_for_status()
        r=finished(client,s['run'])
        assert r['metrics']['findings']==16
        assert r['metrics']['groups']==1001
        assert r['metrics']['records']==2002
        with transaction() as c:
            attempts=many(c,'SELECT token,state,error FROM attempts WHERE work_id=:id ORDER BY token',id=s['work'])
            assert any(a['state']=='EXPIRED' for a in attempts),attempts
            counts=one(c,'SELECT count(*) AS n,count(DISTINCT reconciliation_ref) AS unique_n FROM comparison_groups WHERE run_id=:id',id=r['id'])
            assert counts['n']==counts['unique_n']==1001
            before=query(c,'SELECT count(*) FROM case_events e JOIN cases c ON e.case_id=c.id WHERE c.scope_id=:id',id=s['scope']).scalar()
        # Real RabbitMQ redelivery after commit (the same safety condition as commit-before-ack crash).
        client.post('/demo/work/'+s['work'],json={'action':'redeliver'}).raise_for_status()
        time.sleep(2)
        with transaction() as c:
            after=query(c,'SELECT count(*) FROM case_events e JOIN cases c ON e.case_id=c.id WHERE c.scope_id=:id',id=s['scope']).scalar()
            assert before==after
        print(json.dumps({'result':'PASS','run_id':r['id'],'metrics':r['metrics'],'attempts':attempts,'case_events_before_redelivery':before,'case_events_after_redelivery':after},indent=2))


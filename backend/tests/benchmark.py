"""Measurement harness; emits observed timings rather than target numbers."""
import argparse
import json
import time
import statistics
import httpx
from breakwater.db import transaction,query,one,many,uid
from test_integration import upload,ready,run_pair,finished,wait_for

p=argparse.ArgumentParser()
p.add_argument('--rows',type=int,default=10000)
p.add_argument('--buckets',type=int,default=8)
args=p.parse_args()
sid=uid()
with transaction() as c:
    query(c,"""INSERT INTO scopes(id,broker_id,broker_name,business_date,dataset_id,account_ids,hidden)
      VALUES(:id,'BRK01','Benchmark','2026-11-06',:id,'["ACC01","ACC02"]',true)""",id=sid)
with httpx.Client(base_url='http://api:8000/api',timeout=60) as client:
    start=time.monotonic()
    a,_,_=upload(client,sid,'INTERNAL',count=args.rows)
    b,_,_=upload(client,sid,'BROKER',count=args.rows)
    registered=time.monotonic()
    for batch in [a,b]:
        result=wait_for(lambda:client.get('/batches/'+batch['id']).json(),lambda x:x['status'] in ['READY','BLOCKED','FAILED'],seconds=300)
        assert result['status']=='READY',result
    ingested=time.monotonic()
    r=run_pair(client,sid,a,b,buckets=args.buckets)
    latencies=[]
    peak_connections=0
    deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        tick=time.monotonic()
        r=client.get('/runs/'+r['id']).json()
        latencies.append((time.monotonic()-tick)*1000)
        with transaction() as c:
            peak_connections=max(peak_connections,query(c,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()").scalar())
        if r['status'] in ['SUCCEEDED','FAILED']:
            break
        time.sleep(.5)
    assert r['status']=='SUCCEEDED',r
    complete=time.monotonic()
    report=wait_for(lambda:client.get('/runs/'+r['id']).json(),lambda x:x['report_status'] in ['READY','FAILED'],seconds=120)
    reported=time.monotonic()
    with transaction() as c:
        stages=many(c,"""SELECT w.kind,count(*) AS tasks,round(sum(extract(epoch FROM(a.finished_at-a.started_at)))::numeric,3) AS worker_seconds
          FROM work w JOIN attempts a ON a.work_id=w.id WHERE (w.run_id=:r OR w.batch_id IN (:i,:b)) AND a.state='SUCCEEDED' GROUP BY w.kind ORDER BY w.kind""",r=r['id'],i=a['id'],b=b['id'])
    print(json.dumps({'rows_per_baseline_side':args.rows,'bucket_count':args.buckets,'run_id':r['id'],
        'upload_seconds':round(registered-start,3),'ingestion_after_upload_seconds':round(ingested-registered,3),
        'reconciliation_seconds':round(complete-ingested,3),'report_after_publication_seconds':round(reported-complete,3),
        'observed_api_read_median_ms':round(statistics.median(latencies),3),'observed_api_read_max_ms':round(max(latencies),3),
        'sampled_peak_db_connections':peak_connections,'metrics':r['metrics'],'report_status':report['report_status'],'stages':stages},indent=2,default=str))

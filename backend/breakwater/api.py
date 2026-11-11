import io
import json
import os
from typing import Literal

from fastapi import FastAPI,UploadFile,File,Form,Header,HTTPException,Query
from fastapi.responses import FileResponse
from pydantic import BaseModel,Field,ValidationError

from breakwater.db import transaction,query,one,many,uid,dumps
from breakwater.domain import Manifest
from breakwater.service import require,conflict,register_upload,start_run,case_action
from breakwater import storage

app=FastAPI(title='Breakwater',version='0.1.0',description='Explainable end-of-day execution reconciliation. Local demo identities; not production authentication.')
DEMO=os.getenv('DEMO_MODE','false').lower()=='true'


def demo_only():
    if not DEMO:
        raise HTTPException(403,'Demo controls are disabled')


@app.get('/api/health')
def health():
    with transaction() as c:
        query(c,'SELECT 1')
    return {'status':'ok','product':'breakwater','demo_mode':DEMO}


@app.get('/api/scopes')
def scopes():
    with transaction() as c:
        return many(c,"""SELECT s.*,r.metrics,r.status AS published_status,r.sequence AS published_sequence
            FROM scopes s LEFT JOIN runs r ON r.id=s.published_run_id WHERE NOT s.hidden ORDER BY s.business_date DESC,s.id""")


@app.get('/api/scopes/{sid}/deliveries')
def deliveries(sid:str):
    with transaction() as c:
        scope=require(one(c,'SELECT * FROM scopes WHERE id=:id',id=sid),'Scope')
        ds=many(c,"SELECT *,CASE WHEN arrived_at IS NOT NULL THEN 'ARRIVED' WHEN deadline<now() THEN 'OVERDUE' ELSE 'EXPECTED' END AS state FROM deliveries WHERE scope_id=:id ORDER BY side DESC",id=sid)
        batches=many(c,'SELECT * FROM batches WHERE scope_id=:id ORDER BY revision DESC,created_at DESC',id=sid)
        for d in ds:
            d['batches']=[b for b in batches if b['side']==d['side']]
        return {'scope':scope,'deliveries':ds}


@app.post('/api/uploads',status_code=202)
def upload(file:UploadFile=File(...),manifest:str=Form(...),idempotency_key:str=Header(...)):
    try:
        m=Manifest.model_validate_json(manifest)
    except ValidationError as e:
        raise HTTPException(422,json.loads(e.json())) from None
    try:
        key,size=storage.store_stream(file.file)
    except ValueError as e:
        raise HTTPException(413,str(e)) from None
    return register_upload(m,key,size,file.filename,idempotency_key)


@app.get('/api/batches/{bid}')
def batch(bid:str):
    with transaction() as c:
        b=require(one(c,'SELECT * FROM batches WHERE id=:id',id=bid),'Batch')
        b['work']=many(c,'SELECT * FROM work WHERE batch_id=:id ORDER BY created_at,ordinal',id=bid)
        b['committed_accepted']=query(c,'SELECT count(*) FROM records WHERE batch_id=:id',id=bid).scalar()
        b['committed_rejected']=query(c,'SELECT count(*) FROM rejections WHERE batch_id=:id',id=bid).scalar()
        return b


@app.post('/api/workflows',status_code=202)
def upload_workflow(internal_file:UploadFile=File(...),broker_file:UploadFile=File(...),
                    scope_id:str=Form(...),complete:bool=Form(False),idempotency_key:str=Header(...)):
    from breakwater.workflows import create_workflow,declared_row_count
    if not complete:
        raise HTTPException(422,'Confirm that both files are complete replacement snapshots for the selected scope')
    files={}
    for side,file in [('INTERNAL',internal_file),('BROKER',broker_file)]:
        try:
            key,size=storage.store_stream(file.file)
        except ValueError as e:
            raise HTTPException(413,str(e)) from None
        files[side]={'key':key,'size':size,'filename':file.filename,'rows':declared_row_count(key,side)}
    return create_workflow(scope_id,files,complete,idempotency_key)


@app.get('/api/workflows')
def latest_workflow(scope_id:str):
    from breakwater.workflows import WORKFLOW_QUERY
    with transaction() as c:
        return one(c,WORKFLOW_QUERY+' WHERE w.scope_id=:scope ORDER BY w.created_at DESC,w.id LIMIT 1',scope=scope_id)


@app.get('/api/workflows/{wid}')
def workflow(wid:str):
    from breakwater.workflows import WORKFLOW_QUERY
    with transaction() as c:
        return require(one(c,WORKFLOW_QUERY+' WHERE w.id=:id',id=wid),'Workflow')


@app.get('/api/batches/{bid}/rejections')
def rejections(bid:str,offset:int=Query(0,ge=0),limit:int=Query(25,ge=1,le=100)):
    with transaction() as c:
        require(one(c,'SELECT id FROM batches WHERE id=:id',id=bid),'Batch')
        return {'items':many(c,'SELECT * FROM rejections WHERE batch_id=:id ORDER BY record_number LIMIT :limit OFFSET :offset',id=bid,limit=limit,offset=offset),
                'total':query(c,'SELECT count(*) FROM rejections WHERE batch_id=:id',id=bid).scalar()}


@app.get('/api/batches/{bid}/source')
def source(bid:str):
    with transaction() as c:
        b=require(one(c,'SELECT * FROM batches WHERE id=:id',id=bid),'Batch')
    return FileResponse(storage.path(b['object_key']),media_type='text/plain',filename=b['filename'])


class RunRequest(BaseModel):
    scope_id:str
    internal_batch_id:str
    broker_batch_id:str
    rule_version:Literal['execution-v1']='execution-v1'
    bucket_count:int=Field(default=8,ge=1,le=32)
    pause_bucket:int|None=Field(default=None,ge=0,le=31)
    fail_bucket:int|None=Field(default=None,ge=0,le=31)


@app.post('/api/runs',status_code=202)
def create_run(data:RunRequest,idempotency_key:str=Header(...)):
    if data.pause_bucket is not None or data.fail_bucket is not None:
        demo_only()
        if any(x is not None and x>=data.bucket_count for x in [data.pause_bucket,data.fail_bucket]):
            raise HTTPException(422,'Demo bucket must be less than bucket count')
    return start_run(data.model_dump(),idempotency_key)


@app.get('/api/runs')
def runs(scope_id:str='demo-scope',limit:int=Query(50,ge=1,le=100)):
    with transaction() as c:
        return many(c,'SELECT * FROM runs WHERE scope_id=:scope ORDER BY sequence DESC LIMIT :limit',scope=scope_id,limit=limit)


@app.get('/api/runs/{rid}')
def run(rid:str):
    with transaction() as c:
        r=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
        r['inputs']=many(c,'SELECT * FROM batches WHERE id IN (:i,:b) ORDER BY side DESC',i=r['internal_batch_id'],b=r['broker_batch_id'])
        r['partitions']=many(c,"SELECT * FROM work WHERE run_id=:id AND kind='partition' ORDER BY ordinal",id=rid)
        r['work']=many(c,'SELECT * FROM work WHERE run_id=:id ORDER BY created_at,ordinal',id=rid)
        return r


@app.get('/api/runs/{rid}/partitions')
def partitions(rid:str):
    with transaction() as c:
        return {'items':many(c,"SELECT * FROM work WHERE run_id=:id AND kind='partition' ORDER BY ordinal",id=rid),
            'attempts':many(c,'SELECT a.* FROM attempts a JOIN work w ON w.id=a.work_id WHERE w.run_id=:id ORDER BY a.started_at',id=rid)}


@app.get('/api/runs/{rid}/results')
def results(rid:str,outcome:str|None=None,offset:int=Query(0,ge=0),limit:int=Query(25,ge=1,le=100)):
    with transaction() as c:
        r=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
        where='run_id=:id'+(' AND outcome=:outcome' if outcome else '')
        params={'id':rid,'outcome':outcome}
        return {'provisional':r['status'] not in ['SUCCEEDED','SUPERSEDED'],
                'items':many(c,f'SELECT * FROM comparison_groups WHERE {where} ORDER BY reconciliation_ref LIMIT :limit OFFSET :offset',**params,limit=limit,offset=offset),
                'total':query(c,f'SELECT count(*) FROM comparison_groups WHERE {where}',**params).scalar()}


@app.get('/api/runs/{rid}/diff')
def diff(rid:str,previous_run_id:str):
    with transaction() as c:
        a=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
        b=require(one(c,'SELECT * FROM runs WHERE id=:id',id=previous_run_id),'Previous run')
        if a['scope_id']!=b['scope_id'] or any(r['status'] not in ['SUCCEEDED','SUPERSEDED'] for r in [a,b]):
            conflict('Compare completed runs from the same scope')
        rows=many(c,"""WITH a AS (SELECT * FROM findings WHERE run_id=:a), b AS (SELECT * FROM findings WHERE run_id=:b)
            SELECT coalesce(a.reconciliation_ref,b.reconciliation_ref) AS reference,coalesce(a.family,b.family) AS family,
            CASE WHEN b.id IS NULL THEN 'NEW' WHEN a.id IS NOT NULL THEN 'PERSISTED'
              WHEN EXISTS(SELECT 1 FROM comparison_groups g WHERE g.run_id=:a AND g.reconciliation_ref=b.reconciliation_ref AND g.outcome='MATCHED') THEN 'CLEARED'
              WHEN EXISTS(SELECT 1 FROM comparison_groups g WHERE g.run_id=:a AND g.reconciliation_ref=b.reconciliation_ref) THEN 'UNASSESSABLE'
              ELSE 'NOT_PRESENT_IN_INPUTS' END AS change
            FROM a FULL OUTER JOIN b ON a.reconciliation_ref=b.reconciliation_ref AND a.family=b.family
            ORDER BY reference,family""",a=rid,b=previous_run_id)
        return {'items':rows,'counts':{k:sum(x['change']==k for x in rows) for k in ['NEW','PERSISTED','CLEARED','NOT_PRESENT_IN_INPUTS','UNASSESSABLE']}}


@app.get('/api/runs/{rid}/report')
def report(rid:str):
    with transaction() as c:
        r=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
    if r['report_status']!='READY':
        conflict('Report is not ready',report_status=r['report_status'])
    return FileResponse(storage.path(r['report_key']),media_type='text/csv',filename=f'breakwater-run-{r["sequence"]}.csv',headers={'X-Artifact-SHA256':r['report_sha256']})


@app.get('/api/cases')
def cases(scope_id:str='demo-scope',q:str='',family:str='',workflow:str='',detection:str='',assignee:str='',account:str='',instrument:str='',
          offset:int=Query(0,ge=0),limit:int=Query(25,ge=1,le=100)):
    where=['scope_id=:scope']
    params={'scope':scope_id,'q':f'%{q}%','family':family,'workflow':workflow,'detection':detection,'assignee':assignee,'account':account,'instrument':instrument}
    for name,value in [('family',family),('workflow',workflow),('detection',detection),('assignee',assignee)]:
        if value:
            where.append(f'{name}=:{name}')
    if q:
        where.append('(reconciliation_ref ILIKE :q OR id ILIKE :q)')
    for field,value in [('account',account),('instrument',instrument)]:
        if value:
            where.append(f"EXISTS(SELECT 1 FROM jsonb_array_elements((evidence->'internal') || (evidence->'broker')) r WHERE r->'normalized'->>'{field}'=:{field})")
    sql=' AND '.join(where)
    with transaction() as c:
        return {'items':many(c,f'SELECT * FROM cases WHERE {sql} ORDER BY updated_at DESC,id LIMIT :limit OFFSET :offset',**params,limit=limit,offset=offset),
            'total':query(c,f'SELECT count(*) FROM cases WHERE {sql}',**params).scalar(),
            'summary':many(c,'SELECT detection,workflow,count(*) AS count FROM cases WHERE scope_id=:scope GROUP BY detection,workflow',scope=scope_id)}


@app.get('/api/cases/{cid}')
def case(cid:str):
    with transaction() as c:
        r=require(one(c,'SELECT * FROM cases WHERE id=:id',id=cid),'Case')
        r['events']=many(c,'SELECT * FROM case_events WHERE case_id=:id ORDER BY created_at,id',id=cid)
        r['observations']=many(c,'SELECT o.*,r.sequence FROM observations o JOIN runs r ON r.id=o.run_id WHERE case_id=:id ORDER BY r.sequence DESC',id=cid)
        return r


class CaseAction(BaseModel):
    expected_version:int=Field(ge=1)
    actor:Literal['Alice','Bob']
    action:Literal['claim','note','waive','resolve','review']
    reason:str=Field(default='',max_length=4000)


@app.post('/api/cases/{cid}/actions')
def action(cid:str,data:CaseAction):
    demo_only()
    return case_action(cid,data.model_dump())


class DemoRequest(BaseModel):
    scope_id:str='demo-scope'
    rows:int=Field(default=240,ge=60,le=100000)


@app.post('/api/demo/scenarios/{name}',status_code=202)
def demo(name:Literal['breaks','corrected','missing','empty','corrupt','rejected'],data:DemoRequest):
    demo_only()
    from breakwater.demo import fixture
    with transaction() as c:
        scope=require(one(c,'SELECT * FROM scopes WHERE id=:id',id=data.scope_id),'Scope')
    uploaded=[]
    for side in ['INTERNAL','BROKER']:
        if name=='missing' and side=='BROKER':
            continue
        with transaction() as c:
            rev=query(c,'SELECT coalesce(max(revision),0)+1 FROM batches WHERE scope_id=:id AND side=:side',id=scope['id'],side=side).scalar()
        content,manifest,_=fixture(side,name,data.rows,rev,str(scope['business_date']),scope['dataset_id'])
        key,size=storage.store_stream(io.BytesIO(content))
        uploaded.append(register_upload(Manifest(**manifest),key,size,f'{name}-{side.lower()}-r{rev}.csv',uid()))
    return {'batches':uploaded}


class WorkAction(BaseModel):
    action:Literal['pause','release','redeliver','retry','fail_once']


@app.post('/api/demo/work/{wid}')
def work_action(wid:str,data:WorkAction):
    demo_only()
    with transaction() as c:
        w=require(one(c,'SELECT * FROM work WHERE id=:id FOR UPDATE',id=wid),'Work')
        if data.action in ['pause','release']:
            query(c,'UPDATE work SET pause=:pause WHERE id=:id',id=wid,pause=data.action=='pause')
        elif data.action=='fail_once':
            if w['state'] not in ['PENDING','QUEUED','RETRYING']:
                conflict('Inject a failure before the work starts')
            query(c,'UPDATE work SET fail_remaining=1 WHERE id=:id',id=wid)
        elif data.action=='redeliver':
            # Intentionally dispatch even completed work; execute() must have no logical effect.
            from breakwater.jobs import celery
            celery.send_task('breakwater.execute',args=[wid],retry=False)
        elif data.action=='retry':
            if w['state']!='FAILED':
                conflict('Only failed work can be retried')
            if w['kind']!='report':
                conflict('For failed inputs upload a new revision; for failed runs start a new pinned run')
            query(c,"UPDATE work SET state='PENDING',attempts=0,error=NULL WHERE id=:id",id=wid)
            query(c,"UPDATE runs SET report_status='PENDING' WHERE id=:id",id=w['run_id'])
            query(c,'INSERT INTO outbox(id,work_id) VALUES(:id,:work)',id=uid(),work=wid)
    return {'ok':True}


@app.get('/api/operations')
def operations():
    with transaction() as c:
        return {'work':many(c,'SELECT kind,state,count(*) AS count FROM work GROUP BY kind,state ORDER BY kind,state'),
            'pending_dispatch':query(c,"SELECT count(*) FROM outbox o JOIN work w ON w.id=o.work_id WHERE o.sent_at IS NULL AND w.state IN ('PENDING','QUEUED','RETRYING')").scalar(),
            'recent_attempts':many(c,'SELECT a.*,w.kind,w.ordinal,w.run_id FROM attempts a JOIN work w ON w.id=a.work_id ORDER BY a.started_at DESC LIMIT 30'),
            'demo_mode':DEMO}

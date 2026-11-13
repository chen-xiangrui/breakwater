# HTTP endpoints validate requests, call services, and expose stored processing results.
import io
import json
import os
from datetime import date
from typing import Literal

from fastapi import FastAPI,UploadFile,File,Form,Header,HTTPException,Query,Response
from fastapi.responses import FileResponse
from pydantic import BaseModel,Field,ValidationError

from breakwater.db import transaction,query,one,many,uid,dumps
from breakwater.domain import Manifest
from breakwater.service import require,conflict,register_upload,start_run,case_action
from breakwater import storage
from breakwater.scopes import ScopeCreate,ScopeResponse,create_scope

# Register API documentation and read the switch for demo-only mutation controls.
app=FastAPI(title='Breakwater',version='0.1.0',description='Explainable end-of-day execution reconciliation. Local demo identities; not production authentication.')
DEMO=os.getenv('DEMO_MODE','false').lower()=='true'


def demo_only():
    # Stop fixture generation, failure injection, and demo identity writes outside demo mode.
    if not DEMO:
        raise HTTPException(403,'Demo controls are disabled')


@app.get('/api/health')
def health():
    # Check database connectivity and expose the product/demo status to the frontend.
    with transaction() as c:
        query(c,'SELECT 1')
    return {'status':'ok','product':'breakwater','demo_mode':DEMO}


@app.get('/api/scopes')
def scopes(business_date:date|None=None,broker_id:str|None=None,dataset_id:str|None=None):
    # List visible scope metadata with optional filters and each scope's published run summary.
    with transaction() as c:
        return many(c,"""SELECT s.*,r.metrics,r.status AS published_status,r.sequence AS published_sequence
            FROM scopes s LEFT JOIN runs r ON r.id=s.published_run_id WHERE NOT s.hidden
            AND (CAST(:day AS date) IS NULL OR s.business_date=CAST(:day AS date))
            AND (CAST(:broker AS text) IS NULL OR s.broker_id=:broker)
            AND (CAST(:dataset AS text) IS NULL OR s.dataset_id=:dataset)
            ORDER BY s.business_date DESC,s.id""",day=business_date,broker=broker_id,dataset=dataset_id)


@app.post('/api/scopes',status_code=201,response_model=ScopeResponse,responses={200:{'model':ScopeResponse,'description':'Existing scope'}})
def register_scope(data:ScopeCreate,response:Response,idempotency_key:str=Header(...)):
    # Create expected daily deliveries, or return an existing matching scope on a retry.
    scope,created=create_scope(data,idempotency_key)
    if not created:
        response.status_code=200
    return scope


@app.get('/api/scopes/{sid}/deliveries')
def deliveries(sid:str):
    # Load expected deliveries and derive arrival/overdue status from timestamps.
    with transaction() as c:
        scope=require(one(c,'SELECT * FROM scopes WHERE id=:id',id=sid),'Scope')
        ds=many(c,"SELECT *,CASE WHEN arrived_at IS NOT NULL THEN 'ARRIVED' WHEN deadline<now() THEN 'OVERDUE' ELSE 'EXPECTED' END AS state FROM deliveries WHERE scope_id=:id ORDER BY side DESC",id=sid)
        batches=many(c,'SELECT * FROM batches WHERE scope_id=:id ORDER BY revision DESC,created_at DESC',id=sid)
        # Attach each source's uploaded file revisions to its expected delivery entry.
        for d in ds:
            d['batches']=[b for b in batches if b['side']==d['side']]
        return {'scope':scope,'deliveries':ds}


@app.post('/api/uploads',status_code=202)
def upload(file:UploadFile=File(...),manifest:str=Form(...),idempotency_key:str=Header(...)):
    # Validate the producer-supplied manifest before registering the uploaded file.
    try:
        m=Manifest.model_validate_json(manifest)
    except ValidationError as e:
        raise HTTPException(422,json.loads(e.json())) from None
    # Stream file bytes into shared storage and report files that exceed the upload limit.
    try:
        key,size=storage.store_stream(file.file)
    except ValueError as e:
        raise HTTPException(413,str(e)) from None
    # The service stores batch metadata and schedules parsing when initial checks pass.
    return register_upload(m,key,size,file.filename,idempotency_key)


@app.get('/api/batches/{bid}')
def batch(bid:str):
    # Return batch metadata, task progress, and counts of rows already committed by workers.
    with transaction() as c:
        b=require(one(c,'SELECT * FROM batches WHERE id=:id',id=bid),'Batch')
        b['work']=many(c,'SELECT * FROM work WHERE batch_id=:id ORDER BY created_at,ordinal',id=bid)
        b['committed_accepted']=query(c,'SELECT count(*) FROM records WHERE batch_id=:id',id=bid).scalar()
        b['committed_rejected']=query(c,'SELECT count(*) FROM rejections WHERE batch_id=:id',id=bid).scalar()
        return b


@app.post('/api/workflows',status_code=202)
def upload_workflow(internal_file:UploadFile=File(...),broker_file:UploadFile=File(...),
                    scope_id:str=Form(...),complete:bool=Form(False),idempotency_key:str=Header(...)):
    # Accept a CSV pair and the uploader's declaration that both snapshots are complete.
    from breakwater.workflows import create_workflow,declared_row_count
    if not complete:
        raise HTTPException(422,'Confirm that both files are complete replacement snapshots for the selected scope')
    files={}
    for side,file in [('INTERNAL',internal_file),('BROKER',broker_file)]:
        # Save each source and calculate file controls used by its generated manifest.
        try:
            key,size=storage.store_stream(file.file)
        except ValueError as e:
            raise HTTPException(413,str(e)) from None
        files[side]={'key':key,'size':size,'filename':file.filename,'rows':declared_row_count(key,side)}
    # Register both inputs together; the dispatcher starts a run after they become READY.
    return create_workflow(scope_id,files,complete,idempotency_key)


@app.get('/api/workflows')
def latest_workflow(scope_id:str):
    # Support frontend polling of the latest upload-to-reconciliation workflow for a scope.
    from breakwater.workflows import WORKFLOW_QUERY
    with transaction() as c:
        return one(c,WORKFLOW_QUERY+' WHERE w.scope_id=:scope ORDER BY w.created_at DESC,w.id LIMIT 1',scope=scope_id)


@app.get('/api/workflows/{wid}')
def workflow(wid:str):
    # Return joined input/run progress for one workflow, or a 404 if it does not exist.
    from breakwater.workflows import WORKFLOW_QUERY
    with transaction() as c:
        return require(one(c,WORKFLOW_QUERY+' WHERE w.id=:id',id=wid),'Workflow')


@app.get('/api/batches/{bid}/rejections')
def rejections(bid:str,offset:int=Query(0,ge=0),limit:int=Query(25,ge=1,le=100)):
    # Page through invalid source rows and their validation errors.
    with transaction() as c:
        require(one(c,'SELECT id FROM batches WHERE id=:id',id=bid),'Batch')
        return {'items':many(c,'SELECT * FROM rejections WHERE batch_id=:id ORDER BY record_number LIMIT :limit OFFSET :offset',id=bid,limit=limit,offset=offset),
                'total':query(c,'SELECT count(*) FROM rejections WHERE batch_id=:id',id=bid).scalar()}


@app.get('/api/batches/{bid}/source')
def source(bid:str):
    # Look up the original upload's storage key and stream its bytes as a download.
    with transaction() as c:
        b=require(one(c,'SELECT * FROM batches WHERE id=:id',id=bid),'Batch')
    return FileResponse(storage.path(b['object_key']),media_type='text/plain',filename=b['filename'])


# Pin the scope, source revisions, comparison rules, and bucket count for a new run.
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
    # Restrict deliberate pauses/failures to demo mode and require valid bucket indices.
    if data.pause_bucket is not None or data.fail_bucket is not None:
        demo_only()
        if any(x is not None and x>=data.bucket_count for x in [data.pause_bucket,data.fail_bucket]):
            raise HTTPException(422,'Demo bucket must be less than bucket count')
    # Register a durable run and its first prepare task; processing continues asynchronously.
    return start_run(data.model_dump(),idempotency_key)


@app.get('/api/runs')
def runs(scope_id:str='demo-scope',limit:int=Query(50,ge=1,le=100)):
    # Return the newest run sequences for the selected scope.
    with transaction() as c:
        return many(c,'SELECT * FROM runs WHERE scope_id=:scope ORDER BY sequence DESC LIMIT :limit',scope=scope_id,limit=limit)


@app.get('/api/runs/{rid}')
def run(rid:str):
    # Combine run metadata with its pinned input batches and processing tasks.
    with transaction() as c:
        r=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
        r['inputs']=many(c,'SELECT * FROM batches WHERE id IN (:i,:b) ORDER BY side DESC',i=r['internal_batch_id'],b=r['broker_batch_id'])
        r['partitions']=many(c,"SELECT * FROM work WHERE run_id=:id AND kind='partition' ORDER BY ordinal",id=rid)
        r['work']=many(c,'SELECT * FROM work WHERE run_id=:id ORDER BY created_at,ordinal',id=rid)
        return r


@app.get('/api/runs/{rid}/partitions')
def partitions(rid:str):
    # Expose bucket task states and the attempt history of all tasks belonging to this run.
    with transaction() as c:
        return {'items':many(c,"SELECT * FROM work WHERE run_id=:id AND kind='partition' ORDER BY ordinal",id=rid),
            'attempts':many(c,'SELECT a.* FROM attempts a JOIN work w ON w.id=a.work_id WHERE w.run_id=:id ORDER BY a.started_at',id=rid)}


@app.get('/api/runs/{rid}/results')
def results(rid:str,outcome:str|None=None,offset:int=Query(0,ge=0),limit:int=Query(25,ge=1,le=100)):
    # Page/filter comparison groups and mark results provisional until the run completes.
    with transaction() as c:
        r=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
        where='run_id=:id'+(' AND outcome=:outcome' if outcome else '')
        params={'id':rid,'outcome':outcome}
        return {'provisional':r['status'] not in ['SUCCEEDED','SUPERSEDED'],
                'items':many(c,f'SELECT * FROM comparison_groups WHERE {where} ORDER BY reconciliation_ref LIMIT :limit OFFSET :offset',**params,limit=limit,offset=offset),
                'total':query(c,f'SELECT count(*) FROM comparison_groups WHERE {where}',**params).scalar()}


@app.get('/api/runs/{rid}/diff')
def diff(rid:str,previous_run_id:str):
    # Compare completed runs from one scope using stable reference/issue-family identities.
    with transaction() as c:
        a=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
        b=require(one(c,'SELECT * FROM runs WHERE id=:id',id=previous_run_id),'Previous run')
        if a['scope_id']!=b['scope_id'] or any(r['status'] not in ['SUCCEEDED','SUPERSEDED'] for r in [a,b]):
            conflict('Compare completed runs from the same scope')
        # Classify findings as new, persisted, cleared, absent, or unassessable in the newer run.
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
    # Only serve a completed report, including its checksum in the response headers.
    with transaction() as c:
        r=require(one(c,'SELECT * FROM runs WHERE id=:id',id=rid),'Run')
    if r['report_status']!='READY':
        conflict('Report is not ready',report_status=r['report_status'])
    return FileResponse(storage.path(r['report_key']),media_type='text/csv',filename=f'breakwater-run-{r["sequence"]}.csv',headers={'X-Artifact-SHA256':r['report_sha256']})


@app.get('/api/cases')
def cases(scope_id:str='demo-scope',q:str='',family:str='',workflow:str='',detection:str='',assignee:str='',account:str='',instrument:str='',
          offset:int=Query(0,ge=0),limit:int=Query(25,ge=1,le=100)):
    # Build optional queue filters while binding user-supplied values as SQL parameters.
    where=['scope_id=:scope']
    params={'scope':scope_id,'q':f'%{q}%','family':family,'workflow':workflow,'detection':detection,'assignee':assignee,'account':account,'instrument':instrument}
    for name,value in [('family',family),('workflow',workflow),('detection',detection),('assignee',assignee)]:
        if value:
            where.append(f'{name}=:{name}')
    if q:
        where.append('(reconciliation_ref ILIKE :q OR id ILIKE :q)')
    # Account/instrument filters search normalized records inside the saved case evidence.
    for field,value in [('account',account),('instrument',instrument)]:
        if value:
            where.append(f"EXISTS(SELECT 1 FROM jsonb_array_elements((evidence->'internal') || (evidence->'broker')) r WHERE r->'normalized'->>'{field}'=:{field})")
    sql=' AND '.join(where)
    with transaction() as c:
        # Return the filtered page and total, plus workflow/detection counts for the whole scope.
        return {'items':many(c,f'SELECT * FROM cases WHERE {sql} ORDER BY updated_at DESC,id LIMIT :limit OFFSET :offset',**params,limit=limit,offset=offset),
            'total':query(c,f'SELECT count(*) FROM cases WHERE {sql}',**params).scalar(),
            'summary':many(c,'SELECT detection,workflow,count(*) AS count FROM cases WHERE scope_id=:scope GROUP BY detection,workflow',scope=scope_id)}


@app.get('/api/cases/{cid}')
def case(cid:str):
    # Attach immutable audit events and per-run observations to the current case record.
    with transaction() as c:
        r=require(one(c,'SELECT * FROM cases WHERE id=:id',id=cid),'Case')
        r['events']=many(c,'SELECT * FROM case_events WHERE case_id=:id ORDER BY created_at,id',id=cid)
        r['observations']=many(c,'SELECT o.*,r.sequence FROM observations o JOIN runs r ON r.id=o.run_id WHERE case_id=:id ORDER BY r.sequence DESC',id=cid)
        return r


# Investigator commands carry the case version they were based on to detect stale edits.
class CaseAction(BaseModel):
    expected_version:int=Field(ge=1)
    actor:Literal['Alice','Bob']
    action:Literal['claim','note','waive','resolve','review']
    reason:str=Field(default='',max_length=4000)


@app.post('/api/cases/{cid}/actions')
def action(cid:str,data:CaseAction):
    # Demo identities may mutate case state only while demo controls are enabled.
    demo_only()
    return case_action(cid,data.model_dump())


# Select the fixture scope and generated dataset size for a demo scenario.
class DemoRequest(BaseModel):
    scope_id:str='demo-scope'
    rows:int=Field(default=240,ge=60,le=100000)


@app.post('/api/demo/scenarios/{name}',status_code=202)
def demo(name:Literal['breaks','corrected','missing','empty','corrupt','rejected'],data:DemoRequest):
    # Generate synthetic files for the selected scope and ingest them through the real service.
    demo_only()
    from breakwater.demo import fixture
    with transaction() as c:
        scope=require(one(c,'SELECT * FROM scopes WHERE id=:id',id=data.scope_id),'Scope')
    uploaded=[]
    for side in ['INTERNAL','BROKER']:
        # The missing-delivery scenario deliberately omits the broker upload.
        if name=='missing' and side=='BROKER':
            continue
        with transaction() as c:
            rev=query(c,'SELECT coalesce(max(revision),0)+1 FROM batches WHERE scope_id=:id AND side=:side',id=scope['id'],side=side).scalar()
        content,manifest,_=fixture(side,name,data.rows,rev,str(scope['business_date']),scope['dataset_id'])
        key,size=storage.store_stream(io.BytesIO(content))
        uploaded.append(register_upload(Manifest(**manifest),key,size,f'{name}-{side.lower()}-r{rev}.csv',uid()))
    return {'batches':uploaded}


# Enumerate the manual task controls exposed for demo and recovery verification.
class WorkAction(BaseModel):
    action:Literal['pause','release','redeliver','retry','fail_once']


@app.post('/api/demo/work/{wid}')
def work_action(wid:str,data:WorkAction):
    # Lock the selected task before changing its pause, failure, or retry settings.
    demo_only()
    with transaction() as c:
        w=require(one(c,'SELECT * FROM work WHERE id=:id FOR UPDATE',id=wid),'Work')
        if data.action in ['pause','release']:
            # Running workers poll this flag while maintaining their heartbeat.
            query(c,'UPDATE work SET pause=:pause WHERE id=:id',id=wid,pause=data.action=='pause')
        elif data.action=='fail_once':
            # Arrange one transient failure before the next attempt begins execution.
            if w['state'] not in ['PENDING','QUEUED','RETRYING']:
                conflict('Inject a failure before the work starts')
            query(c,'UPDATE work SET fail_remaining=1 WHERE id=:id',id=wid)
        elif data.action=='redeliver':
            # Intentionally dispatch even completed work; execute() must have no logical effect.
            from breakwater.jobs import celery
            celery.send_task('breakwater.execute',args=[wid],retry=False)
        elif data.action=='retry':
            # Requeue failed reports independently; input/run corrections require new work.
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
    # Summarize tasks, pending dispatch entries, and the latest 30 attempts by start time.
    with transaction() as c:
        return {'work':many(c,'SELECT kind,state,count(*) AS count FROM work GROUP BY kind,state ORDER BY kind,state'),
            'pending_dispatch':query(c,"SELECT count(*) FROM outbox o JOIN work w ON w.id=o.work_id WHERE o.sent_at IS NULL AND w.state IN ('PENDING','QUEUED','RETRYING')").scalar(),
            'recent_attempts':many(c,'SELECT a.*,w.kind,w.ordinal,w.run_id FROM attempts a JOIN work w ON w.id=a.work_id ORDER BY a.started_at DESC LIMIT 30'),
            'demo_mode':DEMO}

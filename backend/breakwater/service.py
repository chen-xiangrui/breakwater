import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from breakwater.db import transaction, query, one, many, uid, dumps
from breakwater.domain import Manifest, fingerprint


def conflict(message, **extra):
    raise HTTPException(409, jsonable_encoder({'message':message, **extra}))


def require(row, noun='Resource'):
    if not row:
        raise HTTPException(404,f'{noun} not found')
    return row


def new_work(c,kind,*,batch_id=None,run_id=None,ordinal=0,payload=None):
    wid = uid()
    inserted=query(c,"""INSERT INTO work(id,kind,batch_id,run_id,ordinal,payload)
        VALUES (:id,:kind,:batch,:run,:ordinal,CAST(:payload AS jsonb)) ON CONFLICT DO NOTHING RETURNING id""",
        id=wid,kind=kind,batch=batch_id,run=run_id,ordinal=ordinal,payload=dumps(payload or {}))
    if inserted.scalar():
        query(c,'INSERT INTO outbox(id,work_id) VALUES(:id,:work)',id=uid(),work=wid)
    return wid


def idempotent(c,key,payload,kind):
    if not key or len(key)>160:
        raise HTTPException(422,'Idempotency-Key header is required (maximum 160 characters)')
    query(c,'SELECT pg_advisory_xact_lock(hashtextextended(:key,0))',key=key)
    existing=one(c,'SELECT * FROM idempotency WHERE key=:key',key=key)
    fp=fingerprint(payload)
    if existing and (existing['fingerprint']!=fp or existing['kind']!=kind):
        conflict('Idempotency key was already used for a different request')
    return existing,fp


def save_key(c,key,fp,rid,kind):
    query(c,'INSERT INTO idempotency(key,fingerprint,resource_id,kind) VALUES (:key,:fp,:rid,:kind)',
          key=key,fp=fp,rid=rid,kind=kind)


def register_upload(manifest:Manifest,key,size,filename,idempotency_key):
    with transaction() as c:
        return register_upload_in_transaction(c,manifest,key,size,filename,idempotency_key)


def register_upload_in_transaction(c,manifest,key,size,filename,idempotency_key,manifest_origin='PRODUCER'):
    m=manifest.model_dump(mode='json')
    old,fp=idempotent(c,idempotency_key,{'manifest':m,'sha256':key},'upload')
    if old:
        return one(c,'SELECT * FROM batches WHERE id=:id',id=old['resource_id'])
    scope=one(c,"""SELECT * FROM scopes WHERE broker_id=:broker AND business_date=:date AND dataset_id=:dataset FOR UPDATE""",
              broker=m['broker_id'],date=manifest.business_date,dataset=m['dataset_id'])
    if not scope:
        raise HTTPException(422,'Unknown delivery scope; use the configured broker/date/dataset')
    old=one(c,'SELECT * FROM batches WHERE scope_id=:scope AND side=:side AND revision=:rev',scope=scope['id'],side=m['side'],rev=m['revision'])
    if old:
        if old['sha256']!=key or old['manifest']!=m:
            conflict('This revision already contains different content. Supply a new revision.')
        save_key(c,idempotency_key,fp,old['id'],'upload')
        return old
    bid=uid()
    checks={'sha256':key==m['sha256'],'byte_count':size==m['expected_bytes'],
            'complete':m['complete'],'coverage':sorted(m['account_ids'])==sorted(scope['account_ids']) and len(set(m['account_ids']))==len(m['account_ids']),
            'schema':True,'row_count':None}
    failed=[k for k,v in checks.items() if v is False]
    status='BLOCKED' if failed else 'RECEIVED'
    error='Manifest verification failed: '+', '.join(failed) if failed else None
    query(c,"""INSERT INTO batches(id,scope_id,side,revision,filename,object_key,sha256,byte_size,manifest,status,checks,error,manifest_origin)
        VALUES(:id,:scope,:side,:rev,:filename,:key,:key,:size,CAST(:manifest AS jsonb),:status,CAST(:checks AS jsonb),:error,:origin)""",
        id=bid,scope=scope['id'],side=m['side'],rev=m['revision'],filename=Path(filename or 'delivery.csv').name[:200],
        key=key,size=size,manifest=dumps(m),status=status,checks=dumps(checks),error=error,origin=manifest_origin)
    query(c,'UPDATE deliveries SET arrived_at=COALESCE(arrived_at,now()) WHERE scope_id=:scope AND side=:side',scope=scope['id'],side=m['side'])
    if not failed:
        new_work(c,'parse',batch_id=bid)
    save_key(c,idempotency_key,fp,bid,'upload')
    return one(c,'SELECT * FROM batches WHERE id=:id',id=bid)


def start_run(payload,key):
    with transaction() as c:
        return start_run_in_transaction(c,payload,key)


def start_run_in_transaction(c,payload,key):
    old,fp=idempotent(c,key,payload,'run')
    if old:
        return one(c,'SELECT * FROM runs WHERE id=:id',id=old['resource_id'])
    query(c,'SELECT pg_advisory_xact_lock(918274)')
    scope=require(one(c,'SELECT * FROM scopes WHERE id=:id FOR UPDATE',id=payload['scope_id']),'Scope')
    batches=[require(one(c,'SELECT * FROM batches WHERE id=:id',id=payload[field]),'Batch') for field in ['internal_batch_id','broker_batch_id']]
    rules=require(one(c,'SELECT * FROM rule_sets WHERE id=:id',id=payload['rule_version']),'Rule set')
    for b,side in zip(batches,['INTERNAL','BROKER']):
        if b['scope_id']!=scope['id'] or b['side']!=side or b['status']!='READY':
            conflict('Both selected inputs must be READY and belong to the same scope with the correct source sides')
        if b['mapping_version']!=rules['config']['mapping_version']:
            conflict('Input mapping version is incompatible with the selected rule set')
    active=query(c,"SELECT count(*) FROM runs WHERE status IN ('QUEUED','RUNNING','FINALIZING')").scalar()
    if active>=4:
        conflict('WAITING_FOR_CAPACITY: four runs are already active; retry when a run completes')
    rid=uid()
    query(c,"""INSERT INTO runs(id,scope_id,sequence,internal_batch_id,broker_batch_id,rule_version,bucket_count)
        VALUES(:id,:scope,:seq,:internal,:broker,:rule,:buckets)""",id=rid,scope=scope['id'],seq=scope['next_sequence'],
        internal=batches[0]['id'],broker=batches[1]['id'],rule=payload['rule_version'],buckets=payload['bucket_count'])
    query(c,'UPDATE scopes SET next_sequence=next_sequence+1 WHERE id=:id',id=scope['id'])
    new_work(c,'prepare',run_id=rid,payload={'pause_bucket':payload.get('pause_bucket'), 'fail_bucket':payload.get('fail_bucket')})
    save_key(c,key,fp,rid,'run')
    return one(c,'SELECT * FROM runs WHERE id=:id',id=rid)


def case_action(cid,data):
    with transaction() as c:
        case=require(one(c,'SELECT * FROM cases WHERE id=:id FOR UPDATE',id=cid),'Case')
        if case['version']!=data['expected_version']:
            conflict('Case changed; review latest values',current=case)
        action,actor,reason=data['action'],data['actor'],data.get('reason','').strip()
        workflow,assignee=case['workflow'],case['assignee']
        expiry=case['waiver_expires_at']
        waiver=case['waiver_fingerprint']
        if action=='claim':
            assignee=actor
            if case['detection']!='CLEARED':
                workflow='IN_REVIEW'
            reason=reason or f'Claimed by {actor}'
        elif action=='note':
            if not reason:
                raise HTTPException(422,'A note is required')
        elif action=='waive':
            if not reason or case['detection']!='PRESENT':
                conflict('Only a present discrepancy can be waived, with a reason')
            workflow='WAIVED'
            waiver=case['fingerprint']
            expiry=datetime.now(timezone.utc)+timedelta(hours=24)
        elif action=='resolve':
            if case['detection']!='CLEARED':
                conflict('Resolution requires cleared evidence from a published run. Use a reasoned waiver for an outstanding difference.')
            workflow='RESOLVED'
            reason=reason or 'Confirmed cleared evidence'
        elif action=='review':
            workflow='IN_REVIEW'
            reason=reason or 'Requested review'
        query(c,"""UPDATE cases SET workflow=:workflow,assignee=:assignee,version=version+1,
            waiver_fingerprint=:waiver,waiver_expires_at=:expiry,updated_at=now() WHERE id=:id AND version=:version""",
            id=cid,version=case['version'],workflow=workflow,assignee=assignee,waiver=waiver,expiry=expiry)
        event(c,cid,actor,action.upper(),reason,case['version'],case['version']+1)
        return one(c,'SELECT * FROM cases WHERE id=:id',id=cid)


def event(c,cid,actor,kind,reason,before,after,event_id=None):
    query(c,"""INSERT INTO case_events(id,case_id,actor,kind,reason,before_version,after_version)
        VALUES(:id,:cid,:actor,:kind,:reason,:before,:after) ON CONFLICT DO NOTHING""",
        id=event_id or uid(),cid=cid,actor=actor,kind=kind,reason=reason,before=before,after=after)

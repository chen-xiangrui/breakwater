"""Bounded ingestion and SQL-keyed reconciliation, committed by fenced workers."""
import csv
import hashlib
import io
import json
import os
import tempfile
import time
from datetime import datetime, timezone

from breakwater import storage
from breakwater.db import query, one, many, uid, dumps
from breakwater.domain import INTERNAL, BROKER, normalize, stable_digest, fingerprint, compare, evidence_fingerprint, safe_csv
from breakwater.service import new_work, event

CHUNK_SIZE=int(os.getenv('CHUNK_SIZE','5000'))
csv.field_size_limit(65536)


class PermanentError(Exception):
    pass


def parse(c,w):
    batch=one(c,'SELECT * FROM batches WHERE id=:id',id=w['batch_id'])
    manifest=batch['manifest']
    with storage.path(batch['object_key']).open('rb') as f:
        if hashlib.file_digest(f,'sha256').hexdigest()!=batch['sha256']:
            raise PermanentError('Stored file checksum no longer matches the registered bytes')
    rows,number,chunk_no=[],0,0
    def flush():
        nonlocal rows,chunk_no
        if not rows:
            return
        key=storage.store_bytes(dumps(rows).encode())
        new_work(c,'chunk',batch_id=batch['id'],ordinal=chunk_no,payload={'object_key':key,'count':len(rows)})
        rows=[]
        chunk_no+=1
    try:
        with storage.path(batch['object_key']).open(encoding='utf-8-sig',newline='') as f:
            reader=csv.reader(f,strict=True)
            headers=next(reader,None)
            expected=INTERNAL if batch['side']=='INTERNAL' else BROKER
            if headers!=expected:
                raise PermanentError('CSV header must exactly match '+','.join(expected))
            for number,values in enumerate(reader,1):
                if number>1000000:
                    raise PermanentError('The demo limit is 1,000,000 records per input')
                if sum(len(v) for v in values)>65536:
                    raise PermanentError(f'Record {number} exceeds 64 KiB')
                raw=dict(zip(headers,values))
                if len(values)>len(headers):
                    raw['_extra_columns']=values[len(headers):]
                rows.append({'number':number,'raw':raw, 'structural_error':len(values)!=len(headers)})
                if len(rows)>=CHUNK_SIZE:
                    flush()
            flush()
    except (UnicodeDecodeError,csv.Error) as e:
        raise PermanentError(f'Structural CSV error: {e}') from e
    checks={**batch['checks'],'row_count':number==manifest['expected_data_rows']}
    status='VALIDATING' if checks['row_count'] else 'BLOCKED'
    error=None if checks['row_count'] else f'Manifest declares {manifest["expected_data_rows"]} rows; parsed {number}'
    query(c,'UPDATE batches SET parsed=:n,status=:status,checks=CAST(:checks AS jsonb),error=:error WHERE id=:id',
          n=number,status=status,checks=dumps(checks),error=error,id=batch['id'])
    if status=='BLOCKED':
        query(c,"UPDATE work SET state='CANCELLED' WHERE batch_id=:id AND kind='chunk'",id=batch['id'])
    if not number and status!='BLOCKED':
        new_work(c,'batch_finalize',batch_id=batch['id'])
    return {'parsed':number,'chunks':chunk_no}


def chunk(c,w):
    batch=one(c,'SELECT * FROM batches WHERE id=:id',id=w['batch_id'])
    if batch['status'] in ['BLOCKED','FAILED']:
        return {'skipped':True}
    rows=json.loads(storage.path(w['payload']['object_key']).read_text())
    accepted,rejected=0,0
    for row in rows:
        normalized,errors=normalize(row['raw'],batch['side'],batch['manifest']['account_ids'])
        if row['structural_error']:
            errors.append({'field':'record','code':'COLUMN_COUNT','message':'Row has a different number of fields than the header'})
        if errors:
            rejected+=1
            query(c,"""INSERT INTO rejections(batch_id,record_number,raw,errors)
                VALUES(:batch,:number,CAST(:raw AS jsonb),CAST(:errors AS jsonb))""",
                batch=batch['id'],number=row['number'],raw=dumps(row['raw']),errors=dumps(errors))
        else:
            accepted+=1
            normalized['business_date']=batch['manifest']['business_date']
            query(c,"""INSERT INTO records(batch_id,record_number,reconciliation_ref,key_digest,account,instrument,side,currency,quantity,price,normalized,raw,payload_hash)
              VALUES(:batch,:number,:ref,:digest,:account,:instrument,:side,:currency,:quantity,:price,CAST(:normalized AS jsonb),CAST(:raw AS jsonb),:hash)""",
              batch=batch['id'],number=row['number'],ref=normalized['reconciliation_ref'],digest=stable_digest(batch['scope_id'],normalized['reconciliation_ref']),
              account=normalized['account'],instrument=normalized['instrument'],side=normalized['side'],currency=normalized['currency'],
              quantity=normalized['quantity'],price=normalized['execution_price'],normalized=dumps(normalized),raw=dumps(row['raw']),hash=fingerprint(normalized))
    return {'accepted':accepted,'rejected':rejected}


def batch_finalize(c,w):
    batch=one(c,'SELECT * FROM batches WHERE id=:id FOR UPDATE',id=w['batch_id'])
    if batch['status'] in ['BLOCKED','FAILED','READY']:
        return {}
    pending=query(c,"SELECT count(*) FROM work WHERE batch_id=:id AND kind='chunk' AND state<>'SUCCEEDED'",id=batch['id']).scalar()
    if pending:
        raise RuntimeError('Chunks are not complete')
    accepted=query(c,'SELECT count(*) FROM records WHERE batch_id=:id',id=batch['id']).scalar()
    rejected=query(c,'SELECT count(*) FROM rejections WHERE batch_id=:id',id=batch['id']).scalar()
    if accepted+rejected!=batch['parsed']:
        raise PermanentError('Ingestion counts do not balance')
    query(c,"""UPDATE batches SET accepted=:accepted,rejected=:rejected,status=:status,error=:error,
        ready_at=CASE WHEN :rejected=0 THEN now() ELSE NULL END WHERE id=:id""",id=batch['id'],accepted=accepted,rejected=rejected,
        status='BLOCKED' if rejected else 'READY',error=f'{rejected} rejected records need correction' if rejected else None)
    return {'accepted':accepted,'rejected':rejected}


def prepare(c,w):
    run=one(c,'SELECT * FROM runs WHERE id=:id',id=w['run_id'])
    if run['status']=='FAILED':
        return {}
    query(c,"UPDATE runs SET status='RUNNING',started_at=COALESCE(started_at,now()) WHERE id=:id",id=run['id'])
    query(c,"""INSERT INTO assignments(run_id,record_id,bucket)
        SELECT :run,id,key_digest % :buckets FROM records WHERE batch_id IN (:internal,:broker)""",
        run=run['id'],buckets=run['bucket_count'],internal=run['internal_batch_id'],broker=run['broker_batch_id'])
    for bucket in range(run['bucket_count']):
        wid=new_work(c,'partition',run_id=run['id'],ordinal=bucket)
        query(c,'UPDATE work SET pause=:pause,fail_remaining=:fail WHERE id=:id',id=wid,
              pause=w['payload'].get('pause_bucket')==bucket,fail=1 if w['payload'].get('fail_bucket')==bucket else 0)
    return {'partitions':run['bucket_count']}


def partition(c,w):
    run=one(c,'SELECT * FROM runs WHERE id=:id',id=w['run_id'])
    if run['status']=='FAILED':
        return {'skipped':True}
    rules=one(c,'SELECT config FROM rule_sets WHERE id=:id',id=run['rule_version'])['config']
    large=one(c,"""SELECT r.reconciliation_ref,count(*) AS n FROM records r JOIN assignments a ON a.record_id=r.id
        WHERE a.run_id=:run AND a.bucket=:bucket GROUP BY r.reconciliation_ref HAVING count(*)>100 LIMIT 1""",run=run['id'],bucket=w['ordinal'])
    if large:
        raise PermanentError(f'Key {large["reconciliation_ref"]} exceeds the 100-row ambiguity limit')
    # Aggregate each side before FULL OUTER JOIN: duplicate keys can never multiply joined rows.
    sql="""WITH grouped AS (
      SELECT r.reconciliation_ref,b.side,jsonb_agg(jsonb_build_object('id',r.id,'record_number',r.record_number,
        'batch_id',b.id,'filename',b.filename,'revision',b.revision,'sha256',b.sha256,'raw',r.raw,'normalized',r.normalized)
        ORDER BY r.record_number) AS members
      FROM assignments a JOIN records r ON r.id=a.record_id JOIN batches b ON b.id=r.batch_id
      WHERE a.run_id=:run AND a.bucket=:bucket GROUP BY r.reconciliation_ref,b.side
    ), i AS (SELECT * FROM grouped WHERE side='INTERNAL'), b AS (SELECT * FROM grouped WHERE side='BROKER')
    SELECT coalesce(i.reconciliation_ref,b.reconciliation_ref) AS ref,
      coalesce(i.members,'[]'::jsonb) AS internal,coalesce(b.members,'[]'::jsonb) AS broker
    FROM i FULL OUTER JOIN b ON i.reconciliation_ref=b.reconciliation_ref
    ORDER BY coalesce(i.reconciliation_ref,b.reconciliation_ref)"""
    from sqlalchemy import text
    rows=c.execution_options(stream_results=True).execute(text(sql),{'run':run['id'],'bucket':w['ordinal']}).mappings()
    counts={'groups':0,'records':0,'findings':0}
    # Use a separate Connection wrapper with streaming disabled for INSERT statements.
    c.execution_options(stream_results=False)
    for row in rows:
        outcome,families,evidence=compare(row['internal'],row['broker'],rules)
        gid=uid()
        query(c,"""INSERT INTO comparison_groups(id,run_id,work_id,winning_token,reconciliation_ref,outcome,internal_count,broker_count,evidence)
          VALUES(:id,:run,:work,:token,:ref,:outcome,:ic,:bc,CAST(:evidence AS jsonb))""",
          id=gid,run=run['id'],work=w['id'],token=w['token'],ref=row['ref'],outcome=outcome,
          ic=len(row['internal']),bc=len(row['broker']),evidence=dumps(evidence))
        for family in families:
            query(c,"""INSERT INTO findings(id,group_id,run_id,reconciliation_ref,family,evidence,fingerprint)
              VALUES(:id,:gid,:run,:ref,:family,CAST(:evidence AS jsonb),:fp)""",id=uid(),gid=gid,run=run['id'],ref=row['ref'],family=family,
              evidence=dumps(evidence),fp=evidence_fingerprint(family,evidence))
        counts['groups']+=1
        counts['records']+=len(row['internal'])+len(row['broker'])
        counts['findings']+=len(families)
    return counts


def finalize(c,w):
    started=time.monotonic()
    run=one(c,'SELECT * FROM runs WHERE id=:id',id=w['run_id'])
    scope=one(c,'SELECT * FROM scopes WHERE id=:id FOR UPDATE',id=run['scope_id'])
    if run['status'] in ['SUCCEEDED','SUPERSEDED','FAILED']:
        return {}
    partitions=many(c,"SELECT * FROM work WHERE run_id=:id AND kind='partition' ORDER BY ordinal",id=run['id'])
    if len(partitions)!=run['bucket_count'] or any(p['state']!='SUCCEEDED' for p in partitions):
        raise PermanentError('Cannot publish an incomplete partition set')
    invalid=query(c,"""SELECT count(*) FROM comparison_groups g JOIN work w ON w.id=g.work_id
      WHERE g.run_id=:run AND (g.winning_token<>w.token OR w.state<>'SUCCEEDED')""",run=run['id']).scalar()
    counts=many(c,'SELECT outcome,count(*) AS count FROM comparison_groups WHERE run_id=:id GROUP BY outcome',id=run['id'])
    metrics={r['outcome'].lower():r['count'] for r in counts}
    metrics['groups']=sum(metrics.values())
    metrics['findings']=query(c,'SELECT count(*) FROM findings WHERE run_id=:id',id=run['id']).scalar()
    metrics['records']=int(query(c,'SELECT coalesce(sum(internal_count+broker_count),0) FROM comparison_groups WHERE run_id=:id',id=run['id']).scalar())
    expected=query(c,'SELECT sum(accepted) FROM batches WHERE id IN (:i,:b)',i=run['internal_batch_id'],b=run['broker_batch_id']).scalar()
    if invalid or metrics['records']!=expected:
        raise PermanentError('Result coverage or winning-attempt validation failed')
    newer=one(c,'SELECT sequence FROM runs WHERE id=:id',id=scope['published_run_id']) if scope['published_run_id'] else None
    if newer and newer['sequence']>run['sequence']:
        query(c,"UPDATE runs SET status='SUPERSEDED',metrics=CAST(:m AS jsonb),completed_at=clock_timestamp() WHERE id=:id",m=dumps(metrics),id=run['id'])
        return metrics
    # Bounded publication: explicitly reject pathological case volume before mutating projections.
    existing=many(c,'SELECT * FROM cases WHERE scope_id=:id ORDER BY reconciliation_ref,family FOR UPDATE',id=scope['id'])
    if metrics['findings']+len(existing)>20000:
        raise PermanentError('Publication exceeds 20,000 case observations; reduce scope size')
    current={(x['reconciliation_ref'],x['family']):x for x in existing}
    findings=many(c,'SELECT * FROM findings WHERE run_id=:id ORDER BY reconciliation_ref,family',id=run['id'])
    present=set()
    for f in findings:
        key=(f['reconciliation_ref'],f['family'])
        present.add(key)
        case=current.get(key)
        if case:
            workflow=case['workflow']
            waiver_valid=(case['waiver_fingerprint']==f['fingerprint'] and case['waiver_expires_at'] and case['waiver_expires_at']>datetime.now(timezone.utc))
            if workflow=='RESOLVED':
                workflow='OPEN'
            if workflow=='WAIVED' and not waiver_valid:
                workflow='IN_REVIEW'
            cid,version=case['id'],case['version']+1
            query(c,"""UPDATE cases SET detection='PRESENT',workflow=:workflow,last_run_id=:run,evidence=CAST(:e AS jsonb),
                fingerprint=:fp,version=:version,updated_at=now() WHERE id=:id""",workflow=workflow,run=run['id'],
                e=dumps(f['evidence']),fp=f['fingerprint'],version=version,id=cid)
        else:
            cid,version=uid(),1
            query(c,"""INSERT INTO cases(id,scope_id,reconciliation_ref,family,first_run_id,last_run_id,evidence,fingerprint)
              VALUES(:id,:scope,:ref,:family,:run,:run,CAST(:e AS jsonb),:fp)""",id=cid,scope=scope['id'],ref=f['reconciliation_ref'],
              family=f['family'],run=run['id'],e=dumps(f['evidence']),fp=f['fingerprint'])
        query(c,"INSERT INTO observations(run_id,case_id,detection,evidence) VALUES(:run,:cid,'PRESENT',CAST(:e AS jsonb))",run=run['id'],cid=cid,e=dumps(f['evidence']))
        event(c,cid,'System','OBSERVED',f'Run #{run["sequence"]}: discrepancy {"persists" if case else "detected"}',version-1 if case else None,version,f'{run["id"]}:{cid}')
    for key,case in current.items():
        if key in present:
            continue
        group=one(c,'SELECT outcome FROM comparison_groups WHERE run_id=:run AND reconciliation_ref=:ref',run=run['id'],ref=case['reconciliation_ref'])
        detection='NOT_PRESENT_IN_INPUTS' if not group else ('CLEARED' if group['outcome']=='MATCHED' else 'UNASSESSABLE')
        # A different discrepancy cannot prove that the previous issue has cleared.
        workflow='RESOLVED' if detection=='CLEARED' else 'IN_REVIEW'
        query(c,"UPDATE cases SET detection=:d,workflow=:w,last_run_id=:run,version=version+1,updated_at=now() WHERE id=:id",
              d=detection,w=workflow,run=run['id'],id=case['id'])
        query(c,'INSERT INTO observations(run_id,case_id,detection,evidence) VALUES(:run,:cid,:d,CAST(:e AS jsonb))',
              run=run['id'],cid=case['id'],d=detection,e=dumps({'group_outcome':group['outcome'] if group else None,'previous_evidence':case['evidence']}))
        reason={'CLEARED':'key now matches; issue cleared','NOT_PRESENT_IN_INPUTS':'key absent from both inputs; review required','UNASSESSABLE':'key has a different discrepancy; previous issue requires review'}[detection]
        event(c,case['id'],'System',detection,f'Run #{run["sequence"]}: {reason}',case['version'],case['version']+1,f'{run["id"]}:{case["id"]}')
    metrics['publication_ms']=round((time.monotonic()-started)*1000,2)
    query(c,"""UPDATE runs SET status='SUCCEEDED',outcome=:outcome,metrics=CAST(:metrics AS jsonb),completed_at=clock_timestamp() WHERE id=:id""",
          outcome='BREAKS_FOUND' if metrics['findings'] else 'MATCHED',metrics=dumps(metrics),id=run['id'])
    query(c,'UPDATE scopes SET published_run_id=:run WHERE id=:id',run=run['id'],id=scope['id'])
    new_work(c,'report',run_id=run['id'])
    return metrics


def report(c,w):
    run=one(c,'SELECT * FROM runs WHERE id=:id',id=w['run_id'])
    batches=many(c,'SELECT side,sha256,revision FROM batches WHERE id IN (:i,:b)',i=run['internal_batch_id'],b=run['broker_batch_id'])
    with tempfile.TemporaryFile(mode='w+',encoding='utf-8',newline='') as f:
        writer=csv.writer(f)
        writer.writerow(['Breakwater reconciliation report',run['id'],run['rule_version'],dumps(run['metrics'])])
        for b in batches:
            writer.writerow([b['side'],'revision',b['revision'],'SHA-256',b['sha256']])
        writer.writerow(['reference','outcome','internal_records','broker_records','differences','input_lineage'])
        from sqlalchemy import text
        rows=c.execution_options(stream_results=True).execute(text('SELECT * FROM comparison_groups WHERE run_id=:id ORDER BY reconciliation_ref'),{'id':run['id']}).mappings()
        for g in rows:
            e=g['evidence']
            lineage=[{k:r[k] for k in ['batch_id','record_number','revision','sha256']} for s in ['internal','broker'] for r in e[s]]
            writer.writerow([safe_csv(x) for x in [g['reconciliation_ref'],g['outcome'],g['internal_count'],g['broker_count'],dumps(e['differences']),dumps(lineage)]])
        c.execution_options(stream_results=False)
        f.seek(0)
        # Copy in bounded buffers through the content-addressed store.
        with tempfile.TemporaryFile() as data:
            while part:=f.read(65536):
                data.write(part.encode())
            data.seek(0)
            key,size=storage.store_stream(data,limit=storage.MAX_ARTIFACT)
    query(c,"UPDATE runs SET report_status='READY',report_key=:key,report_sha256=:key WHERE id=:id",key=key,id=run['id'])
    return {'bytes':size}


HANDLERS={'parse':parse,'chunk':chunk,'batch_finalize':batch_finalize,'prepare':prepare,'partition':partition,'finalize':finalize,'report':report}

import logging
import os
import threading
import time
from celery import Celery

from breakwater.db import transaction, query, one, uid, dumps
from breakwater.pipeline import HANDLERS, PermanentError

log=logging.getLogger(__name__)
LEASE_SECONDS=int(os.getenv('LEASE_SECONDS','20'))
MAX_ATTEMPTS=4
celery=Celery('breakwater',broker=os.getenv('BROKER_URL','amqp://breakwater:breakwater@localhost:5672//'))
celery.conf.update(task_acks_late=True,task_reject_on_worker_lost=True,worker_prefetch_multiplier=1,
    task_ignore_result=True,task_serializer='json',accept_content=['json'],
    broker_connection_retry_on_startup=True,broker_connection_timeout=5,
    broker_transport_options={'confirm_publish':True},task_soft_time_limit=240,task_time_limit=270)


class StaleAttempt(Exception):
    pass


def heartbeat(wid,token,stop):
    while not stop.wait(max(1,LEASE_SECONDS//3)):
        try:
            with transaction() as c:
                query(c,"""UPDATE work SET heartbeat_at=now(),lease_until=now()+make_interval(secs=>:lease)
                  WHERE id=:id AND token=:token AND state='RUNNING'""",id=wid,token=token,lease=LEASE_SECONDS)
        except Exception:
            log.exception('heartbeat failed for %s',wid)


def mark_failed(c,w,error,permanent):
    failed=permanent or w['attempts']>=MAX_ATTEMPTS
    state='FAILED' if failed else 'RETRYING'
    query(c,"""UPDATE work SET state=:state,error=:error,lease_until=NULL,
        available_at=now()+make_interval(secs=>:delay) WHERE id=:id""",id=w['id'],state=state,error=error,
        delay=min(30,2**w['attempts']))
    query(c,"UPDATE attempts SET state=:state,error=:error,finished_at=clock_timestamp() WHERE work_id=:id AND token=:token AND state='RUNNING'",
          id=w['id'],token=w['token'],state='FAILED' if failed else 'RETRYING',error=error)
    if failed:
        if w['batch_id']:
            query(c,"UPDATE batches SET status=:status,error=:error WHERE id=:id AND status<>'READY'",
                  id=w['batch_id'],status='BLOCKED' if permanent else 'FAILED',error=error)
        elif w['kind']=='report':
            query(c,"UPDATE runs SET report_status='FAILED' WHERE id=:id",id=w['run_id'])
        else:
            query(c,"UPDATE runs SET status='FAILED',error=:error,completed_at=clock_timestamp() WHERE id=:id AND status NOT IN ('SUCCEEDED','SUPERSEDED')",id=w['run_id'],error=error)
    else:
        query(c,"INSERT INTO outbox(id,work_id,available_at) VALUES(:id,:work,now()+make_interval(secs=>:delay))",id=uid(),work=w['id'],delay=min(30,2**w['attempts']))


@celery.task(name='breakwater.execute')
def execute(wid):
    with transaction() as c:
        w=one(c,'SELECT * FROM work WHERE id=:id FOR UPDATE',id=wid)
        if not w or w['state'] not in ['QUEUED','RETRYING']:
            return {'duplicate_delivery':True}
        if w['attempts']>=MAX_ATTEMPTS:
            mark_failed(c,w,'Retry budget exhausted',True)
            return
        query(c,"""UPDATE work SET state='RUNNING',token=token+1,attempts=attempts+1,
            heartbeat_at=now(),lease_until=now()+make_interval(secs=>:lease) WHERE id=:id""",id=wid,lease=LEASE_SECONDS)
        w=one(c,'SELECT * FROM work WHERE id=:id',id=wid)
        query(c,'INSERT INTO attempts(id,work_id,token) VALUES(:id,:work,:token)',id=uid(),work=wid,token=w['token'])
        if w['kind']=='parse':
            query(c,"UPDATE batches SET status='PARSING' WHERE id=:id AND status IN ('RECEIVED','PARSING','FAILED')",id=w['batch_id'])
        if w['kind']=='finalize':
            query(c,"UPDATE runs SET status='FINALIZING' WHERE id=:id AND status='RUNNING'",id=w['run_id'])
        injected=w['fail_remaining']>0
        if injected:
            query(c,'UPDATE work SET fail_remaining=fail_remaining-1 WHERE id=:id',id=wid)
    stop=threading.Event()
    thread=threading.Thread(target=heartbeat,args=(wid,w['token'],stop),daemon=True)
    thread.start()
    try:
        if injected:
            raise RuntimeError('Demo: injected transient failure (one attempt)')
        while True:
            with transaction() as c:
                current=one(c,'SELECT pause,token,state FROM work WHERE id=:id',id=wid)
            if current['token']!=w['token'] or current['state']!='RUNNING':
                raise StaleAttempt()
            if not current['pause']:
                break
            time.sleep(0.5)
        with transaction() as c:
            metrics=HANDLERS[w['kind']](c,w)
            # Results and winning-work transition commit together, or are rolled back together.
            current=one(c,'SELECT * FROM work WHERE id=:id FOR UPDATE',id=wid)
            if current['token']!=w['token'] or current['state']!='RUNNING':
                raise StaleAttempt()
            query(c,"""UPDATE work SET state='SUCCEEDED',metrics=CAST(:metrics AS jsonb),completed_at=clock_timestamp(),
                lease_until=NULL,error=NULL WHERE id=:id""",id=wid,metrics=dumps(metrics))
            query(c,"UPDATE attempts SET state='SUCCEEDED',finished_at=clock_timestamp() WHERE work_id=:id AND token=:token",id=wid,token=w['token'])
        return metrics
    except StaleAttempt:
        log.warning('Discarded fenced attempt %s token %s',wid,w['token'])
        with transaction() as c:
            query(c,"UPDATE attempts SET state='OBSOLETE',finished_at=clock_timestamp(),error='Fencing token is no longer current' WHERE work_id=:id AND token=:token AND state='RUNNING'",id=wid,token=w['token'])
    except Exception as e:
        log.exception('Work %s failed',wid)
        with transaction() as c:
            current=one(c,'SELECT * FROM work WHERE id=:id FOR UPDATE',id=wid)
            if current['token']==w['token'] and current['state']=='RUNNING':
                mark_failed(c,current,str(e)[:1000],isinstance(e,PermanentError))
    finally:
        stop.set()
        thread.join(timeout=2)


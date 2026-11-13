# Celery worker entry point: claim durable work, run its handler, and record attempts.
import logging
import os
import threading
import time
from celery import Celery

from breakwater.db import transaction, query, one, uid, dumps
from breakwater.pipeline import HANDLERS, PermanentError

log=logging.getLogger(__name__)
# A lease and changing attempt token let the dispatcher replace unresponsive workers safely.
LEASE_SECONDS=int(os.getenv('LEASE_SECONDS','20'))
MAX_ATTEMPTS=4
# RabbitMQ transports work IDs; pipeline payloads and results remain in PostgreSQL/storage.
celery=Celery('breakwater',broker=os.getenv('BROKER_URL','amqp://breakwater:breakwater@localhost:5672//'))
# Acknowledge after execution, requeue on worker loss, and reserve one message per process.
# JSON messages, publisher confirms, and task time limits bound delivery/execution behavior.
celery.conf.update(task_acks_late=True,task_reject_on_worker_lost=True,worker_prefetch_multiplier=1,
    task_ignore_result=True,task_serializer='json',accept_content=['json'],
    broker_connection_retry_on_startup=True,broker_connection_timeout=5,
    broker_transport_options={'confirm_publish':True},task_soft_time_limit=240,task_time_limit=270)


class StaleAttempt(Exception):
    # Raised when another attempt owns the task; this attempt's staged effects must roll back.
    pass


def heartbeat(wid,token,stop):
    # Renew only the current RUNNING attempt's lease until its execution thread finishes.
    while not stop.wait(max(1,LEASE_SECONDS//3)):
        try:
            with transaction() as c:
                query(c,"""UPDATE work SET heartbeat_at=now(),lease_until=now()+make_interval(secs=>:lease)
                  WHERE id=:id AND token=:token AND state='RUNNING'""",id=wid,token=token,lease=LEASE_SECONDS)
        except Exception:
            # A later heartbeat can recover; lease expiry lets the dispatcher detect long outages.
            log.exception('heartbeat failed for %s',wid)


def mark_failed(c,w,error,permanent):
    # Retry temporary errors within the attempt budget; permanent errors fail immediately.
    failed=permanent or w['attempts']>=MAX_ATTEMPTS
    state='FAILED' if failed else 'RETRYING'
    # Save the error, release the lease, and set exponential retry availability.
    query(c,"""UPDATE work SET state=:state,error=:error,lease_until=NULL,
        available_at=now()+make_interval(secs=>:delay) WHERE id=:id""",id=w['id'],state=state,error=error,
        delay=min(30,2**w['attempts']))
    query(c,"UPDATE attempts SET state=:state,error=:error,finished_at=clock_timestamp() WHERE work_id=:id AND token=:token AND state='RUNNING'",
          id=w['id'],token=w['token'],state='FAILED' if failed else 'RETRYING',error=error)
    if failed:
        if w['batch_id']:
            # Permanent source problems block the batch; exhausted transient failures fail it.
            query(c,"UPDATE batches SET status=:status,error=:error WHERE id=:id AND status<>'READY'",
                  id=w['batch_id'],status='BLOCKED' if permanent else 'FAILED',error=error)
        elif w['kind']=='report':
            # Report failures can be retried without invalidating the published reconciliation.
            query(c,"UPDATE runs SET report_status='FAILED' WHERE id=:id",id=w['run_id'])
        else:
            # A failed processing stage prevents the run from publishing results.
            query(c,"UPDATE runs SET status='FAILED',error=:error,completed_at=clock_timestamp() WHERE id=:id AND status NOT IN ('SUCCEEDED','SUPERSEDED')",id=w['run_id'],error=error)
    else:
        # Persist the delayed retry message so the dispatcher can send it later.
        query(c,"INSERT INTO outbox(id,work_id,available_at) VALUES(:id,:work,now()+make_interval(secs=>:delay))",id=uid(),work=w['id'],delay=min(30,2**w['attempts']))


@celery.task(name='breakwater.execute')
def execute(wid):
    # Lock work while claiming it, ignoring messages for already completed/active tasks.
    with transaction() as c:
        w=one(c,'SELECT * FROM work WHERE id=:id FOR UPDATE',id=wid)
        if not w or w['state'] not in ['QUEUED','RETRYING']:
            return {'duplicate_delivery':True}
        if w['attempts']>=MAX_ATTEMPTS:
            mark_failed(c,w,'Retry budget exhausted',True)
            return
        # Claim a fresh attempt token and lease, and append its attempt-history row.
        query(c,"""UPDATE work SET state='RUNNING',token=token+1,attempts=attempts+1,
            heartbeat_at=now(),lease_until=now()+make_interval(secs=>:lease) WHERE id=:id""",id=wid,lease=LEASE_SECONDS)
        w=one(c,'SELECT * FROM work WHERE id=:id',id=wid)
        query(c,'INSERT INTO attempts(id,work_id,token) VALUES(:id,:work,:token)',id=uid(),work=wid,token=w['token'])
        # Reflect stage startup in the batch/run status displayed by the API.
        if w['kind']=='parse':
            query(c,"UPDATE batches SET status='PARSING' WHERE id=:id AND status IN ('RECEIVED','PARSING','FAILED')",id=w['batch_id'])
        if w['kind']=='finalize':
            query(c,"UPDATE runs SET status='FINALIZING' WHERE id=:id AND status='RUNNING'",id=w['run_id'])
        # Consume a requested one-time demo failure before the handler transaction begins.
        injected=w['fail_remaining']>0
        if injected:
            query(c,'UPDATE work SET fail_remaining=fail_remaining-1 WHERE id=:id',id=wid)
    # Keep the lease alive in a separate thread during processing or a deliberate pause.
    stop=threading.Event()
    thread=threading.Thread(target=heartbeat,args=(wid,w['token'],stop),daemon=True)
    thread.start()
    try:
        if injected:
            raise RuntimeError('Demo: injected transient failure (one attempt)')
        # Wait for release while checking that this attempt still owns the RUNNING task.
        while True:
            with transaction() as c:
                current=one(c,'SELECT pause,token,state FROM work WHERE id=:id',id=wid)
            if current['token']!=w['token'] or current['state']!='RUNNING':
                raise StaleAttempt()
            if not current['pause']:
                break
            time.sleep(0.5)
        # Run the selected pipeline handler inside the transaction that will save its result.
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
        # Discard this superseded attempt after its handler transaction has rolled back.
        log.warning('Discarded fenced attempt %s token %s',wid,w['token'])
        with transaction() as c:
            query(c,"UPDATE attempts SET state='OBSOLETE',finished_at=clock_timestamp(),error='Fencing token is no longer current' WHERE work_id=:id AND token=:token AND state='RUNNING'",id=wid,token=w['token'])
    except Exception as e:
        log.exception('Work %s failed',wid)
        with transaction() as c:
            current=one(c,'SELECT * FROM work WHERE id=:id FOR UPDATE',id=wid)
            # Only the current owner may record a failure or schedule another attempt.
            if current['token']==w['token'] and current['state']=='RUNNING':
                mark_failed(c,current,str(e)[:1000],isinstance(e,PermanentError))
    finally:
        # Always stop lease renewal when this execution path finishes.
        stop.set()
        thread.join(timeout=2)


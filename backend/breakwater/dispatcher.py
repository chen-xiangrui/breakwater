"""Transactional dispatch plus lease and workflow recovery. Safe to run multiple copies."""
import logging
import time
from breakwater.db import transaction,query,one,many,uid
from breakwater.jobs import celery,mark_failed
from breakwater.service import new_work,event

logging.basicConfig(level=logging.INFO)
log=logging.getLogger('dispatcher')


def supervise():
    with transaction() as c:
        # Keep scheduling decisions globally bounded across dispatcher processes.
        if not query(c,'SELECT pg_try_advisory_xact_lock(918273)').scalar():
            return
        expired=many(c,"SELECT * FROM work WHERE state='RUNNING' AND lease_until<now() FOR UPDATE SKIP LOCKED")
        for w in expired:
            query(c,"UPDATE attempts SET state='EXPIRED',error='Worker lease expired',finished_at=clock_timestamp() WHERE work_id=:id AND token=:token",id=w['id'],token=w['token'])
            mark_failed(c,w,'Worker lease expired; recovery scheduled a fenced replacement',False)
        # A confirmed message that was lost or consumed during a lease is safe to redeliver.
        stalled=many(c,"""SELECT w.* FROM work w WHERE w.state='QUEUED' AND NOT EXISTS
            (SELECT 1 FROM outbox o WHERE o.work_id=w.id AND (o.sent_at IS NULL OR o.sent_at>now()-interval '30 seconds')) FOR UPDATE""")
        for w in stalled:
            query(c,'INSERT INTO outbox(id,work_id) VALUES(:id,:work)',id=uid(),work=w['id'])
        for b in many(c,"""SELECT b.id FROM batches b WHERE b.status='VALIDATING'
            AND NOT EXISTS(SELECT 1 FROM work w WHERE w.batch_id=b.id AND w.kind='chunk' AND w.state<>'SUCCEEDED')"""):
            new_work(c,'batch_finalize',batch_id=b['id'])
        for r in many(c,"""SELECT r.id FROM runs r WHERE r.status='RUNNING'
            AND (SELECT count(*) FROM work w WHERE w.run_id=r.id AND w.kind='partition' AND w.state='SUCCEEDED')=r.bucket_count"""):
            new_work(c,'finalize',run_id=r['id'])
        # An expired waiver changes the current workflow even without a subsequent run.
        for case in many(c,"SELECT * FROM cases WHERE workflow='WAIVED' AND waiver_expires_at<=now() ORDER BY id FOR UPDATE SKIP LOCKED"):
            query(c,"UPDATE cases SET workflow='IN_REVIEW',version=version+1,updated_at=now() WHERE id=:id",id=case['id'])
            event(c,case['id'],'System','WAIVER_EXPIRED','24-hour waiver expired; review required',case['version'],case['version']+1)
        active=query(c,"SELECT count(*) FROM work WHERE state IN ('QUEUED','RUNNING')").scalar()
        pending=many(c,"""SELECT * FROM work WHERE state IN ('PENDING','RETRYING') AND available_at<=now()
            ORDER BY CASE WHEN kind IN ('finalize','batch_finalize') THEN 0 ELSE 1 END,created_at,id
            LIMIT :slots FOR UPDATE SKIP LOCKED""",slots=max(0,12-active))
        for w in pending:
            query(c,"UPDATE work SET state='QUEUED' WHERE id=:id",id=w['id'])


def dispatch_one():
    with transaction() as c:
        o=one(c,"""SELECT o.* FROM outbox o JOIN work w ON w.id=o.work_id
            WHERE o.sent_at IS NULL AND o.available_at<=now() AND w.state='QUEUED'
            ORDER BY o.created_at LIMIT 1 FOR UPDATE OF o SKIP LOCKED""")
        if not o:
            return False
        return publish_event(c,o)


def publish_event(c,o):
    try:
        celery.send_task('breakwater.execute',args=[o['work_id']],task_id=o['id'],retry=False)
        query(c,'UPDATE outbox SET sent_at=clock_timestamp(),attempts=attempts+1,error=NULL WHERE id=:id',id=o['id'])
    except Exception as e:
        query(c,"""UPDATE outbox SET attempts=attempts+1,error=:error,
            available_at=now()+interval '5 seconds' WHERE id=:id""",id=o['id'],error=str(e)[:300])
        return False
    return True


def main():
    from breakwater.workflows import advance_workflows
    while True:
        try:
            supervise()
            advance_workflows()
            for _ in range(12):
                if not dispatch_one():
                    break
        except Exception:
            log.exception('Dispatcher unavailable; durable work will retry')
        time.sleep(1)


if __name__=='__main__':
    main()

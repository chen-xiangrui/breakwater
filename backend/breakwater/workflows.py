"""A manual CSV pair triggers validation followed by one durable pinned run."""
import csv

from fastapi import HTTPException

from breakwater import storage
from breakwater.db import transaction, query, one, many, uid
from breakwater.domain import Manifest, INTERNAL, BROKER
from breakwater.service import (
    require, idempotent, save_key, register_upload_in_transaction,
    start_run_in_transaction,
)


def declared_row_count(key, side):
    """Count logical CSV records; this does not establish upstream completeness."""
    try:
        # Validate source headers and count CSV records, including quoted multiline records.
        with storage.path(key).open(encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f, strict=True)
            expected = INTERNAL if side == 'INTERNAL' else BROKER
            if next(reader, None) != expected:
                raise HTTPException(422, f'{side.title()} CSV headers must be: '+','.join(expected))
            count = 0
            for count, values in enumerate(reader, 1):
                if count > 1000000 or sum(len(v) for v in values) > 65536:
                    raise HTTPException(422, f'{side.title()} CSV exceeds the supported record limit')
            return count
    except (UnicodeDecodeError, csv.Error) as e:
        # Return an actionable upload error before registering an unreadable CSV pair.
        raise HTTPException(422, f'{side.title()} CSV could not be read: {e}') from None


def create_workflow(scope_id, files, complete, key):
    # Require the uploader's declaration that both files are complete replacement snapshots.
    if not complete:
        raise HTTPException(422, 'Confirm that both files are complete replacement snapshots for the selected scope')
    # Fingerprint scope, completeness, and file bytes so retries do not create new revisions.
    payload = {'scope_id': scope_id, 'complete': complete,
               'files': {side: {'sha256': f['key'], 'size': f['size']} for side, f in files.items()}}
    with transaction() as c:
        old, fp = idempotent(c, key, payload, 'workflow')
        if old:
            return require(one(c, 'SELECT * FROM workflows WHERE id=:id', id=old['resource_id']))
        # Lock the scope while allocating file revisions and registering both inputs.
        scope = require(one(c, 'SELECT * FROM scopes WHERE id=:id FOR UPDATE', id=scope_id), 'Scope')
        batches = {}
        wid = uid()
        for side in ['INTERNAL', 'BROKER']:
            # Allocate the next revision independently for the internal and broker files.
            f = files[side]
            revision = query(c, 'SELECT coalesce(max(revision),0)+1 FROM batches WHERE scope_id=:scope AND side=:side',
                             scope=scope_id, side=side).scalar()
            # Combine scope metadata with calculated file controls into an uploader manifest.
            manifest = Manifest(schema_version=1, side=side, broker_id=scope['broker_id'],
                business_date=scope['business_date'], dataset_id=scope['dataset_id'], revision=revision,
                representation='EXECUTION', account_ids=scope['account_ids'], expected_bytes=f['size'],
                expected_data_rows=f['rows'], sha256=f['key'], complete=True)
            # Register each batch and its parse task inside this shared transaction.
            batches[side] = register_upload_in_transaction(c, manifest, f['key'], f['size'], f['filename'],
                                                           f'workflow:{wid}:{side}', manifest_origin='OPERATOR')
        # Link the two batches in a workflow that waits for validated inputs.
        query(c, '''INSERT INTO workflows(id,scope_id,internal_batch_id,broker_batch_id)
            VALUES(:id,:scope,:internal,:broker)''', id=wid, scope=scope_id,
              internal=batches['INTERNAL']['id'], broker=batches['BROKER']['id'])
        # Save the request-to-workflow mapping before the transaction commits.
        save_key(c, key, fp, wid, 'workflow')
        return one(c, 'SELECT * FROM workflows WHERE id=:id', id=wid)


def advance_workflows():
    # Own transaction: do not hold unrelated case/lease locks while creating a run.
    with transaction() as c:
        pending = many(c, '''SELECT * FROM workflows WHERE status IN ('WAITING_INPUTS','WAITING_FOR_CAPACITY')
            ORDER BY created_at LIMIT 8 FOR UPDATE SKIP LOCKED''')
        for workflow in pending:
            # Read both input states; propagate blocked/failed inputs to the workflow.
            inputs = many(c, 'SELECT * FROM batches WHERE id IN (:i,:b)',
                          i=workflow['internal_batch_id'], b=workflow['broker_batch_id'])
            blocked = [b for b in inputs if b['status'] in ['BLOCKED', 'FAILED']]
            if blocked:
                query(c, "UPDATE workflows SET status='BLOCKED',error=:error WHERE id=:id", id=workflow['id'],
                      error='; '.join(f'{b["side"].title()}: {b["error"]}' for b in blocked))
                continue
            # Leave the workflow waiting until both files are ready for comparison.
            if len(inputs) != 2 or any(b['status'] != 'READY' for b in inputs):
                continue
            payload = {'scope_id': workflow['scope_id'], 'internal_batch_id': workflow['internal_batch_id'],
                       'broker_batch_id': workflow['broker_batch_id'], 'rule_version': 'execution-v1', 'bucket_count': 8}
            try:
                # A deterministic key ensures repeated polling starts only one pinned run.
                run = start_run_in_transaction(c, payload, 'workflow-run:'+workflow['id'])
            except HTTPException as e:
                message = str(e.detail)
                if e.status_code == 409 and 'WAITING_FOR_CAPACITY' in message:
                    # Keep the workflow queued for a later poll when run capacity is available.
                    query(c, "UPDATE workflows SET status='WAITING_FOR_CAPACITY',error=NULL WHERE id=:id", id=workflow['id'])
                    continue
                raise
            # Attach the created run so the UI can follow its processing and report status.
            query(c, "UPDATE workflows SET status='RUNNING',run_id=:run,error=NULL WHERE id=:id",
                  id=workflow['id'], run=run['id'])


# Join workflow, input validation, and run progress into the status returned to the UI.
WORKFLOW_QUERY = '''SELECT w.*,coalesce(r.status,w.status) AS status,
    coalesce(r.error,w.error) AS error,r.sequence AS run_sequence,r.report_status,
    i.filename AS internal_filename,i.status AS internal_status,i.error AS internal_error,
    b.filename AS broker_filename,b.status AS broker_status,b.error AS broker_error
    FROM workflows w JOIN batches i ON i.id=w.internal_batch_id JOIN batches b ON b.id=w.broker_batch_id
    LEFT JOIN runs r ON r.id=w.run_id'''

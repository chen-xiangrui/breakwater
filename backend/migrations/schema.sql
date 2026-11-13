-- Reconciliation context per broker/date/dataset, including the current published run.
CREATE TABLE scopes (
 id text PRIMARY KEY, broker_id text NOT NULL, broker_name text NOT NULL,
 business_date date NOT NULL, dataset_id text NOT NULL, account_ids jsonb NOT NULL,
 next_sequence integer NOT NULL DEFAULT 1, published_run_id text,
 UNIQUE(broker_id,business_date,dataset_id)
);
-- Expected source arrivals and deadlines are registered before files arrive.
CREATE TABLE deliveries (
 scope_id text NOT NULL REFERENCES scopes(id), side text NOT NULL CHECK(side IN ('INTERNAL','BROKER')),
 deadline timestamptz NOT NULL, arrived_at timestamptz, PRIMARY KEY(scope_id,side)
);
-- One uploaded file revision with its manifest, storage key, validation checks, and counts.
CREATE TABLE batches (
 id text PRIMARY KEY, scope_id text NOT NULL REFERENCES scopes(id), side text NOT NULL,
 revision integer NOT NULL CHECK(revision>0), filename text NOT NULL, object_key text NOT NULL,
 sha256 text NOT NULL, byte_size bigint NOT NULL, manifest jsonb NOT NULL,
 status text NOT NULL DEFAULT 'RECEIVED', generation integer NOT NULL DEFAULT 1,
 mapping_version text NOT NULL DEFAULT 'equities-v1', adapter_version text NOT NULL DEFAULT 'csv-v1',
 parsed integer NOT NULL DEFAULT 0, accepted integer NOT NULL DEFAULT 0, rejected integer NOT NULL DEFAULT 0,
 checks jsonb NOT NULL DEFAULT '{}', error text, created_at timestamptz NOT NULL DEFAULT now(), ready_at timestamptz,
 UNIQUE(scope_id,side,revision)
);
-- Accepted rows retain raw/normalized evidence and exact numeric trade values.
CREATE TABLE records (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, batch_id text NOT NULL REFERENCES batches(id),
 record_number integer NOT NULL, reconciliation_ref text NOT NULL, key_digest bigint NOT NULL,
 account text NOT NULL, instrument text NOT NULL, side text NOT NULL, currency text NOT NULL,
 quantity numeric(24,8) NOT NULL, price numeric(24,8) NOT NULL,
 normalized jsonb NOT NULL, raw jsonb NOT NULL, payload_hash text NOT NULL,
 UNIQUE(batch_id,record_number)
);
-- Support reference lookup within selected source batches during comparison.
CREATE INDEX records_key ON records(batch_id,reconciliation_ref);
-- Rejected rows retain their original values and structured validation errors.
CREATE TABLE rejections (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, batch_id text NOT NULL REFERENCES batches(id),
 record_number integer NOT NULL, raw jsonb NOT NULL, errors jsonb NOT NULL,
 UNIQUE(batch_id,record_number)
);
-- Versioned rule configuration is pinned by each reconciliation run.
CREATE TABLE rule_sets (
 id text PRIMARY KEY, config jsonb NOT NULL
);
INSERT INTO rule_sets VALUES ('execution-v1', '{"price_tolerance":"0.01","quantity_tolerance":"0","mapping_version":"equities-v1","mode":"EXECUTION","timestamp_comparison":false}');
-- A run pins two input batches and rules, and tracks comparison and report progress.
CREATE TABLE runs (
 id text PRIMARY KEY, scope_id text NOT NULL REFERENCES scopes(id), sequence integer NOT NULL,
 internal_batch_id text NOT NULL REFERENCES batches(id), broker_batch_id text NOT NULL REFERENCES batches(id),
 rule_version text NOT NULL REFERENCES rule_sets(id), code_version text NOT NULL DEFAULT '0.1.0',
 bucket_count integer NOT NULL, hash_version text NOT NULL DEFAULT 'sha256-60-v1',
 status text NOT NULL DEFAULT 'QUEUED', outcome text, metrics jsonb NOT NULL DEFAULT '{}',
 report_status text NOT NULL DEFAULT 'PENDING', report_key text, report_sha256 text,
 error text, created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, completed_at timestamptz,
 UNIQUE(scope_id,sequence)
);
-- Add this reference after runs exists because scopes and runs refer to each other.
ALTER TABLE scopes ADD CONSTRAINT published_run_fk FOREIGN KEY(published_run_id) REFERENCES runs(id);
-- Map each accepted input record into one stable bucket for a particular run.
CREATE TABLE assignments (
 run_id text NOT NULL REFERENCES runs(id), record_id bigint NOT NULL REFERENCES records(id), bucket integer NOT NULL,
 PRIMARY KEY(run_id,record_id)
);
CREATE INDEX assignments_bucket ON assignments(run_id,bucket,record_id);
-- Durable tasks: kind selects the handler; ordinal identifies its chunk or bucket.
-- Tokens and leases let replacement attempts fence off older worker executions.
CREATE TABLE work (
 id text PRIMARY KEY, kind text NOT NULL, batch_id text REFERENCES batches(id), run_id text REFERENCES runs(id),
 ordinal integer NOT NULL DEFAULT 0, payload jsonb NOT NULL DEFAULT '{}',
 state text NOT NULL DEFAULT 'PENDING', token integer NOT NULL DEFAULT 0,
 attempts integer NOT NULL DEFAULT 0, lease_until timestamptz, heartbeat_at timestamptz,
 available_at timestamptz NOT NULL DEFAULT now(), created_at timestamptz NOT NULL DEFAULT now(),
 completed_at timestamptz, error text, metrics jsonb NOT NULL DEFAULT '{}',
 pause boolean NOT NULL DEFAULT false, fail_remaining integer NOT NULL DEFAULT 0,
 UNIQUE(kind,batch_id,ordinal), UNIQUE(kind,run_id,ordinal)
);
CREATE INDEX work_queue ON work(state,available_at);
-- Preserve every claimed execution attempt separately from the task's current state.
CREATE TABLE attempts (
 id text PRIMARY KEY, work_id text NOT NULL REFERENCES work(id), token integer NOT NULL,
 state text NOT NULL DEFAULT 'RUNNING', started_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz, error text, UNIQUE(work_id,token)
);
-- Task messages awaiting dispatch, with send confirmation and retry timing/history.
CREATE TABLE outbox (
 id text PRIMARY KEY, work_id text NOT NULL REFERENCES work(id), sent_at timestamptz,
 attempts integer NOT NULL DEFAULT 0, available_at timestamptz NOT NULL DEFAULT now(), error text,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending ON outbox(available_at) WHERE sent_at IS NULL;
-- One result per run/reference, including all source evidence and the winning attempt token.
CREATE TABLE comparison_groups (
 id text PRIMARY KEY, run_id text NOT NULL REFERENCES runs(id), work_id text NOT NULL REFERENCES work(id),
 winning_token integer NOT NULL, reconciliation_ref text NOT NULL, outcome text NOT NULL,
 internal_count integer NOT NULL, broker_count integer NOT NULL, evidence jsonb NOT NULL,
 UNIQUE(run_id,reconciliation_ref)
);
CREATE INDEX groups_run ON comparison_groups(run_id,outcome,reconciliation_ref);
-- Individual issue families found within comparison groups become trackable cases.
CREATE TABLE findings (
 id text PRIMARY KEY, group_id text NOT NULL REFERENCES comparison_groups(id),
 run_id text NOT NULL REFERENCES runs(id), reconciliation_ref text NOT NULL, family text NOT NULL,
 evidence jsonb NOT NULL, fingerprint text NOT NULL,
 UNIQUE(run_id,reconciliation_ref,family)
);
-- Current issue state across runs; versions prevent stale investigator updates.
-- Waivers are tied to evidence fingerprints and expire at a recorded timestamp.
CREATE TABLE cases (
 id text PRIMARY KEY, scope_id text NOT NULL REFERENCES scopes(id), reconciliation_ref text NOT NULL,
 family text NOT NULL, workflow text NOT NULL DEFAULT 'OPEN', detection text NOT NULL DEFAULT 'PRESENT',
 assignee text, version integer NOT NULL DEFAULT 1, first_run_id text NOT NULL REFERENCES runs(id),
 last_run_id text NOT NULL REFERENCES runs(id), evidence jsonb NOT NULL, fingerprint text NOT NULL,
 waiver_fingerprint text, waiver_expires_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(scope_id,reconciliation_ref,family)
);
CREATE INDEX cases_queue ON cases(scope_id,detection,workflow,updated_at);
-- Immutable snapshots record each published run's detection/evidence for a case.
CREATE TABLE observations (
 run_id text NOT NULL REFERENCES runs(id), case_id text NOT NULL REFERENCES cases(id),
 detection text NOT NULL, evidence jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(run_id,case_id)
);
-- Append-only audit history explains system and investigator case changes.
CREATE TABLE case_events (
 id text PRIMARY KEY, case_id text NOT NULL REFERENCES cases(id), actor text NOT NULL,
 kind text NOT NULL, reason text NOT NULL, before_version integer, after_version integer NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX case_events_order ON case_events(case_id,created_at,id);
-- Map a client request key and payload fingerprint to the resource already created.
CREATE TABLE idempotency (
 key text PRIMARY KEY, fingerprint text NOT NULL, resource_id text NOT NULL, kind text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
-- Block source-row changes once their parent batch is READY.
CREATE FUNCTION protect_records() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target text;
BEGIN
 target := CASE WHEN TG_OP='DELETE' THEN OLD.batch_id ELSE NEW.batch_id END;
 PERFORM 1 FROM batches WHERE id=target AND status='READY' FOR SHARE;
 IF FOUND THEN RAISE EXCEPTION 'READY inputs are immutable'; END IF;
 IF TG_OP='UPDATE' AND OLD.batch_id<>NEW.batch_id THEN
   PERFORM 1 FROM batches WHERE id=OLD.batch_id AND status='READY' FOR SHARE;
   IF FOUND THEN RAISE EXCEPTION 'READY inputs are immutable'; END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_records BEFORE INSERT OR UPDATE OR DELETE ON records FOR EACH ROW EXECUTE FUNCTION protect_records();
-- Keep READY batch metadata immutable so pinned run inputs cannot change later.
CREATE FUNCTION protect_ready_batch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status='READY' THEN RAISE EXCEPTION 'READY batch metadata is immutable'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_batch BEFORE UPDATE OR DELETE ON batches FOR EACH ROW EXECUTE FUNCTION protect_ready_batch();
-- Reuse this guard to prevent edits/deletes of audit evidence and comparison rules.
CREATE FUNCTION append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Audit evidence is append-only'; END $$;
CREATE TRIGGER immutable_events BEFORE UPDATE OR DELETE ON case_events FOR EACH ROW EXECUTE FUNCTION append_only();
CREATE TRIGGER immutable_observations BEFORE UPDATE OR DELETE ON observations FOR EACH ROW EXECUTE FUNCTION append_only();
CREATE TRIGGER immutable_rules BEFORE UPDATE OR DELETE ON rule_sets FOR EACH ROW EXECUTE FUNCTION append_only();
CREATE TRIGGER immutable_groups BEFORE UPDATE OR DELETE ON comparison_groups FOR EACH ROW EXECUTE FUNCTION append_only();
CREATE TRIGGER immutable_findings BEFORE UPDATE OR DELETE ON findings FOR EACH ROW EXECUTE FUNCTION append_only();
-- Preserve a run's selected inputs and prevent changes to published outcomes or metrics.
CREATE FUNCTION protect_run_inputs() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.internal_batch_id,NEW.broker_batch_id,NEW.rule_version,NEW.bucket_count,NEW.scope_id,NEW.sequence)
 IS DISTINCT FROM (OLD.internal_batch_id,OLD.broker_batch_id,OLD.rule_version,OLD.bucket_count,OLD.scope_id,OLD.sequence)
 THEN RAISE EXCEPTION 'Run inputs are immutable'; END IF;
 IF OLD.status IN ('SUCCEEDED','SUPERSEDED') AND
 (NEW.status,NEW.metrics,NEW.outcome) IS DISTINCT FROM (OLD.status,OLD.metrics,OLD.outcome)
 THEN RAISE EXCEPTION 'Published results are immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_run BEFORE UPDATE ON runs FOR EACH ROW EXECUTE FUNCTION protect_run_inputs();


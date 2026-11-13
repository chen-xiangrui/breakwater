"""Guard terminal evidence and distinguish test scopes."""
from alembic import op

revision='002'
down_revision='001'


def upgrade():
    # Keep integration, recovery, and benchmark scopes out of the normal UI list.
    op.execute('ALTER TABLE scopes ADD COLUMN hidden boolean NOT NULL DEFAULT false')
    op.execute("UPDATE scopes SET hidden=true WHERE broker_name IN ('Integration test','Worker recovery test','Benchmark')")
    # Prevent workers from adding comparison evidence after a run is complete.
    op.execute('''CREATE FUNCTION protect_published_insert() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      PERFORM 1 FROM runs WHERE id=NEW.run_id AND status IN ('SUCCEEDED','SUPERSEDED') FOR SHARE;
      IF FOUND THEN RAISE EXCEPTION 'Completed run evidence is immutable'; END IF;
      RETURN NEW;
    END $$''')
    # Attach the completed-run guard to every table containing partition results/assignments.
    for table in ['comparison_groups','findings','assignments']:
        op.execute(f'CREATE TRIGGER protect_published_insert BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION protect_published_insert()')
    # Extend immutable evidence protection to assignments and READY-batch rejections.
    op.execute('CREATE TRIGGER immutable_assignments BEFORE UPDATE OR DELETE ON assignments FOR EACH ROW EXECUTE FUNCTION append_only()')
    op.execute('CREATE TRIGGER immutable_rejections BEFORE INSERT OR UPDATE OR DELETE ON rejections FOR EACH ROW EXECUTE FUNCTION protect_records()')


def downgrade():
    # These protections form part of the persisted audit contract and are forward-only.
    raise RuntimeError('Audit protection migrations are forward-only')


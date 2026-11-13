"""Manual CSV pair uploads and durable workflow orchestration."""
from alembic import op

revision = '003'
down_revision = '002'


def upgrade():
    # Distinguish supplied producer manifests from controls generated for operator uploads.
    op.execute("ALTER TABLE batches ADD COLUMN manifest_origin text NOT NULL DEFAULT 'PRODUCER' CHECK (manifest_origin IN ('PRODUCER','OPERATOR'))")
    # Link an uploaded CSV pair to the single run created after both batches are READY.
    op.execute('''CREATE TABLE workflows (
        id text PRIMARY KEY,
        scope_id text NOT NULL REFERENCES scopes(id),
        internal_batch_id text NOT NULL REFERENCES batches(id),
        broker_batch_id text NOT NULL REFERENCES batches(id),
        status text NOT NULL DEFAULT 'WAITING_INPUTS',
        run_id text UNIQUE REFERENCES runs(id),
        error text,
        created_at timestamptz NOT NULL DEFAULT now()
    )''')
    # Support polling waiting workflows in creation order.
    op.execute('CREATE INDEX workflows_pending ON workflows(status,created_at)')


def downgrade():
    # Keep durable upload/run links available rather than deleting workflow history.
    raise RuntimeError('Preserve uploaded inputs and workflow history')


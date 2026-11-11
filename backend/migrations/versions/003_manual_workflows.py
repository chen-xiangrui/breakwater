"""Manual CSV pair uploads and durable workflow orchestration."""
from alembic import op

revision = '003'
down_revision = '002'


def upgrade():
    op.execute("ALTER TABLE batches ADD COLUMN manifest_origin text NOT NULL DEFAULT 'PRODUCER' CHECK (manifest_origin IN ('PRODUCER','OPERATOR'))")
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
    op.execute('CREATE INDEX workflows_pending ON workflows(status,created_at)')


def downgrade():
    raise RuntimeError('Preserve uploaded inputs and workflow history')


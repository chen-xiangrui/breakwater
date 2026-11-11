"""Initial durable reconciliation schema."""
from pathlib import Path
from alembic import op

revision = "001"
down_revision = None


def upgrade():
    op.get_bind().exec_driver_sql((Path(__file__).parents[1] / "schema.sql").read_text())


def downgrade():
    raise RuntimeError("Preserve audit evidence: destructive downgrade is intentionally unsupported")


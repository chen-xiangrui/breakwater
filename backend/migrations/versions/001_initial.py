"""Initial durable reconciliation schema."""
from pathlib import Path
from alembic import op

# First migration in the ordered Alembic revision chain.
revision = "001"
down_revision = None


def upgrade():
    # Install the initial tables, indexes, rules, and evidence-protection triggers.
    op.get_bind().exec_driver_sql((Path(__file__).parents[1] / "schema.sql").read_text())


def downgrade():
    # Preserve uploaded and audit data by rejecting destructive reversal of this schema.
    raise RuntimeError("Preserve audit evidence: destructive downgrade is intentionally unsupported")


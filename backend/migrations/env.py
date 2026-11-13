# Alembic runs schema upgrades using the same environment-configured engine as the app.
from alembic import context
from breakwater.db import engine

with engine.connect() as connection:
    # Bind migration execution to a managed connection and transactional upgrade scope.
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


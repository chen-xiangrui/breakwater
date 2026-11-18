"""Initialize an empty database from the current schema without version tracking."""
from importlib.resources import files

from breakwater.db import transaction, query


# Create the complete schema once, preserving existing databases on repeated startup.
def initialize():
    with transaction() as c:
        # Serialize concurrent startup processes until schema creation commits.
        query(c, 'SELECT pg_advisory_xact_lock(918275)')
        if query(c, "SELECT to_regclass('scopes')").scalar() is not None:
            return

        schema = files('breakwater').joinpath('schema.sql').read_text()
        c.exec_driver_sql(schema)


if __name__ == '__main__':
    initialize()

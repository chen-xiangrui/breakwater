import json
import os
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine, text

# Read connection settings from the environment; keep a bounded pool per engine/process.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://breakwater:breakwater@localhost:5432/breakwater")

# Retain four pooled connections, allow two overflow connections, and check for stale ones.
# PostgreSQL cancels statements after 60 seconds and lock waits after 10 seconds.
engine = create_engine(DATABASE_URL, pool_size=4, max_overflow=2, pool_pre_ping=True,
                       connect_args={"options": "-c statement_timeout=60000 -c lock_timeout=10000"})


# Generate a unique string ID for a stored entity.
def uid():
    return str(uuid4())


# Serialize values as stable JSON, converting unsupported types to strings.
def dumps(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


# Execute SQL with bound named parameters.
def query(conn, sql, **params):
    return conn.execute(text(sql), params)


# Return the first query result as a dictionary, or None.
def one(conn, sql, **params):
    row = query(conn, sql, **params).mappings().first()
    return dict(row) if row else None


# Return all query results as a list of dictionaries.
def many(conn, sql, **params):
    return [dict(r) for r in query(conn, sql, **params).mappings()]


# Yield a database connection and commit on success or roll back on failure.
@contextmanager
def transaction():
    with engine.begin() as conn:
        # Lend the connection to the caller's with block while this helper is paused.
        yield conn


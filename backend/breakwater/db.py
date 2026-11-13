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


def uid():
    # Generate unique string IDs for scopes, batches, tasks, and other durable entities.
    return str(uuid4())


def dumps(value):
    # Serialize evidence and payloads as stable JSON; dates and decimals become strings.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def query(conn, sql, **params):
    # Bind keyword values to named SQL placeholders such as :id without string interpolation.
    return conn.execute(text(sql), params)


def one(conn, sql, **params):
    # Read the first result as named columns, then return a dictionary or None.
    row = query(conn, sql, **params).mappings().first()
    return dict(row) if row else None


def many(conn, sql, **params):
    # Materialize all result rows as dictionaries; no matches produce an empty list.
    return [dict(r) for r in query(conn, sql, **params).mappings()]


@contextmanager
def transaction():
    # Borrow a connection and let engine.begin() commit or roll back when the caller exits.
    with engine.begin() as conn:
        # Lend the connection to the caller's with block while this helper is paused.
        yield conn


import json
import os
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://breakwater:breakwater@localhost:5432/breakwater")
engine = create_engine(DATABASE_URL, pool_size=4, max_overflow=2, pool_pre_ping=True,
                       connect_args={"options": "-c statement_timeout=60000 -c lock_timeout=10000"})


def uid():
    return str(uuid4())


def dumps(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def query(conn, sql, **params):
    return conn.execute(text(sql), params)


def one(conn, sql, **params):
    row = query(conn, sql, **params).mappings().first()
    return dict(row) if row else None


def many(conn, sql, **params):
    return [dict(r) for r in query(conn, sql, **params).mappings()]


@contextmanager
def transaction():
    with engine.begin() as conn:
        yield conn


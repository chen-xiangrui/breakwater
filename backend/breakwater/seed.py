# Populate the initial demo workspace after database migrations have completed.
from breakwater.db import transaction, query


def seed():
    with transaction() as c:
        # Repeated startup preserves the existing scope rather than resetting its history.
        query(c,"""INSERT INTO scopes(id,broker_id,broker_name,business_date,dataset_id,account_ids)
            VALUES ('demo-scope','BRK01','Northstar Securities','2026-11-06','equities_accounts_01','["ACC01","ACC02"]')
            ON CONFLICT DO NOTHING""")
        # Register one expected arrival/deadline per source without changing prior arrivals.
        query(c,"""INSERT INTO deliveries(scope_id,side,deadline)
            VALUES ('demo-scope','INTERNAL','2026-11-06T22:00:00Z'),('demo-scope','BROKER','2026-11-06T22:30:00Z')
            ON CONFLICT DO NOTHING""")


if __name__=='__main__':
    # The Compose migrate service invokes this entry point after alembic upgrade head.
    seed()

from breakwater.db import transaction, query


def seed():
    with transaction() as c:
        query(c,"""INSERT INTO scopes(id,broker_id,broker_name,business_date,dataset_id,account_ids)
            VALUES ('demo-scope','BRK01','Northstar Securities','2026-11-06','equities_accounts_01','["ACC01","ACC02"]')
            ON CONFLICT DO NOTHING""")
        query(c,"""INSERT INTO deliveries(scope_id,side,deadline)
            VALUES ('demo-scope','INTERNAL','2026-11-06T22:00:00Z'),('demo-scope','BROKER','2026-11-06T22:30:00Z')
            ON CONFLICT DO NOTHING""")


if __name__=='__main__':
    seed()

"""Create daily reconciliation scopes and their expected deliveries atomically."""
from datetime import date
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from breakwater.db import transaction, query, one, many, uid, dumps
from breakwater.service import conflict, idempotent, save_key


# Reusable validation limits for scope identifiers and account coverage entries.
Identifier = Annotated[str, Field(min_length=1, max_length=80)]
Account = Annotated[str, Field(min_length=1, max_length=128)]


class ScopeCreate(BaseModel):
    # Trim strings and reject unknown request fields; delivery deadlines require timezones.
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)

    broker_id: Identifier
    broker_name: Annotated[str, Field(min_length=1, max_length=160)]
    business_date: date
    dataset_id: Identifier
    account_ids: list[Account] = Field(min_length=1, max_length=100)
    internal_deadline: AwareDatetime
    broker_deadline: AwareDatetime

    @field_validator('account_ids')
    @classmethod
    def unique_accounts(cls, accounts):
        # Reject duplicate coverage entries and sort accounts for stable request fingerprints.
        if len(set(accounts)) != len(accounts):
            raise ValueError('Account IDs must be unique')
        return sorted(accounts)


class ScopeResponse(BaseModel):
    # Describe scope metadata and the optional published-run summary in API responses.
    id: str
    broker_id: str
    broker_name: str
    business_date: date
    dataset_id: str
    account_ids: list[str]
    published_run_id: str | None = None
    published_status: str | None = None
    published_sequence: int | None = None
    metrics: dict | None = None


def create_scope(data: ScopeCreate, key: str):
    # Serialize validated input so a retried creation request can reuse its saved result.
    payload = data.model_dump(mode='json')
    with transaction() as c:
        previous, fingerprint = idempotent(c, key, payload, 'scope')
        if previous:
            return one(c, 'SELECT * FROM scopes WHERE id=:id', id=previous['resource_id']), False

        # The unique broker/date/dataset key also protects concurrent creation requests.
        scope = one(c, '''INSERT INTO scopes(id,broker_id,broker_name,business_date,dataset_id,account_ids)
            VALUES(:id,:broker_id,:broker_name,:business_date,:dataset_id,CAST(:accounts AS jsonb))
            ON CONFLICT(broker_id,business_date,dataset_id) DO NOTHING RETURNING *''',
            id=uid(), broker_id=data.broker_id, broker_name=data.broker_name,
            business_date=data.business_date, dataset_id=data.dataset_id,
            accounts=dumps(data.account_ids))
        created = scope is not None
        deadlines = {'INTERNAL': data.internal_deadline, 'BROKER': data.broker_deadline}
        if created:
            # Save both expected deliveries in the same transaction as the new scope.
            for side, deadline in deadlines.items():
                query(c, 'INSERT INTO deliveries(scope_id,side,deadline) VALUES(:id,:side,:deadline)',
                      id=scope['id'], side=side, deadline=deadline)
        else:
            # Lock the existing scope and compare its setup with the requested configuration.
            scope = one(c, '''SELECT * FROM scopes WHERE broker_id=:broker AND business_date=:day
                AND dataset_id=:dataset FOR UPDATE''', broker=data.broker_id,
                day=data.business_date, dataset=data.dataset_id)
            existing_deadlines = {d['side']: d['deadline'] for d in many(c,
                'SELECT side,deadline FROM deliveries WHERE scope_id=:id', id=scope['id'])}
            if (scope['hidden'] or scope['broker_name'] != data.broker_name
                    or sorted(scope['account_ids']) != data.account_ids or existing_deadlines != deadlines):
                conflict('A scope already exists for this broker, business date, and dataset with different details. Select the existing scope.')
        # Reuse matching scopes, reject conflicting details, and remember this request key.
        save_key(c, key, fingerprint, scope['id'], 'scope')
        return scope, created

# Breakwater

An end-of-day trade reconciliation workspace: receive complete CSV deliveries, compare pinned input revisions, investigate explainable discrepancies, and rerun without losing history.

Built from [the project plan](trade-reconciliation-project-plan.md). The implemented scope is execution-to-execution cash-equity reconciliation with the core delivery, recovery, and investigation workflow.

## Run locally

Install Docker Desktop (or Docker Engine with Compose), start Docker, then:

```sh
docker compose up --build -d
```

- **Application:** http://localhost:3000
- **OpenAPI:** http://localhost:8000/docs
- **RabbitMQ management:** http://localhost:15672 — `breakwater` / `breakwater`

The migration service creates the schema and an empty Northstar Securities scope for **6 November 2026**, covering `ACC01` and `ACC02`. No market-data service or cloud credentials are required. First startup downloads container images and dependencies. Named volumes preserve the database and immutable objects across restarts.

In the **Reconciliation** workspace, choose an **Internal trades CSV** and a **Broker trades CSV**, confirm that they are complete snapshots, and press **Run workflow**. Validation, reconciliation, and publication then run automatically. No separate manifest is required for this manual path.

For a quick demo, select `fixtures/small/internal.csv` and `fixtures/small/broker.csv` (60 baseline trades per side), or the pair in `fixtures/medium/` (10,000 baseline trades per side). Each directory also includes corrected CSVs, independent manifests for the advanced path, expected results, and injected-change details.

Under **Advanced options → Demo & recovery tools → Demo scenarios**, **A day with discrepancies** loads the original 240-trade scenario. Use **Existing inputs & manifests → Run selected inputs** to reconcile those deliveries. The result is **241 comparison groups: 225 matched, 10 mismatches, 3 missing, and 3 duplicate groups**, producing 16 investigation cases. The counts distinguish groups, cases, and source records.

Choose **Corrected replacement files** and explicitly run again to observe clearing, preserved notes/assignments, and the broker-only key becoming *not present in inputs* rather than falsely matched.

## Workspace

The sidebar has two destinations: **Reconciliation** and **Run history**. Reconciliation keeps paired CSV uploads, delivery revisions/readiness, results, and the full investigation table together. The table retains reference and case ID, discrepancy type, separate instrument/account/side, differences, status, owner, pagination, and all filters. Its default view includes all cases, as in the original workflow.

**Run history** lists previous runs; selecting one opens its complete comparison table with separate internal and broker row counts, original evidence, previous-run differences, and report downloads. **Processing details** expands partition progress and pinned inputs. Demo controls and manifest uploads are under **Advanced options**. Investigator identity is selected inside the case drawer. Navigation and visual changes do not alter reconciliation, publication, or case-action rules.

## What is implemented

- React/TypeScript operations dashboard, revision selectors, uploads, validation inspection, pinned-run review, partition progress, result browsing, investigation drawer, audit timeline, run comparison, CSV reports, and mobile layout.
- Producer manifests: expected hash, byte/row counts, schema, account coverage, completeness, explicit empty files, and rejected source records.
- Content-addressed files and chunk artifacts; PostgreSQL triggers protect READY inputs and completed evidence.
- `NUMERIC(24,8)` / Python `Decimal`; canonical identities, categorical comparisons, exact quantity, and inclusive absolute price tolerance `0.01`.
- CSV ingestion in logical records, bounded 5,000-record chunks, stable SHA-256 partitions, and duplicate-safe PostgreSQL full outer joins.
- Celery/RabbitMQ with late acknowledgements, worker-loss rejection, publisher confirms, transactional outbox, database leases, heartbeats, bounded retry attempts, and fencing.
- Atomic publication, scope sequence ordering, stable cases, immutable observations, optimistic investigator edits, reasoned 24-hour waivers, and independent report recovery.
- Synthetic fixtures, integration tests against real PostgreSQL/RabbitMQ, browser tests, a real SIGKILL recovery test, and a benchmark harness.

## Use your own files

Use the two-file **Upload files** form to upload both CSVs and trigger the complete workflow. Revisions, logical row counts, checksums, and sizes are assigned automatically. Your completeness checkbox is recorded as an **uploader declaration**; calculated file controls do not establish upstream business completeness.

Open **Advanced options → Existing inputs & manifests** for revision selection, validation details, and uploads with an independently supplied JSON manifest. The exact headers and examples are in [fixtures](fixtures/). Uploads are limited to 50 MiB and one million logical records. The demo supports the configured `BRK01` / `2026-11-06` / `equities_accounts_01` scope and the explicit reference-data allowlist in [domain.py](backend/breakwater/domain.py).

Identical content and manifest under the same revision reuse the existing batch. Different content under the same revision returns HTTP 409. Corrections require a higher/new revision; source rows cannot be edited from the UI. The manifest is a producer declaration of completeness, not proof of its upstream trade capture.

Manual workflow requests use `POST /api/workflows` with multipart `internal_file`, `broker_file`, `scope_id`, and `complete=true`; status is durable at `GET /api/workflows/{id}`. The dispatcher starts one pinned run after both inputs are READY. Rejected inputs block that workflow.

Job-creation API requests require `Idempotency-Key`. The same key and payload return the same resource; a changed payload returns 409. A deliberately repeated run uses a new key. The API documentation contains the request schemas.

## Verify

Run backend domain and live integration tests with the stack running:

```sh
docker compose exec -T api pytest -q
```

The tests create isolated, hidden scopes; they retain evidence rather than deleting it. They need the real dispatcher and worker processes and do not rely on Celery eager mode or SQLite.

Browser checks use Node 22+ and Chrome. On macOS, the default Chrome path is configured; on Linux set `CHROME_PATH` to the installed Chrome/Chromium executable.

```sh
cd frontend
npm ci
npm run build
npm run test:e2e
```

The Playwright configuration and tests use CommonJS to avoid the older Playwright ESM loader hanging under Node 26. Browser tests append demo revisions in the visible workspace, test Alice/Bob conflicts in separate contexts, and retain screenshots/traces under `frontend/test-results` on failure.

For a **real worker termination**, from the repository root:

```sh
python3 scripts/verify-recovery.py
```

This starts an isolated run, pauses a partition, sends SIGKILL **only to the Breakwater worker**, starts the worker again, verifies a new fencing token, releases the partition, and checks that the results and audit events are not duplicated. Do not run it during other verification jobs.

Measure larger inputs:

```sh
docker compose exec -T api python tests/benchmark.py --rows 10000
docker compose exec -T api python tests/benchmark.py --rows 100000
```

Regenerate source fixtures:

```sh
docker compose exec -T api python -m breakwater.demo --output fixtures --rows 240
docker compose exec -T api python -m breakwater.demo --output fixtures --demo-sizes
```

For frontend development, run `npm run dev` in `frontend`; Vite proxies `/api` to the Compose API. Rebuild backend services after Python changes. Test files and fixtures are mounted into the API container for local iteration.

## Design and remaining scope

Read [architecture and invariants](docs/architecture.md), the [walkthrough](docs/demo-script.md), [verification evidence](docs/verification.md), and [measured performance](docs/benchmark-report.md).

This is a local interview application. Alice/Bob are deliberately seeded demo identities, not authentication. Compose binds exposed ports to loopback. `DEMO_MODE=false` disables fixture/failure controls and demo case writes; production authorization is not implemented.

Deferred as permitted by the plan: block-to-fills matching, a second broker format, S3, production SSO, and the million-row/1–2–4 worker scaling study. Additional implementation limits are documented in the architecture notes. No cloud deployment has been created.

Stop services while keeping data:

```sh
docker compose stop
```

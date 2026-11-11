# Verification record

Verified locally on **9 November 2026**, with manual CSV workflow and demo-dataset checks on **10 November 2026**, against the running Docker Compose stack.

## Backend

The suite contains **40 tests** covering:

- Exact decimal boundaries, finite/positive quantities, fixed precision/scale, large exact notional arithmetic, categorical mismatches, and cross-currency notional suppression.
- Both duplicate families, all participating evidence, missing-side semantics, stable evidence fingerprints, reference-data validation, timestamp normalization, and spreadsheet formula neutralization.
- Actual HTTP file uploads and background ingestion using PostgreSQL, RabbitMQ, Celery workers, and the outbox dispatcher.
- Missing-file gating, explicit empty files, corrupted manifests, rejected records, same-revision conflicts, file idempotency, and run idempotency-key conflicts.
- Two concurrent investigator edits: one HTTP 200 and one HTTP 409, preserving the winning owner.
- Correction reruns, immutable prior results, preserved ownership/notes, cleared observations, and absent-key review.
- Database-enforced immutability, result coverage, order/partition independence, and agreement with an independent small Python reference classification.
- A deliberately failed partition preserving the previous published projection; older completion becoming SUPERSEDED.
- Stale work completion rolling back its transaction after a replacement token wins.
- An injected sender connection failure retaining an unsent outbox event, and an actual RabbitMQ-confirmed send followed by a database rollback/redelivery without duplicated effects.

The five manual-workflow integration checks additionally exercise both the 60- and 10,000-baseline-trade sizes, exactly one pinned run under duplicate submission, changed-payload conflicts, provenance of uploader declarations, rejected-input blocking, required inputs/completeness, incorrect headers, and an explicitly empty broker CSV.

The eight new fixture CSVs (original and corrected pairs in both sizes) were checked against their manifests for SHA-256, byte counts, and data-row counts.

Command: `docker compose exec -T api pytest -q`.

## Browser

**4 Playwright tests passed** in local Chrome for the compact sidebar layout and full operational tables:

1. Manual paired CSV uploads start only on the explicit workflow action; navigating the two sidebar destinations preserves selected files. The original case-table columns, case IDs, account/side detail, default all-cases filter, complete comparison-table columns, Alice/Bob edit conflicts, and corrected-run history are verified.
2. A 390 × 844 mobile viewport has no document-level horizontal overflow, retains every table column, and exposes exactly the same two navigation destinations. Wide tables scroll within their containers.
3. Independent manifest uploads, revision selection, and validation evidence remain accessible through Advanced options.
4. Rejected manual CSVs expose their original invalid values without changing the published reconciliation.

The React production build also passed TypeScript checking and Vite compilation. Desktop and mobile screenshots were visually inspected. Test artifacts are generated under `frontend/test-results/` and are excluded from version control.

## Real worker termination

`python3 scripts/verify-recovery.py` completed with **PASS**. It sent SIGKILL to the actual worker container during an in-flight partition and restarted it.

Observed proof for run `4a811112-db72-4e92-86ad-c8d2e39d720f`:

| Check | Observed result |
| --- | --- |
| Original attempt | Token 1, EXPIRED, “Worker lease expired” |
| Replacement attempt | Token 2, SUCCEEDED |
| Source-record coverage | 2,002 accepted records |
| Comparison groups | 1,001, all unique |
| Matched groups | 985 |
| Findings | 16 |
| Case events before task redelivery | 16 |
| Case events after task redelivery | 16 |

This is an actual process-death test before completion. Post-commit safety is exercised by real task redelivery and a confirmed-send/database-rollback test; a precisely timed process kill between result commit and broker acknowledgement was not separately injected. Whole-database and whole-broker outages, competing finalizer processes, and all possible timing permutations are not exhaustively established by this suite.

Performance observations and their measurement limits are in [the benchmark report](benchmark-report.md). Scope limits and operational tradeoffs are in [architecture.md](architecture.md).

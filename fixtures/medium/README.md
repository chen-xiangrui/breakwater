# Medium demo dataset

10,000 baseline trades per side for BRK01 / 2026-11-06 / equities_accounts_01 (ACC01 and ACC02).

1. Choose `internal.csv` and `broker.csv` in Breakwater's **Upload files** form.
2. Confirm both files are complete snapshots, then press **Run workflow**.
3. Expect 10,001 comparison groups: 9,985 matched, 10 mismatches, 3 missing, and 3 duplicate groups, producing 16 cases.
4. Upload `corrected-internal.csv` and `corrected-broker.csv` and run again. Expect 10,000 matched groups and no new findings. Existing case history is preserved; the vanished broker-only reference needs review.

CSV-only uploads do not require the accompanying manifest files. The `.manifest.json` sidecars support the advanced upload dialog; update their revision if it has already been used in your workspace.

Exact counts are in `expected-results.json`; individual injected differences are listed in `injected-changes.json`.

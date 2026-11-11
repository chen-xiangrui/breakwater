# Demo CSV datasets

Upload each pair in Breakwater's **Upload files** form, confirm complete snapshots, and press **Run workflow**. The selected scope should be Northstar / BRK01, 6 November 2026, accounts ACC01 and ACC02.

| Dataset | Baseline trades per side | Rows in each discrepancy CSV | Expected matched groups | Findings |
| --- | ---: | ---: | ---: | ---: |
| [Small](small/README.md) | 60 | 61 | 45 | 16 |
| [Medium](medium/README.md) | 10,000 | 10,001 | 9,985 | 16 |

For either size:

- First run: choose `internal.csv` and `broker.csv` from that directory.
- Correction rerun: choose `corrected-internal.csv` and `corrected-broker.csv` from the same directory. Every baseline trade then matches.
- `expected-results.json` records the expected totals; `injected-changes.json` lists the deliberate differences.
- `.manifest.json` sidecars are optional for the main CSV-only flow and support the advanced manifest upload dialog. Their revision numbers may need updating if used more than once.

The original 240-trade examples remain in this directory, including corrupt, rejected, and explicitly empty broker files for validation demonstrations.

Regenerate both demo sizes without changing those original examples:

```sh
docker compose exec -T api python -m breakwater.demo --output fixtures --demo-sizes
```

# Ten-minute walkthrough

## Manual CSV demo

Use `fixtures/small/` for 60 baseline trades per side and `fixtures/medium/` for 10,000. Both include the same 16 illustrative findings and corrected replacements.

1. In the workspace, select `internal.csv` and `broker.csv` in the **Upload files** form. Selecting files alone does not start processing.
2. Confirm that both are complete snapshots for the displayed scope, then press **Run workflow**. No manifest file is needed. Watch validation, automatic run creation, partition progress, publication, and report readiness.
3. Inspect the discrepancies. Small: 61 groups / 45 matched / 16 findings. Medium: 10,001 groups / 9,985 matched / 16 findings.
4. Close the case drawer and select `corrected-internal.csv` and `corrected-broker.csv` from the same dataset. Press **Run workflow** again. All baseline trades now match and the prior case history remains visible.
5. Review the uploader declaration in a batch's validation detail. The server-calculated file controls are distinguished from independently supplied producer controls.

## Infrastructure and investigation walkthrough

1. **Missing delivery.** On a fresh checkout, the workspace has two overdue expected deliveries and no trade findings. Load *Internal delivery only*. It becomes Ready, but the run button stays disabled because no broker file has arrived.
2. **Receive complete inputs.** Open **Advanced options → Demo & recovery tools → Demo scenarios → A day with discrepancies**. Both CSVs and their independent manifests are registered as new revisions. Use **Existing inputs & manifests → View validation** to inspect hash/size/row/coverage controls. Alternatively upload the files in `fixtures/` with their matching manifests.
3. **Pin the run.** Open **Existing inputs & manifests** and select **Run selected inputs**. Review exact revisions, checksums, batch IDs, rule version, and eight partitions. Check *Inject one transient failure* to demonstrate a bounded retry. Submit explicitly.
4. **Watch real progress.** Expand **Processing details** to see committed partition states and attempts. Findings are provisional until publication. The result is 241 groups / 482 rows / 16 cases, with 225 matched groups. Download the report once its independent job completes.
5. **Investigate.** Use the filter button above the discrepancy list and select Field mismatch. Inspect T1003 for price, T1007 for quantity, T1015 for account, T1028 for currency, T1011 for missing broker, T1031 for an internal duplicate, or B9001 for missing internally. Expand source lineage to view raw/normalized values and logical record numbers. Duplicate groups retain every row.
6. **Demonstrate conflicting edits.** Copy the case URL to a second browser context. Select Bob inside that case drawer. Load the case in both contexts before claiming it as Alice. Bob's stale claim returns “Case changed; review latest values.” Reload latest values explicitly; no claim is silently overwritten. Add a note as Alice.
7. **Correct and rerun.** Load *Corrected replacement files*. Review the new revisions and start a new run. The prior run remains immutable. The new run has 240 matches and zero findings. The original mismatch case is resolved, with the same ID, assignee, and note. B9001 is absent from both inputs, so it requires review. Open **Run history** in the sidebar, select the newer run, then **Compare previous**.
8. **Real crash.** Run `python3 scripts/verify-recovery.py` from the repository root. It pauses work in an isolated hidden scope, kills the actual worker container, restarts it, waits for lease recovery, releases the replacement attempt, and validates unique results and stable event counts after redelivery. The local failure panel does not pretend a pause is a crash.
9. **Bad data.** Load *Truncated broker delivery* or *Invalid decimal record*. The latest broker revision is blocked, the previous published results remain available, and no authoritative run starts from the invalid pair. You can explicitly choose an earlier Ready revision.
10. **Explain the boundary.** Show the benchmark report, one invariant in the architecture notes, and the documented deferred features. These are synthetic executions and demo identities, not production broker connectivity or SSO.

Repeat scenarios by adding revisions; no reset or evidence deletion is required. The *Internal delivery only* scenario leaves previously received broker data intact, so use a fresh empty workspace to demonstrate a genuinely missing broker.

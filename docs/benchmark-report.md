# Local performance measurements

Measured on **9 November 2026** with the implemented execution matcher. These are single observed runs, not service-level targets or a scaling claim.

Environment: Docker Desktop Linux VM, `aarch64`, Linux `6.12.76-linuxkit`, 10 visible logical CPUs, 8,126,672 KiB VM memory (about 7.75 GiB). PostgreSQL 17.5, RabbitMQ 4.1.0, Python 3.12, Celery 5.5.3. The worker used **2 prefork processes**, 5,000-record chunks, 8 reconciliation partitions, and a 12-work dispatch window. No per-container CPU or memory cgroup quota was configured. The API and browser workspace remained available during measurement.

| Observed metric | 10,000 baseline rows/side | 100,000 baseline rows/side |
| --- | ---: | ---: |
| Upload registration, including fixture generation | 0.034 s | 0.267 s |
| Remaining ingestion after registration | 4.025 s | 16.088 s |
| Run request → published result | 3.055 s | 14.650 s |
| Publication → report ready | 1.241 s | 2.433 s |
| Finalizer validation/publication handler | 33.66 ms | 236.33 ms |
| Sampled run-detail API latency, median | 2.614 ms | 2.512 ms |
| Sampled run-detail API latency, maximum | 8.905 ms | 9.180 ms |
| Sampled peak database connections | 5 | 6 |
| Accepted source records | 20,002 | 200,002 |
| Comparison groups | 10,001 | 100,001 |
| Matched groups | 9,985 | 99,985 |
| Investigation findings | 16 | 16 |
| Report status | READY | READY |

Each fixture begins with a matched baseline and injects the same explainable discrepancies, including duplicates and one broker-only key. That is why source-record and group totals exceed the baseline.

Wall times include the one-second dispatcher cadence, scheduling, and polling. `publication_ms` measures the finalizer handler including validation and the case-projection writes; it is not a standalone database commit-latency measurement. API latency was sampled roughly twice per second through the internal Compose API, not over the public Internet or from a browser paint. The 100k run overlapped the start of the small browser acceptance workflow; these results are indicative, not isolated laboratory timings.

Raw machine-readable observations, run IDs, and summed successful-attempt durations are retained in [10k results](measurements/10000.json) and [100k results](measurements/100000.json). Attempt finish timestamps use PostgreSQL `clock_timestamp()`; transaction-start `now()` would understate long-running work duration.

The 100k run used 26.538 summed worker-seconds for normalization chunks, 18.620 for partition comparison, 2.971 for assignment preparation, and 1.952 for report generation. Parallel work overlaps, so summed worker duration is not wall time. The matcher still performs per-group evidence inserts and ingestion performs per-record validation/inserts. COPY, batched result inserts, and partition-index tuning are reasonable next experiments; no unmeasured speedup is claimed.

Not measured: peak worker RSS, queue-depth time series, warm/cold repetitions, confidence intervals, 1/2/4-worker comparisons, alternate chunk sizes, 1m rows, or realistic highly skewed production datasets. These remain the plan’s later performance milestone.

Reproduce with the stack idle except for normal UI polling:

```sh
docker compose exec -T api python tests/benchmark.py --rows 10000
docker compose exec -T api python tests/benchmark.py --rows 100000
```

The harness creates hidden scopes and retains their inputs, results, and report artifacts for inspection. It does not reset or delete the visible demo workspace.

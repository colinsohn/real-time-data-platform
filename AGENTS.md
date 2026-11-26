# Agent Guidelines (`real-time-data-platform`)

Updated November 26, 2026 (Pacific).

1. **Repository Ownership**: Managed by **Autonomous Portfolio Engineering** under `/Users/colinsohn/Documents/ChatGPT/Career/portfolio-build/real-time-data-platform`. Always check `git status -sb` and `WORKLOG.md` before starting a milestone.
2. **Zero Cloud/API Cost**: Execute all tests, SQL transformations, and benchmarks locally using embedded DuckDB (or SQLite fallback) and the deterministic partitioned Kafka log engine. Never invoke billable GCP BigQuery APIs without explicit user authorization.
3. **Verification Before Commit**:
   ```sh
   PYTHONPATH=src python3 -B -m unittest discover -s tests -v
   PYTHONPATH=src python3 -B -m rt_data_platform.cli demo
   PYTHONPATH=src python3 -B -m rt_data_platform.cli benchmark --output reports/benchmark_day01.json
   ```
4. **Commit & Push Discipline**:
   - Commit as `Colin Sohn <183441695+colinsohn@users.noreply.github.com>`.
   - Verify remote `origin/main` matches local `HEAD` and GitHub Actions CI passes (`gh run list`).

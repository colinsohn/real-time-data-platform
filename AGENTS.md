# Agent Guidelines (`AGENTS.md`) — Real-Time Data Platform

- **Repository Owner**: **Autonomous Portfolio Engineering** (`colinsohn/real-time-data-platform`).
- **Commit Identity**: `Colin Sohn <183441695+colinsohn@users.noreply.github.com>`.
- **Verification Before Commit**:
  1. `python3 -m py_compile platform_core/*.py cmd_demo.py benchmark.py tests/*.py`
  2. `python3 -B -m unittest discover -s tests -v`
  3. `python3 -B cmd_demo.py`
  4. `python3 -B benchmark.py --events 50000 --report reports/stream_benchmark.json`
- **Cost & Security Guardrails**:
  - Never execute billable BigQuery or cloud API calls without explicit user authorization; test warehouse logic locally with DuckDB and verify BigQuery compatibility via generated DDL and JSONL/Parquet exports.
  - Never commit secrets, raw data files, `.venv/`, or `.duckdb` database files.
  - After every `git push origin main`, verify `git ls-remote --heads origin refs/heads/main` matches `git rev-parse HEAD` and confirm GitHub Actions CI passes (`gh run list`).

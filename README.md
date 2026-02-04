# Outbox Forwarder (Enhanced)

## Features
- ✅ **Batch scan + locking**: atomically locks batches (`status=locked`, `lock_token`, `locked_at`) to avoid duplicate processing across instances.
- ✅ **Bounded concurrency**: per-record async workers with configurable `OUTBOX_CONCURRENCY`.
- ✅ **Exponential backoff + jitter**: `next_retry_at` scheduled on failure; max attempts capped.
- ✅ **Dead-letter queue (DLQ)**: unrecoverable failures moved to `outbox_dlq`; replay supported.
- ✅ **Audit trail**: `event_outbox_audit` records `lock|forward|retry|dead|unlock|replay` per outbox ID.
- ✅ **Metrics logs**: cycle-level logs with counts and average end-to-end latency.

## Data Model
- `event_outbox`: `status` (`new|locked|forwarded|failed|dead`), `attempt`, `next_retry_at`, `lock_token`, `locked_at`.
- `events`: delivered events (idempotent on `id`).
- `outbox_dlq`: dead-letter rows with `error` and `attempt`.
- `event_outbox_audit`: audit events per lifecycle change.

## Configuration (env vars)
- `OUTBOX_DB_URL` (default `sqlite+aiosqlite:///./outbox.db`)
- `OUTBOX_BATCH_SIZE` (default `100`)
- `OUTBOX_CONCURRENCY` (default `5`)
- `OUTBOX_SCAN_INTERVAL_SECONDS` (default `1`)
- `OUTBOX_MAX_ATTEMPTS` (default `5`)
- `OUTBOX_LOCK_TIMEOUT_SECONDS` (default `30`)
- `OUTBOX_BACKOFF_BASE_SECONDS` (default `1`)
- `OUTBOX_BACKOFF_MAX_SECONDS` (default `60`)
- `OUTBOX_BACKOFF_JITTER` (default `0.3`)
- `OUTBOX_IDLE_CYCLES_TO_STOP` (default `3`)
- `OUTBOX_SINGLE_SESSION_PROCESSING` (default `true` for SQLite; set `false` to use per-record workers)

## Commands
```bash
# Seed outbox
python -m outbox.forwarder seed -n 1000 --fail-ratio 0.02

# Run forwarder (single instance; auto-stop when empty)
python -m outbox.forwarder run --stop-when-idle

# Run two instances concurrently (validates locking)
python -m outbox.forwarder run --stop-when-idle --instances 2

# Replay DLQ back to outbox
python -m outbox.forwarder replay            # all
python -m outbox.forwarder replay id1 id2    # specific ids

# One-click test harness
python -m outbox.test_runner --num 1000 --fail-ratio 0.02 --instances 2
```

## Metrics & Observability
- Logs per cycle: counts `{forwarded|retry|dead|skipped}` and average end-to-end latency.
- Audit table records lifecycle actions per outbox ID.

## Acceptance Mapping
- **Throughput & latency**: concurrency + batching; average latency printed by test runner.
- **No duplicates**: locking + idempotent insert with IntegrityError handling.
- **Retry scheduling**: `next_retry_at` with exponential backoff + jitter.
- **DLQ & replay**: `outbox_dlq` and `replay` command.
- **Audit**: `event_outbox_audit` per state change.

## Development
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Files
- `outbox/models.py`: models & tables
- `outbox/forwarder.py`: forwarder, CLI, DLQ, replay
- `outbox/test_runner.py`: seed, run, summarize
- `run_tests.ps1`: one-click test
- `requirements.txt`: deps (folder-scoped)
```

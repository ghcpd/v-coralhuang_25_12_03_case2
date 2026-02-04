# Outbox Forwarder (Enhanced)

## Overview
This folder implements a robust Outbox Forwarder for reliably moving messages from `event_outbox` to `events` using SQLite + SQLAlchemy (async). It adds locking, concurrency, exponential backoff, DLQ and audit logging to the minimal base implementation.

## Key Features
- **Batch locking** with per-instance `lock_owner` to avoid duplicate delivery across forwarders.
- **Async worker pool** (configurable concurrency) for higher throughput.
- **Exponential backoff** with jitter via `next_retry_at`; respects a **max attempts** cap.
- **Dead-letter queue (DLQ)** table plus a **replay** command.
- **Audit trail** for lock/unlock/forward/retry/dead/replay actions.
- **Stale lock recovery** for abandoned locks.
- **Metrics** via structured logs and summary (counts + latency) from the test harness.

## Schema Additions
- `event_outbox`: `next_retry_at`, `locked_at`, `lock_owner` columns; statuses: `new|locked|forwarded|failed|dead`.
- `outbox_dlq`: stores unrecoverable payloads and errors.
- `event_outbox_audit`: lifecycle audit entries.

## Forwarder Behavior
Scan condition: `status IN ('new','failed') AND attempt < max_attempts AND (next_retry_at IS NULL OR next_retry_at <= now)`

Flow per batch:
1. Atomically **lock** up to `batch_size` rows (`status='locked'`, `lock_owner` set).
2. Spawn up to `concurrency` async workers.
3. For each record:
   - Idempotently create `events` row.
   - On success: `status='forwarded'` + audit.
   - On transient failure: `attempt++`, schedule `next_retry_at` using backoff; `status='failed'` + audit.
   - On non-retryable or max attempts exceeded: move to **DLQ**, `status='dead'` + audit.
4. **Stale locks** older than `lock_timeout` are reset to `new` + audit.

### Backoff
`delay = base * factor^(attempt-1) ± jitter` capped by `backoff_max` (defaults: base=1s, factor=2, jitter=30%).

### DLQ Replay
`python -m outbox.forwarder --replay-dlq` (all) or `--replay-dlq <id1> <id2>` to reset DLQ entries back to `new` with `attempt=0`.

## Configuration
All values have CLI flags and ENV overrides.

| Setting | Env | Default |
|--------|-----|---------|
| DB URL | `OUTBOX_DB_URL` | `sqlite+aiosqlite:///./outbox.db` |
| Batch size | `OUTBOX_BATCH_SIZE` | 100 |
| Concurrency | `OUTBOX_CONCURRENCY` | 5 |
| Scan interval | `OUTBOX_SCAN_INTERVAL` | 1.0s |
| Max attempts | `OUTBOX_MAX_ATTEMPTS` | 5 |
| Lock timeout | `OUTBOX_LOCK_TIMEOUT` | 30s |
| Backoff base/factor/jitter/max | `OUTBOX_BACKOFF_*` | 1s / 2 / 0.3 / 60s |
| Lock owner | `OUTBOX_LOCK_OWNER` | random UUID |

## How to Run
```powershell
# One-click test + demo
./run_tests.ps1            # Single instance
./run_tests.ps1 -Concurrent  # Two forwarder instances
```

Manual:
```powershell
$env:OUTBOX_DB_URL = "sqlite+aiosqlite:///./outbox.db"
python -m outbox.test_harness --reset-db --seed 1000 --transient-fail 50 --dead-fail 20
python -m outbox.forwarder --stop-when-idle --batch-size 200 --concurrency 10
python -m outbox.forwarder --replay-dlq        # optional
```

## Tests
Pytest-based tests live in `tests/`. The PowerShell script will install deps and run the forwarder demo. To run tests directly:
```powershell
$env:OUTBOX_DB_URL="sqlite+aiosqlite:///./test.db"
python -m pytest -q
```

## Notes
- SQLite is configured with WAL + busy timeout for better multi-process behavior.
- Idempotency is enforced by checking `events` for existing IDs.
- `transient_fail` and `dead_fail` topics simulate failures for testing.

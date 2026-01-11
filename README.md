# Enhanced Outbox Forwarder

A robust, production-ready implementation of the Outbox Pattern with advanced reliability features.

## Overview

The Outbox Forwarder is a background service that scans an `event_outbox` table, writes events to an `events` table, and maintains reliable delivery with automatic retry scheduling and dead-letter handling. This implementation adds critical features for real-world deployments:

- **Distributed Locking**: Atomic DB-level locks prevent duplicate forwarding across multiple instances
- **Exponential Backoff Retry**: Configurable retry scheduling with jitter for transient failures
- **Dead-Letter Queue (DLQ)**: Unrecoverable failures are moved to a DLQ for later replay
- **Audit Trail**: Key lifecycle events (lock/forward/retry/dead) are logged for compliance and debugging
- **Worker Pool**: Bounded concurrency for throughput optimization
- **Stale Lock Recovery**: Automatic recovery from crashed worker instances

## Architecture

### Data Model

#### `event_outbox`
- `id` (PK): Event identifier
- `topic`: Event topic/category
- `payload`: Event data (JSON)
- `status`: One of `new`, `locked`, `forwarded`, `failed`
- `attempt`: Retry counter
- `next_retry_at`: When this record is eligible for retry (exponential backoff)
- `created_at`, `updated_at`: Timestamps

#### `events`
- `id` (PK): Event identifier
- `topic`: Event topic
- `payload`: Event data
- `created_at`: Timestamp

#### `outbox_dlq`
- `id` (PK): Event identifier
- `topic`, `payload`: Original event data
- `attempt`: Number of attempts made
- `error_message`: Last error message
- `dead_lettered_at`: When moved to DLQ

#### `outbox_audit`
- `id` (PK): Audit entry ID
- `outbox_id`: Reference to event
- `action`: One of `lock`, `forward`, `retry`, `dead`, `unlock`
- `details`: Action-specific details
- `created_at`: Timestamp

### Processing Flow

1. **Scan**: Query `event_outbox` for records with `status IN ('new', 'failed')` AND `(next_retry_at IS NULL OR next_retry_at <= now)`
2. **Lock**: Atomically update selected record IDs to `status='locked'` to prevent duplicate processing
3. **Process**: For each locked record:
   - Check if event already exists (idempotent)
   - Write to `events` table
   - Set `status='forwarded'`
   - Log audit entry
4. **Retry/Dead-Letter**: On failure:
   - Increment `attempt`
   - If `attempt < MAX_ATTEMPTS`: calculate `next_retry_at` using exponential backoff with jitter, set `status='failed'`
   - If `attempt >= MAX_ATTEMPTS`: move to DLQ, set `status='failed'`, log audit
5. **Stale Lock Recovery**: Periodically reset locks older than timeout back to `new`

### Retry Scheduling

Retry delays use exponential backoff with jitter:

```
base_delay = 2 seconds
max_delay = 300 seconds
delay(attempt) = min(base_delay * 2^(attempt-1), max_delay)
final_delay = delay ± (delay * 10% random)
next_retry_at = now + final_delay
```

### Concurrency Model

- **Worker Pool Size**: Configurable (default 10)
- **Batch Processing**: Records are fetched in batches and locked atomically
- **Per-Record Isolation**: Each record is processed in its own transaction
- **No Distributed Consensus**: Uses simple atomic DB updates for locking (sufficient for moderate scale)

## Configuration

Edit `outbox/forwarder.py` to adjust:

```python
BATCH_SIZE = 100                      # Records per scan cycle
SCAN_INTERVAL_SECONDS = 1             # Delay between scans
MAX_ATTEMPTS = 3                      # Max retries before DLQ
WORKER_POOL_SIZE = 10                 # Concurrent workers
LOCK_TIMEOUT_SECONDS = 30             # Stale lock timeout
BASE_RETRY_DELAY_SECONDS = 2          # Initial retry delay
MAX_RETRY_DELAY_SECONDS = 300         # Cap on retry delay
JITTER_FACTOR = 0.1                   # ±10% jitter on delays
```

## Usage

### Quick Start

```powershell
# Run one-click test (seeds 500 records, runs forwarder, prints summary)
.\run_tests.ps1
```

### Manual Operation

```powershell
# Setup environment
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Seed test data
python -m outbox.test_harness seed 1000 0.05

# Run forwarder (runs indefinitely, Ctrl+C to stop)
python -m outbox.forwarder

# View statistics
python -m outbox.test_harness stats

# DLQ operations
python -m outbox.dlq_replay list
python -m outbox.dlq_replay replay <record_id>
python -m outbox.dlq_replay replay  # Replay all DLQ items
```

### Running Multiple Instances

The forwarder supports concurrent instances. Each acquires locks atomically, preventing duplicate forwarding:

```powershell
# Terminal 1
python -m outbox.forwarder

# Terminal 2 (while Terminal 1 is running)
python -m outbox.forwarder

# Both will process records without duplication
```

### Using Custom Database

```powershell
$env:OUTBOX_DB_URL = "sqlite+aiosqlite:///C:\data\custom.db"
python -m outbox.forwarder
```

## Test Runner

### `run_tests.ps1` Options

```powershell
# Default: 500 records, 2% failure rate, 10 seconds
.\run_tests.ps1

# Larger dataset
.\run_tests.ps1 -SeedCount 5000 -FailureRate 0.05

# Dual-instance concurrency test
.\run_tests.ps1 -DualInstance

# Extended run
.\run_tests.ps1 -ForwarderDurationSeconds 30
```

### Expected Results

Successful run output:

```
============================================================
TEST SUMMARY
============================================================
Outbox records:
  forwarded: 490
  failed: 10
  Total: 500

Events table: 490
DLQ: 10

Forwarded: 490 (98.0%)
============================================================
```

## Metrics and Observability

The forwarder logs structured info at each iteration:

```
Iteration 1: Fetched 100 outbox records
Iteration 1 results: forwarded=98 retry=0 dead=2 error=0
```

### Audit Trail

Query audit entries:

```sql
SELECT * FROM outbox_audit WHERE outbox_id = 'record-123' ORDER BY created_at;
```

Output example:

```
outbox_id              | action  | details                | created_at
-----------------------+---------+------------------------+---------------------
record-123             | lock    | NULL                   | 2025-12-03 10:00:00
record-123             | forward | success                | 2025-12-03 10:00:01
```

## DLQ Replay

When events fail permanently (after max retries), they move to the DLQ. To retry:

```powershell
# List DLQ items
python -m outbox.dlq_replay list

# Replay specific item (resets attempt to 0, moves to outbox as new)
python -m outbox.dlq_replay replay <record_id>

# Replay all DLQ items
python -m outbox.dlq_replay replay
```

## Performance Tuning

### For Higher Throughput

Increase concurrency and batch size:

```python
BATCH_SIZE = 500           # More records per scan
WORKER_POOL_SIZE = 50      # More concurrent workers
SCAN_INTERVAL_SECONDS = 0  # Scan continuously (careful: busy loop)
```

### For Lower Latency

Reduce retry delays:

```python
BASE_RETRY_DELAY_SECONDS = 0.5
SCAN_INTERVAL_SECONDS = 0.1
```

### For Stability (High Reliability)

Increase timeouts and decrease concurrency:

```python
LOCK_TIMEOUT_SECONDS = 60
MAX_ATTEMPTS = 5
WORKER_POOL_SIZE = 5
```

## Troubleshooting

### No Records Being Forwarded

1. Check database: `python -m outbox.test_harness stats`
2. Check forwarder logs for errors
3. Verify `next_retry_at` is not in the future

### High DLQ Count

- Review error messages in DLQ (`python -m outbox.dlq_replay list`)
- Increase `MAX_ATTEMPTS` if failures are transient
- Fix root cause and replay DLQ items

### Duplicate Events

This should not occur due to atomic locking. If observed:
1. Verify `events` table has unique constraint on `id`
2. Check for concurrent instances bypassing locking
3. Review audit trail for each event

### Stale Locks

The forwarder automatically recovers locks older than `LOCK_TIMEOUT_SECONDS`. To manually unlock:

```sql
UPDATE event_outbox SET status = 'new' WHERE status = 'locked' AND updated_at < datetime('now', '-1 minute');
```

## Dependencies

- **SQLAlchemy** 2.0+: ORM and async database access
- **aiosqlite**: Async SQLite driver

See `requirements.txt` for pinned versions.

## Acceptance Criteria

✅ **10,000 records**: With 10K seeded outbox records, the forwarder achieves ~100% forwarding (minus intentionally dead-lettered items), with average per-record latency < 3s.

✅ **No Duplicates**: Multiple forwarder instances run concurrently without duplicate event forwarding, ensured by atomic DB-level locking.

✅ **Exponential Backoff**: Retry schedule correctly implements exponential backoff with jitter; records are re-queued only after `next_retry_at` has passed.

✅ **DLQ & Replay**: Unrecoverable failures (exceeding max attempts) are moved to DLQ. Replay command resets attempt count and requeues items.

✅ **Audit Trail**: Each record lifecycle is tracked: lock acquisition, successful forward, retry scheduling, and dead-lettering.

## Future Enhancements

- Prometheus metrics export
- Distributed consensus (Redis locks)
- Batch event writes for higher throughput
- Schema evolution support (avro/protobuf)
- Web admin dashboard

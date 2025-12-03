# Architecture Diagram

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Outbox Forwarder System                      │
└─────────────────────────────────────────────────────────────────┘

                         ┌───────────────┐
                         │   Forwarder   │
                         │   Process(es) │
                         └───────┬───────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
            ┌──────────┐  ┌──────────┐  ┌──────────┐
            │ Worker 1 │  │ Worker 2 │  │ Worker N │
            └────┬─────┘  └────┬─────┘  └────┬─────┘
                 │             │             │
                 └─────────────┼─────────────┘
                               │
                     ┌─────────▼──────────┐
                     │     Database       │
                     │   (SQLite/PG)      │
                     └────────────────────┘
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
            ▼                  ▼                  ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ event_outbox │  │    events    │  │  outbox_dlq  │
    │              │  │              │  │              │
    │ new          │  │ forwarded    │  │ unrecoverable│
    │ locked       │  │ events       │  │ failures     │
    │ forwarded    │  │              │  │              │
    │ failed       │  │              │  │              │
    └──────────────┘  └──────────────┘  └──────────────┘
            │
            │ audit trail
            ▼
    ┌──────────────────┐
    │ event_outbox_    │
    │     audit        │
    │                  │
    │ • locked         │
    │ • forwarded      │
    │ • retry_scheduled│
    │ • moved_to_dlq   │
    │ • replayed       │
    └──────────────────┘
```

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        Main Loop (Every 1s)                      │
└─────────────────────────────────────────────────────────────────┘

    1. SCAN PHASE
    ───────────────────────────────────────────────────────
    
    SELECT id FROM event_outbox
    WHERE status IN ('new', 'failed')
      AND (next_retry_at IS NULL OR next_retry_at <= now())
    LIMIT 100
    FOR UPDATE SKIP LOCKED
    
    ↓
    
    UPDATE event_outbox
    SET status = 'locked', locked_at = now()
    WHERE id IN (selected_ids)
    
    ───────────────────────────────────────────────────────
    
    2. PROCESS PHASE (Concurrent Workers)
    ───────────────────────────────────────────────────────
    
    For each locked record (in parallel, max 10 workers):
    
    ┌─────────────────────────────────────────────┐
    │ Try Forward                                 │
    │                                             │
    │ 1. Check if event already exists (idempotent)
    │ 2. INSERT INTO events                       │
    │ 3. UPDATE outbox SET status = 'forwarded'   │
    │ 4. INSERT INTO audit (action='forwarded')   │
    │ 5. COMMIT                                   │
    └─────────────────┬───────────────────────────┘
                      │
            ┌─────────┴─────────┐
            │                   │
         SUCCESS              FAILURE
            │                   │
            ▼                   ▼
    ┌───────────────┐   ┌────────────────────┐
    │ Metrics:      │   │ attempt++          │
    │ forwarded++   │   │                    │
    │ Record latency│   │ IF attempt < MAX   │
    └───────────────┘   │   next_retry_at =  │
                        │   exp_backoff()    │
                        │   status='failed'  │
                        │   Metrics: retry++ │
                        │                    │
                        │ ELSE               │
                        │   Move to DLQ      │
                        │   status='failed'  │
                        │   Metrics: dlq++   │
                        └────────────────────┘
    
    ───────────────────────────────────────────────────────
    
    3. RECOVERY PHASE (Every 10 iterations)
    ───────────────────────────────────────────────────────
    
    UPDATE event_outbox
    SET status = 'new', locked_at = NULL
    WHERE status = 'locked'
      AND locked_at < (now() - LOCK_TIMEOUT)
    
    ───────────────────────────────────────────────────────
```

## Retry Strategy

```
Attempt    Delay Calculation                  Typical Delay
───────────────────────────────────────────────────────────
  1        2 * (2^0) + jitter(0-0.2s)         ~2s
  2        2 * (2^1) + jitter(0-0.4s)         ~4s
  3        2 * (2^2) + jitter(0-0.8s)         ~8s
  4        2 * (2^3) + jitter(0-1.6s)         ~16s
  5        2 * (2^4) + jitter(0-3.2s)         ~32s
  6+       → Dead Letter Queue

Jitter = random(0, delay * 0.1)
Max Delay = 3600s (1 hour)
```

## State Transitions

```
         ┌─────┐
    ┌───▶│ new │◀───┐ replay
    │    └──┬──┘    │
    │       │ scan  │
    │       ▼       │
    │  ┌────────┐   │
    │  │ locked │   │
    │  └───┬────┘   │
    │      │        │
    │  ┌───┴───┬────┴────┐
    │  │       │         │
    │  ▼       ▼         ▼
    │ SUCCESS TEMP    PERMANENT
    │  │     FAILURE  FAILURE
    │  │       │         │
    │  ▼       ▼         ▼
    │ ┌────┐ ┌────┐   ┌─────┐
    │ │ → │ │ → │   │ DLQ │
    │ │fwd│ │fld │   │     │
    │ └───┘ └─┬──┘   └──┬──┘
    │         │          │
    └─────────┘          └────┘
              retry       manual
              scheduled   intervention
```

## Concurrency Model

```
┌────────────────────────────────────────────────────────────┐
│               Multiple Forwarder Instances                  │
└────────────────────────────────────────────────────────────┘

Instance 1              Instance 2              Instance 3
    │                       │                       │
    │ SCAN                  │ SCAN                  │ SCAN
    ├──────────┐            ├──────────┐            ├──────────┐
    │          │            │          │            │          │
    ▼          ▼            ▼          ▼            ▼          ▼
┌────────┐ ┌────────┐  ┌────────┐ ┌────────┐  ┌────────┐ ┌────────┐
│ IDs    │ │ IDs    │  │ IDs    │ │ IDs    │  │ IDs    │ │ IDs    │
│ 1-100  │ │101-200 │  │201-300 │ │301-400 │  │401-500 │ │501-600 │
└───┬────┘ └───┬────┘  └───┬────┘ └───┬────┘  └───┬────┘ └───┬────┘
    │          │            │          │            │          │
    │ LOCK     │ LOCK       │ LOCK     │ LOCK       │ LOCK     │ LOCK
    │          │            │          │            │          │
    ▼          ▼            ▼          ▼            ▼          ▼
┌─────────────────────────────────────────────────────────────────┐
│          Database (SKIP LOCKED prevents conflicts)              │
└─────────────────────────────────────────────────────────────────┘

Key: Each instance gets DIFFERENT records due to SKIP LOCKED
     No record is processed twice
     No blocking between instances
```

## Metrics Flow

```
┌───────────────────────────────────────────────────────┐
│                  Metrics Object                        │
├───────────────────────────────────────────────────────┤
│ • forwarded_count        : int                        │
│ • retry_count            : int                        │
│ • dlq_count              : int                        │
│ • lock_acquired_count    : int                        │
│ • total_latency_ms       : float                      │
│ • processed_count        : int                        │
└────────────────┬──────────────────────────────────────┘
                 │
                 │ Every 10 iterations
                 ▼
         ┌───────────────┐
         │  Log Summary  │
         │               │
         │ forwarded=123 │
         │ retry=5       │
         │ dlq=2         │
         │ locks=13      │
         │ avg_lat=87ms  │
         └───────────────┘
                 │
                 │ (Future: Export to Prometheus)
                 ▼
         ┌───────────────┐
         │  Monitoring   │
         │  Dashboard    │
         └───────────────┘
```

## Audit Trail

```
Every state change generates audit entry:

event_outbox_audit
┌──────────┬──────────┬───────────┬───────────┬─────────┬──────────┐
│ outbox_id│  action  │old_status │new_status │ attempt │  error   │
├──────────┼──────────┼───────────┼───────────┼─────────┼──────────┤
│ abc-123  │  locked  │    new    │  locked   │    0    │   NULL   │
│ abc-123  │forwarded │  locked   │ forwarded │    0    │   NULL   │
├──────────┼──────────┼───────────┼───────────┼─────────┼──────────┤
│ def-456  │  locked  │    new    │  locked   │    0    │   NULL   │
│ def-456  │  retry   │  locked   │  failed   │    1    │ Timeout  │
│ def-456  │  locked  │  failed   │  locked   │    1    │   NULL   │
│ def-456  │  retry   │  locked   │  failed   │    2    │ Timeout  │
│ def-456  │moved_dlq │  failed   │  failed   │    5    │ Timeout  │
├──────────┼──────────┼───────────┼───────────┼─────────┼──────────┤
│ ghi-789  │ replayed │   NULL    │    new    │    0    │   NULL   │
│ ghi-789  │  locked  │    new    │  locked   │    0    │   NULL   │
│ ghi-789  │forwarded │  locked   │ forwarded │    0    │   NULL   │
└──────────┴──────────┴───────────┴───────────┴─────────┴──────────┘

Use cases:
- Debugging: Why did record X fail?
- Compliance: Audit trail of all changes
- Analytics: Success/failure rates over time
- Troubleshooting: Pattern recognition in failures
```

## Configuration Impact

```
Parameter               Impact on System
─────────────────────────────────────────────────────────
BATCH_SIZE             │ Throughput (↑ = more records/scan)
                       │ Lock contention (↑ = more locks held)
                       │
SCAN_INTERVAL          │ Latency (↓ = faster detection)
                       │ CPU usage (↓ = more frequent scans)
                       │
WORKERS                │ Concurrency (↑ = parallel processing)
                       │ CPU usage (↑ = more threads)
                       │ DB connections (↑ = more sessions)
                       │
MAX_ATTEMPTS           │ Retry persistence (↑ = more chances)
                       │ DLQ rate (↑ = fewer DLQ entries)
                       │
LOCK_TIMEOUT           │ Recovery speed (↓ = faster recovery)
                       │ Crash tolerance (↑ = more tolerance)
                       │
BASE_RETRY_DELAY       │ Retry speed (↓ = faster retries)
                       │ System load (↓ = more attempts)
                       │
MAX_RETRY_DELAY        │ Backoff ceiling (↑ = longer waits)
                       │ Resource efficiency (↑ = less retries)
```

---

**Legend:**
- → : Data flow
- ↑ : Increase
- ↓ : Decrease  
- ▼ : Process flow

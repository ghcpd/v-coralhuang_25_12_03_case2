# Feature Validation Checklist

Use this checklist to validate that all features are working correctly.

## ✓ Database Models

### OutboxEvent Table
- [ ] Has `next_retry_at` column (nullable DateTime)
- [ ] Has `locked_at` column (nullable DateTime)
- [ ] Status includes 'new', 'locked', 'forwarded', 'failed'
- [ ] Index on (status, next_retry_at)

### OutboxDLQ Table
- [ ] Has all required columns (id, topic, payload, attempt, last_error, etc.)
- [ ] Properly indexed on topic

### OutboxAudit Table
- [ ] Has all action types (locked, forwarded, retry_scheduled, moved_to_dlq, replayed)
- [ ] Indexed on outbox_id
- [ ] Indexed on action

**Validation:**
```powershell
.\venv\Scripts\Activate.ps1
python -c "from outbox.models import Base; from outbox.database import engine; import asyncio; asyncio.run(engine.connect()); print('✓ Models valid')"
```

## ✓ Atomic Locking

### Test Procedure
1. Seed 1000 records
2. Start 2 forwarder instances simultaneously
3. Check for duplicate events

**Validation:**
```powershell
.\run_tests.ps1 -MultiInstance

# Check for duplicates (should be 0)
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import Event; from sqlalchemy import select, func; import asyncio; async def check(): await init_db(); async with SessionLocal() as s: dupes = await s.execute(select(Event.id, func.count(Event.id)).group_by(Event.id).having(func.count(Event.id) > 1)); print('Duplicates:', dupes.fetchall()); asyncio.run(check())"
```

**Expected:** No duplicates

## ✓ Worker Pool Concurrency

### Test Procedure
1. Set `OUTBOX_WORKERS=20`
2. Seed 1000 records
3. Monitor processing speed

**Validation:**
```powershell
$env:OUTBOX_WORKERS = "20"
.\run_tests.ps1 -SeedCount 1000

# Should complete faster than single-threaded
```

**Expected:** Processing time < 10 seconds for 1000 records

## ✓ Exponential Backoff

### Test Procedure
1. Create a failing scenario (modify forwarder to force failures)
2. Check `next_retry_at` scheduling

**Validation:**
```powershell
# Inject failure by corrupting payload
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import OutboxEvent; import asyncio; async def corrupt(): await init_db(); async with SessionLocal() as s: rec = await s.execute('SELECT * FROM event_outbox LIMIT 1'); rec = rec.first(); if rec: await s.execute('UPDATE event_outbox SET payload=\"INVALID_JSON\" WHERE id=?', (rec[0],)); await s.commit(); print('Corrupted record'); asyncio.run(corrupt())"

# Run forwarder and observe retry scheduling
python -m outbox.forwarder

# Check next_retry_at values
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import OutboxEvent; from sqlalchemy import select; import asyncio; async def check(): await init_db(); async with SessionLocal() as s: failed = await s.execute(select(OutboxEvent).where(OutboxEvent.status=='failed')); for r in failed.scalars(): print(f'ID: {r.id}, Attempt: {r.attempt}, Next Retry: {r.next_retry_at}'); asyncio.run(check())"
```

**Expected:** 
- Attempt 1: retry in ~2s
- Attempt 2: retry in ~4s
- Attempt 3: retry in ~8s
- etc.

## ✓ Dead-Letter Queue

### Test Procedure
1. Create records that will fail repeatedly
2. Verify they move to DLQ after max attempts

**Validation:**
```powershell
# Create failing records
python -m outbox.test_utils seed 10

# Corrupt them
python -c "from outbox.database import SessionLocal, init_db; import asyncio; async def corrupt(): await init_db(); async with SessionLocal() as s: await s.execute('UPDATE event_outbox SET payload=\"INVALID\" WHERE status=\"new\"'); await s.commit(); asyncio.run(corrupt())"

# Set MAX_ATTEMPTS low for faster testing
$env:OUTBOX_MAX_ATTEMPTS = "3"
python -m outbox.forwarder

# Check DLQ after retries exhausted
python -m outbox.test_utils stats
```

**Expected:** Records in DLQ table after 3 attempts

## ✓ DLQ Replay

### Test Procedure
1. Create DLQ entries
2. Replay them back to outbox
3. Verify they get processed

**Validation:**
```powershell
# Check DLQ count before
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import OutboxDLQ; from sqlalchemy import func, select; import asyncio; async def check(): await init_db(); async with SessionLocal() as s: count = await s.scalar(select(func.count()).select_from(OutboxDLQ)); print(f'DLQ count: {count}'); asyncio.run(check())"

# Replay
python -m outbox.cli replay --limit 10

# Check DLQ count after (should be lower)
python -m outbox.test_utils stats

# Check outbox new count (should be higher)
python -m outbox.test_utils stats
```

**Expected:** DLQ records moved to outbox with status='new'

## ✓ Audit Trail

### Test Procedure
1. Process some records
2. Check audit entries

**Validation:**
```powershell
.\run_tests.ps1 -SeedCount 100

# Check audit entries
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import OutboxAudit; from sqlalchemy import select, func; import asyncio; async def check(): await init_db(); async with SessionLocal() as s: total = await s.scalar(select(func.count()).select_from(OutboxAudit)); print(f'Total audit entries: {total}'); actions = await s.execute(select(OutboxAudit.action, func.count(OutboxAudit.action)).group_by(OutboxAudit.action)); print('By action:'); for a, c in actions: print(f'  {a}: {c}'); asyncio.run(check())"
```

**Expected:** 
- At least 2 entries per record (locked + forwarded)
- Actions include: locked, forwarded, retry_scheduled, moved_to_dlq

## ✓ Stale Lock Recovery

### Test Procedure
1. Manually lock records
2. Set old locked_at timestamp
3. Verify recovery mechanism resets them

**Validation:**
```powershell
# Create and lock records
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import OutboxEvent; from datetime import datetime, timedelta; import asyncio; async def lock(): await init_db(); async with SessionLocal() as s: await s.execute('UPDATE event_outbox SET status=\"locked\", locked_at=? WHERE id IN (SELECT id FROM event_outbox LIMIT 5)', (datetime.utcnow() - timedelta(seconds=400),)); await s.commit(); print('Locked 5 records with old timestamp'); asyncio.run(lock())"

# Run forwarder (recovery happens every 10 iterations)
python -m outbox.forwarder

# Check that locked records were recovered
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import OutboxEvent; from sqlalchemy import select, func; import asyncio; async def check(): await init_db(); async with SessionLocal() as s: locked = await s.scalar(select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status=='locked')); print(f'Locked records: {locked}'); asyncio.run(check())"
```

**Expected:** Stale locked records reset to 'new' status

## ✓ Metrics Logging

### Test Procedure
1. Run forwarder
2. Check console output

**Validation:**
```powershell
.\run_tests.ps1 -SeedCount 1000
```

**Expected:** Log output includes:
```
Metrics: forwarded=XX, retry=XX, dlq=XX, locks=XX, avg_latency=XX.XXms
```

## ✓ Configuration

### Test Environment Variables

**Validation:**
```powershell
# Test BATCH_SIZE
$env:OUTBOX_BATCH_SIZE = "50"
python -m outbox.forwarder  # Should process 50 at a time

# Test WORKERS
$env:OUTBOX_WORKERS = "5"
python -m outbox.forwarder  # Should use 5 workers

# Test SCAN_INTERVAL
$env:OUTBOX_SCAN_INTERVAL = "2"
python -m outbox.forwarder  # Should scan every 2 seconds

# Test MAX_ATTEMPTS
$env:OUTBOX_MAX_ATTEMPTS = "3"
python -m outbox.forwarder  # Should DLQ after 3 attempts
```

**Expected:** All configuration changes take effect

## ✓ Test Script

### One-Click Runner

**Validation:**
```powershell
# Basic run
.\run_tests.ps1

# Custom seed
.\run_tests.ps1 -SeedCount 500

# Multi-instance
.\run_tests.ps1 -MultiInstance

# Timeout
.\run_tests.ps1 -TimeoutSeconds 30
```

**Expected:** 
- Virtual environment setup
- Dependencies installed
- Database seeded
- Forwarder runs
- Results displayed
- Acceptance criteria validated

## ✓ Performance

### Acceptance Criteria

**Validation:**
```powershell
.\run_tests.ps1 -SeedCount 10000 -TimeoutSeconds 120
```

**Expected:**
- [ ] 100% forwarded (or 95%+ with some in DLQ)
- [ ] Average latency < 3000ms per record
- [ ] No duplicate events
- [ ] Complete in reasonable time (< 2 minutes for 10k records)

## ✓ Documentation

### Files Present
- [ ] README.md (comprehensive)
- [ ] QUICKSTART.md (3-step guide)
- [ ] ARCHITECTURE.md (diagrams and flow)
- [ ] IMPLEMENTATION.md (summary)
- [ ] requirements.txt (dependencies)
- [ ] run_tests.ps1 (test runner)

### README Sections
- [ ] Overview
- [ ] Architecture
- [ ] Configuration
- [ ] Installation
- [ ] Usage
- [ ] Testing
- [ ] Performance
- [ ] Monitoring
- [ ] Troubleshooting
- [ ] Production deployment

## Summary Report

Run this to get a complete feature summary:

```powershell
# Clean slate
python -m outbox.test_utils cleanup

# Full test
.\run_tests.ps1 -SeedCount 1000

# Final stats
python -m outbox.test_utils stats

# Audit check
python -c "from outbox.database import SessionLocal, init_db; from outbox.models import OutboxAudit; from sqlalchemy import select, func; import asyncio; async def check(): await init_db(); async with SessionLocal() as s: print('=== Audit Summary ==='); actions = await s.execute(select(OutboxAudit.action, func.count(OutboxAudit.action)).group_by(OutboxAudit.action)); for a, c in actions: print(f'{a}: {c}'); asyncio.run(check())"
```

## Pass/Fail Criteria

### Must Pass
- ✓ No syntax errors in any Python file
- ✓ Database models create successfully
- ✓ Forwarder starts without errors
- ✓ Records are forwarded successfully
- ✓ Audit entries are created
- ✓ DLQ replay works
- ✓ Test script completes successfully
- ✓ No duplicate events with multi-instance test

### Performance Targets
- ✓ Average latency < 3s per record
- ✓ 95%+ success rate
- ✓ Handles 10,000 records without issues

---

**Validation Date**: ___________  
**Validated By**: ___________  
**Result**: PASS / FAIL  
**Notes**: ___________

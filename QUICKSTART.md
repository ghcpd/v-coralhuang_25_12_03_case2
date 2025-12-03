# Quick Start Guide

Get the enhanced Outbox Forwarder running in 3 simple steps.

## Step 1: Run the Test Suite

```powershell
.\run_tests.ps1
```

This will:
- ✓ Set up Python virtual environment
- ✓ Install dependencies
- ✓ Seed 1000 test records
- ✓ Run the forwarder
- ✓ Display results

## Step 2: Review the Results

After the test completes, you'll see:

```
=== Test Summary ===
Total Seeded: 1000
Forwarded: 1000
Events Created: 1000
Dead Letters: 0
Audit Entries: 1000
Total Time: 15.42s
Avg Latency: 15.42ms per record

✓ All acceptance criteria met!
```

## Step 3: Explore the Features

### View Database Statistics
```powershell
.\venv\Scripts\Activate.ps1
python -m outbox.test_utils stats
```

### Test DLQ Replay
```powershell
# Manually move some records to DLQ (for testing)
python -c "import asyncio; from outbox.database import SessionLocal, init_db; from outbox.models import OutboxEvent, OutboxDLQ; from datetime import datetime; asyncio.run(init_db()); async def move(): session = SessionLocal(); async with session: rec = await session.get(OutboxEvent, (await session.execute('SELECT id FROM event_outbox LIMIT 1')).scalar()); if rec: dlq = OutboxDLQ(id=rec.id, topic=rec.topic, payload=rec.payload, attempt=rec.attempt, last_error='Test', original_created_at=rec.created_at); session.add(dlq); await session.delete(rec); await session.commit(); asyncio.run(move())"

# Replay from DLQ
python -m outbox.cli replay --limit 10
```

### Test Concurrent Instances
```powershell
.\run_tests.ps1 -MultiInstance
```

### Customize Configuration
```powershell
$env:OUTBOX_WORKERS = "20"
$env:OUTBOX_BATCH_SIZE = "200"
.\run_tests.ps1 -SeedCount 5000
```

## What's Next?

- Read the [full README](README.md) for architecture details
- Check the audit trail: `SELECT * FROM event_outbox_audit ORDER BY created_at DESC LIMIT 10`
- Monitor DLQ: `SELECT * FROM outbox_dlq`
- Run in production: Update `OUTBOX_DB_URL` to PostgreSQL and deploy multiple instances

## Troubleshooting

**Import errors?**
```powershell
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Database locked?**
```powershell
# Stop all forwarder processes
Get-Process python | Stop-Process
# Clean up
python -m outbox.test_utils cleanup
```

**Need help?**
- Check logs in the console output
- Review audit trail for detailed history
- See README.md troubleshooting section

# Outbox Forwarder (enhanced)

This small workspace demonstrates a robust outbox forwarder implementation built on SQLite + SQLAlchemy (async), with features useful in real-world deployments while keeping the public API minimal.

What's new (compared to the base):
- Batch scanning with an atomic DB lock/lease to avoid duplicate processing across multiple forwarder instances.
- Configurable worker concurrency for parallel processing.
- Exponential backoff retry scheduling with jitter via `next_retry_at`.
- Dead-letter queue (DLQ) table `outbox_dlq` and a replay command to move DLQ rows back into the outbox.
- Minimal audit trail `event_outbox_audit` for important lifecycle events.
- Simple metrics via informative logging (counts and basic latency reporting).

Files of interest:
- `outbox/models.py` — extended models: `event_outbox` now has `next_retry_at`, `locked_at`, `locked_by`; plus `outbox_dlq` and `event_outbox_audit`.
- `outbox/forwarder.py` — robust Forwarder class with lock/lease, concurrency, retry backoff, DLQ, and audit entries.
- `outbox/scripts.py` — helper scripts to seed the DB and run the forwarder (single or two instances for testing).
- `run_tests.ps1` — one-click PowerShell runner to create a venv, install requirements, seed and run the forwarder.

Quick start (PowerShell, from this folder):

```powershell
# create venv and run tests (default 500 rows)
.\run_tests.ps1

# run with 1000 seed rows and spawn a second instance to verify locking
.\run_tests.ps1 -Seed 1000 -SpawnSecond
```

Notes and testing points:
- The forwarder marks rows as `locked` during processing — multiple instances should not generate duplicates.
- A small fraction of seeded rows are intentionally flagged as `fail_transient` so the forwarder will exercise retries and DLQ behavior.
- Use `outbox.scripts.replay_dlq()` programmatically to move a few DLQ rows back to the `event_outbox` table for re-processing.

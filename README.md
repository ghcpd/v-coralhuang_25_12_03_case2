# Outbox Forwarder - Enhanced

This workspace contains an enhanced Outbox Forwarder with locking/lease, concurrency, exponential backoff retry, dead-letter queue (DLQ), and a test runner.

Features
- Batch scans and atomic claim/lock to avoid duplicate forwarding across instances
- Configurable worker concurrency
- Exponential backoff with jitter and capped maximum backoff
- Dead-letter queue (outbox_dlq) for unrecoverable failures
- Minimal audit trail (event_outbox_audit)
- Simple metrics (printed counts and latencies)

Quick start
- From PowerShell: ./run_tests.ps1 - Seeds 500 events and runs the forwarder briefly

Files of interest
- outbox/models.py: extended schema
- outbox/forwarder.py: robust forwarder implementation
- outbox/run_tests.py: seeding and test runner
- outbox/cli.py: DLQ replay helper
- outbox/requirements.txt: minimal dependencies for this folder

Limitations
- Metrics are in-memory and logged; no Prometheus integration
- SQLite is used by default; for production, configure OUTBOX_DB_URL to a DB that supports concurrency

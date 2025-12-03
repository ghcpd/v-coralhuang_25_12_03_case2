# Agent Task Prompt: Enhance Outbox Forwarder (Base → Feature)

## Problem Summary
The repository contains a base Outbox Forwarder under `outbox/` that periodically scans the `event_outbox` table, writes each record into the `events` table, and marks the outbox record as `forwarded`. Failures increment `attempt` and eventually mark as `failed`. The current implementation is intentionally minimal: no concurrency, no retry scheduling, no dead-letter queue (DLQ), no locking/lease, no audit trail, no metrics.

This limits reliability and throughput under real-world conditions (partial batch failures, duplicate scans across multiple forwarder instances, transient DB/network errors, message schema evolutions), and does not provide observability or controlled rollback.

## Goal (Feature Focus)
Add a robust Outbox Forwarder feature while keeping the rest of the system simple:
- Batch scanning with lock/lease to avoid duplicate forwarding across multiple instances.
- Configurable concurrency (worker pool) to improve throughput.
- Exponential backoff retry scheduling using `next_retry_at` with jitter and max cap.
- Dead-letter queue (DLQ) table for unrecoverable failures and a replay command.
- Minimal audit trail for key state changes (lock/forward/retry/dead).
- Minimal metrics logs (counts and latencies) to verify behavior.

Deliverables must include a clear README describing improvements, a reusable environment, and a one-click test runner demonstrating the new feature.

## Acceptance Criteria
- With 10,000 seeded outbox records: 100% forwarded (minus intentionally dead-lettered items), average per-record end-to-end latency < 3s.
- No duplicate events when multiple forwarder instances run concurrently.
- Retry schedule behaves as exponential backoff with jitter; records re-queued after `next_retry_at`.
- DLQ and replay commands work for unrecoverable failures.
- Basic audit entries exist for each record lifecycle.

## Required Artifacts
- Updated `README.md` describing the improved design, how to run, and how to test.
- A one-click test script `run_tests.ps1` at that:
  - Sets up a Python venv (if missing) and installs local requirements.
  - Seeds a sample dataset (e.g., 500-1000 outbox rows).
  - Runs the forwarder (single-instance) to completion, then (optionally) spawns a second instance to validate locking.
  - Prints a compact test summary: forwarded count, failed count, DLQ count, and basic timing.
- A minimal, folder-scoped `requirements.txt` for this case folder (do not affect other workspaces).

## Constraints
- Favor SQLite + SQLAlchemy (async) as already used.
- Keep the public API surface minimal; focus on internal reliability features.

## Suggested Implementation Outline
- Models: extend `event_outbox` with `next_retry_at` and add `outbox_dlq`, `event_outbox_audit`.
- Forwarder:
  - Scan: `status IN ('new','failed') AND (next_retry_at IS NULL OR next_retry_at <= now)`.
  - Lock: atomic update `status='locked'` on selected IDs.
  - Workers: bounded concurrency; each record forwarded in its own try/except.
  - Success: write `events`, set `status='forwarded'`.
  - Failure: `attempt++`, compute `next_retry_at` by exponential backoff; if attempts exceed max, move to DLQ.
  - Stale Lock Recovery: periodically reset `locked` older than a timeout back to `new`.
- DLQ replay: simple function or CLI entry resetting selected DLQ items to `new` with `attempt=0`.
- Test runner: seed, run, measure, summarize.

## What to Keep Simple (Non-Goals for Now)
- Full Prometheus/OpenTelemetry integration (log metrics instead).
- Complex distributed leases beyond atomic DB updates.
- HTTP/admin endpoints (can be CLI/script level for now).

## Expected Output

- Extended models and forwarder implementation.
- Updated `README.md`.
- `requirements.txt` and `run_tests.ps1`.
- Optional: `tests/` with a simple Python test harness.

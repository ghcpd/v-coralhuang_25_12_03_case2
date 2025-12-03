from __future__ import annotations
import asyncio
from datetime import datetime, timedelta
import logging
import random
from typing import List, Dict
from sqlalchemy import select, update, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

BATCH_SIZE = 100
SCAN_INTERVAL_SECONDS = 1
MAX_ATTEMPTS = 5
WORKER_CONCURRENCY = 10
LOCK_TIMEOUT_SECONDS = 30  # stale lock recovery
BASE_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 60
JITTER_FACTOR = 0.2

# Simple in-memory metrics for verification
METRICS = {
    "forwarded": 0,
    "failed": 0,
    "dlq": 0,
    "latencies": [],
}

async def claim_batch(session: AsyncSession, worker_id: str) -> List[OutboxEvent]:
    now = datetime.utcnow()
    stale_cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)

    # first select candidate ids (atomic behavior depends on DB but this pattern works for SQLite)
    stmt = (
        select(OutboxEvent.id)
        .where(
            OutboxEvent.status.in_(("new", "failed")),
            or_(OutboxEvent.next_retry_at == None, OutboxEvent.next_retry_at <= now),
            or_(OutboxEvent.locked_by == None, OutboxEvent.lock_acquired_at <= stale_cutoff),
        )
        .limit(BATCH_SIZE)
    )
    ids = [r[0] for r in (await session.execute(stmt)).all()]
    if not ids:
        return []

    # claim by updating rows to locked state
    upd = (
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(ids))
        .where(OutboxEvent.status.in_(("new", "failed")))
        .values(status="locked", locked_by=worker_id, lock_acquired_at=now, updated_at=now)
        .execution_options(synchronize_session="fetch")
    )
    await session.execute(upd)
    await session.commit()

    # fetch locked rows for this worker
    stmt2 = select(OutboxEvent).where(OutboxEvent.locked_by == worker_id).limit(BATCH_SIZE)
    rows = (await session.execute(stmt2)).scalars().all()
    return rows

async def release_lock(session: AsyncSession, record: OutboxEvent, reason: str | None = None):
    record.locked_by = None
    record.lock_acquired_at = None
    record.updated_at = datetime.utcnow()
    if reason:
        audit = OutboxAudit(outbox_id=record.id, action="unlock", detail=reason)
        session.add(audit)

async def move_to_dlq(session: AsyncSession, record: OutboxEvent, reason: str):
    dlq = OutboxDLQ(id=record.id, original_id=record.id, topic=record.topic, payload=record.payload, reason=reason)
    session.add(dlq)
    audit = OutboxAudit(outbox_id=record.id, action="dead", detail=reason)
    session.add(audit)
    await session.flush()
    # delete original track
    await session.delete(record)
    METRICS["dlq"] += 1

async def record_audit(session: AsyncSession, outbox_id: str, action: str, detail: str | None = None):
    session.add(OutboxAudit(outbox_id=outbox_id, action=action, detail=detail))

def compute_next_retry(attempt: int) -> datetime:
    backoff = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** attempt))
    jitter = random.uniform(-JITTER_FACTOR * backoff, JITTER_FACTOR * backoff)
    delay = max(0, backoff + jitter)
    return datetime.utcnow() + timedelta(seconds=delay)

async def forward_with_retry(session: AsyncSession, record: OutboxEvent):
    start = datetime.utcnow()
    try:
        existed = await session.get(Event, record.id)
        if existed:
            record.status = "forwarded"
            record.updated_at = datetime.utcnow()
            await record_audit(session, record.id, "forwarded", "already existed")
            METRICS["forwarded"] += 1
            return

        evt = Event(id=record.id, topic=record.topic, payload=record.payload)
        session.add(evt)
        record.status = "forwarded"
        record.updated_at = datetime.utcnow()
        await record_audit(session, record.id, "forwarded", None)
        METRICS["forwarded"] += 1
    except Exception as exc:
        record.attempt += 1
        record.updated_at = datetime.utcnow()
        if record.attempt >= MAX_ATTEMPTS:
            await move_to_dlq(session, record, f"max attempts reached: {exc}")
            METRICS["failed"] += 1
        else:
            record.status = "failed"
            record.next_retry_at = compute_next_retry(record.attempt)
            await record_audit(session, record.id, "retry", f"attempt={record.attempt} error={exc}")
            METRICS["failed"] += 1
    finally:
        lat = (datetime.utcnow() - start).total_seconds()
        METRICS["latencies"].append(lat)

async def process_locked_batch(session_factory, batch: List[OutboxEvent]):
    sem = asyncio.Semaphore(WORKER_CONCURRENCY)

    async def worker(rec: OutboxEvent):
        async with sem:
            async with SessionLocal() as session:
                # re-load record in fresh session
                r = await session.get(OutboxEvent, rec.id)
                if not r:
                    return
                await forward_with_retry(session, r)
                # release if still present and locked
                if r and r.status != "locked":
                    # already changed (forwarded/failed/moved)
                    pass
                else:
                    await release_lock(session, r, "processed")
                await session.commit()

    tasks = [asyncio.create_task(worker(r)) for r in batch]
    await asyncio.gather(*tasks)

async def recover_stale_locks(session: AsyncSession):
    now = datetime.utcnow()
    stale_cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    upd = (
        update(OutboxEvent)
        .where(OutboxEvent.status == "locked")
        .where(OutboxEvent.lock_acquired_at < stale_cutoff)
        .values(status="new", locked_by=None, lock_acquired_at=None, updated_at=now)
        .execution_options(synchronize_session="fetch")
    )
    res = await session.execute(upd)
    if res.rowcount:
        await session.commit()
        LOGGER.info("Recovered %d stale locks", res.rowcount)

async def run_once(worker_id: str) -> Dict[str, int]:
    await init_db()
    async with SessionLocal() as session:
        await recover_stale_locks(session)
        batch = await claim_batch(session, worker_id)
        if not batch:
            return {"claimed": 0}
        LOGGER.info("Worker %s claimed %d records", worker_id, len(batch))
        await process_locked_batch(SessionLocal, batch)
        return {"claimed": len(batch)}

async def run_loop():
    await init_db()
    LOGGER.info("Outbox forwarder started (batch=%d interval=%ds concurrency=%d)", BATCH_SIZE, SCAN_INTERVAL_SECONDS, WORKER_CONCURRENCY)
    worker_counter = 0
    try:
        while True:
            worker_id = f"worker-{worker_counter}"
            async with SessionLocal() as session:
                await recover_stale_locks(session)
            result = await run_once(worker_id)
            if result.get("claimed", 0) == 0:
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            worker_counter += 1
    except asyncio.CancelledError:
        LOGGER.info("Forwarder cancelled")

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")

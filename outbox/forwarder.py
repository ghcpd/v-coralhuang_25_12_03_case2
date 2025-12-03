from __future__ import annotations
import asyncio
from datetime import datetime, timedelta
import logging
import random
from typing import List, Optional
from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# Configuration
BATCH_SIZE = 100
SCAN_INTERVAL_SECONDS = 1
MAX_ATTEMPTS = 3
WORKER_POOL_SIZE = 10
LOCK_TIMEOUT_SECONDS = 30
BASE_RETRY_DELAY_SECONDS = 2
MAX_RETRY_DELAY_SECONDS = 300
JITTER_FACTOR = 0.1

async def fetch_batch(session: AsyncSession, batch_size: int = BATCH_SIZE) -> List[OutboxEvent]:
    """Fetch batch of records ready for processing (new or retry candidates)"""
    now = datetime.utcnow()
    stmt = select(OutboxEvent).where(
        and_(
            OutboxEvent.status.in_(["new", "failed"]),
            or_(
                OutboxEvent.next_retry_at.is_(None),
                OutboxEvent.next_retry_at <= now
            )
        )
    ).limit(batch_size)
    rows = (await session.execute(stmt)).scalars().all()
    return rows

async def acquire_locks(session: AsyncSession, record_ids: List[str]) -> int:
    """Atomically lock records for processing. Returns count of locked records."""
    stmt = update(OutboxEvent).where(
        and_(
            OutboxEvent.id.in_(record_ids),
            OutboxEvent.status.in_(["new", "failed"])
        )
    ).values(status="locked", updated_at=datetime.utcnow())
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount

async def release_stale_locks(session: AsyncSession, timeout_seconds: int = LOCK_TIMEOUT_SECONDS):
    """Release locks older than timeout to recover from crashed workers"""
    stale_time = datetime.utcnow() - timedelta(seconds=timeout_seconds)
    stmt = update(OutboxEvent).where(
        and_(
            OutboxEvent.status == "locked",
            OutboxEvent.updated_at < stale_time
        )
    ).values(status="new", updated_at=datetime.utcnow())
    result = await session.execute(stmt)
    if result.rowcount > 0:
        LOGGER.info("Released %d stale locks", result.rowcount)
    await session.commit()

async def audit_log(session: AsyncSession, outbox_id: str, action: str, details: Optional[str] = None):
    """Log audit entry for lifecycle event"""
    audit = OutboxAudit(outbox_id=outbox_id, action=action, details=details)
    session.add(audit)

async def calculate_next_retry(attempt: int) -> datetime:
    """Calculate next retry time using exponential backoff with jitter"""
    if attempt < 1:
        attempt = 1
    delay = BASE_RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
    delay = min(delay, MAX_RETRY_DELAY_SECONDS)
    jitter = delay * JITTER_FACTOR * random.uniform(-1, 1)
    final_delay = delay + jitter
    return datetime.utcnow() + timedelta(seconds=max(0.1, final_delay))

async def forward_record(session: AsyncSession, record: OutboxEvent):
    """Forward a single record: write to events table, mark as forwarded"""
    # Check if already forwarded (idempotent)
    existed = await session.get(Event, record.id)
    if existed:
        record.status = "forwarded"
        record.updated_at = datetime.utcnow()
        await audit_log(session, record.id, "forward", "already_existed")
        return
    
    # Create event
    evt = Event(id=record.id, topic=record.topic, payload=record.payload)
    session.add(evt)
    record.status = "forwarded"
    record.updated_at = datetime.utcnow()
    await audit_log(session, record.id, "forward", "success")

async def process_record(record: OutboxEvent) -> dict:
    """Process single record in own session. Returns status dict."""
    try:
        async with SessionLocal() as session:
            # Fetch fresh copy
            fresh = await session.get(OutboxEvent, record.id)
            if not fresh:
                return {"id": record.id, "status": "not_found"}
            
            if fresh.status != "locked":
                return {"id": record.id, "status": "not_locked"}
            
            try:
                await forward_record(session, fresh)
                await session.commit()
                LOGGER.info("Forwarded outbox_id=%s", record.id)
                return {"id": record.id, "status": "forwarded"}
            except Exception as e:
                await session.rollback()
                LOGGER.warning("Forward failed id=%s attempt=%d error=%s", record.id, fresh.attempt + 1, e)
                
                # Update record with retry schedule or move to DLQ
                fresh.attempt += 1
                if fresh.attempt >= MAX_ATTEMPTS:
                    # Move to DLQ
                    dlq = OutboxDLQ(
                        id=record.id,
                        topic=fresh.topic,
                        payload=fresh.payload,
                        attempt=fresh.attempt,
                        error_message=str(e)
                    )
                    session.add(dlq)
                    fresh.status = "failed"
                    await audit_log(session, record.id, "dead", f"max_attempts_exceeded: {e}")
                    LOGGER.error("Moved to DLQ id=%s attempts=%d", record.id, fresh.attempt)
                    result = {"id": record.id, "status": "dead"}
                else:
                    # Schedule retry
                    fresh.next_retry_at = await calculate_next_retry(fresh.attempt)
                    fresh.status = "failed"
                    await audit_log(session, record.id, "retry", f"attempt {fresh.attempt}, retry at {fresh.next_retry_at}")
                    LOGGER.info("Scheduled retry id=%s next_retry_at=%s", record.id, fresh.next_retry_at)
                    result = {"id": record.id, "status": "retry"}
                
                await session.commit()
                return result
    except Exception as e:
        LOGGER.error("Unexpected error processing id=%s: %s", record.id, e)
        return {"id": record.id, "status": "error"}

async def process_batch(session: AsyncSession, records: List[OutboxEvent]) -> dict:
    """Process batch with bounded concurrency. Must be called within session context."""
    if not records:
        return {"total": 0, "forwarded": 0, "retry": 0, "dead": 0, "error": 0}
    
    # Lock the records atomically
    record_ids = [r.id for r in records]
    locked_count = await acquire_locks(session, record_ids)
    await session.commit()
    
    if locked_count == 0:
        LOGGER.warning("Failed to lock any records from batch of %d", len(records))
        return {"total": len(records), "forwarded": 0, "retry": 0, "dead": 0, "error": 0}
    
    LOGGER.info("Locked %d/%d records", locked_count, len(records))
    
    # Process with worker pool
    results = {"total": len(records), "forwarded": 0, "retry": 0, "dead": 0, "error": 0}
    tasks = [process_record(r) for r in records]
    
    for coro in asyncio.as_completed(tasks, timeout=30):
        try:
            result = await coro
            status = result.get("status", "error")
            if status == "forwarded":
                results["forwarded"] += 1
            elif status == "retry":
                results["retry"] += 1
            elif status == "dead":
                results["dead"] += 1
            else:
                results["error"] += 1
        except asyncio.TimeoutError:
            results["error"] += 1
        except Exception as e:
            LOGGER.error("Error in worker: %s", e)
            results["error"] += 1
    
    return results

async def run_loop(worker_pool_size: int = WORKER_POOL_SIZE):
    """Main forwarder loop: scan, lock, process, repeat"""
    await init_db()
    LOGGER.info(
        "Outbox forwarder started (batch=%d workers=%d interval=%ds)",
        BATCH_SIZE, worker_pool_size, SCAN_INTERVAL_SECONDS
    )
    
    iteration = 0
    total_forwarded = 0
    total_retry = 0
    total_dead = 0
    total_error = 0
    
    try:
        while True:
            iteration += 1
            LOGGER.debug("=== Iteration %d ===", iteration)
            
            async with SessionLocal() as session:
                # Release stale locks first
                await release_stale_locks(session, LOCK_TIMEOUT_SECONDS)
                
                # Fetch next batch
                batch = await fetch_batch(session, BATCH_SIZE)
                if batch:
                    LOGGER.info("Iteration %d: Fetched %d outbox records", iteration, len(batch))
                    results = await process_batch(session, batch)
                    total_forwarded += results["forwarded"]
                    total_retry += results["retry"]
                    total_dead += results["dead"]
                    total_error += results["error"]
                    LOGGER.info(
                        "Iteration %d results: forwarded=%d retry=%d dead=%d error=%d (cumulative: %d/%d/%d/%d)",
                        iteration, results["forwarded"], results["retry"], results["dead"], results["error"],
                        total_forwarded, total_retry, total_dead, total_error
                    )
                else:
                    LOGGER.debug("No new records")
            
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
    finally:
        LOGGER.info(
            "Forwarder stopped. Total: forwarded=%d retry=%d dead=%d error=%d",
            total_forwarded, total_retry, total_dead, total_error
        )

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")

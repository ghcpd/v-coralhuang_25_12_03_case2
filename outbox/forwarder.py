from __future__ import annotations
import asyncio
from datetime import datetime, timedelta
import logging
import os
import random
import time
from typing import List, Optional
from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# Configuration
BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", "100"))
SCAN_INTERVAL_SECONDS = float(os.getenv("OUTBOX_SCAN_INTERVAL", "1"))
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "5"))
WORKER_CONCURRENCY = int(os.getenv("OUTBOX_WORKERS", "10"))
LOCK_TIMEOUT_SECONDS = int(os.getenv("OUTBOX_LOCK_TIMEOUT", "300"))
BASE_RETRY_DELAY_SECONDS = int(os.getenv("OUTBOX_BASE_RETRY_DELAY", "2"))
MAX_RETRY_DELAY_SECONDS = int(os.getenv("OUTBOX_MAX_RETRY_DELAY", "3600"))

# Metrics tracking
class Metrics:
    def __init__(self):
        self.forwarded_count = 0
        self.retry_count = 0
        self.dlq_count = 0
        self.lock_acquired_count = 0
        self.total_latency_ms = 0
        self.processed_count = 0

    def record_forward(self, latency_ms: float):
        self.forwarded_count += 1
        self.processed_count += 1
        self.total_latency_ms += latency_ms

    def record_retry(self):
        self.retry_count += 1
        self.processed_count += 1

    def record_dlq(self):
        self.dlq_count += 1
        self.processed_count += 1

    def record_lock(self):
        self.lock_acquired_count += 1

    def get_avg_latency(self) -> float:
        if self.processed_count == 0:
            return 0.0
        return self.total_latency_ms / self.processed_count

    def log_summary(self):
        LOGGER.info(
            "Metrics: forwarded=%d, retry=%d, dlq=%d, locks=%d, avg_latency=%.2fms",
            self.forwarded_count,
            self.retry_count,
            self.dlq_count,
            self.lock_acquired_count,
            self.get_avg_latency()
        )

metrics = Metrics()

def calculate_next_retry(attempt: int) -> datetime:
    """Calculate next retry time with exponential backoff and jitter."""
    delay = min(BASE_RETRY_DELAY_SECONDS * (2 ** attempt), MAX_RETRY_DELAY_SECONDS)
    jitter = random.uniform(0, delay * 0.1)  # 10% jitter
    return datetime.utcnow() + timedelta(seconds=delay + jitter)

async def create_audit(session: AsyncSession, outbox_id: str, action: str,
                      old_status: Optional[str], new_status: Optional[str],
                      attempt: Optional[int], error_message: Optional[str] = None):
    """Create audit trail entry."""
    audit = OutboxAudit(
        outbox_id=outbox_id,
        action=action,
        old_status=old_status,
        new_status=new_status,
        attempt=attempt,
        error_message=error_message
    )
    session.add(audit)

async def fetch_and_lock_batch(session: AsyncSession) -> List[str]:
    """Atomically fetch and lock a batch of records ready for processing."""
    now = datetime.utcnow()
    
    # Find records ready for processing
    stmt = select(OutboxEvent.id).where(
        and_(
            or_(OutboxEvent.status == "new", OutboxEvent.status == "failed"),
            or_(OutboxEvent.next_retry_at.is_(None), OutboxEvent.next_retry_at <= now)
        )
    ).limit(BATCH_SIZE).with_for_update(skip_locked=True)
    
    result = await session.execute(stmt)
    ids = [row[0] for row in result.fetchall()]
    
    if not ids:
        return []
    
    # Atomic lock update
    update_stmt = update(OutboxEvent).where(
        OutboxEvent.id.in_(ids)
    ).values(
        status="locked",
        locked_at=now,
        updated_at=now
    )
    
    await session.execute(update_stmt)
    await session.commit()
    
    metrics.record_lock()
    LOGGER.info("Locked %d records for processing", len(ids))
    
    return ids

async def recover_stale_locks(session: AsyncSession):
    """Reset stale locked records back to 'new' status."""
    timeout_threshold = datetime.utcnow() - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    
    stmt = update(OutboxEvent).where(
        and_(
            OutboxEvent.status == "locked",
            OutboxEvent.locked_at < timeout_threshold
        )
    ).values(
        status="new",
        locked_at=None,
        updated_at=datetime.utcnow()
    )
    
    result = await session.execute(stmt)
    await session.commit()
    
    if result.rowcount > 0:
        LOGGER.warning("Recovered %d stale locked records", result.rowcount)

async def forward_record(session: AsyncSession, record_id: str) -> bool:
    """Forward a single record. Returns True on success, False on failure."""
    start_time = time.time()
    
    # Fetch the locked record
    record = await session.get(OutboxEvent, record_id)
    if not record or record.status != "locked":
        LOGGER.warning("Record %s not in locked state, skipping", record_id)
        return False
    
    old_status = record.status
    
    try:
        # Check if already forwarded (idempotent)
        existed = await session.get(Event, record.id)
        if not existed:
            # Create event
            evt = Event(id=record.id, topic=record.topic, payload=record.payload)
            session.add(evt)
        
        # Mark as forwarded
        record.status = "forwarded"
        record.updated_at = datetime.utcnow()
        
        # Audit trail
        await create_audit(session, record.id, "forwarded", old_status, "forwarded", record.attempt)
        
        await session.commit()
        
        latency_ms = (time.time() - start_time) * 1000
        metrics.record_forward(latency_ms)
        LOGGER.debug("Forwarded record %s in %.2fms", record.id, latency_ms)
        return True
        
    except Exception as e:
        await session.rollback()
        LOGGER.error("Forward failed for record %s: %s", record.id, e)
        
        # Refetch record after rollback
        record = await session.get(OutboxEvent, record_id)
        if not record:
            return False
        
        record.attempt += 1
        record.updated_at = datetime.utcnow()
        
        if record.attempt >= MAX_ATTEMPTS:
            # Move to DLQ
            dlq = OutboxDLQ(
                id=record.id,
                topic=record.topic,
                payload=record.payload,
                attempt=record.attempt,
                last_error=str(e),
                original_created_at=record.created_at
            )
            session.add(dlq)
            record.status = "failed"
            
            await create_audit(session, record.id, "moved_to_dlq", old_status, "failed",
                             record.attempt, str(e))
            metrics.record_dlq()
            LOGGER.warning("Moved record %s to DLQ after %d attempts", record.id, record.attempt)
        else:
            # Schedule retry with exponential backoff
            record.next_retry_at = calculate_next_retry(record.attempt)
            record.status = "failed"
            
            await create_audit(session, record.id, "retry_scheduled", old_status, "failed",
                             record.attempt, str(e))
            metrics.record_retry()
            LOGGER.info("Scheduled retry for record %s at %s (attempt %d)",
                       record.id, record.next_retry_at, record.attempt)
        
        await session.commit()
        return False

async def process_worker(record_ids: List[str], semaphore: asyncio.Semaphore):
    """Worker to process records with controlled concurrency."""
    for record_id in record_ids:
        async with semaphore:
            async with SessionLocal() as session:
                await forward_record(session, record_id)

async def process_batch(locked_ids: List[str]):
    """Process locked records with worker pool concurrency."""
    if not locked_ids:
        return
    
    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
    
    # Split work across workers
    workers = []
    chunk_size = max(1, len(locked_ids) // WORKER_CONCURRENCY)
    
    for i in range(0, len(locked_ids), chunk_size):
        chunk = locked_ids[i:i + chunk_size]
        workers.append(process_worker(chunk, semaphore))
    
    await asyncio.gather(*workers)

async def run_loop():
    """Main forwarder loop."""
    await init_db()
    LOGGER.info(
        "Outbox forwarder started (batch=%d, interval=%ds, workers=%d, max_attempts=%d)",
        BATCH_SIZE, SCAN_INTERVAL_SECONDS, WORKER_CONCURRENCY, MAX_ATTEMPTS
    )
    
    iteration = 0
    
    while True:
        iteration += 1
        
        # Periodically recover stale locks
        if iteration % 10 == 0:
            async with SessionLocal() as session:
                await recover_stale_locks(session)
        
        # Fetch and lock batch
        async with SessionLocal() as session:
            locked_ids = await fetch_and_lock_batch(session)
        
        if locked_ids:
            await process_batch(locked_ids)
        
        # Log metrics periodically
        if iteration % 10 == 0:
            metrics.log_summary()
        
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def replay_from_dlq(limit: int = 100):
    """Replay records from DLQ back to outbox."""
    await init_db()
    
    async with SessionLocal() as session:
        # Fetch DLQ records
        stmt = select(OutboxDLQ).limit(limit)
        dlq_records = (await session.execute(stmt)).scalars().all()
        
        if not dlq_records:
            LOGGER.info("No DLQ records to replay")
            return
        
        replayed_count = 0
        for dlq_record in dlq_records:
            # Check if already exists in outbox
            existing = await session.get(OutboxEvent, dlq_record.id)
            if existing:
                LOGGER.warning("Record %s already exists in outbox, skipping", dlq_record.id)
                continue
            
            # Create new outbox record
            outbox_record = OutboxEvent(
                id=dlq_record.id,
                topic=dlq_record.topic,
                payload=dlq_record.payload,
                status="new",
                attempt=0,
                next_retry_at=None,
                created_at=dlq_record.original_created_at,
                updated_at=datetime.utcnow()
            )
            session.add(outbox_record)
            
            # Audit trail
            await create_audit(session, dlq_record.id, "replayed", None, "new", 0)
            
            # Remove from DLQ
            await session.delete(dlq_record)
            replayed_count += 1
        
        await session.commit()
        LOGGER.info("Replayed %d records from DLQ", replayed_count)

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")
        metrics.log_summary()

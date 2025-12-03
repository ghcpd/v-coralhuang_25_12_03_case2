from __future__ import annotations
import asyncio
from datetime import datetime
import logging
from typing import List
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

BATCH_SIZE = 100
SCAN_INTERVAL_SECONDS = 1
MAX_ATTEMPTS = 3

async def fetch_batch(session: AsyncSession) -> List[OutboxEvent]:
    stmt = select(OutboxEvent).where(OutboxEvent.status == "new").limit(BATCH_SIZE)
    rows = (await session.execute(stmt)).scalars().all()
    return rows

async def forward_record(session: AsyncSession, record: OutboxEvent):
    # Check if already forwarded (idempotent)
    existed = await session.get(Event, record.id)
    if existed:
        record.status = "forwarded"
        record.updated_at = datetime.utcnow()
        return
    # Create event
    evt = Event(id=record.id, topic=record.topic, payload=record.payload)
    session.add(evt)
    record.status = "forwarded"
    record.updated_at = datetime.utcnow()

async def process_batch(session: AsyncSession, batch: List[OutboxEvent]):
    for r in batch:
        try:
            await forward_record(session, r)
        except Exception as e:  # minimal failure handling
            LOGGER.error("Forward failed id=%s error=%s", r.id, e)
            r.attempt += 1
            if r.attempt >= MAX_ATTEMPTS:
                r.status = "failed"
            r.updated_at = datetime.utcnow()
    await session.commit()

async def run_loop():
    await init_db()
    LOGGER.info("Outbox forwarder started (batch=%d interval=%ds)", BATCH_SIZE, SCAN_INTERVAL_SECONDS)
    while True:
        async with SessionLocal() as session:
            batch = await fetch_batch(session)
            if batch:
                LOGGER.info("Fetched %d outbox records", len(batch))
                await process_batch(session, batch)
            else:
                LOGGER.debug("No new records")
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")

"""DLQ (Dead-Letter Queue) replay utilities"""
from __future__ import annotations
import logging
from typing import Optional, List
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("dlq_replay")

async def list_dlq(limit: int = 100) -> List[dict]:
    """List dead-lettered items"""
    await init_db()
    async with SessionLocal() as session:
        stmt = select(OutboxDLQ).limit(limit)
        rows = (await session.execute(stmt)).scalars().all()
        return [
            {
                "id": r.id,
                "topic": r.topic,
                "attempt": r.attempt,
                "error_message": r.error_message,
                "dead_lettered_at": r.dead_lettered_at,
            }
            for r in rows
        ]

async def replay_dlq_item(dlq_id: str) -> bool:
    """Replay a single DLQ item: move back to outbox with attempt=0"""
    await init_db()
    async with SessionLocal() as session:
        # Get DLQ item
        dlq_item = await session.get(OutboxDLQ, dlq_id)
        if not dlq_item:
            LOGGER.warning("DLQ item not found: %s", dlq_id)
            return False
        
        # Check if already in outbox
        existing = await session.get(OutboxEvent, dlq_id)
        if existing:
            LOGGER.warning("Item already in outbox: %s", dlq_id)
            return False
        
        # Create new outbox record
        new_outbox = OutboxEvent(
            id=dlq_item.id,
            topic=dlq_item.topic,
            payload=dlq_item.payload,
            status="new",
            attempt=0,
            next_retry_at=None
        )
        session.add(new_outbox)
        
        # Delete from DLQ
        await session.delete(dlq_item)
        await session.commit()
        
        LOGGER.info("Replayed DLQ item: %s", dlq_id)
        return True

async def replay_dlq_all() -> int:
    """Replay all DLQ items back to outbox with attempt=0"""
    await init_db()
    async with SessionLocal() as session:
        # Get all DLQ items
        stmt = select(OutboxDLQ)
        dlq_items = (await session.execute(stmt)).scalars().all()
        
        count = 0
        for dlq_item in dlq_items:
            # Check if already in outbox
            existing = await session.get(OutboxEvent, dlq_item.id)
            if existing:
                LOGGER.warning("Item already in outbox, skipping: %s", dlq_item.id)
                continue
            
            # Create new outbox record
            new_outbox = OutboxEvent(
                id=dlq_item.id,
                topic=dlq_item.topic,
                payload=dlq_item.payload,
                status="new",
                attempt=0,
                next_retry_at=None
            )
            session.add(new_outbox)
            count += 1
        
        # Delete all DLQ items
        await session.execute(delete(OutboxDLQ))
        await session.commit()
        
        LOGGER.info("Replayed %d DLQ items", count)
        return count

if __name__ == "__main__":
    import asyncio
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "list":
        items = asyncio.run(list_dlq())
        for item in items:
            print(f"  {item['id']}: {item['topic']} (attempts: {item['attempt']}, error: {item['error_message']})")
    elif len(sys.argv) > 1 and sys.argv[1] == "replay":
        if len(sys.argv) > 2:
            asyncio.run(replay_dlq_item(sys.argv[2]))
        else:
            count = asyncio.run(replay_dlq_all())
            print(f"Replayed {count} items")
    else:
        print("Usage: python -m outbox.dlq_replay [list|replay [id]]")

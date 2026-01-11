"""Test utilities for seeding and validating Outbox Forwarder."""
from __future__ import annotations
import asyncio
import uuid
import json
from datetime import datetime
from sqlalchemy import select, func
from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

async def seed_outbox(count: int = 1000):
    """Seed outbox with test records."""
    await init_db()
    
    async with SessionLocal() as session:
        records = []
        for i in range(count):
            record = OutboxEvent(
                id=str(uuid.uuid4()),
                topic=f"test.topic.{i % 10}",
                payload=json.dumps({"index": i, "data": f"test_data_{i}"}),
                status="new",
                attempt=0
            )
            records.append(record)
        
        session.add_all(records)
        await session.commit()
    
    print(f"Seeded {count} outbox records")

async def get_stats():
    """Get current statistics from database."""
    async with SessionLocal() as session:
        # Outbox stats
        outbox_new = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "new")
        )
        outbox_forwarded = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "forwarded")
        )
        outbox_failed = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "failed")
        )
        outbox_locked = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "locked")
        )
        
        # Events count
        events_count = await session.scalar(select(func.count()).select_from(Event))
        
        # DLQ count
        dlq_count = await session.scalar(select(func.count()).select_from(OutboxDLQ))
        
        # Audit count
        audit_count = await session.scalar(select(func.count()).select_from(OutboxAudit))
        
        return {
            "outbox_new": outbox_new or 0,
            "outbox_forwarded": outbox_forwarded or 0,
            "outbox_failed": outbox_failed or 0,
            "outbox_locked": outbox_locked or 0,
            "events": events_count or 0,
            "dlq": dlq_count or 0,
            "audit": audit_count or 0
        }

async def print_stats():
    """Print current statistics."""
    stats = await get_stats()
    print("\n=== Database Statistics ===")
    print(f"Outbox - New: {stats['outbox_new']}")
    print(f"Outbox - Forwarded: {stats['outbox_forwarded']}")
    print(f"Outbox - Failed: {stats['outbox_failed']}")
    print(f"Outbox - Locked: {stats['outbox_locked']}")
    print(f"Events Table: {stats['events']}")
    print(f"DLQ: {stats['dlq']}")
    print(f"Audit Entries: {stats['audit']}")
    print("===========================\n")

async def cleanup_database():
    """Clean up all tables for fresh start."""
    await init_db()
    
    async with SessionLocal() as session:
        # Delete all records
        for table in [OutboxAudit, OutboxDLQ, Event, OutboxEvent]:
            await session.execute(table.__table__.delete())
        await session.commit()
    
    print("Database cleaned up")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "seed":
            count = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
            asyncio.run(seed_outbox(count))
        elif command == "stats":
            asyncio.run(print_stats())
        elif command == "cleanup":
            asyncio.run(cleanup_database())
        else:
            print("Unknown command. Available: seed, stats, cleanup")
    else:
        print("Usage: python -m outbox.test_utils <seed|stats|cleanup> [count]")

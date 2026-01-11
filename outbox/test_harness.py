"""Test harness: seed, run, measure"""
import asyncio
from datetime import datetime
import logging
import sys
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("test_harness")

async def seed_outbox(count: int = 1000, failure_rate: float = 0.0) -> int:
    """Seed test data into outbox"""
    await init_db()
    async with SessionLocal() as session:
        records = []
        for i in range(count):
            # Simulate some records that will fail (inject invalid data)
            if failure_rate > 0 and i % int(1.0 / failure_rate) == 0:
                payload = '{"invalid": json}'  # Invalid JSON to trigger error
            else:
                payload = f'{{"order_id": {i}, "user_id": "user_{i % 100}"}}'
            
            record = OutboxEvent(
                id=str(uuid.uuid4()),
                topic=f"orders.{i % 5}",
                payload=payload,
                status="new",
                attempt=0,
                next_retry_at=None
            )
            records.append(record)
        
        session.add_all(records)
        await session.commit()
        LOGGER.info("Seeded %d outbox records", count)
        return count

async def get_stats() -> dict:
    """Get current statistics"""
    async with SessionLocal() as session:
        # Count by status
        outbox_stats = (await session.execute(
            select(OutboxEvent.status, func.count(OutboxEvent.id))
            .group_by(OutboxEvent.status)
        )).all()
        
        forwarded_count = await session.scalar(select(func.count(Event.id)))
        dlq_count = await session.scalar(select(func.count(OutboxDLQ.id)))
        
        stats = {
            "outbox_by_status": {status: count for status, count in outbox_stats},
            "events_table": forwarded_count or 0,
            "dlq": dlq_count or 0,
        }
        return stats

async def print_summary():
    """Print test summary"""
    stats = await get_stats()
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    outbox_stats = stats["outbox_by_status"]
    total_outbox = sum(outbox_stats.values())
    
    print(f"Outbox records:")
    for status, count in sorted(outbox_stats.items()):
        print(f"  {status}: {count}")
    print(f"  Total: {total_outbox}")
    print(f"\nEvents table: {stats['events_table']}")
    print(f"DLQ: {stats['dlq']}")
    print(f"\nForwarded: {stats['events_table']} ({100.0 * stats['events_table'] / max(total_outbox, 1):.1f}%)")
    print("=" * 60 + "\n")

async def cleanup():
    """Clear all tables"""
    try:
        async with SessionLocal() as session:
            from sqlalchemy import delete
            # Try to delete from each table, ignore if table doesn't exist
            try:
                await session.execute(delete(OutboxAudit))
            except:
                pass
            try:
                await session.execute(delete(OutboxDLQ))
            except:
                pass
            try:
                await session.execute(delete(Event))
            except:
                pass
            try:
                await session.execute(delete(OutboxEvent))
            except:
                pass
            await session.commit()
            LOGGER.info("Cleaned up all tables")
    except Exception as e:
        LOGGER.info("No tables to clean (first run): %s", e)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seed":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        failure_rate = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
        asyncio.run(seed_outbox(count, failure_rate))
    elif len(sys.argv) > 1 and sys.argv[1] == "stats":
        asyncio.run(print_summary())
    elif len(sys.argv) > 1 and sys.argv[1] == "cleanup":
        asyncio.run(cleanup())
    else:
        print("Usage: python -m outbox.test_harness [seed [count [failure_rate]]|stats|cleanup]")

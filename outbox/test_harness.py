from __future__ import annotations

import argparse
import asyncio
import random
import time
import uuid
from datetime import datetime

from sqlalchemy import select, func

from outbox.database import init_db, reset_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ


def gen_id() -> str:
    return uuid.uuid4().hex


async def seed_outbox(total: int, transient_fail: int = 0, dead_fail: int = 0) -> dict:
    await init_db()
    normal = max(total - transient_fail - dead_fail, 0)
    topics = ([("transient_fail", transient_fail), ("dead_fail", dead_fail), ("normal", normal)])
    records = []
    for topic, count in topics:
        for _ in range(count):
            records.append(OutboxEvent(id=gen_id(), topic=topic, payload=f"payload-{topic}-{gen_id()}", status="new", attempt=0))
    random.shuffle(records)
    async with SessionLocal() as session:
        session.add_all(records)
        await session.commit()
    return {"seeded": len(records), "normal": normal, "transient_fail": transient_fail, "dead_fail": dead_fail}


async def summarize() -> dict:
    await init_db()
    async with SessionLocal() as session:
        counts = {}
        for status in ["new", "locked", "failed", "forwarded", "dead"]:
            res = await session.execute(select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == status))
            counts[status] = res.scalar_one()
        res_events = await session.execute(select(func.count()).select_from(Event))
        res_dlq = await session.execute(select(func.count()).select_from(OutboxDLQ))
        counts["events"] = res_events.scalar_one()
        counts["dlq"] = res_dlq.scalar_one()
    # Latency stats (end-to-end) for forwarded events
    async with SessionLocal() as session:
        rows = (
            await session.execute(
                select(OutboxEvent.created_at, Event.created_at).join(Event, Event.id == OutboxEvent.id)
            )
        ).all()
        if rows:
            latencies = [(e - o).total_seconds() for (o, e) in rows]
            counts["latency_avg_sec"] = sum(latencies) / len(latencies)
            counts["latency_p95_sec"] = sorted(latencies)[int(0.95 * len(latencies)) - 1]
        else:
            counts["latency_avg_sec"] = None
            counts["latency_p95_sec"] = None
    return counts


async def main():
    parser = argparse.ArgumentParser(description="Outbox test harness")
    parser.add_argument("--seed", type=int, default=None, help="Seed N outbox records")
    parser.add_argument("--transient-fail", type=int, default=0, help="How many transient failure records to seed")
    parser.add_argument("--dead-fail", type=int, default=0, help="How many dead failure records to seed")
    parser.add_argument("--summary", action="store_true", help="Print summary counts")
    parser.add_argument("--reset-db", action="store_true", help="Reset database before actions")
    args = parser.parse_args()

    if args.reset_db:
        await reset_db()

    results = {}
    if args.seed is not None:
        t0 = time.monotonic()
        results["seed"] = await seed_outbox(args.seed, transient_fail=args.transient_fail, dead_fail=args.dead_fail)
        results["seed"]["seconds"] = time.monotonic() - t0

    if args.summary:
        results["summary"] = await summarize()

    if results:
        import json

        print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations
import asyncio
import uuid
from datetime import datetime, timedelta
import random
import logging

from .database import init_db, SessionLocal
from .models import OutboxEvent, Event, OutboxDLQ
from sqlalchemy import select, func
from .forwarder import Forwarder

LOGGER = logging.getLogger("outbox_scripts")


async def seed_outbox(count: int = 500, fail_ratio: float = 0.02):
    await init_db()
    now = datetime.utcnow()
    async with SessionLocal() as session:
        # create some sample rows (some intentionally failing topics)
        for i in range(count):
            rid = uuid.uuid4().hex
            topic = "good_event"
            # cause deterministic failures for a small fraction
            if random.random() < fail_ratio:
                topic = "fail_transient"
            payload = f"{{\"n\":{i}}}"
            row = OutboxEvent(id=rid, topic=topic, payload=payload, status="new", attempt=0, created_at=now, updated_at=now)
            session.add(row)
        await session.commit()
    LOGGER.info("seeded %d outbox rows (fail_ratio=%s)", count, fail_ratio)


async def run_one_instance(wait_until_done: bool = True):
    f = Forwarder()
    res = await f.run_until_empty(quiet=True)
    return res


async def run_two_instances():
    f1 = Forwarder()
    f2 = Forwarder()
    # run both concurrently (they should not duplicate events)
    t1 = asyncio.create_task(f1.run_until_empty(quiet=True))
    t2 = asyncio.create_task(f2.run_until_empty(quiet=True))
    res1 = await t1
    res2 = await t2
    return res1, res2


async def db_counts() -> dict:
    async with SessionLocal() as session:
        events = (await session.execute(select(func.count()).select_from(Event))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxEvent))).scalar_one()
        dlq = (await session.execute(select(func.count()).select_from(OutboxDLQ))).scalar_one()
        return {"events": events, "outbox": outbox, "dlq": dlq}


async def run_tests(seed_count: int = 1000, spawn_second_instance: bool = True):
    await seed_outbox(seed_count)
    before = await db_counts()
    print(f"Before: {before}")
    # first single instance run
    print("Running single forwarder instance...")
    single = await run_one_instance()
    after_single = await db_counts()
    print(f"After single instance: {after_single} stats={single}")

    if spawn_second_instance:
        # seed some more rows and run two forwarders concurrently to validate locking
        await seed_outbox(200)
        print("Running two concurrent forwarders to test locking...")
        r1, r2 = await run_two_instances()
        after_multi = await db_counts()
        print(f"After concurrent runs: {after_multi} stats1={r1} stats2={r2}")

    # final summary
    final = await db_counts()
    print("*** Final summary ***")
    print(final)
    return final


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=500, help="how many rows to seed")
    p.add_argument("--spawn-second", action="store_true")
    args = p.parse_args()
    asyncio.run(run_tests(seed_count=args.count, spawn_second_instance=args.spawn_second))

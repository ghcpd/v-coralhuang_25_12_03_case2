from __future__ import annotations
import asyncio
import random
import string
import time
from datetime import datetime
from typing import List
from sqlalchemy import select, func

from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, OutboxAudit, Event

NUM_EVENTS = 500

async def seed_outbox(n: int = NUM_EVENTS):
    await init_db()
    async with SessionLocal() as session:
        for i in range(n):
            id_ = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(16))
            evt = OutboxEvent(id=id_, topic="test", payload=f"{{'n':{i}}}")
            session.add(evt)
        await session.commit()
    print(f"Seeded {n} outbox records")

async def summary():
    async with SessionLocal() as session:
        total = (await session.execute(select(func.count()).select_from(OutboxEvent))).scalar()
        forwarded = (await session.execute(select(func.count()).select_from(Event))).scalar()
        dlq = (await session.execute(select(func.count()).select_from(OutboxDLQ))).scalar()
        audits = (await session.execute(select(func.count()).select_from(OutboxAudit))).scalar()
    return {"outbox": total, "forwarded": forwarded, "dlq": dlq, "audit": audits}

async def run_test(seed=True, spawn_second=False, wait_seconds: int = 10):
    if seed:
        await seed_outbox()

    # run one forwarder instance for several cycles (imported instead of new process for simplicity)
    from outbox import forwarder

    task1 = asyncio.create_task(forwarder.run_loop())
    tasks = [task1]
    if spawn_second:
        task2 = asyncio.create_task(forwarder.run_loop())
        tasks.append(task2)

    # let forwarders run for a while then cancel
    start = time.time()
    await asyncio.sleep(wait_seconds)

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    took = time.time() - start
    s = await summary()
    print("--- Test summary ---")
    print(s)
    print(f"Took {took:.2f}s")

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--seed', action='store_true')
    parser.add_argument('--second', action='store_true')
    parser.add_argument('--wait', type=int, default=10)
    args = parser.parse_args()

    asyncio.run(run_test(seed=args.seed, spawn_second=args.second, wait_seconds=args.wait))

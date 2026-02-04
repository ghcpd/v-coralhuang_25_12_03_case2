from __future__ import annotations
import asyncio
import json
import logging
import os
import time
from argparse import ArgumentParser
from sqlalchemy import select, func

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, Event
from outbox.forwarder import seed_outbox, run_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_test_runner")

def parse_args():
    ap = ArgumentParser(description="Run outbox forwarder tests")
    ap.add_argument("--num", type=int, default=1000, help="Number of outbox records to seed")
    ap.add_argument("--fail-ratio", type=float, default=0.02, help="Ratio of records that should fail and go to DLQ")
    ap.add_argument("--instances", type=int, default=1, help="Number of forwarder instances to run concurrently")
    ap.add_argument("--db-path", type=str, default="outbox.db", help="SQLite file path (for cleanup)")
    return ap.parse_args()

async def summarize():
    async with SessionLocal() as session:
        total = await session.scalar(select(func.count()).select_from(OutboxEvent))
        forwarded = await session.scalar(select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "forwarded"))
        failed = await session.scalar(select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "failed"))
        dead = await session.scalar(select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "dead"))
        dlq = await session.scalar(select(func.count()).select_from(OutboxDLQ))
        events = await session.scalar(select(func.count()).select_from(Event))
        lat_rows = (await session.execute(select(Event.created_at, OutboxEvent.created_at).join(OutboxEvent, Event.id == OutboxEvent.id))).all()
        latencies = [(e - o).total_seconds() for (e, o) in lat_rows if e and o]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    return {
        "total_outbox": total or 0,
        "forwarded": forwarded or 0,
        "failed": failed or 0,
        "dead": dead or 0,
        "dlq": dlq or 0,
        "events": events or 0,
        "avg_latency_s": avg_latency,
    }

async def main():
    args = parse_args()
    # Clean DB for reproducibility
    if args.db_path and os.path.exists(args.db_path):
        os.remove(args.db_path)
    await init_db()
    await seed_outbox(args.num, args.fail_ratio)
    start = time.perf_counter()
    await run_loop(stop_when_idle=True, instances=args.instances)
    elapsed = time.perf_counter() - start
    summary = await summarize()
    summary["elapsed_s"] = elapsed
    summary["instances"] = args.instances
    LOGGER.info("Test summary: %s", summary)
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    asyncio.run(main())

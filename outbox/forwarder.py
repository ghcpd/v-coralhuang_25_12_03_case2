from __future__ import annotations
import asyncio
import logging
import os
import random
import socket
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import select, update, delete, or_, func
from sqlalchemy.exc import IntegrityError

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# Configurable via env vars
BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", 500))
SCAN_INTERVAL_SECONDS = float(os.getenv("OUTBOX_SCAN_INTERVAL_SECONDS", 0.2))
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", 5))
CONCURRENCY = int(os.getenv("OUTBOX_CONCURRENCY", 20))
LOCK_TIMEOUT_SECONDS = int(os.getenv("OUTBOX_LOCK_TIMEOUT_SECONDS", 30))
BACKOFF_BASE_SECONDS = float(os.getenv("OUTBOX_BACKOFF_BASE_SECONDS", 1))
BACKOFF_MAX_SECONDS = float(os.getenv("OUTBOX_BACKOFF_MAX_SECONDS", 60))
BACKOFF_JITTER = float(os.getenv("OUTBOX_BACKOFF_JITTER", 0.3))
IDLE_CYCLES_TO_STOP = int(os.getenv("OUTBOX_IDLE_CYCLES_TO_STOP", 3))
USE_SINGLE_SESSION_PROCESSING = os.getenv("OUTBOX_SINGLE_SESSION_PROCESSING", "true").lower() == "true"

def now_utc() -> datetime:
    return datetime.utcnow()

def compute_backoff(attempt: int) -> float:
    base = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    jitter = random.random() * BACKOFF_JITTER * BACKOFF_BASE_SECONDS
    return min(BACKOFF_MAX_SECONDS, base + jitter)

async def add_audit(session, outbox_id: str, action: str, detail: Optional[str] = None, lock_token: Optional[str] = None):
    session.add(OutboxAudit(outbox_id=outbox_id, action=action, detail=detail, lock_token=lock_token))

async def recover_stale_locks(session, now: datetime):
    stale_threshold = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    stale_ids = [row[0] for row in (await session.execute(select(OutboxEvent.id).where(
        OutboxEvent.status == "locked", OutboxEvent.locked_at < stale_threshold
    ))).all()]
    if not stale_ids:
        return 0
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(stale_ids))
        .values(status="new", locked_at=None, lock_token=None, updated_at=now)
    )
    for oid in stale_ids:
        await add_audit(session, oid, "unlock")
    await session.commit()
    LOGGER.warning("Recovered %d stale locks", len(stale_ids))
    return len(stale_ids)

async def lock_batch(session, instance_id: str, batch_size: int) -> List[str]:
    now = now_utc()
    ids = [row[0] for row in (await session.execute(select(OutboxEvent.id).where(
        OutboxEvent.status.in_(["new", "failed"]),
        or_(OutboxEvent.next_retry_at.is_(None), OutboxEvent.next_retry_at <= now)
    ).order_by(OutboxEvent.created_at).limit(batch_size))).all()]
    if not ids:
        return []
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(ids), OutboxEvent.status.in_(["new", "failed"]))
        .values(status="locked", locked_at=now, lock_token=instance_id, updated_at=now)
    )
    for oid in ids:
        await add_audit(session, oid, "lock", lock_token=instance_id)
    await session.commit()
    locked = [row[0] for row in (await session.execute(select(OutboxEvent.id).where(
        OutboxEvent.id.in_(ids), OutboxEvent.status == "locked", OutboxEvent.lock_token == instance_id
    ))).all()]
    return locked

async def forward_record(session, record: OutboxEvent):
    existed = await session.get(Event, record.id)
    if existed:
        return
    session.add(Event(id=record.id, topic=record.topic, payload=record.payload))

async def process_record(instance_id: str, record_id: str):
    async with SessionLocal() as session:
        record = await session.get(OutboxEvent, record_id)
        if not record or record.status != "locked" or record.lock_token != instance_id:
            return {"skipped": 1}
        try:
            now = now_utc()
            # Simulate unrecoverable failure for certain topics
            if record.topic == "fail":
                raise RuntimeError("Intentional failure for testing")
            await forward_record(session, record)
            record.status = "forwarded"
            record.lock_token = None
            record.locked_at = None
            record.next_retry_at = None
            record.updated_at = now
            await add_audit(session, record.id, "forward")
            await session.commit()
            latency = (now - record.created_at).total_seconds()
            return {"forwarded": 1, "latency": latency}
        except IntegrityError as ie:
            await session.rollback()
            # Idempotency: event may have been inserted by another instance
            existed = await session.get(Event, record_id)
            if existed:
                record = await session.get(OutboxEvent, record_id)
                if not record:
                    return {"skipped": 1}
                record.status = "forwarded"
                record.lock_token = None
                record.locked_at = None
                record.next_retry_at = None
                record.updated_at = now_utc()
                await add_audit(session, record.id, "forward")
                await session.commit()
                latency = (record.updated_at - record.created_at).total_seconds()
                return {"forwarded": 1, "latency": latency}
            return await _handle_failure(session, record_id, ie)
        except Exception as e:
            await session.rollback()
            return await _handle_failure(session, record_id, e)

async def _handle_failure(session, record_id: str, exc: Exception):
    record = await session.get(OutboxEvent, record_id)
    if not record:
        return {"skipped": 1}
    record.attempt += 1
    record.lock_token = None
    record.locked_at = None
    record.updated_at = now_utc()
    detail = str(exc)
    if record.attempt >= MAX_ATTEMPTS:
        record.status = "dead"
        session.add(OutboxDLQ(outbox_id=record.id, topic=record.topic, payload=record.payload, attempt=record.attempt, error=detail))
        await add_audit(session, record.id, "dead", detail=detail)
        await session.commit()
        return {"dead": 1}
    else:
        record.status = "failed"
        delay = compute_backoff(record.attempt)
        record.next_retry_at = record.updated_at + timedelta(seconds=delay)
        await add_audit(session, record.id, "retry", detail=detail)
        await session.commit()
        return {"retry": 1}

async def process_locked_batch_single_session(instance_id: str, record_ids: List[str]):
    async with SessionLocal() as session:
        agg = {"forwarded": 0, "retry": 0, "dead": 0, "skipped": 0, "latencies": []}
        for rid in record_ids:
            record = await session.get(OutboxEvent, rid)
            if not record or record.status != "locked" or record.lock_token != instance_id:
                agg["skipped"] += 1
                continue
            now = now_utc()
            try:
                if record.topic == "fail":
                    raise RuntimeError("Intentional failure for testing")
                existed = await session.get(Event, rid)
                if not existed:
                    session.add(Event(id=record.id, topic=record.topic, payload=record.payload))
                record.status = "forwarded"
                record.lock_token = None
                record.locked_at = None
                record.next_retry_at = None
                record.updated_at = now
                await add_audit(session, record.id, "forward")
                agg["forwarded"] += 1
                agg["latencies"].append((now - record.created_at).total_seconds())
            except Exception as e:
                record.attempt += 1
                record.lock_token = None
                record.locked_at = None
                record.updated_at = now
                detail = str(e)
                if record.attempt >= MAX_ATTEMPTS:
                    record.status = "dead"
                    session.add(OutboxDLQ(outbox_id=record.id, topic=record.topic, payload=record.payload, attempt=record.attempt, error=detail))
                    await add_audit(session, record.id, "dead", detail=detail)
                    agg["dead"] += 1
                else:
                    record.status = "failed"
                    delay = compute_backoff(record.attempt)
                    record.next_retry_at = now + timedelta(seconds=delay)
                    await add_audit(session, record.id, "retry", detail=detail)
                    agg["retry"] += 1
        await session.commit()
        return agg

async def process_locked_batch(instance_id: str, record_ids: List[str]):
    if USE_SINGLE_SESSION_PROCESSING:
        return await process_locked_batch_single_session(instance_id, record_ids)
    sem = asyncio.Semaphore(CONCURRENCY)
    async def worker(rid):
        async with sem:
            return await process_record(instance_id, rid)
    results = await asyncio.gather(*(worker(rid) for rid in record_ids))
    agg = {"forwarded": 0, "retry": 0, "dead": 0, "skipped": 0, "latencies": []}
    for r in results:
        if not r:
            continue
        for k in ("forwarded", "retry", "dead", "skipped"):
            agg[k] += r.get(k, 0)
        if "latency" in r:
            agg["latencies"].append(r["latency"])
    return agg

async def run_loop(stop_when_idle: bool = False, instances: int = 1):
    await init_db()
    forwarders = [OutboxForwarder() for _ in range(instances)]
    await asyncio.gather(*(f.run(stop_when_idle=stop_when_idle) for f in forwarders))

class OutboxForwarder:
    def __init__(self, instance_id: Optional[str] = None):
        self.instance_id = instance_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

    async def run(self, stop_when_idle: bool = False):
        LOGGER.info("Forwarder %s started (batch=%d interval=%.1fs concurrency=%d)", self.instance_id, BATCH_SIZE, SCAN_INTERVAL_SECONDS, CONCURRENCY)
        idle_cycles = 0
        while True:
            pending = 0
            next_due = None
            async with SessionLocal() as session:
                now = now_utc()
                await recover_stale_locks(session, now)
                locked_ids = await lock_batch(session, self.instance_id, BATCH_SIZE)
                if not locked_ids:
                    pending, next_due = await pending_stats(session)
            if not locked_ids:
                if pending == 0:
                    idle_cycles += 1
                    if stop_when_idle and idle_cycles >= IDLE_CYCLES_TO_STOP:
                        LOGGER.info("Forwarder %s stopping idle", self.instance_id)
                        break
                else:
                    idle_cycles = 0
                wait = SCAN_INTERVAL_SECONDS
                if next_due and next_due > now:
                    wait = min(SCAN_INTERVAL_SECONDS, max((next_due - now).total_seconds(), 0))
                await asyncio.sleep(wait)
                continue
            idle_cycles = 0
            agg = await process_locked_batch(self.instance_id, locked_ids)
            latencies = agg.pop("latencies", [])
            avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
            LOGGER.info("Forwarder %s processed=%s avg_latency=%.3fs", self.instance_id, agg, avg_latency)
            await asyncio.sleep(0)  # yield

async def seed_outbox(n: int = 1000, fail_ratio: float = 0.0):
    await init_db()
    async with SessionLocal() as session:
        to_add = []
        for i in range(n):
            oid = uuid.uuid4().hex
            topic = "fail" if (fail_ratio > 0 and random.random() < fail_ratio) else "demo"
            payload = f"payload-{oid}"
            to_add.append(OutboxEvent(id=oid, topic=topic, payload=payload))
        session.add_all(to_add)
        await session.commit()
    LOGGER.info("Seeded %d outbox rows (fail_ratio=%.2f)", n, fail_ratio)

async def replay_dlq(ids: Optional[List[str]] = None):
    await init_db()
    async with SessionLocal() as session:
        if ids:
            dlq_rows = (await session.execute(select(OutboxDLQ).where(OutboxDLQ.outbox_id.in_(ids)))).scalars().all()
        else:
            dlq_rows = (await session.execute(select(OutboxDLQ))).scalars().all()
        for row in dlq_rows:
            ob = await session.get(OutboxEvent, row.outbox_id)
            if ob:
                ob.status = "new"
                ob.attempt = 0
                ob.next_retry_at = None
                ob.locked_at = None
                ob.lock_token = None
                ob.updated_at = now_utc()
                await add_audit(session, ob.id, "replay")
            else:
                await add_audit(session, row.outbox_id, "replay")
        if dlq_rows:
            await session.execute(delete(OutboxDLQ).where(OutboxDLQ.outbox_id.in_([r.outbox_id for r in dlq_rows])))
        await session.commit()
        LOGGER.info("Replayed %d DLQ rows", len(dlq_rows))

def _parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Outbox forwarder")
    sub = parser.add_subparsers(dest="cmd")

    run_p = sub.add_parser("run", help="Run forwarder")
    run_p.add_argument("--stop-when-idle", action="store_true", help="Stop when queue is drained")
    run_p.add_argument("--instances", type=int, default=1, help="Number of forwarder instances to run")

    seed_p = sub.add_parser("seed", help="Seed outbox records")
    seed_p.add_argument("-n", "--num", type=int, default=1000)
    seed_p.add_argument("--fail-ratio", type=float, default=0.0)

    replay_p = sub.add_parser("replay", help="Replay DLQ back to outbox")
    replay_p.add_argument("ids", nargs="*", help="Outbox IDs to replay (default all)")

    return parser.parse_args()

async def _main_async():
    args = _parse_args()
    if args.cmd == "seed":
        await seed_outbox(args.num, args.fail_ratio)
    elif args.cmd == "replay":
        await replay_dlq(args.ids or None)
    else:
        await run_loop(stop_when_idle=getattr(args, "stop_when_idle", False), instances=getattr(args, "instances", 1))

def main():
    asyncio.run(_main_async())

if __name__ == "__main__":
    main()

async def pending_stats(session):
    count, next_due = (await session.execute(
        select(func.count(OutboxEvent.id), func.min(OutboxEvent.next_retry_at)).where(
            OutboxEvent.status.in_(["new", "failed"])
        )
    )).one()
    return (count or 0, next_due)

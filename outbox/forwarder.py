from __future__ import annotations

import asyncio
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Dict

from sqlalchemy import select, update, or_
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, EventOutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")


class NonRetryableError(Exception):
    """Raised to indicate an error that should go directly to DLQ."""


DEFAULT_CONFIG = {
    "batch_size": int(os.getenv("OUTBOX_BATCH_SIZE", 100)),
    "scan_interval": float(os.getenv("OUTBOX_SCAN_INTERVAL", 1.0)),
    "max_attempts": int(os.getenv("OUTBOX_MAX_ATTEMPTS", 5)),
    "concurrency": int(os.getenv("OUTBOX_CONCURRENCY", 5)),
    "lock_timeout": float(os.getenv("OUTBOX_LOCK_TIMEOUT", 30.0)),
    "backoff_base": float(os.getenv("OUTBOX_BACKOFF_BASE", 1.0)),
    "backoff_factor": float(os.getenv("OUTBOX_BACKOFF_FACTOR", 2.0)),
    "backoff_jitter": float(os.getenv("OUTBOX_BACKOFF_JITTER", 0.3)),
    "backoff_max": float(os.getenv("OUTBOX_BACKOFF_MAX", 60.0)),
}

LOCK_OWNER = os.getenv("OUTBOX_LOCK_OWNER", str(uuid.uuid4()))


def compute_backoff(attempt: int, base: float, factor: float, jitter: float, max_delay: float) -> float:
    delay = base * (factor ** max(attempt - 1, 0))
    jitter_delta = delay * jitter
    delay = delay + random.uniform(-jitter_delta, jitter_delta)
    return min(max_delay, max(0.0, delay))


async def record_audit(session: AsyncSession, *, outbox_id: str, action: str, status_from: Optional[str], status_to: Optional[str], attempt: Optional[int], message: Optional[str]):
    audit = EventOutboxAudit(
        outbox_id=outbox_id,
        action=action,
        status_from=status_from,
        status_to=status_to,
        attempt=attempt,
        message=message,
    )
    session.add(audit)


async def lock_batch(session: AsyncSession, *, owner: str, batch_size: int, max_attempts: int) -> List[str]:
    now_ts = datetime.utcnow()
    subq = (
        select(OutboxEvent.id)
        .where(
            OutboxEvent.status.in_(["new", "failed"]),
            OutboxEvent.attempt < max_attempts,
            or_(OutboxEvent.next_retry_at.is_(None), OutboxEvent.next_retry_at <= now_ts),
        )
        .order_by(OutboxEvent.created_at)
        .limit(batch_size)
    )
    stmt = (
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(subq))
        .values(status="locked", locked_at=now_ts, lock_owner=owner, updated_at=now_ts)
    )
    result = await session.execute(stmt)
    await session.commit()
    if result.rowcount:
        rows = (
            await session.execute(
                select(OutboxEvent.id).where(
                    OutboxEvent.lock_owner == owner, OutboxEvent.locked_at == now_ts, OutboxEvent.status == "locked"
                )
            )
        ).scalars().all()
        return list(rows)
    return []


async def recover_stale_locks(session: AsyncSession, *, lock_timeout: float) -> int:
    if lock_timeout <= 0:
        return 0
    deadline = datetime.utcnow() - timedelta(seconds=lock_timeout)
    stale_ids = (
        await session.execute(
            select(OutboxEvent.id, OutboxEvent.status).where(OutboxEvent.status == "locked", OutboxEvent.locked_at < deadline)
        )
    ).all()
    if not stale_ids:
        return 0
    ids = [row[0] for row in stale_ids]
    stmt = (
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(ids))
        .values(status="new", lock_owner=None, locked_at=None, updated_at=datetime.utcnow())
    )
    await session.execute(stmt)
    for row in stale_ids:
        await record_audit(
            session,
            outbox_id=row[0],
            action="unlock",
            status_from="locked",
            status_to="new",
            attempt=None,
            message="stale lock recovered",
        )
    await session.commit()
    return len(ids)


async def forward_record(session: AsyncSession, rec: OutboxEvent):
    # Idempotency: if event already exists, mark forwarded
    existed = await session.get(Event, rec.id)
    if existed:
        return
    # Simulated non-retryable failure hook
    if rec.topic == "dead_fail":
        raise NonRetryableError("non-retryable failure")
    # Simulated transient failure hook
    if rec.topic == "transient_fail":
        raise Exception("transient failure")
    evt = Event(id=rec.id, topic=rec.topic, payload=rec.payload)
    session.add(evt)


async def process_record(
    record_id: str,
    *,
    owner: str,
    session_factory,
    cfg: Dict,
    metrics: Dict,
):
    start = time.monotonic()
    async with session_factory() as session:
        rec = await session.get(OutboxEvent, record_id)
        if not rec or rec.lock_owner != owner or rec.status != "locked":
            return
        status_from = rec.status
        try:
            await forward_record(session, rec)
            rec.status = "forwarded"
            rec.lock_owner = None
            rec.locked_at = None
            rec.next_retry_at = None
            rec.updated_at = datetime.utcnow()
            await record_audit(session, outbox_id=rec.id, action="forward", status_from=status_from, status_to=rec.status, attempt=rec.attempt, message=None)
            await session.commit()
            metrics["forwarded"] += 1
        except NonRetryableError as e:
            rec.attempt += 1
            rec.status = "dead"
            rec.lock_owner = None
            rec.locked_at = None
            rec.updated_at = datetime.utcnow()
            error_msg = str(e)
            dlq = OutboxDLQ(outbox_id=rec.id, topic=rec.topic, payload=rec.payload, attempt=rec.attempt, error=error_msg)
            session.add(dlq)
            await record_audit(session, outbox_id=rec.id, action="dead", status_from=status_from, status_to=rec.status, attempt=rec.attempt, message=error_msg)
            await session.commit()
            metrics["dead"] += 1
        except Exception as e:
            rec.attempt += 1
            error_msg = repr(e)
            if rec.attempt >= cfg["max_attempts"]:
                rec.status = "dead"
                rec.lock_owner = None
                rec.locked_at = None
                rec.updated_at = datetime.utcnow()
                dlq = OutboxDLQ(outbox_id=rec.id, topic=rec.topic, payload=rec.payload, attempt=rec.attempt, error=error_msg)
                session.add(dlq)
                await record_audit(session, outbox_id=rec.id, action="dead", status_from=status_from, status_to=rec.status, attempt=rec.attempt, message=error_msg)
                await session.commit()
                metrics["dead"] += 1
            else:
                backoff = compute_backoff(rec.attempt, cfg["backoff_base"], cfg["backoff_factor"], cfg["backoff_jitter"], cfg["backoff_max"])
                rec.next_retry_at = datetime.utcnow() + timedelta(seconds=backoff)
                rec.status = "failed"
                rec.lock_owner = None
                rec.locked_at = None
                rec.updated_at = datetime.utcnow()
                await record_audit(session, outbox_id=rec.id, action="retry", status_from=status_from, status_to=rec.status, attempt=rec.attempt, message=error_msg)
                await session.commit()
                metrics["retried"] += 1
        finally:
            metrics["processed"] += 1
            metrics["latencies"].append(time.monotonic() - start)


async def pending_exists(session: AsyncSession, max_attempts: int, include_future: bool = False) -> bool:
    now_ts = datetime.utcnow()
    filters = [OutboxEvent.status.in_( ["new", "failed"] ), OutboxEvent.attempt < max_attempts]
    if not include_future:
        filters.append(or_(OutboxEvent.next_retry_at.is_(None), OutboxEvent.next_retry_at <= now_ts))
    stmt = select(OutboxEvent.id).where(*filters).limit(1)
    res = await session.execute(stmt)
    return res.scalar_one_or_none() is not None


async def run_forwarder(
    *,
    batch_size: int,
    scan_interval: float,
    max_attempts: int,
    concurrency: int,
    lock_timeout: float,
    backoff_base: float,
    backoff_factor: float,
    backoff_jitter: float,
    backoff_max: float,
    stop_when_idle: bool = False,
    run_for_seconds: Optional[float] = None,
    lock_owner: Optional[str] = None,
):
    cfg = dict(
        batch_size=batch_size,
        scan_interval=scan_interval,
        max_attempts=max_attempts,
        concurrency=concurrency,
        lock_timeout=lock_timeout,
        backoff_base=backoff_base,
        backoff_factor=backoff_factor,
        backoff_jitter=backoff_jitter,
        backoff_max=backoff_max,
    )
    owner = lock_owner or LOCK_OWNER
    await init_db()
    LOGGER.info("Forwarder started owner=%s batch=%d concurrency=%d", owner, batch_size, concurrency)
    session_factory = SessionLocal
    start_time = time.monotonic()
    while True:
        if run_for_seconds is not None and time.monotonic() - start_time > run_for_seconds:
            LOGGER.info("Forwarder stopping after run_for_seconds=%s", run_for_seconds)
            break
        async with session_factory() as session:
            recovered = await recover_stale_locks(session, lock_timeout=lock_timeout)
            if recovered:
                LOGGER.warning("Recovered %d stale locks", recovered)
            locked_ids = await lock_batch(session, owner=owner, batch_size=batch_size, max_attempts=max_attempts)
        if not locked_ids:
            if stop_when_idle:
                async with session_factory() as session:
                    if not await pending_exists(session, max_attempts, include_future=True):
                        LOGGER.info("No pending records; exiting")
                        break
            await asyncio.sleep(scan_interval)
            continue
        LOGGER.info("Locked %d records", len(locked_ids))
        sem = asyncio.Semaphore(concurrency)
        metrics = {"processed": 0, "forwarded": 0, "retried": 0, "dead": 0, "latencies": []}

        async def worker(record_id: str):
            async with sem:
                await process_record(
                    record_id,
                    owner=owner,
                    session_factory=session_factory,
                    cfg=cfg,
                    metrics=metrics,
                )

        await asyncio.gather(*(worker(rid) for rid in locked_ids))
        if metrics["processed"]:
            avg_latency = sum(metrics["latencies"]) / len(metrics["latencies"])
        else:
            avg_latency = 0.0
        LOGGER.info(
            "Batch done processed=%d forwarded=%d retried=%d dead=%d avg_latency=%.3fs",
            metrics["processed"],
            metrics["forwarded"],
            metrics["retried"],
            metrics["dead"],
            avg_latency,
        )
        # Optional small sleep to prevent tight loop
        await asyncio.sleep(0)


async def replay_dlq(ids: Optional[List[int]] = None):
    await init_db()
    async with SessionLocal() as session:
        if ids:
            dlq_entries = (
                await session.execute(select(OutboxDLQ).where(OutboxDLQ.id.in_(ids)))
            ).scalars().all()
        else:
            dlq_entries = (await session.execute(select(OutboxDLQ))).scalars().all()
        for entry in dlq_entries:
            rec = await session.get(OutboxEvent, entry.outbox_id)
            if not rec:
                rec = OutboxEvent(
                    id=entry.outbox_id,
                    topic=entry.topic,
                    payload=entry.payload,
                    status="new",
                    attempt=0,
                    next_retry_at=None,
                )
                session.add(rec)
            else:
                rec.status = "new"
                rec.attempt = 0
                rec.next_retry_at = None
                rec.lock_owner = None
                rec.locked_at = None
                rec.updated_at = datetime.utcnow()
            await record_audit(session, outbox_id=rec.id, action="replay", status_from="dead", status_to="new", attempt=rec.attempt, message=f"replayed dlq {entry.id}")
            await session.delete(entry)
        await session.commit()
    LOGGER.info("Replayed %d DLQ entries", len(dlq_entries))


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(description="Outbox forwarder")
    parser.add_argument("--stop-when-idle", action="store_true", help="Exit when no pending records remain")
    parser.add_argument("--run-seconds", type=float, default=None, help="Max seconds to run")
    parser.add_argument("--replay-dlq", nargs="*", help="Replay DLQ entries (all if no ids provided)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_CONFIG["batch_size"])
    parser.add_argument("--scan-interval", type=float, default=DEFAULT_CONFIG["scan_interval"])
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_CONFIG["max_attempts"])
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONFIG["concurrency"])
    parser.add_argument("--lock-timeout", type=float, default=DEFAULT_CONFIG["lock_timeout"])
    parser.add_argument("--backoff-base", type=float, default=DEFAULT_CONFIG["backoff_base"])
    parser.add_argument("--backoff-factor", type=float, default=DEFAULT_CONFIG["backoff_factor"])
    parser.add_argument("--backoff-jitter", type=float, default=DEFAULT_CONFIG["backoff_jitter"])
    parser.add_argument("--backoff-max", type=float, default=DEFAULT_CONFIG["backoff_max"])
    parser.add_argument("--lock-owner", type=str, default=None, help="Optional lock owner id override")
    return parser.parse_args()


async def main():
    args = parse_args()
    # Only used for forwarder
    if args.replay_dlq is not None and len(args.replay_dlq) >= 0:
        ids = None
        if args.replay_dlq:
            ids = [int(x) for x in args.replay_dlq]
        await replay_dlq(ids)
        return
    await run_forwarder(
        batch_size=args.batch_size,
        scan_interval=args.scan_interval,
        max_attempts=args.max_attempts,
        concurrency=args.concurrency,
        lock_timeout=args.lock_timeout,
        backoff_base=args.backoff_base,
        backoff_factor=args.backoff_factor,
        backoff_jitter=args.backoff_jitter,
        backoff_max=args.backoff_max,
        stop_when_idle=args.stop_when_idle,
        run_for_seconds=args.run_seconds,
        lock_owner=args.lock_owner,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")

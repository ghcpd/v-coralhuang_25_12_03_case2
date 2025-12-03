from __future__ import annotations
import asyncio
from datetime import datetime, timedelta
import logging
import time
import random
import uuid
from typing import List, Optional, Tuple

from sqlalchemy import select, update, func, or_

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# configuration
BATCH_SIZE = int(__import__("os").environ.get("OUTBOX_BATCH_SIZE", 200))
SCAN_INTERVAL_SECONDS = float(__import__("os").environ.get("OUTBOX_SCAN_INTERVAL", 0.5))
MAX_ATTEMPTS = int(__import__("os").environ.get("OUTBOX_MAX_ATTEMPTS", 5))
WORKER_CONCURRENCY = int(__import__("os").environ.get("OUTBOX_WORKERS", 8))
LOCK_TIMEOUT_SECONDS = int(__import__("os").environ.get("OUTBOX_LOCK_TIMEOUT", 30))
BACKOFF_BASE_SECONDS = float(__import__("os").environ.get("OUTBOX_BACKOFF_BASE", 0.5))
BACKOFF_MAX_SECONDS = float(__import__("os").environ.get("OUTBOX_BACKOFF_MAX", 30.0))
JITTER_PCT = float(__import__("os").environ.get("OUTBOX_JITTER", 0.25))


def _now() -> datetime:
    return datetime.utcnow()


class Forwarder:
    """Robust outbox forwarder with DB-based locking, worker concurrency, backoff, DLQ and audit.

    Usage: run_loop() will keep scanning; run_until_empty() is helpful for tests.
    """

    def __init__(self, instance_id: Optional[str] = None):
        self.instance_id = instance_id or uuid.uuid4().hex
        self._running = False

    async def _recover_stale_locks(self):
        now = _now()
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        async with SessionLocal() as session:
            stmt = update(OutboxEvent).where(
                OutboxEvent.status == "locked",
                OutboxEvent.locked_at <= cutoff,
            ).values(status="new", locked_at=None, locked_by=None, updated_at=now)
            result = await session.execute(stmt)
            if result.rowcount:
                await session.commit()
                # write audit entries for recovered locks
                for _ in range(result.rowcount):
                    # simple audit entry per-row; not ideal but sufficient for demo
                    session.add(OutboxAudit(outbox_id="<recovered>", action="lock_recovered", detail="stale lock"))
                await session.commit()

    async def _lock_batch(self) -> List[str]:
        """Select candidate ids then atomically mark them locked for this instance."""
        now = _now()
        async with SessionLocal() as session:
            # candidates: status new|failed and next_retry_at is null or due
            stmt = select(OutboxEvent.id).where(
                OutboxEvent.status.in_(["new", "failed"]),
                or_(OutboxEvent.next_retry_at == None, OutboxEvent.next_retry_at <= now),
            ).limit(BATCH_SIZE)
            rows = (await session.execute(stmt)).scalars().all()
            if not rows:
                return []

            # try to lock selected ids atomically
            upd = update(OutboxEvent).where(
                OutboxEvent.id.in_(rows),
                OutboxEvent.status.in_(["new", "failed"]),
                or_(OutboxEvent.next_retry_at == None, OutboxEvent.next_retry_at <= now),
            ).values(status="locked", locked_by=self.instance_id, locked_at=now, updated_at=now)
            result = await session.execute(upd)
            await session.commit()

            # fetch rows we successfully locked
            stmt2 = select(OutboxEvent).where(OutboxEvent.locked_by == self.instance_id, OutboxEvent.status == "locked")
            locked = (await session.execute(stmt2)).scalars().all()
            return [r.id for r in locked]

    async def _process_single(self, outbox_id: str) -> Tuple[str, str]:
        """Process a single outbox id. Returns (id, result) where result is 'ok'|'retry'|'dead'|'noop'."""
        start = time.time()
        async with SessionLocal() as session:
            record = await session.get(OutboxEvent, outbox_id)
            if not record:
                return outbox_id, "noop"

            # idempotency check
            existing = await session.get(Event, record.id)
            if existing:
                record.status = "forwarded"
                record.updated_at = _now()
                session.add(OutboxAudit(outbox_id=record.id, action="forwarded", detail="idempotent"))
                await session.commit()
                LOGGER.debug("idempotent forwarded %s", record.id)
                return outbox_id, "ok"

            # try forwarding (simulate deterministic failures inside try so they are handled)
            try:
                if record.topic and str(record.topic).startswith("fail_"):
                    raise RuntimeError("simulated transient failure for testing")
                evt = Event(id=record.id, topic=record.topic, payload=record.payload, created_at=_now())
                session.add(evt)
                record.status = "forwarded"
                record.updated_at = _now()
                session.add(OutboxAudit(outbox_id=record.id, action="forwarded", detail="success"))
                await session.commit()
                LOGGER.debug("forwarded %s", record.id)
                elapsed = time.time() - start
                return outbox_id, "ok"

            except Exception as exc:  # treat as transient by default
                LOGGER.exception("error forwarding %s", outbox_id)
                # increment attempt and either schedule retry or dead-letter
                record.attempt = (record.attempt or 0) + 1
                detail = str(exc)
                if record.attempt >= MAX_ATTEMPTS:
                    # move to DLQ
                    dlq = OutboxDLQ(id=record.id, topic=record.topic, payload=record.payload, failed_at=_now(), reason=detail)
                    session.add(dlq)
                    session.add(OutboxAudit(outbox_id=record.id, action="dead", detail=detail))
                    # remove from outbox
                    await session.delete(record)
                    await session.commit()
                    LOGGER.warning("moved %s to DLQ after %d attempts", outbox_id, record.attempt)
                    return outbox_id, "dead"
                else:
                    # schedule backoff
                    backoff = min(BACKOFF_BASE_SECONDS * (2 ** (record.attempt - 1)), BACKOFF_MAX_SECONDS)
                    jitter = backoff * JITTER_PCT * (random.random() * 2 - 1)
                    backoff = max(0.0, backoff + jitter)
                    record.next_retry_at = _now() + timedelta(seconds=backoff)
                    record.status = "failed"
                    session.add(OutboxAudit(outbox_id=record.id, action="retry_scheduled", detail=f"attempt={record.attempt} next={record.next_retry_at} err={detail}"))
                    record.updated_at = _now()
                    await session.commit()
                    LOGGER.info("scheduled retry for %s attempt=%d next=%s", outbox_id, record.attempt, record.next_retry_at.isoformat())
                    return outbox_id, "retry"

    async def _process_batch(self, ids: List[str]) -> Tuple[int, int, int]:
        """Process a list of locked outbox ids using a bounded worker pool.

        Returns tuple (ok_count, retry_count, dead_count)
        """
        ok = retry = dead = 0

        sem = asyncio.Semaphore(WORKER_CONCURRENCY)

        async def run_one(iid: str):
            async with sem:
                _, res = await self._process_single(iid)
                return res

        tasks = [asyncio.create_task(run_one(i)) for i in ids]
        for t in asyncio.as_completed(tasks):
            res = await t
            if res == "ok":
                ok += 1
            elif res == "retry":
                retry += 1
            elif res == "dead":
                dead += 1
        return ok, retry, dead

    async def run_once(self) -> Tuple[int, int, int]:
        """Perform a single scan -> lock -> process -> recover stale locks. Returns counts."""
        # recover stale locks first
        await self._recover_stale_locks()
        locked_ids = await self._lock_batch()
        if not locked_ids:
            return 0, 0, 0
        LOGGER.info("locked %d records for processing", len(locked_ids))
        ok, retry, dead = await self._process_batch(locked_ids)
        LOGGER.info("batch processed: ok=%d retry=%d dead=%d", ok, retry, dead)
        return ok, retry, dead

    async def run_until_empty(self, quiet: bool = False, max_loops: Optional[int] = None):
        """Run scan loops until there are no candidate records to process.

        This is helpful in tests; returns a summary dict with counts and durations.
        """
        await init_db()
        self._running = True
        start = time.time()
        total_ok = total_retry = total_dead = 0
        loops = 0
        while self._running:
            loops += 1
            ok, retry, dead = await self.run_once()
            total_ok += ok
            total_retry += retry
            total_dead += dead
            if not ok and not retry and not dead:
                # no more candidates to process
                break
            if max_loops and loops >= max_loops:
                break
            # be a little polite
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

        duration = time.time() - start
        if not quiet:
            LOGGER.info("forwarder done: ok=%d retry=%d dead=%d loops=%d duration=%.2fs", total_ok, total_retry, total_dead, loops, duration)
        return {"ok": total_ok, "retry": total_retry, "dead": total_dead, "loops": loops, "duration": duration}


async def replay_dlq(limit: int = 100) -> int:
    """Replay up to `limit` DLQ items back into the outbox as new (attempt=0).

    Returns the number of records replayed.
    """
    async with SessionLocal() as session:
        stmt = select(OutboxDLQ).limit(limit)
        rows = (await session.execute(stmt)).scalars().all()
        if not rows:
            return 0
        count = 0
        for d in rows:
            # recreate outbox record in event_outbox
            rec = OutboxEvent(id=d.id, topic=d.topic, payload=d.payload, status="new", attempt=0, created_at=_now(), updated_at=_now())
            session.add(rec)
            session.add(OutboxAudit(outbox_id=d.id, action="replayed", detail="replay from dlq"))
            await session.delete(d)
            count += 1
        await session.commit()
        LOGGER.info("replayed %d dlq rows", count)
        return count


if __name__ == "__main__":
    try:
        f = Forwarder()
        asyncio.run(f.run_until_empty())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")

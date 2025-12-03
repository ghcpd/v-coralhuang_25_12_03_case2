import asyncio
import importlib
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import select, func


@pytest_asyncio.fixture
async def fresh_db(tmp_path, monkeypatch):
    # Set DB URL before imports
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OUTBOX_DB_URL", f"sqlite+aiosqlite:///{db_path}")
    from outbox import database as db
    from outbox import forwarder as fwd

    importlib.reload(db)
    importlib.reload(fwd)
    await db.reset_db()
    yield db, fwd


def _make_outbox(id: str, topic: str = "normal"):
    from outbox.models import OutboxEvent

    return OutboxEvent(id=id, topic=topic, payload=f"payload-{topic}-{id}", status="new", attempt=0)


@pytest.mark.asyncio
async def test_locking_no_duplicates(fresh_db):
    db, fwd = fresh_db
    from outbox.models import OutboxEvent

    async with db.SessionLocal() as session:
        session.add_all([_make_outbox(str(i)) for i in range(10)])
        await session.commit()

    async with db.SessionLocal() as session:
        ids1 = await fwd.lock_batch(session, owner="A", batch_size=5, max_attempts=5)
    async with db.SessionLocal() as session:
        ids2 = await fwd.lock_batch(session, owner="B", batch_size=10, max_attempts=5)

    assert len(ids1) == 5
    assert len(ids2) == 5
    assert set(ids1).isdisjoint(set(ids2))


@pytest.mark.asyncio
async def test_retry_and_dlq(fresh_db):
    db, fwd = fresh_db
    from outbox.models import OutboxEvent, OutboxDLQ

    # Seed one transient and one dead
    async with db.SessionLocal() as session:
        session.add_all([
            _make_outbox("transient", topic="transient_fail"),
            _make_outbox("dead", topic="dead_fail"),
        ])
        await session.commit()

    await fwd.run_forwarder(
        batch_size=2,
        scan_interval=0.05,
        max_attempts=2,
        concurrency=2,
        lock_timeout=5,
        backoff_base=0.05,
        backoff_factor=2.0,
        backoff_jitter=0.0,
        backoff_max=0.2,
        stop_when_idle=True,
        lock_owner="owner1",
    )

    async with db.SessionLocal() as session:
        transient = await session.get(OutboxEvent, "transient")
        dead = await session.get(OutboxEvent, "dead")
        dlq_count = (await session.execute(select(func.count()).select_from(OutboxDLQ))).scalar_one()

    assert transient.status == "dead"  # exhausted attempts
    assert dead.status == "dead"
    assert dlq_count == 2


@pytest.mark.asyncio
async def test_replay_dlq(fresh_db):
    db, fwd = fresh_db
    from outbox.models import OutboxEvent, OutboxDLQ

    async with db.SessionLocal() as session:
        session.add(_make_outbox("dead", topic="dead_fail"))
        await session.commit()

    await fwd.run_forwarder(
        batch_size=1,
        scan_interval=0.05,
        max_attempts=1,
        concurrency=1,
        lock_timeout=5,
        backoff_base=0.05,
        backoff_factor=2.0,
        backoff_jitter=0.0,
        backoff_max=0.2,
        stop_when_idle=True,
        lock_owner="owner1",
    )

    await fwd.replay_dlq()

    async with db.SessionLocal() as session:
        rec = await session.get(OutboxEvent, "dead")
        dlq = (await session.execute(select(func.count()).select_from(OutboxDLQ))).scalar_one()
    assert rec.status == "new"
    assert rec.attempt == 0
    assert dlq == 0


@pytest.mark.asyncio
async def test_concurrent_forwarders(fresh_db):
    db, fwd = fresh_db
    from outbox.models import Event

    # Seed 30 normal events
    async with db.SessionLocal() as session:
        session.add_all([_make_outbox(str(i)) for i in range(30)])
        await session.commit()

    async def run(owner: str):
        await fwd.run_forwarder(
            batch_size=10,
            scan_interval=0.05,
            max_attempts=3,
            concurrency=5,
            lock_timeout=5,
            backoff_base=0.05,
            backoff_factor=2.0,
            backoff_jitter=0.0,
            backoff_max=0.2,
            stop_when_idle=True,
            lock_owner=owner,
        )

    await asyncio.gather(run("ownerA"), run("ownerB"))

    async with db.SessionLocal() as session:
        # All events forwarded exactly once
        events_count = (await session.execute(select(func.count()).select_from(Event))).scalar_one()
    assert events_count == 30

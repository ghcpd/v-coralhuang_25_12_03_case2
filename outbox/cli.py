from __future__ import annotations
import asyncio
from datetime import datetime
from typing import List

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxDLQ, OutboxEvent

async def replay_dlq(ids: List[str] | None = None):
    await init_db()
    async with SessionLocal() as session:
        if ids:
            q = await session.execute("select * from outbox_dlq where id in :ids", {'ids': tuple(ids)})
        else:
            q = await session.execute("select * from outbox_dlq")
            rows = q.fetchall()
            for r in rows:
                # re-insert into event_outbox
                ev = OutboxEvent(id=r['id'], topic=r['topic'], payload=r['payload'])
                session.add(ev)
                await session.execute("delete from outbox_dlq where id = :id", {'id': r['id']})
        await session.commit()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--ids', nargs='*')
    args = parser.parse_args()
    asyncio.run(replay_dlq(args.ids if args.ids else None))

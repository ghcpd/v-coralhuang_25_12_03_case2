import asyncio
import os
import tempfile

import pytest

# ensure test DB is used before modules initialize
def _set_tmp_db_path(path):
    os.environ["OUTBOX_DB_URL"] = f"sqlite+aiosqlite:///{path}"



@pytest.mark.asyncio
async def test_forwarder_basic(tmp_path):
    # use a temporary sqlite file for isolation
    db_file = tmp_path / "outbox_test.db"
    _set_tmp_db_path(db_file)

    # tune forwarder settings so retries and DLQ are processed quickly in tests
    os.environ["OUTBOX_BACKOFF_BASE"] = "0.01"
    os.environ["OUTBOX_BACKOFF_MAX"] = "0.1"
    os.environ["OUTBOX_JITTER"] = "0.01"
    os.environ["OUTBOX_SCAN_INTERVAL"] = "0.01"
    os.environ["OUTBOX_WORKERS"] = "10"
    os.environ["OUTBOX_MAX_ATTEMPTS"] = "3"

    # import after env var set so the DB URL is used when modules initialize
    from outbox.scripts import seed_outbox, run_one_instance, db_counts

    # seed a small workload
    await seed_outbox(count=200, fail_ratio=0.05)

    # run the forwarder until empty
    res = await run_one_instance()
    counts = await db_counts()

    # all non-dead rows should be in events table
    assert counts["events"] + counts["dlq"] >= 200

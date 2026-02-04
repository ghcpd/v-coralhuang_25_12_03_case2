from __future__ import annotations
import os
import pathlib
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
from sqlalchemy import text
from .models import Base

DB_URL = os.getenv("OUTBOX_DB_URL", "sqlite+aiosqlite:///./outbox.db")
_connect_args = {"timeout": 30} if DB_URL.startswith("sqlite+aiosqlite") else {}
engine = create_async_engine(DB_URL, echo=False, future=True, connect_args=_connect_args)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def init_db():
    async with engine.begin() as conn:
        url = make_url(DB_URL)
        if url.drivername.startswith("sqlite"):
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA busy_timeout=5000"))
        await conn.run_sync(Base.metadata.create_all)


async def reset_db():
    """Dangerous: drop SQLite db file or drop all tables (non-SQLite)."""
    url = make_url(DB_URL)
    if url.drivername.startswith("sqlite"):
        # Close connections before deleting
        db_path = url.database
        if db_path:
            path = pathlib.Path(db_path)
            if path.exists():
                await engine.dispose()
                path.unlink()
        # recreate tables
        await init_db()
    else:
        # For other DBs, just drop all tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

async def get_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session

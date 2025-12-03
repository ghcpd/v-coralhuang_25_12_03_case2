from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, Integer, Index, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class OutboxEvent(Base):
    __tablename__ = "event_outbox"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="new")  # new|locked|forwarded|failed
    attempt = Column(Integer, nullable=False, default=0)
    # when to try again (for retries)
    next_retry_at = Column(DateTime, nullable=True, default=None, index=True)
    # lease / lock metadata
    locked_at = Column(DateTime, nullable=True, default=None)
    locked_by = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_outbox_status", "status"),
    )

class Event(Base):
    __tablename__ = "events"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class OutboxDLQ(Base):
    __tablename__ = "outbox_dlq"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    failed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    reason = Column(Text, nullable=True)


class OutboxAudit(Base):
    __tablename__ = "event_outbox_audit"
    id = Column(Integer, primary_key=True, autoincrement=True)
    outbox_id = Column(String(64), nullable=False, index=True)
    action = Column(String(32), nullable=False)  # locked, forwarded, retry, dead, replayed
    detail = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

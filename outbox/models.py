from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, Integer, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class OutboxEvent(Base):
    __tablename__ = "event_outbox"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="new")  # new|forwarded|failed
    attempt = Column(Integer, nullable=False, default=0)
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

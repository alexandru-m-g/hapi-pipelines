"""Sector table."""

from sqlalchemy import Column, DateTime, String, text

from hapi.pipelines.database.base import Base


class Sector(Base):
    __tablename__ = "Sector"

    code = Column(String(32), primary_key=True)
    name = Column(String(512), nullable=False, index=True)
    reference_period_start = Column(DateTime, nullable=False, index=True)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

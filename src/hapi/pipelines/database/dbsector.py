"""Sector table."""

from hdx.database.no_timezone import Base
from sqlalchemy import Column, DateTime, String, text


class DBSector(Base):
    __tablename__ = "sector"

    code = Column(String(32), primary_key=True)
    name = Column(String(512), nullable=False, index=True)
    reference_period_start = Column(DateTime, nullable=False, index=True)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

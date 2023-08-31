"""Gender table."""

from hdx.database.no_timezone import Base
from sqlalchemy import Column, DateTime, Integer, String, text


class DBLocation(Base):
    __tablename__ = "location"

    id = Column(Integer, primary_key=True)
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

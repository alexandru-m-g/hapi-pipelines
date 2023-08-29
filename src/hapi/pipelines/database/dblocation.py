"""Gender table."""

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    text,
)

from hapi.pipelines.database.base import Base


class DBLocation(Base):
    __tablename__ = "Location"

    id = Column(Integer, primary_key=True)
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

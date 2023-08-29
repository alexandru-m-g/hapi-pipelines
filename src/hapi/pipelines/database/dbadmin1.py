"""Admin1 table."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class DBAdmin1(Base):
    __tablename__ = "Admin1"

    id = Column(Integer, primary_key=True)
    location_ref = Column(ForeignKey("Location.id"))
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    is_unspecified = Column(Boolean, server_default=text("FALSE"))
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    Location = relationship("DBLocation")

"""Admin1 table."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    text,
)
from sqlalchemy.orm import relationship

from hapi.pipelines.database.base import Base


class DBAdmin1(Base):
    __tablename__ = "Admin1"

    id = Column(Integer, primary_key=True)
    location_ref = Column(ForeignKey("Location.id"))
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    is_unspecified = Column(Boolean, server_default=text("FALSE"))
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    Location = relationship("DBLocation")

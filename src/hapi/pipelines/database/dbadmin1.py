"""Admin1 table."""

from hdx.database.no_timezone import Base
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

from hapi.pipelines.database.dblocation import DBLocation  # noqa


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

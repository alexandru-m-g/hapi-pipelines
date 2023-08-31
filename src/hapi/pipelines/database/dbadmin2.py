"""Admin2 table."""

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


class DBAdmin2(Base):
    __tablename__ = "admin2"

    id = Column(Integer, primary_key=True)
    admin1_ref = Column(ForeignKey("admin1.id"))
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    is_unspecified = Column(Boolean, server_default=text("FALSE"))
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    admin1 = relationship("DBAdmin1")

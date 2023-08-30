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
    __tablename__ = "Admin2"

    id = Column(Integer, primary_key=True)
    admin1_ref = Column(ForeignKey("Admin1.id"))
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    is_unspecified = Column(Boolean, server_default=text("FALSE"))
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    Admin1 = relationship("Admin1")

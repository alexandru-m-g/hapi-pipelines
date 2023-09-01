"""OperationalPresence table."""

from hdx.database.no_timezone import Base
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
    text,
)


class DBOperationalPresence(Base):
    __tablename__ = "operational_presence"

    id = Column(Integer, primary_key=True)
    resource_ref = Column(Integer, nullable=False)
    org_ref = Column(Integer, nullable=False)
    sector_code = Column(String(32), nullable=False)
    admin2_ref = Column(Integer, nullable=False)
    reference_period_start = Column(DateTime, nullable=False, index=True)
    reference_period_end = Column(DateTime, server_default=text("NULL"))
    source_data = Column(Text)

"""Org table."""

from hdx.database.no_timezone import Base
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    text,
)
from sqlalchemy.orm import relationship


class DBOrg(Base):
    __tablename__ = "Org"

    id = Column(Integer, primary_key=True)
    hdx_link = Column(String(1024), nullable=False)
    acronym = Column(String(32), nullable=False, index=True)
    name = Column(String(512), nullable=False)
    org_type_code = Column(ForeignKey("OrgType.code"))
    reference_period_start = Column(DateTime, nullable=False, index=True)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    OrgType = relationship("DBOrgType")

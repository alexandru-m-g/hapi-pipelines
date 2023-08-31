"""OrgType table."""

from hdx.database.no_timezone import Base
from sqlalchemy import Column, String


class DBOrgType(Base):
    __tablename__ = "org_type"

    code = Column(String(32), primary_key=True)
    description = Column(String(512), nullable=False)

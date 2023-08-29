"""OrgType table."""

from sqlalchemy import Column, String

from hapi.pipelines.database.base import Base


class DBOrgType(Base):
    __tablename__ = "OrgType"

    code = Column(String(32), primary_key=True)
    description = Column(String(512), nullable=False)

"""OrgType table."""

from sqlalchemy import Column, String

from hapi.pipelines.database.base import Base


class OrgType(Base):
    __tablename__ = "OrgType"

    code = Column(String(32), primary_key=True)
    description = Column(String(512), nullable=False)

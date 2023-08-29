"""Resource table."""

from sqlalchemy import Column, Integer, String

from hapi.pipelines.database.base import Base


class DBAgeRange(Base):
    __tablename__ = "AgeRange"

    code = Column(String(32), primary_key=True)
    age_min = Column(Integer, nullable=False)
    age_max = Column(Integer)

"""Resource table."""

from hdx.database.no_timezone import Base
from sqlalchemy import Column, Integer, String


class DBAgeRange(Base):
    __tablename__ = "age_range"

    code = Column(String(32), primary_key=True)
    age_min = Column(Integer, nullable=False)
    age_max = Column(Integer)

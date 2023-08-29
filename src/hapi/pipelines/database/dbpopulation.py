"""Population table."""

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Text,
    text,
)
from sqlalchemy.orm import relationship

from hapi.pipelines.database.base import Base


class DBPopulation(Base):
    __tablename__ = "Population"

    id = Column(Integer, primary_key=True)
    resource_ref = Column(Integer, nullable=False)
    admin2_ref = Column(Integer, nullable=False)
    gender_code = Column(ForeignKey("Gender.code"))
    age_range_code = Column(ForeignKey("AgeRange.code"))
    population = Column(Integer, nullable=False, index=True)
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))
    source_data = Column(Text)

    AgeRange = relationship("DBAgeRange")
    Gender = relationship("DBGender")
